"""
DolphinDB 数据库客户端
替代 PostgreSQL 客户端，提供相同的接口供系统其他模块调用
使用 dolphindb Python API 连接 DolphinDB 分布式数据库
"""
import re
import threading
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl
import dolphindb as ddb

from app.core.config import settings
from app.core.logger import logger


class DolphinDBClient:
    """DolphinDB 数据库客户端（线程安全单例）"""

    def __init__(self):
        self._host = settings.database.dolphindb_host
        self._port = settings.database.dolphindb_port
        self._user = settings.database.dolphindb_user
        self._password = settings.database.dolphindb_password
        self._db_path = settings.database.db_path
        self._meta_db_path = settings.database.meta_db_path

        # 会话与线程锁
        self._session: Optional[ddb.Session] = None
        self._lock = threading.Lock()

        self._connect()
        logger.info(
            f"DolphinDB client initialized: {self._host}:{self._port}, "
            f"db={self._db_path}, meta={self._meta_db_path}"
        )

    # ------------------------------------------------------------------
    #  连接管理
    # ------------------------------------------------------------------

    def _connect(self):
        """建立 DolphinDB 连接"""
        try:
            self._session = ddb.Session(enableASYNC=False)
            success = self._session.connect(
                self._host, self._port, self._user, self._password
            )
            if not success:
                raise ConnectionError(
                    f"无法连接 DolphinDB {self._host}:{self._port}"
                )
            logger.info("DolphinDB 连接成功")
        except Exception as e:
            logger.error(f"DolphinDB 连接失败: {e}")
            raise

    def _ensure_connected(self):
        """确保连接可用，断线自动重连"""
        try:
            # 简单心跳检测
            self._session.run("1+1")
        except Exception:
            logger.warning("DolphinDB 连接已断开，正在重连...")
            self._connect()

    # ------------------------------------------------------------------
    #  SQL 参数替换与语法转换
    # ------------------------------------------------------------------

    @staticmethod
    def _convert_date_format(value: str) -> str:
        """
        将 YYYYMMDD 格式的日期字符串转换为 DolphinDB 日期格式
        例: '20200101' -> '2020.01.01'
        """
        if isinstance(value, str) and re.match(r"^\d{8}$", value):
            return f"{value[:4]}.{value[4:6]}.{value[6:8]}"
        return value

    @staticmethod
    def _escape_value(value: Any) -> str:
        """
        将 Python 值转换为 DolphinDB SQL 字面量
        处理字符串引号转义、日期格式、None 等
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            return f"{value.strftime('%Y.%m.%dT%H:%M:%S')}"
        if isinstance(value, date):
            return f"{value.strftime('%Y.%m.%d')}"
        # 字符串类型：检查是否为 YYYYMMDD 日期
        s = str(value)
        if re.match(r"^\d{8}$", s):
            converted = DolphinDBClient._convert_date_format(s)
            return f'"{converted}"'
        # 普通字符串，转义双引号
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{s}"'

    def _substitute_params(self, sql: str, params: Optional[tuple]) -> str:
        """
        将 PostgreSQL 风格的 %s 占位符替换为实际值
        DolphinDB 不支持参数化查询，需要手动拼接
        """
        if not params:
            return sql
        parts = sql.split("%s")
        if len(parts) - 1 != len(params):
            raise ValueError(
                f"参数数量不匹配: SQL 中有 {len(parts) - 1} 个占位符，"
                f"但提供了 {len(params)} 个参数"
            )
        result = parts[0]
        for i, param in enumerate(params):
            result += self._escape_value(param) + parts[i + 1]
        return result

    def _adapt_sql_syntax(self, sql: str) -> str:
        """
        将 PostgreSQL SQL 语法适配为 DolphinDB 兼容语法
        - 裸表名 → loadTable("db_path", "table_name")
        - CURRENT_TIMESTAMP → now()
        - LIMIT N 保持不变（DolphinDB 也支持）
        """
        # CURRENT_TIMESTAMP -> now()
        sql = re.sub(r"\bCURRENT_TIMESTAMP\b", "now()", sql, flags=re.IGNORECASE)

        # 替换 FROM / JOIN 后面的裸表名为 loadTable(...)
        def _replace_table_ref(match):
            keyword = match.group(1)  # FROM / JOIN
            table_name = match.group(2)
            db_path = self._resolve_db_path(table_name)
            return f'{keyword} loadTable("{db_path}", "{table_name}")'

        # 匹配 FROM table_name 或 JOIN table_name（仅匹配已知表名）
        known = "|".join(sorted(self._ALL_TABLES, key=len, reverse=True))
        sql = re.sub(
            rf'\b(FROM|JOIN)\s+({known})\b',
            _replace_table_ref,
            sql,
            flags=re.IGNORECASE,
        )

        return sql

    def _build_sql(self, sql: str, params: Optional[tuple] = None) -> str:
        """完整的 SQL 构建流程：参数替换 + 语法适配"""
        sql = self._substitute_params(sql, params)
        sql = self._adapt_sql_syntax(sql)
        return sql

    # ------------------------------------------------------------------
    #  核心查询接口
    # ------------------------------------------------------------------

    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        stream: bool = False,
        batch_size: int = 10000,
    ) -> pl.DataFrame:
        """
        执行查询并返回 Polars DataFrame

        Args:
            sql: SQL 查询语句（支持 %s 占位符）
            params: 查询参数
            stream: 是否分批返回（DolphinDB 中简化为一次性返回）
            batch_size: 流式查询批次大小（保留参数兼容性）

        Returns:
            pl.DataFrame
        """
        final_sql = self._build_sql(sql, params)
        try:
            with self._lock:
                self._ensure_connected()
                result = self._session.run(final_sql)

            # 将结果转换为 Polars DataFrame
            return self._to_polars(result)
        except Exception as e:
            logger.error(f"查询失败: {final_sql[:200]}... 错误: {e}")
            raise

    def _to_polars(self, result: Any) -> pl.DataFrame:
        """
        将 DolphinDB 返回结果统一转换为 Polars DataFrame
        session.run() 可能返回 pandas DataFrame、numpy array、标量等
        """
        if result is None:
            return pl.DataFrame()
        if isinstance(result, pd.DataFrame):
            return pl.from_pandas(result)
        if isinstance(result, (list, tuple)):
            # 单列结果
            return pl.DataFrame({"value": result})
        # 标量结果（如 SELECT 1、SELECT COUNT(*) 等）
        return pl.DataFrame({"value": [result]})

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """
        执行 SQL 语句（不返回结果）

        Args:
            sql: SQL 语句
            params: 参数
        """
        final_sql = self._build_sql(sql, params)
        try:
            with self._lock:
                self._ensure_connected()
                self._session.run(final_sql)
        except Exception as e:
            logger.error(f"执行失败: {final_sql[:200]}... 错误: {e}")
            raise

    # ------------------------------------------------------------------
    #  表操作
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        db_path = self._resolve_db_path(table_name)
        try:
            with self._lock:
                self._ensure_connected()
                result = self._session.run(
                    f"existsTable('{db_path}', '{table_name}')"
                )
            return bool(result)
        except Exception as e:
            logger.error(f"检查表是否存在失败 [{table_name}]: {e}")
            return False

    def create_table(
        self,
        table_name: str,
        schema: Dict[str, Dict[str, Any]],
        primary_keys: List[str],
    ) -> None:
        """
        创建表（简化版）

        DolphinDB 分布式表的创建比较复杂，需要指定分区方案。
        生产环境中表应由初始化脚本预先创建。
        此方法仅记录警告日志，不执行实际建表操作。

        Args:
            table_name: 表名
            schema: 列定义字典 {列名: {type, nullable, comment}}
            primary_keys: 主键列表
        """
        logger.warning(
            f"DolphinDB 分布式表需要通过初始化脚本创建。"
            f"跳过自动建表: {table_name}, 主键: {primary_keys}"
        )
        logger.info(
            f"请确保表 {table_name} 已在 DolphinDB 中预先创建，"
            f"schema 列数: {len(schema)}"
        )

    # ------------------------------------------------------------------
    #  数据写入

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        列出两个数据库中所有已存在的表及其行数和列信息

        Returns:
            [{"table_name": str, "row_count": int, "columns": [str], "column_count": int}, ...]
        """
        results = []
        for table_name in sorted(self._ALL_TABLES):
            if not self.table_exists(table_name):
                continue
            try:
                db_path = self._resolve_db_path(table_name)
                with self._lock:
                    self._ensure_connected()
                    schema_info = self._session.run(
                        f"schema(loadTable('{db_path}', '{table_name}'))"
                    )
                columns = []
                if isinstance(schema_info, dict) and "colDefs" in schema_info:
                    col_defs = schema_info["colDefs"]
                    if isinstance(col_defs, pd.DataFrame) and "name" in col_defs.columns:
                        columns = col_defs["name"].tolist()

                # 获取行数
                with self._lock:
                    self._ensure_connected()
                    row_count = self._session.run(
                        f"exec count(*) from loadTable('{db_path}', '{table_name}')"
                    )
                    row_count = int(row_count) if row_count is not None else 0

                results.append({
                    "table_name": table_name,
                    "row_count": row_count,
                    "columns": columns,
                    "column_count": len(columns),
                })
            except Exception as e:
                logger.warning(f"获取表信息失败 [{table_name}]: {e}")
                results.append({
                    "table_name": table_name,
                    "row_count": 0,
                    "columns": [],
                    "column_count": 0,
                })
        return results

    def get_table_columns(self, table_name: str) -> List[str]:
        """获取指定表的列名列表"""
        db_path = self._resolve_db_path(table_name)
        try:
            with self._lock:
                self._ensure_connected()
                schema_info = self._session.run(
                    f"schema(loadTable('{db_path}', '{table_name}'))"
                )
            if isinstance(schema_info, dict) and "colDefs" in schema_info:
                col_defs = schema_info["colDefs"]
                if isinstance(col_defs, pd.DataFrame) and "name" in col_defs.columns:
                    return col_defs["name"].tolist()
            return []
        except Exception as e:
            logger.error(f"获取表列信息失败 [{table_name}]: {e}")
            return []
    # ------------------------------------------------------------------

    # 元数据库中的表名集合（维度表，存储在 quant_meta）
    _META_TABLES = frozenset({
        "sync_log", "sync_log_history", "stock_basic",
        "factor_metadata", "factor_analysis",
        "dag_run_log", "dag_task_log",
        "production_task_run", "trade_cal",
    })

    # 行情库中的表名集合（TSDB 分区表，存储在 quant_research）
    _TSDB_TABLES = frozenset({
        "daily_data", "daily_basic", "adj_factor",
        "index_daily", "moneyflow", "factor_values",
    })

    # 所有已知表名
    _ALL_TABLES = _META_TABLES | _TSDB_TABLES

    def _resolve_db_path(self, table_name: str) -> str:
        """根据表名返回所属数据库路径"""
        if table_name in self._META_TABLES:
            return self._meta_db_path
        return self._db_path

    def upsert(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: List[str],
    ) -> None:
        """
        插入或更新数据

        行情库 (quant_research) 的 TSDB 表使用 keepDuplicates=LAST 自动去重。
        元数据库 (quant_meta) 的维度表需要手动 delete + insert。

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 主键列
        """
        if df.is_empty():
            logger.warning(f"空 DataFrame，跳过写入: {table_name}")
            return

        db_path = self._resolve_db_path(table_name)

        try:
            pdf = df.to_pandas()
            with self._lock:
                self._ensure_connected()
                tmp_var = f"_tmp_{table_name}_{threading.current_thread().ident}"
                self._session.upload({tmp_var: pdf})
                self._session.run(
                    f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                    f"tableInsert({table_name}_handle, {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(
                f"写入 {len(df)} 行到 {table_name}，"
                f"主键列: {key_columns}"
            )
        except Exception as e:
            logger.error(f"写入失败 [{table_name}]: {e}")
            raise

    def upsert_daily(self, df: pl.DataFrame) -> None:
        """插入或更新日线数据（兼容旧接口）"""
        self.upsert("daily_data", df, ["trade_date", "ts_code"])

    def bulk_copy(
        self,
        table_name: str,
        df: pl.DataFrame,
        columns: List[str] = None,
    ) -> int:
        """
        批量写入数据

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            columns: 列名列表，默认使用 DataFrame 的列名

        Returns:
            写入的行数
        """
        if df.is_empty():
            return 0

        db_path = self._resolve_db_path(table_name)
        cols = columns or df.columns
        rows = len(df)

        try:
            pdf = df.select(cols).to_pandas()
            with self._lock:
                self._ensure_connected()
                tmp_var = f"_bulk_{table_name}_{threading.current_thread().ident}"
                self._session.upload({tmp_var: pdf})
                self._session.run(
                    f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                    f"tableInsert({table_name}_handle, {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(f"批量写入 {rows} 行到 {table_name}")
            return rows
        except Exception as e:
            logger.error(f"批量写入失败 [{table_name}]: {e}")
            raise

    # ------------------------------------------------------------------
    #  同步日志（存储在 meta 数据库）
    # ------------------------------------------------------------------

    def get_last_sync_date(self, source: str, data_type: str) -> Optional[str]:
        """
        获取最后同步日期

        从 meta 数据库的 sync_log 表中查询指定数据源和类型的最后同步日期
        """
        try:
            sql = (
                f'SELECT last_date FROM loadTable("{self._meta_db_path}", "sync_log") '
                f'WHERE source = "{source}" AND data_type = "{data_type}" LIMIT 1'
            )
            with self._lock:
                self._ensure_connected()
                result = self._session.run(sql)

            if result is None:
                return None
            if isinstance(result, pd.DataFrame):
                if result.empty:
                    return None
                val = result["last_date"].iloc[0]
                return str(val) if val is not None else None
            return None
        except Exception as e:
            logger.debug(f"获取同步日期失败 [{source}/{data_type}]: {e}")
            return None

    def update_sync_log(
        self, source: str, data_type: str, last_date: str
    ) -> None:
        """
        更新同步日志

        sync_log 现在是维度表（非分区），不支持 keepDuplicates 自动去重。
        使用 delete + insert 实现 upsert 语义。
        """
        try:
            pdf = pd.DataFrame(
                {
                    "source": [source],
                    "data_type": [data_type],
                    "last_date": [last_date],
                    "updated_at": [datetime.now()],
                }
            )
            with self._lock:
                self._ensure_connected()
                # 先删除旧记录
                self._session.run(
                    f'sync_log_handle = loadTable("{self._meta_db_path}", "sync_log");'
                    f'delete from sync_log_handle where source = "{source}" and data_type = "{data_type}"'
                )
                # 再插入新记录
                tmp_var = f"_sync_log_{threading.current_thread().ident}"
                self._session.upload({tmp_var: pdf})
                self._session.run(
                    f'sync_log_handle = loadTable("{self._meta_db_path}", "sync_log");'
                    f"tableInsert(sync_log_handle, {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(f"同步日志已更新: {source}/{data_type} -> {last_date}")
        except Exception as e:
            logger.error(f"更新同步日志失败 [{source}/{data_type}]: {e}")
            raise

    # ------------------------------------------------------------------
    #  连接关闭
    # ------------------------------------------------------------------

    def close(self) -> None:
        """关闭 DolphinDB 连接"""
        with self._lock:
            if self._session:
                try:
                    self._session.close()
                    logger.info("DolphinDB 连接已关闭")
                except Exception as e:
                    logger.warning(f"关闭 DolphinDB 连接时出错: {e}")
                finally:
                    self._session = None


# 单例实例
db_client = DolphinDBClient()
