"""
DolphinDB 数据库客户端
替代 PostgreSQL 客户端，提供相同的接口供系统其他模块调用
使用 dolphindb Python API 连接 DolphinDB 分布式数据库
"""
import json
import re
import threading
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import numpy as np
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

        # 会话与线程锁
        self._session: Optional[ddb.Session] = None
        self._lock = threading.Lock()

        self._connect()
        logger.info(
            f"DolphinDB client initialized: {self._host}:{self._port}, "
            f"db={self._db_path}"
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
            return converted
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
        - SQL 聚合函数大写 → 小写（DolphinDB 函数名区分大小写）
        - 裸表名 → loadTable("db_path", "table_name")
        - CURRENT_TIMESTAMP → now()
        - LIMIT N 保持不变（DolphinDB 也支持）
        """
        # CURRENT_TIMESTAMP -> now()
        sql = re.sub(r"\bCURRENT_TIMESTAMP\b", "now()", sql, flags=re.IGNORECASE)

        # SQL 聚合/标量函数：大写 → 小写（DolphinDB 要求小写）
        for fn in ("MAX", "MIN", "COUNT", "SUM", "AVG",
                    "STDDEV", "VARIANCE", "FIRST", "LAST", "ISNULL"):
            sql = re.sub(rf'\b{fn}\s*\(', f'{fn.lower()}(', sql)

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
    ) -> pl.DataFrame:
        """
        执行查询并返回 Polars DataFrame

        Args:
            sql: SQL 查询语句（支持 %s 占位符）
            params: 查询参数

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
            df = pl.from_pandas(result)
            # DolphinDB DATE 列经 pandas 转为 datetime[ns]，统一转为 YYYYMMDD 字符串
            for col_name in df.columns:
                if col_name.endswith("_date") or col_name == "date":
                    dtype_str = str(df[col_name].dtype)
                    if dtype_str == "Date" or dtype_str.startswith("Datetime"):
                        df = df.with_columns(
                            pl.col(col_name).dt.strftime("%Y%m%d").alias(col_name)
                        )
            return df
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
        在数据库中创建维度表

        Args:
            table_name: 表名
            schema: 列定义字典 {列名: {type, nullable, comment}}
            primary_keys: 主键列表
        """
        if not schema:
            logger.warning(f"schema 为空，跳过建表: {table_name}")
            return

        # 先注册到 _META_TABLES，确保后续 _resolve_db_path 路由正确
        self.register_meta_table(table_name)

        db_path = self._db_path
        try:
            with self._lock:
                self._ensure_connected()
                exists = self._session.run(f"existsTable('{db_path}', '{table_name}')")
            if exists:
                logger.info(f"表 {table_name} 已存在，跳过建表")
                return

            # 构建列定义
            col_defs = ",".join([
                f"array({col_def.get('type', 'STRING')},0) as {col_name}"
                for col_name, col_def in schema.items()
            ])

            # TSDB 引擎要求 primaryKey 最后一列为时间或整数类型
            # 确定 sort key 列：优先用 primary_keys，末尾追加一个时间/整数列
            _TEMPORAL_INT_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "INT", "LONG", "SHORT"}
            pk_list = list(primary_keys) if primary_keys else [list(schema.keys())[0]]
            last_pk_type = schema.get(pk_list[-1], {}).get("type", "STRING").upper()
            if last_pk_type not in _TEMPORAL_INT_TYPES:
                # 找 schema 中可用的时间/整数列追加到末尾
                for col_name, col_def in schema.items():
                    if col_def.get("type", "STRING").upper() in _TEMPORAL_INT_TYPES and col_name not in pk_list:
                        pk_list.append(col_name)
                        break
                else:
                    # 没有合适的列，补一个 created_at TIMESTAMP 列
                    col_defs += ",array(TIMESTAMP,0) as created_at"
                    pk_list.append("created_at")
            pk_str = "`" + "`".join(pk_list)
            script = (
                f"dbMeta = database('{db_path}');"
                f"schema_{table_name} = table({col_defs});"
                f"createTable(dbHandle=dbMeta, table=schema_{table_name}, tableName=`{table_name}, primaryKey={pk_str});"
            )
            with self._lock:
                self._ensure_connected()
                self._session.run(script)
            logger.info(f"Created table {table_name} with {len(schema)} columns, primary_keys: {primary_keys}")
        except Exception as e:
            logger.error(f"建表失败 [{table_name}]: {e}")
            raise

    # ------------------------------------------------------------------
    #  数据写入

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        列出数据库中所有已存在的表及其行数和列信息
        动态查询数据库，不依赖硬编码的表名列表

        Returns:
            [{"table_name": str, "row_count": int, "columns": [str], "column_count": int}, ...]
        """
        results = []
        db_path = self._db_path

        try:
            with self._lock:
                self._ensure_connected()
                # 使用 getTables() 函数获取数据库中的所有表
                tables_result = self._session.run(
                    f"getTables(database('{db_path}'))"
                )

            table_names = []
            if tables_result is not None:
                if isinstance(tables_result, np.ndarray):
                    # getTables() 返回 numpy 数组
                    table_names = tables_result.tolist()
                elif isinstance(tables_result, list):
                    table_names = tables_result
        except Exception as e:
            logger.error(f"查询数据库 {db_path} 的表列表失败: {e}")
            return []

        # 获取每个表的详细信息
        for table_name in sorted(table_names):
            try:
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
                # 跳过无法获取信息的表
                continue

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

    def drop_table(self, table_name: str) -> None:
        """删除指定表"""
        db_path = self._resolve_db_path(table_name)
        with self._lock:
            self._ensure_connected()
            self._session.run(f"dropTable(database('{db_path}'), '{table_name}')")
        logger.info(f"Dropped table {table_name}")
    # ------------------------------------------------------------------

    # 维度表名集合（不分区，使用 createTable 创建）
    # 注意：sync_stock_basic / sync_trade_cal 不由 ensure_meta_tables 自动建表，
    # 而是在首次同步任务时由 create_table 动态创建
    _META_TABLES = frozenset({
        "sync_log", "sync_log_history", "sync_stock_basic",
        "factor_metadata", "factor_analysis",
        "factor_task_run", "sync_trade_cal",
        "sync_task_config", "etl_task_config",
        "factor_data_config",
    })

    # TSDB 分区表名集合（使用 createPartitionedTable 创建）
    _TSDB_TABLES = frozenset({
        "sync_daily_data", "sync_daily_basic", "sync_adj_factor",
        "sync_index_daily", "sync_moneyflow", "factor_values",
    })

    # 所有已知表名
    _ALL_TABLES = _META_TABLES | _TSDB_TABLES

    # factor_values 等非 sync 任务表仍需硬编码（无 sync task schema 可查）
    _EXTRA_DATE_COLUMNS: Dict[str, List[str]] = {
        "factor_values": ["trade_date"],
    }

    # ------------------------------------------------------------------
    #  维度表 Schema 定义（用于动态建表）
    #  格式: { "table_name": ("DolphinDB table(...) 建表表达式", [primaryKey列]) }
    #  TSDB 引擎要求 createTable 也必须指定 sortColumns 或 primaryKey
    # ------------------------------------------------------------------
    _META_TABLE_SCHEMAS: Dict[str, tuple] = {
        "sync_log": (
            "table("
            "array(SYMBOL,0) as source,"
            "array(SYMBOL,0) as data_type,"
            "array(STRING,0) as last_date,"
            "array(TIMESTAMP,0) as updated_at)",
            ["source", "data_type", "updated_at"],
        ),
        "sync_log_history": (
            "table("
            "array(SYMBOL,0) as source,"
            "array(SYMBOL,0) as data_type,"
            "array(STRING,0) as last_date,"
            "array(STRING,0) as sync_date,"
            "array(INT,0) as rows_synced,"
            "array(SYMBOL,0) as status,"
            "array(STRING,0) as error_message,"
            "array(STRING,0) as params,"
            "array(TIMESTAMP,0) as created_at)",
            ["source", "created_at"],
        ),
        "factor_metadata": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(STRING,0) as description,"
            "array(STRING,0) as category,"
            "array(STRING,0) as compute_mode,"
            "array(STRING,0) as storage_target,"
            "array(STRING,0) as depends_on,"
            "array(STRING,0) as params,"
            "array(STRING,0) as code,"
            "array(STRING,0) as last_computed_date,"
            "array(TIMESTAMP,0) as last_computed_at,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["factor_id", "updated_at"],
        ),
        "factor_analysis": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(TIMESTAMP,0) as analysis_date,"
            "array(STRING,0) as start_date,"
            "array(STRING,0) as end_date,"
            "array(STRING,0) as periods,"
            "array(DOUBLE,0) as ic_mean,"
            "array(DOUBLE,0) as ic_std,"
            "array(DOUBLE,0) as rank_ic_mean,"
            "array(DOUBLE,0) as rank_ic_std,"
            "array(DOUBLE,0) as ic_ir,"
            "array(DOUBLE,0) as turnover_mean,"
            "array(TIMESTAMP,0) as created_at)",
            ["factor_id", "analysis_date"],
        ),
        "factor_task_run": (
            "table("
            "array(SYMBOL,0) as factor_id,"
            "array(SYMBOL,0) as mode,"
            "array(SYMBOL,0) as status,"
            "array(STRING,0) as start_date,"
            "array(STRING,0) as end_date,"
            "array(INT,0) as rows_affected,"
            "array(DOUBLE,0) as duration_seconds,"
            "array(BOOL,0) as filter_st,"
            "array(BOOL,0) as filter_new_stock,"
            "array(INT,0) as new_stock_days,"
            "array(BOOL,0) as handle_suspension,"
            "array(BOOL,0) as mark_limit,"
            "array(STRING,0) as adjust_price,"
            "array(STRING,0) as preprocess,"
            "array(STRING,0) as error_message,"
            "array(TIMESTAMP,0) as created_at)",
            ["factor_id", "created_at"],
        ),
        "sync_task_config": (
            "table("
            "array(SYMBOL,0) as task_id,"
            "array(SYMBOL,0) as api_name,"
            "array(STRING,0) as description,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as params_json,"
            "array(SYMBOL,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(STRING,0) as schema_json,"
            "array(BOOL,0) as enabled,"
            "array(INT,0) as api_limit,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["task_id", "updated_at"],
        ),
        "etl_task_config": (
            "table("
            "array(SYMBOL,0) as task_id,"
            "array(STRING,0) as description,"
            "array(STRING,0) as script,"
            "array(SYMBOL,0) as sync_type,"
            "array(STRING,0) as date_field,"
            "array(STRING,0) as primary_keys_json,"
            "array(SYMBOL,0) as table_name,"
            "array(BOOL,0) as enabled,"
            "array(TIMESTAMP,0) as created_at,"
            "array(TIMESTAMP,0) as updated_at)",
            ["task_id", "updated_at"],
        ),
        "factor_data_config": (
            "table("
            "array(SYMBOL,0) as field_key,"
            "array(STRING,0) as description,"
            "array(SYMBOL,0) as table_name,"
            "array(SYMBOL,0) as column_name,"
            "array(STRING,0) as extra_config,"
            "array(TIMESTAMP,0) as updated_at)",
            ["field_key", "updated_at"],
        ),
    }

    def ensure_meta_tables(self) -> None:
        """
        检查并创建所有缺失的维度表。
        对已存在的表，补加代码定义里有但实际表缺少的列。
        应在应用首次启动时调用一次。
        """
        db_path = self._db_path
        created = []
        altered = []
        with self._lock:
            self._ensure_connected()
            for tbl, (schema_expr, pk_cols) in self._META_TABLE_SCHEMAS.items():
                try:
                    exists = self._session.run(
                        f"existsTable('{db_path}', '{tbl}')"
                    )
                    if not exists:
                        pk_str = "`" + "`".join(pk_cols)
                        script = (
                            f"dbMeta = database('{db_path}');"
                            f"schema_{tbl} = {schema_expr};"
                            f"createTable(dbHandle=dbMeta, table=schema_{tbl}, tableName=`{tbl}, primaryKey={pk_str});"
                        )
                        self._session.run(script)
                        created.append(tbl)
                    else:
                        # 补加代码定义里有但实际表缺少的列
                        schema_info = self._session.run(
                            f"schema(loadTable('{db_path}', '{tbl}')).colDefs"
                        )
                        existing_cols = set(schema_info['name'].tolist()) if schema_info is not None else set()

                        # 从 schema_expr 解析期望的列定义
                        col_defs = re.findall(r'array\((\w+),0\)\s+as\s+(\w+)', schema_expr)
                        # col_defs: [(type, name), ...]
                        added = []
                        for dfs_type, col_name in col_defs:
                            if col_name not in existing_cols:
                                try:
                                    self._session.run(
                                        f"tbl_handle = loadTable('{db_path}', '{tbl}');"
                                        f"addColumn(tbl_handle, `{col_name}, {dfs_type})"
                                    )
                                    added.append(col_name)
                                except Exception as add_err:
                                    logger.warning(f"给表 [{tbl}] 加列 [{col_name}] 失败: {add_err}")
                        if added:
                            altered.append(f"{tbl}({', '.join(added)})")
                except Exception as e:
                    logger.error(f"动态创建/更新维度表失败 [{tbl}]: {e}")
                    raise
        if created:
            logger.info(f"动态创建了 {len(created)} 张维度表: {', '.join(created)}")
        if altered:
            logger.info(f"补加了缺失列: {'; '.join(altered)}")
        if not created and not altered:
            logger.info("所有维度表已存在且列完整，无需变更")

    def seed_sync_task_config(self) -> None:
        """
        如果 sync_task_config 表为空，则写入默认同步任务定义。
        仅在首次启动时生效，后续可通过 API 增删改。
        """
        try:
            count = self.query("SELECT count(*) as cnt FROM sync_task_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("sync_task_config 已有数据，跳过 seed")
                return
        except Exception:
            pass  # 表可能刚创建，继续 seed

        now = datetime.now()
        # ---------------------------------------------------------------
        # 默认同步任务定义（基于 Tushare Pro API）
        # sync_type: "full"=全量同步  "incremental"=增量按日同步
        # params 中 {date} 占位符会被 SyncTaskExecutor 替换为实际日期
        # schema 类型对应 DolphinDB 表定义（init_dolphindb.dos）
        # ---------------------------------------------------------------
        tasks = [
            # ==================== 全量同步 ====================
            {
                "task_id": "sync_stock_basic",
                "api_name": "stock_basic",
                "description": "股票基础信息（代码、名称、行业、上市日期等）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_stock_basic",
                "params": {
                    "exchange": "",
                    "list_status": "",
                    "fields": "ts_code,symbol,name,area,industry,market,list_date",
                },
                "primary_keys": ["ts_code"],
                "enabled": True,
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "symbol": {"type": "STRING"},
                    "name": {"type": "STRING"},
                    "area": {"type": "STRING"},
                    "industry": {"type": "STRING"},
                    "market": {"type": "STRING"},
                    "list_date": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_trade_cal",
                "api_name": "trade_cal",
                "description": "交易日历（SSE/SZSE 开市日期、前一交易日）",
                "sync_type": "full",
                "date_field": "cal_date",
                "table_name": "sync_trade_cal",
                "params": {
                    "exchange": "",
                    "start_date": "",
                    "end_date": "",
                    "is_open": "",
                },
                "primary_keys": ["exchange", "cal_date"],
                "enabled": True,
                "api_limit": 5000,
                "schema": {
                    "exchange": {"type": "SYMBOL"},
                    "cal_date": {"type": "STRING"},
                    "is_open": {"type": "INT"},
                    "pretrade_date": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_sw_index_member_Y",
                "api_name": "index_member_all",
                "description": "申万行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_sw_index_member_Y",
                "params": {
                    "is_new": "Y",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "enabled": True,
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_sw_index_member_N",
                "api_name": "index_member_all",
                "description": "申万行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_sw_index_member_N",
                "params": {
                    "is_new": "N",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "enabled": True,
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_ci_index_member_Y",
                "api_name": "ci_index_member",
                "description": "中信行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_ci_index_member_Y",
                "params": {
                    "is_new": "Y",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "enabled": True,
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_ci_index_member_N",
                "api_name": "ci_index_member",
                "description": "中信行业成分构成（三级分类，含纳入/剔除日期）",
                "sync_type": "full",
                "date_field": "",
                "table_name": "sync_ci_index_member_N",
                "params": {
                    "is_new": "N",
                    "fields": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
                },
                "primary_keys": [],
                "enabled": True,
                "api_limit": 2000,
                "schema": {
                    "l1_code": {"type": "SYMBOL"},
                    "l1_name": {"type": "STRING"},
                    "l2_code": {"type": "SYMBOL"},
                    "l2_name": {"type": "STRING"},
                    "l3_code": {"type": "SYMBOL"},
                    "l3_name": {"type": "STRING"},
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "in_date": {"type": "STRING"},
                    "out_date": {"type": "STRING"},
                    "is_new": {"type": "STRING"},
                },
            },
            # ==================== 增量同步 ====================
            {
                "task_id": "sync_daily",
                "api_name": "daily",
                "description": "A股日线行情（OHLCV、涨跌幅、成交量成交额）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_daily_data",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "enabled": True,
                "api_limit": 5000,
                "schema": {
                    "trade_date": {"type": "DATE"},
                    "ts_code": {"type": "SYMBOL"},
                    "open": {"type": "DOUBLE"},
                    "high": {"type": "DOUBLE"},
                    "low": {"type": "DOUBLE"},
                    "close": {"type": "DOUBLE"},
                    "pre_close": {"type": "DOUBLE"},
                    "change": {"type": "DOUBLE"},
                    "pct_chg": {"type": "DOUBLE"},
                    "vol": {"type": "DOUBLE"},
                    "amount": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_adj_factor",
                "api_name": "adj_factor",
                "description": "复权因子（用于计算前/后复权价格）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_adj_factor",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,adj_factor",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "enabled": True,
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "adj_factor": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_daily_basic",
                "api_name": "daily_basic",
                "description": "每日指标（换手率、量比、市盈率、市净率、市值等）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_daily_basic",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "enabled": True,
                "api_limit": 6000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "close": {"type": "DOUBLE"},
                    "turnover_rate": {"type": "DOUBLE"},
                    "turnover_rate_f": {"type": "DOUBLE"},
                    "volume_ratio": {"type": "DOUBLE"},
                    "pe": {"type": "DOUBLE"},
                    "pe_ttm": {"type": "DOUBLE"},
                    "pb": {"type": "DOUBLE"},
                    "ps": {"type": "DOUBLE"},
                    "ps_ttm": {"type": "DOUBLE"},
                    "dv_ratio": {"type": "DOUBLE"},
                    "dv_ttm": {"type": "DOUBLE"},
                    "total_share": {"type": "DOUBLE"},
                    "float_share": {"type": "DOUBLE"},
                    "free_share": {"type": "DOUBLE"},
                    "total_mv": {"type": "DOUBLE"},
                    "circ_mv": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_stk_limit",
                "api_name": "stk_limit",
                "description": "每日涨跌停价格（涨停价、跌停价、昨日收盘价）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_stk_limit",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,pre_close,up_limit,down_limit",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "enabled": True,
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "pre_close": {"type": "DOUBLE"},
                    "up_limit": {"type": "DOUBLE"},
                    "down_limit": {"type": "DOUBLE"},
                },
            },
            {
                "task_id": "sync_suspend_d",
                "api_name": "suspend_d",
                "description": "每日停复牌信息（停牌/复牌类型、日内停牌时间段）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_suspend_d",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,trade_date,suspend_timing,suspend_type",
                },
                "primary_keys": ["ts_code", "trade_date", "suspend_type"],
                "enabled": True,
                "api_limit": 5000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "trade_date": {"type": "DATE"},
                    "suspend_timing": {"type": "STRING"},
                    "suspend_type": {"type": "STRING"},
                },
            },
            {
                "task_id": "sync_stock_st",
                "api_name": "stock_st",
                "description": "ST股票列表（每日ST/退市风险警示股票）",
                "sync_type": "incremental",
                "date_field": "trade_date",
                "table_name": "sync_stock_st",
                "params": {
                    "trade_date": "{date}",
                    "fields": "ts_code,name,trade_date,type,type_name",
                },
                "primary_keys": ["ts_code", "trade_date"],
                "enabled": True,
                "api_limit": 1000,
                "schema": {
                    "ts_code": {"type": "SYMBOL"},
                    "name": {"type": "STRING"},
                    "trade_date": {"type": "DATE"},
                    "type": {"type": "STRING"},
                    "type_name": {"type": "STRING"},
                },
            },
        ]
        seed_df = pl.DataFrame({
            "task_id": [t["task_id"] for t in tasks],
            "api_name": [t["api_name"] for t in tasks],
            "description": [t["description"] for t in tasks],
            "sync_type": [t["sync_type"] for t in tasks],
            "params_json": [json.dumps(t["params"], ensure_ascii=False) for t in tasks],
            "date_field": [t.get("date_field", "") for t in tasks],
            "primary_keys_json": [json.dumps(t["primary_keys"]) for t in tasks],
            "table_name": [t["table_name"] for t in tasks],
            "schema_json": [json.dumps(t["schema"], ensure_ascii=False) for t in tasks],
            "enabled": [t["enabled"] for t in tasks],
            "api_limit": [t.get("api_limit", 5000) for t in tasks],
            "created_at": [now] * len(tasks),
            "updated_at": [now] * len(tasks),
        })
        self.upsert("sync_task_config", seed_df, ["task_id"])
        logger.info(f"已写入 {len(tasks)} 条默认同步任务配置")

    def seed_factor_data_config(self) -> None:
        """
        如果 factor_data_config 表为空，则写入默认字段映射。
        仅在首次启动时生效，后续可通过 API 修改。
        """
        try:
            count = self.query("SELECT count(*) as cnt FROM factor_data_config")
            if not count.is_empty() and count["cnt"][0] > 0:
                logger.info("factor_data_config 已有数据，跳过 seed")
                return
        except Exception:
            pass

        now = datetime.now()
        mappings = [
            {"field_key": "adj_factor", "description": "复权因子", "table_name": "sync_adj_factor", "column_name": "adj_factor", "extra_config": "{}"},
            {"field_key": "list_date", "description": "股票上市日期", "table_name": "sync_stock_basic", "column_name": "list_date", "extra_config": "{}"},
            {
                "field_key": "is_st",
                "description": "是否ST（0=正常, 1=ST/*ST/退市风险警示）。来源: sync_stock_st 表，当日有记录则为1",
                "table_name": "sync_stock_st",
                "column_name": "ts_code",
                "extra_config": '{"mode":"exists_in_table","values":{"0":"正常","1":"ST/*ST/退市风险警示"}}',
            },
            {
                "field_key": "is_suspend",
                "description": "是否停牌（0=正常交易, 1=全天停牌, 2=盘中临时停牌）。来源: sync_suspend_d 表",
                "table_name": "sync_suspend_d",
                "column_name": "suspend_type",
                "extra_config": '{"mode":"exists_in_table","filter":{"suspend_type":"S"},"values":{"0":"正常交易","1":"全天停牌(suspend_timing为空)","2":"盘中临时停牌(suspend_timing非空)"}}',
            },
            {
                "field_key": "is_limit",
                "description": "涨跌停状态（0=未涨跌停, 1=涨停, -1=跌停）。来源: sync_stk_limit + sync_daily_data，收盘价触及涨跌停价",
                "table_name": "sync_stk_limit",
                "column_name": "up_limit,down_limit",
                "extra_config": '{"mode":"compare_with_price","price_table":"sync_daily_data","price_column":"close","values":{"0":"未涨跌停","1":"涨停(close>=up_limit)","-1":"跌停(close<=down_limit)"}}',
            },
            {"field_key": "industry_l1", "description": "股票一级行业", "table_name": "sync_stock_basic", "column_name": "industry", "extra_config": "{}"},
            {"field_key": "industry_l2", "description": "股票二级行业", "table_name": "", "column_name": "", "extra_config": "{}"},
            {"field_key": "market_cap", "description": "股票总市值（万元）", "table_name": "sync_daily_basic", "column_name": "total_mv", "extra_config": "{}"},
        ]
        seed_df = pl.DataFrame({
            "field_key": [m["field_key"] for m in mappings],
            "description": [m["description"] for m in mappings],
            "table_name": [m["table_name"] for m in mappings],
            "column_name": [m["column_name"] for m in mappings],
            "extra_config": [m["extra_config"] for m in mappings],
            "updated_at": [now] * len(mappings),
        })
        self.upsert("factor_data_config", seed_df, ["field_key"])
        logger.info(f"已写入 {len(mappings)} 条默认因子数据配置")

    def _resolve_db_path(self, table_name: str) -> str:
        """根据表名返回所属数据库路径"""
        return self._db_path

    def register_meta_table(self, table_name: str) -> None:
        """将表名注册到元数据表集合（如果尚未注册）"""
        if table_name not in self._META_TABLES:
            self._META_TABLES = self._META_TABLES | frozenset({table_name})
            self._ALL_TABLES = self._META_TABLES | self._TSDB_TABLES

    def _prepare_upload_df(
        self,
        table_name: str,
        df: pl.DataFrame,
        db_path: str,
        known_columns: Optional[List[str]],
        var_prefix: str,
        select_columns: Optional[List[str]] = None,
    ) -> tuple:
        """
        通用的 DataFrame 写入准备逻辑：
        1. 日期列格式转换（YYYYMMDD → date）
        2. 转换为 Pandas
        3. 获取表列顺序并对齐
        4. 上传临时变量

        必须在 self._lock 内调用。

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            db_path: 数据库路径
            known_columns: 已知列顺序（跳过 schema 查询）
            var_prefix: 临时变量前缀
            select_columns: 选择写入的列（默认全部）

        Returns:
            (ordered_cols, tmp_var) — 有序列名列表和上传到 DolphinDB 的临时变量名
        """
        # 获取表列顺序 & 自动检测 DATE 列
        date_cols: List[str] = []
        if known_columns is not None:
            table_cols = known_columns
            # known_columns 场景无法从 schema 检测，回退到额外配置
            date_cols = self._EXTRA_DATE_COLUMNS.get(table_name, [])
        else:
            schema_info = self._session.run(
                f"schema(loadTable('{db_path}', '{table_name}'))"
            )
            table_cols = []
            if isinstance(schema_info, dict) and "colDefs" in schema_info:
                col_defs_df = schema_info["colDefs"]
                if isinstance(col_defs_df, pd.DataFrame) and "name" in col_defs_df.columns:
                    table_cols = col_defs_df["name"].tolist()
                    # 自动从 schema 中提取 DATE 类型列
                    if "typeString" in col_defs_df.columns:
                        date_cols = col_defs_df.loc[
                            col_defs_df["typeString"] == "DATE", "name"
                        ].tolist()
            if not table_cols:
                logger.warning(
                    f"无法从 schema 获取 {table_name} 列信息"
                    f"（schema 类型={type(schema_info).__name__}），"
                    f"回退使用 DataFrame 列写入"
                )
                table_cols = df.columns if select_columns is None else select_columns

        # 合并额外配置的日期列（如 factor_values）
        extra = self._EXTRA_DATE_COLUMNS.get(table_name, [])
        if extra:
            date_cols = list(set(date_cols) | set(extra))

        # 转换日期列：YYYYMMDD 字符串 → date
        for col in date_cols:
            if col in df.columns and df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(col).str.to_date("%Y%m%d").alias(col)
                )

        pdf = df.select(select_columns).to_pandas() if select_columns else df.to_pandas()

        for col in date_cols:
            if col in pdf.columns and pd.api.types.is_datetime64_any_dtype(pdf[col]):
                pdf[col] = pdf[col].dt.date

        # 对齐列顺序
        ordered_cols = [c for c in table_cols if c in pdf.columns]
        if not ordered_cols:
            raise RuntimeError(
                f"写入 {table_name} 时列名无交集: "
                f"表列={table_cols}, DataFrame列={pdf.columns.tolist()}"
            )
        pdf = pdf[ordered_cols]

        # 上传临时变量
        tmp_var = f"{var_prefix}_{table_name}_{threading.current_thread().ident}"
        self._session.upload({tmp_var: pdf})

        return ordered_cols, tmp_var

    def upsert(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: List[str],
        known_columns: Optional[List[str]] = None,
    ) -> None:
        """
        插入或更新数据

        TSDB 分区表使用 keepDuplicates=LAST 自动去重。
        维度表需要手动 delete + insert。

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 主键列
            known_columns: 已知列顺序（跳过 schema 查询，用于刚建表后首次写入）
        """
        if df.is_empty():
            logger.warning(f"空 DataFrame，跳过写入: {table_name}")
            return

        db_path = self._resolve_db_path(table_name)
        is_meta = table_name in self._META_TABLES

        try:
            with self._lock:
                self._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "tmp"
                )
                col_select = ", ".join(ordered_cols)

                if is_meta and key_columns:
                    # 维度表：先按主键删除旧行，再插入（模拟 upsert）
                    handle = f"{table_name}_handle"
                    delete_conds = [f'{kc} in {tmp_var}.{kc}' for kc in key_columns]
                    cond_str = " and ".join(delete_conds)
                    self._session.run(
                        f"{handle} = loadTable('{db_path}', '{table_name}')"
                    )
                    self._session.run(
                        f"delete from {handle} where {cond_str}"
                    )
                    self._session.run(
                        f"tableInsert({handle}, select {col_select} from {tmp_var});"
                        f"undef('{tmp_var}')"
                    )
                else:
                    # TSDB 表：keepDuplicates=LAST 自动去重，直接插入
                    self._session.run(
                        f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                        f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
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
        self.upsert("sync_daily_data", df, ["trade_date", "ts_code"])

    def bulk_copy(
        self,
        table_name: str,
        df: pl.DataFrame,
        columns: List[str] = None,
        known_columns: Optional[List[str]] = None,
    ) -> int:
        """
        批量写入数据

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            columns: 列名列表，默认使用 DataFrame 的列名
            known_columns: 已知列顺序（跳过 schema 查询，用于刚建表后首次写入）

        Returns:
            写入的行数
        """
        if df.is_empty():
            return 0

        db_path = self._resolve_db_path(table_name)
        cols = columns or df.columns
        rows = len(df)

        try:
            with self._lock:
                self._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "bulk",
                    select_columns=cols,
                )
                col_select = ", ".join(ordered_cols)
                self._session.run(
                    f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                    f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
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
                f'SELECT last_date FROM loadTable("{self._db_path}", "sync_log") '
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
                    f'sync_log_handle = loadTable("{self._db_path}", "sync_log");'
                    f'delete from sync_log_handle where source = "{source}" and data_type = "{data_type}"'
                )
                # 再插入新记录
                tmp_var = f"sync_log_{threading.current_thread().ident}"
                self._session.upload({tmp_var: pdf})
                self._session.run(
                    f'sync_log_handle = loadTable("{self._db_path}", "sync_log");'
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
