"""
DolphinDB SQL 查询构建器
负责 SQL 参数替换、语法适配和查询执行
"""
import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd
import polars as pl

from app.core.logger import logger
from .connection import DolphinDBConnection


class QueryBuilder:
    """SQL 查询构建器和执行器"""

    # TSDB 分区表（时间序列数据）
    _TSDB_TABLES: frozenset = frozenset({
        "sync_daily_data", "sync_daily_basic", "sync_adj_factor",
        "sync_index_daily", "sync_moneyflow", "factor_values",
    })

    # 元数据表（维度表）
    _META_TABLES: frozenset = frozenset({
        "sync_log", "sync_log_history", "sync_stock_basic",
        "factor_metadata", "factor_analysis", "dag_run_log",
        "dag_task_log", "production_task_run", "trade_cal",
        "sync_task_config", "etl_task_config", "factor_data_config",
        "task_version_history",
    })

    # 所有已知表名（用于 SQL 语法适配）
    _ALL_TABLES: frozenset = _TSDB_TABLES | _META_TABLES

    def __init__(self, connection: DolphinDBConnection) -> None:
        """
        初始化查询构建器

        Args:
            connection: DolphinDB 连接管理器
        """
        self.conn = connection

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
        # 处理列表和元组（用于 IN 子句）
        if isinstance(value, (list, tuple)):
            if not value:
                return "NULL"  # 空列表转换为 NULL
            escaped_items = [QueryBuilder._escape_value(item) for item in value]
            return ",".join(escaped_items)
        # 检查是否是 YYYYMMDD 格式的日期字符串，使用 temporalParse 转换
        if isinstance(value, str) and re.match(r"^\d{8}$", value):
            return f'temporalParse("{value}", "yyyyMMdd")'
        # 普通字符串，转义双引号
        s = str(value)
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{s}"'

    def _substitute_params(self, sql: str, params: Optional[tuple]) -> str:
        """
        将 PostgreSQL 风格的 %s 占位符替换为实际值
        DolphinDB 不支持参数化查询，需要手动拼接

        Args:
            sql: SQL 语句（包含 %s 占位符）
            params: 参数元组

        Returns:
            替换后的 SQL 语句
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

    def _resolve_db_path(self, table_name: str) -> str:
        """根据表名返回所属数据库路径"""
        return self.conn.db_path

    def _adapt_sql_syntax(self, sql: str) -> str:
        """
        将 PostgreSQL SQL 语法适配为 DolphinDB 兼容语法
        - SQL 聚合函数大写 → 小写（DolphinDB 函数名区分大小写）
        - 裸表名 → loadTable("db_path", "table_name")
        - CURRENT_TIMESTAMP → now()
        - LIMIT N 保持不变（DolphinDB 也支持）

        Args:
            sql: 原始 SQL 语句

        Returns:
            适配后的 SQL 语句
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
        """
        完整的 SQL 构建流程：参数替换 + 语法适配

        Args:
            sql: 原始 SQL 语句
            params: 参数元组

        Returns:
            最终可执行的 SQL 语句
        """
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
        # 验证 SQL 不为空
        if not sql or not sql.strip():
            raise ValueError("SQL 语句不能为空")

        final_sql = self._build_sql(sql, params)
        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                result = self.conn.session.run(final_sql)

            # 将结果转换为 Polars DataFrame
            return self._to_polars(result)
        except Exception as e:
            logger.error(f"查询失败: {final_sql[:200]}... 错误: {e}")
            raise

    def _to_polars(self, result: Any) -> pl.DataFrame:
        """
        将 DolphinDB 返回结果统一转换为 Polars DataFrame
        session.run() 可能返回 pandas DataFrame、numpy array、标量等

        Args:
            result: DolphinDB 查询结果

        Returns:
            pl.DataFrame
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
            with self.conn.lock:
                self.conn._ensure_connected()
                self.conn.session.run(final_sql)
        except Exception as e:
            logger.error(f"执行失败: {final_sql[:200]}... 错误: {e}")
            raise

    def register_meta_table(self, table_name: str) -> None:
        """
        将表名注册到元数据表集合（如果尚未注册）
        用于动态创建的表能被 SQL 语法适配识别

        Args:
            table_name: 表名
        """
        if table_name not in self._META_TABLES:
            # 使用不可变模式更新
            QueryBuilder._META_TABLES = self._META_TABLES | frozenset({table_name})
            QueryBuilder._ALL_TABLES = self._META_TABLES | self._TSDB_TABLES
