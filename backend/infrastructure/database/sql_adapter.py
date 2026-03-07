"""
DolphinDB SQL 适配器模块
负责将 PostgreSQL 风格的 SQL 转换为 DolphinDB 兼容语法
"""
import re
from typing import Optional

from .type_converter import TypeConverter


class SQLAdapter:
    """SQL 语法适配器，将 PostgreSQL SQL 转换为 DolphinDB 兼容语法"""

    def __init__(self, db_path: str):
        """
        初始化 SQL 适配器

        Args:
            db_path: DolphinDB 数据库路径
        """
        self._db_path = db_path
        self._type_converter = TypeConverter()

    def substitute_params(self, sql: str, params: Optional[tuple]) -> str:
        """
        将 PostgreSQL 风格的 %s 占位符替换为实际值
        DolphinDB 不支持参数化查询，需要手动拼接

        Args:
            sql: 包含 %s 占位符的 SQL 语句
            params: 参数元组

        Returns:
            替换后的 SQL 语句

        Raises:
            ValueError: 参数数量不匹配
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
            result += self._type_converter.escape_value(param) + parts[i + 1]
        return result

    def adapt_sql_syntax(self, sql: str) -> str:
        """
        将 PostgreSQL SQL 语法适配为 DolphinDB 兼容语法

        转换规则:
        - SQL 聚合函数大写 → 小写（DolphinDB 函数名区分大小写）
        - 裸表名 → loadTable("db_path", "table_name")
        - CURRENT_TIMESTAMP → now()
        - RETURNING 子句 → 移除（DolphinDB 不支持）

        Args:
            sql: PostgreSQL 风格的 SQL 语句

        Returns:
            DolphinDB 兼容的 SQL 语句
        """
        # 1. 聚合函数大写 → 小写
        sql = re.sub(r"\bCOUNT\b", "count", sql)
        sql = re.sub(r"\bSUM\b", "sum", sql)
        sql = re.sub(r"\bAVG\b", "avg", sql)
        sql = re.sub(r"\bMAX\b", "max", sql)
        sql = re.sub(r"\bMIN\b", "min", sql)

        # 2. CURRENT_TIMESTAMP → now()
        sql = re.sub(r"\bCURRENT_TIMESTAMP\b", "now()", sql, flags=re.IGNORECASE)

        # 3. 移除 RETURNING 子句
        sql = re.sub(r"\s+RETURNING\s+\*", "", sql, flags=re.IGNORECASE)

        # 4. 裸表名 → loadTable()
        sql = self._wrap_bare_table_names(sql)

        return sql

    def _wrap_bare_table_names(self, sql: str) -> str:
        """
        将裸表名包装为 loadTable("db_path", "table_name")

        识别规则:
        - FROM/JOIN 后的裸表名
        - 不包含 loadTable、database 等关键字的表名

        Args:
            sql: SQL 语句

        Returns:
            包装后的 SQL 语句
        """
        # 跳过已经包含 loadTable 或 database 的语句
        if "loadTable" in sql or "database" in sql:
            return sql

        # 匹配 FROM/JOIN 后的裸表名
        pattern = r"\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b"

        def replace_table(match):
            keyword = match.group(1)
            table_name = match.group(2)
            # 跳过子查询别名和常见 SQL 关键字
            if table_name.upper() in ("SELECT", "WHERE", "GROUP", "ORDER", "LIMIT"):
                return match.group(0)
            return f'{keyword} loadTable("{self._db_path}", "{table_name}")'

        return re.sub(pattern, replace_table, sql, flags=re.IGNORECASE)

    def build_sql(self, sql: str, params: Optional[tuple] = None) -> str:
        """
        构建完整的 DolphinDB SQL 语句

        Args:
            sql: PostgreSQL 风格的 SQL 语句
            params: 参数元组

        Returns:
            DolphinDB 兼容的 SQL 语句
        """
        # 1. 替换参数
        sql = self.substitute_params(sql, params)
        # 2. 适配语法
        sql = self.adapt_sql_syntax(sql)
        return sql
