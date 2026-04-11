"""
SQL 注入防护工具
提供表名、列名白名单验证和 SQL 安全检查
"""
import re
from typing import List, Set, Optional
from fastapi import HTTPException

from app.core.logger import logger

# 允许的表名白名单
ALLOWED_TABLES = {
    # 同步数据表
    "sync_daily_data",
    "sync_daily_basic",
    "sync_adj_factor",
    "sync_index_daily",
    "sync_moneyflow",
    "sync_stock_basic",
    "sync_index_member",
    # ETL 输出表
    "etl_index_member",
    "etl_index_member_daily",
    "etl_stock_daily_info",
    # 因子表
    "factor_values",
    "stock_daily_status",
    # 其他系统表
    "sync_trade_cal",
    # ETL任务配置的输出表
    "etl_index_member",
    "etl_index_member_daily",
    "etl_stock_daily_info",
}

# 允许的列名白名单模式（正则）
ALLOWED_COLUMN_PATTERNS = [
    r"^[a-zA-Z][a-zA-Z0-9_]*$",  # 标准标识符
    r"^ts_code$",
    r"^trade_date$",
    r"^open$",
    r"^high$",
    r"^low$",
    r"^close$",
    r"^vol(?:ume)?$",
    r"^amount$",
    r"^pe$",
    r"^pb$",
    r"^turnover_rate$",
    r"^factor_id$",
    r"^factor_value$",
    r"^quality_flag$",
]

# 危险的 SQL 关键字（用于额外检查）
DANGEROUS_KEYWORDS = {
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE",
    "DROPTABLE", "DROPDATABASE", "DROPPARTITION", "EXEC", "EXECUTE",
    "SLEEP", "WAITFOR", "DELAY", "BENCHMARK",
}

# 允许的 SQL 操作
ALLOWED_OPERATIONS = {"SELECT", "DELETE"}


class SQLSecurityError(Exception):
    """SQL 安全违规错误"""
    pass


def validate_table_name(table_name: str) -> bool:
    """
    验证表名是否在白名单中

    Args:
        table_name: 要验证的表名

    Returns:
        是否安全

    Raises:
        SQLSecurityError: 如果表名不在白名单中
    """
    if not table_name:
        raise SQLSecurityError("Table name cannot be empty")

    # 清理表名
    cleaned = table_name.strip()

    # 检查白名单
    if cleaned not in ALLOWED_TABLES:
        # 检查是否是已知的分区表或有后缀的表
        if "::" in cleaned or "$" in cleaned:
            base_name = cleaned.split("::")[0].split("$")[0]
            if base_name in ALLOWED_TABLES:
                return True

        raise SQLSecurityError(f"Table '{cleaned}' is not in allowed list")

    return True


def validate_column_name(column_name: str) -> bool:
    """
    验证列名是否安全

    Args:
        column_name: 要验证的列名

    Returns:
        是否安全

    Raises:
        SQLSecurityError: 如果列名不安全
    """
    if not column_name:
        raise SQLSecurityError("Column name cannot be empty")

    cleaned = column_name.strip()

    # 检查常见的列名模式
    for pattern in ALLOWED_COLUMN_PATTERNS:
        if re.match(pattern, cleaned):
            return True

    # 检查是否包含危险字符
    if any(c in cleaned for c in [";", "--", "/*", "*/", "@", "@@"]):
        raise SQLSecurityError(f"Column '{cleaned}' contains dangerous characters")

    # 检查是否只包含安全字符
    if not re.match(r"^[a-zA-Z0-9_]+$", cleaned):
        raise SQLSecurityError(f"Column '{cleaned}' contains invalid characters")

    return True


def validate_limit_value(limit: int, max_limit: int = 10000) -> int:
    """
    验证并规范化 LIMIT 值

    Args:
        limit: 用户提供的 limit 值
        max_limit: 最大允许值

    Returns:
        安全的 limit 值

    Raises:
        SQLSecurityError: 如果 limit 值无效
    """
    if not isinstance(limit, int):
        raise SQLSecurityError("Limit must be an integer")

    if limit < 0:
        raise SQLSecurityError("Limit cannot be negative")

    return min(limit, max_limit)


def safe_sql_query(sql: str, allowed_tables: Optional[Set[str]] = None) -> str:
    """
    安全化 SQL 查询（用于只读查询）

    Args:
        sql: 原始 SQL
        allowed_tables: 允许的表名集合（默认使用 ALLOWED_TABLES）

    Returns:
        安全的 SQL

    Raises:
        SQLSecurityError: 如果 SQL 包含不安全内容
    """
    if not sql or not sql.strip():
        raise SQLSecurityError("SQL cannot be empty")

    sql_upper = sql.strip().upper()
    allowed = allowed_tables or ALLOWED_TABLES

    # 1. 检查是否以允许的操作开头
    if not any(sql_upper.startswith(op) for op in ALLOWED_OPERATIONS):
        raise SQLSecurityError(f"Only {', '.join(ALLOWED_OPERATIONS)} operations are allowed")

    # 2. 检查危险关键字
    for keyword in DANGEROUS_KEYWORDS - ALLOWED_OPERATIONS:
        if re.search(rf"\b{keyword}\b", sql_upper):
            raise SQLSecurityError(f"Dangerous keyword '{keyword}' is not allowed")

    # 3. 检查注释
    if "--" in sql or "/*" in sql:
        raise SQLSecurityError("SQL comments are not allowed")

    # 4. 检查多个语句
    if ";" in sql.rstrip(";"):
        raise SQLSecurityError("Multiple SQL statements are not allowed")

    return sql


def get_safe_table_list() -> List[str]:
    """获取安全的表名列表"""
    return sorted(list(ALLOWED_TABLES))


def add_allowed_table(table_name: str) -> None:
    """
    动态添加允许的表名（仅用于初始化）

    注意：这不是线程安全的，应该只在应用启动时调用
    """
    ALLOWED_TABLES.add(table_name)
    logger.info(f"Added allowed table: {table_name}")


# FastAPI 依赖项
async def get_safe_table_name(table_name: str) -> str:
    """
    FastAPI 依赖项：验证并返回安全的表名

    Usage:
        @router.get("/tables/{table_name}")
        def get_table(table_name: str = Depends(get_safe_table_name)):
            ...
    """
    try:
        validate_table_name(table_name)
        return table_name.strip()
    except SQLSecurityError as e:
        raise HTTPException(status_code=400, detail=str(e))


async def get_safe_limit(
    limit: int = 1000,
    max_limit: int = 10000
) -> int:
    """
    FastAPI 依赖项：验证并返回安全的 limit 值
    """
    try:
        return validate_limit_value(limit, max_limit)
    except SQLSecurityError as e:
        raise HTTPException(status_code=400, detail=str(e))


def build_safe_delete_query(table_name: str) -> str:
    """
    构建安全的 DELETE 查询

    Args:
        table_name: 表名（已验证）

    Returns:
        安全的 DELETE SQL
    """
    validate_table_name(table_name)
    return f"DELETE FROM {table_name} WHERE 1=1"


def build_safe_select_max_query(table_name: str, date_field: str) -> str:
    """
    构建安全的 SELECT MAX 查询

    Args:
        table_name: 表名（已验证）
        date_field: 日期字段名（已验证）

    Returns:
        安全的 SELECT MAX SQL
    """
    validate_table_name(table_name)
    validate_column_name(date_field)
    return f"SELECT MAX({date_field}) as max_date FROM {table_name}"


def build_safe_select_top_query(table_name: str, top_count: int = 1) -> str:
    """
    构建安全的 SELECT TOP 查询

    Args:
        table_name: 表名（已验证）
        top_count: 返回行数

    Returns:
        安全的 SELECT TOP SQL
    """
    validate_table_name(table_name)
    if top_count < 1:
        raise SQLSecurityError("top_count must be at least 1")
    return f"select top {top_count} * from {table_name}"
