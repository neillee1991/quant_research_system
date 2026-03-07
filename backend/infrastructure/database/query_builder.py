"""
QueryBuilder - 构建参数化SQL查询，防止SQL注入

功能：
- 支持 SELECT, WHERE, WHERE IN, WHERE BETWEEN
- 支持 ORDER BY, LIMIT
- 参数化查询（防止SQL注入）
- 返回 Query 对象（包含 sql 和 params）

使用示例：
    query = QueryBuilder("sync_daily_data") \\
        .select(["ts_code", "trade_date", "close"]) \\
        .where_in("ts_code", ["000001.SZ", "000002.SZ"]) \\
        .where_between("trade_date", "20240101", "20240131") \\
        .order_by(["trade_date DESC"]) \\
        .limit(100) \\
        .build()

    # 执行查询
    result = db_client.execute(query.sql, query.params)
"""
from dataclasses import dataclass
from typing import List, Any, Optional, Union


@dataclass
class Query:
    """查询对象，包含SQL语句和参数"""
    sql: str
    params: tuple

    def __repr__(self) -> str:
        return f"Query(sql={self.sql!r}, params={self.params!r})"


class QueryBuilder:
    """SQL查询构建器（参数化查询，防止SQL注入）"""

    def __init__(self, table: str):
        """
        初始化查询构建器

        Args:
            table: 表名
        """
        self._table = table
        self._select_cols: List[str] = []
        self._where_clauses: List[str] = []
        self._params: List[Any] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None

    def select(self, columns: List[str]) -> 'QueryBuilder':
        """
        指定查询列

        Args:
            columns: 列名列表

        Returns:
            self (支持链式调用)
        """
        self._select_cols = columns
        return self

    def select_all(self) -> 'QueryBuilder':
        """
        查询所有列 (SELECT *)

        Returns:
            self (支持链式调用)
        """
        self._select_cols = []
        return self

    def where(self, column: str, operator: str, value: Any) -> 'QueryBuilder':
        """
        添加 WHERE 条件

        Args:
            column: 列名
            operator: 操作符 (=, >, <, >=, <=, !=, LIKE 等)
            value: 值

        Returns:
            self (支持链式调用)

        Example:
            .where("close", ">", 100)
            .where("name", "LIKE", "%test%")
        """
        self._where_clauses.append(f"{column} {operator} %s")
        self._params.append(value)
        return self

    def where_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        """
        添加 WHERE IN 条件

        Args:
            column: 列名
            values: 值列表

        Returns:
            self (支持链式调用)

        Example:
            .where_in("ts_code", ["000001.SZ", "000002.SZ"])
        """
        if not values:
            return self

        placeholders = ", ".join(["%s"] * len(values))
        self._where_clauses.append(f"{column} IN ({placeholders})")
        self._params.extend(values)
        return self

    def where_between(self, column: str, start: Any, end: Any) -> 'QueryBuilder':
        """
        添加 WHERE BETWEEN 条件

        Args:
            column: 列名
            start: 起始值
            end: 结束值

        Returns:
            self (支持链式调用)

        Example:
            .where_between("trade_date", "20240101", "20240131")
        """
        self._where_clauses.append(f"{column} >= %s AND {column} <= %s")
        self._params.extend([start, end])
        return self

    def where_not_null(self, column: str) -> 'QueryBuilder':
        """
        添加 WHERE column IS NOT NULL 条件

        Args:
            column: 列名

        Returns:
            self (支持链式调用)
        """
        self._where_clauses.append(f"{column} IS NOT NULL")
        return self

    def where_null(self, column: str) -> 'QueryBuilder':
        """
        添加 WHERE column IS NULL 条件

        Args:
            column: 列名

        Returns:
            self (支持链式调用)
        """
        self._where_clauses.append(f"{column} IS NULL")
        return self

    def order_by(self, columns: List[str]) -> 'QueryBuilder':
        """
        添加 ORDER BY 子句

        Args:
            columns: 排序列列表，可包含 ASC/DESC

        Returns:
            self (支持链式调用)

        Example:
            .order_by(["trade_date DESC", "ts_code ASC"])
        """
        self._order_by = columns
        return self

    def limit(self, n: int) -> 'QueryBuilder':
        """
        添加 LIMIT 子句

        Args:
            n: 限制返回行数

        Returns:
            self (支持链式调用)
        """
        self._limit = n
        return self

    def build(self) -> Query:
        """
        构建最终的 Query 对象

        Returns:
            Query 对象，包含 sql 和 params
        """
        # SELECT 子句
        cols = ", ".join(self._select_cols) if self._select_cols else "*"
        sql = f"SELECT {cols} FROM {self._table}"

        # WHERE 子句
        if self._where_clauses:
            sql += " WHERE " + " AND ".join(self._where_clauses)

        # ORDER BY 子句
        if self._order_by:
            sql += " ORDER BY " + ", ".join(self._order_by)

        # LIMIT 子句
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"

        return Query(sql, tuple(self._params))

    def reset(self) -> 'QueryBuilder':
        """
        重置查询构建器（保留表名）

        Returns:
            self (支持链式调用)
        """
        self._select_cols = []
        self._where_clauses = []
        self._params = []
        self._order_by = []
        self._limit = None
        return self
