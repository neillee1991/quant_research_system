"""
PostgreSQL 数据库客户端
使用 psycopg2 和 SQLAlchemy 提供数据库操作接口
"""
import polars as pl
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.core.logger import logger


class PostgreSQLClient:
    """PostgreSQL 数据库客户端"""

    def __init__(self):
        self.db_config = {
            'host': settings.database.postgres_host,
            'port': settings.database.postgres_port,
            'database': settings.database.postgres_db,
            'user': settings.database.postgres_user,
            'password': settings.database.postgres_password,
        }

        # 创建连接池
        self._pool: Optional[ThreadedConnectionPool] = None
        self._init_pool()

        # 创建 SQLAlchemy engine (用于 Polars 集成)
        self.engine = self._create_engine()

        logger.info(f"PostgreSQL client initialized: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")

    def _init_pool(self):
        """初始化连接池"""
        try:
            self._pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=settings.database.connection_pool_size,
                **self.db_config
            )
            logger.info(f"Connection pool created with size {settings.database.connection_pool_size}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise

    def _create_engine(self):
        """创建 SQLAlchemy engine"""
        connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        return create_engine(
            connection_string,
            poolclass=NullPool,  # 使用 psycopg2 连接池
            echo=False
        )

    @contextmanager
    def get_connection(self):
        """获取数据库连接（上下文管理器）"""
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self._pool.putconn(conn)

    def connect(self):
        """获取连接（兼容 DuckDB 接口）"""
        return self._pool.getconn()

    def query(self, sql: str, params: Optional[tuple] = None) -> pl.DataFrame:
        """
        执行查询并返回 Polars DataFrame

        Args:
            sql: SQL 查询语句
            params: 查询参数（使用 %s 占位符）

        Returns:
            Polars DataFrame
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if params:
                        cur.execute(sql, params)
                    else:
                        cur.execute(sql)

                    # 获取列名
                    columns = [desc[0] for desc in cur.description] if cur.description else []

                    # 获取数据
                    rows = cur.fetchall()

                    if not rows:
                        return pl.DataFrame(schema={col: pl.Utf8 for col in columns})

                    # 转换为 Polars DataFrame
                    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                    return pl.DataFrame(data)
        except Exception as e:
            logger.error(f"Query failed: {sql[:100]}... Error: {e}")
            raise

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """
        执行 SQL 语句（不返回结果）

        Args:
            sql: SQL 语句
            params: 参数
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if params:
                        cur.execute(sql, params)
                    else:
                        cur.execute(sql)
        except Exception as e:
            logger.error(f"Execute failed: {sql[:100]}... Error: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """
        result = self.query(sql, (table_name,))
        return result[0, 0] if not result.is_empty() else False

    def create_table(self, table_name: str, schema: Dict[str, Dict[str, Any]], primary_keys: List[str]) -> None:
        """
        创建表

        Args:
            table_name: 表名
            schema: 列定义字典 {列名: {type, nullable, comment}}
            primary_keys: 主键列表
        """
        try:
            # 构建列定义
            col_defs = []
            comments = []

            for col_name, col_info in schema.items():
                col_type = col_info.get("type", "VARCHAR")
                # 转换 DuckDB 类型到 PostgreSQL 类型
                if col_type == "DOUBLE":
                    col_type = "DOUBLE PRECISION"

                nullable = "" if col_info.get("nullable", True) else "NOT NULL"
                col_defs.append(f"{col_name} {col_type} {nullable}")

                # 收集注释
                if "comment" in col_info:
                    comments.append((col_name, col_info["comment"]))

            # 添加主键约束
            if primary_keys:
                pk_str = ", ".join(primary_keys)
                col_defs.append(f"PRIMARY KEY ({pk_str})")

            # 创建表
            columns_sql = ",\n    ".join(col_defs)
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_sql}\n)"

            self.execute(create_sql)

            # 添加注释
            for col_name, comment in comments:
                comment_sql = f"COMMENT ON COLUMN {table_name}.{col_name} IS %s"
                self.execute(comment_sql, (comment,))

            logger.info(f"Created table {table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise

    def upsert(self, table_name: str, df: pl.DataFrame, key_columns: List[str]) -> None:
        """
        插入或更新数据（使用 ON CONFLICT）

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 用于冲突检测的列（通常是主键）
        """
        if df.is_empty():
            logger.warning(f"Empty dataframe for table {table_name}")
            return

        try:
            columns = df.columns
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            # 构建 UPDATE 子句
            update_cols = [col for col in columns if col not in key_columns]
            update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

            # 构建 SQL
            conflict_cols = ", ".join(key_columns)
            sql = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES %s
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {update_str}
            """

            # 转换数据
            values = [tuple(row) for row in df.iter_rows()]

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, values, template=f"({placeholders})")

            logger.info(f"Upserted {len(df)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Upsert failed for table {table_name}: {e}")
            raise

    def upsert_daily(self, df: pl.DataFrame) -> None:
        """插入或更新日线数据（兼容旧接口）"""
        self.upsert("daily_data", df, ["trade_date", "ts_code"])

    def get_last_sync_date(self, source: str, data_type: str) -> Optional[str]:
        """获取最后同步日期"""
        sql = """
            SELECT last_date
            FROM sync_log
            WHERE source = %s AND data_type = %s
            LIMIT 1
        """
        result = self.query(sql, (source, data_type))
        return result["last_date"][0] if not result.is_empty() else None

    def update_sync_log(self, source: str, data_type: str, last_date: str) -> None:
        """更新同步日志"""
        sql = """
            INSERT INTO sync_log (source, data_type, last_date, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (source, data_type)
            DO UPDATE SET
                last_date = EXCLUDED.last_date,
                updated_at = EXCLUDED.updated_at
        """
        self.execute(sql, (source, data_type, last_date))

    def close(self):
        """关闭连接池"""
        if self._pool:
            self._pool.closeall()
            logger.info("Connection pool closed")


# 单例实例
db_client = PostgreSQLClient()
