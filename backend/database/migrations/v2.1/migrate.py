"""
数据库迁移脚本 v2.1
用途: 将 PostgreSQL 中 *_json TEXT 列迁移为 JSONB 类型
"""
import sys
import psycopg2
import psycopg2.extras
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from loguru import logger

MIGRATION_VERSION = "v2.1"
MIGRATION_DATE = "2026-04-25"

# 需要迁移的表和列
JSON_COLUMNS = {
    "sync_task_configs": ["params_json", "schema_json", "primary_keys_json", "column_mapping_json"],
    "etl_task_configs": ["schema_json", "primary_keys_json"],
    "factor_configs": ["params"],
}


def get_connection():
    return psycopg2.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        dbname=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )


def migrate():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for table, columns in JSON_COLUMNS.items():
                    for col in columns:
                        # 检查列是否存在且为 TEXT 类型
                        cur.execute("""
                            SELECT data_type FROM information_schema.columns
                            WHERE table_name = %s AND column_name = %s
                        """, (table, col))
                        row = cur.fetchone()
                        if not row:
                            logger.warning(f"列不存在，跳过: {table}.{col}")
                            continue
                        if row[0] == "jsonb":
                            logger.info(f"已是 JSONB，跳过: {table}.{col}")
                            continue

                        logger.info(f"迁移 {table}.{col}: TEXT → JSONB")
                        # 先清理无效 JSON（置为 NULL），再转换类型
                        cur.execute(f"""
                            UPDATE {table}
                            SET {col} = NULL
                            WHERE {col} IS NOT NULL
                              AND {col}::text !~ '^\\s*(\\{{|\\[|"|-?[0-9]|true|false|null)'
                        """)
                        # DROP DEFAULT → ALTER TYPE → SET DEFAULT
                        cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} DROP DEFAULT")
                        cur.execute(f"""
                            ALTER TABLE {table}
                            ALTER COLUMN {col} TYPE JSONB
                            USING {col}::jsonb
                        """)
                        # 根据列名恢复合适的默认值
                        if col.endswith("_json") and "keys" in col:
                            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT '[]'::jsonb")
                        elif col in ("params_json", "schema_json", "params"):
                            cur.execute(f"ALTER TABLE {table} ALTER COLUMN {col} SET DEFAULT '{{}}'::jsonb")
                        logger.info(f"完成: {table}.{col}")

        logger.info("迁移 v2.1 完成")
    finally:
        conn.close()


def rollback():
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                for table, columns in JSON_COLUMNS.items():
                    for col in columns:
                        cur.execute("""
                            SELECT data_type FROM information_schema.columns
                            WHERE table_name = %s AND column_name = %s
                        """, (table, col))
                        row = cur.fetchone()
                        if not row or row[0] != "jsonb":
                            continue
                        logger.info(f"回滚 {table}.{col}: JSONB → TEXT")
                        cur.execute(f"""
                            ALTER TABLE {table}
                            ALTER COLUMN {col} TYPE TEXT
                            USING {col}::text
                        """)
        logger.info("回滚 v2.1 完成")
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "rollback":
        rollback()
    else:
        migrate()
