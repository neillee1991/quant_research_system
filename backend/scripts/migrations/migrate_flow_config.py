"""
从 DolphinDB 迁移 flow_config 数据到 PostgreSQL
"""
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parents[2]
sys.path.insert(0, str(backend_dir))

import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.logger import logger
from app.core.config import settings
from infrastructure.database.dolphindb_client import db_client


def get_pg_connection():
    """获取 PostgreSQL 连接"""
    return psycopg2.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        database=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )


def migrate_flow_config():
    """迁移 flow_config 表"""
    logger.info("开始迁移 flow_config 数据...")

    # 从 DolphinDB 读取数据
    try:
        df = db_client.query("SELECT * FROM flow_config ORDER BY name")
        if df.is_empty():
            logger.warning("DolphinDB 中没有 flow_config 数据")
            return 0

        rows = df.to_dicts()
        logger.info(f"从 DolphinDB 读取到 {len(rows)} 条 flow_config 记录")
    except Exception as e:
        logger.error(f"从 DolphinDB 读取失败: {e}", exc_info=True)
        raise

    # 写入 PostgreSQL
    success_count = 0
    now = datetime.now()

    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    try:
                        # 解析 JSON 字段
                        tags = row.get("tags")
                        if isinstance(tags, str):
                            tags = json.loads(tags) if tags else []

                        tasks = row.get("tasks")
                        if isinstance(tasks, str):
                            tasks = json.loads(tasks) if tasks else []

                        # 插入 PostgreSQL
                        cur.execute(
                            """
                            INSERT INTO flow_config
                            (name, description, cron, timezone, tags, tasks,
                             date_offset_days, enabled, version, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (name) DO UPDATE SET
                                description = EXCLUDED.description,
                                cron = EXCLUDED.cron,
                                timezone = EXCLUDED.timezone,
                                tags = EXCLUDED.tags,
                                tasks = EXCLUDED.tasks,
                                date_offset_days = EXCLUDED.date_offset_days,
                                enabled = EXCLUDED.enabled,
                                version = flow_config.version + 1,
                                updated_at = EXCLUDED.updated_at
                            """,
                            (
                                row["name"],
                                row.get("description", ""),
                                row.get("cron"),
                                row.get("timezone", "Asia/Shanghai"),
                                json.dumps(tags) if tags else "[]",
                                json.dumps(tasks) if tasks else "[]",
                                int(row.get("date_offset_days", 0)),
                                bool(row.get("enabled", True)),
                                int(row.get("version", 1)),
                                row.get("created_at", now),
                                row.get("updated_at", now),
                            ),
                        )
                        success_count += 1
                        logger.info(f"迁移成功: {row['name']}")
                    except Exception as e:
                        logger.error(f"迁移失败: {row.get('name')}, {e}", exc_info=True)

            conn.commit()

    except Exception as e:
        logger.error(f"PostgreSQL 写入失败: {e}", exc_info=True)
        raise

    logger.info(f"flow_config 迁移完成，成功 {success_count}/{len(rows)} 条")
    return success_count


if __name__ == "__main__":
    print("=" * 60)
    print("Flow Config 数据迁移: DolphinDB → PostgreSQL")
    print("=" * 60)

    try:
        count = migrate_flow_config()
        print(f"\n✅ 迁移成功: {count} 条记录")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 迁移失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
