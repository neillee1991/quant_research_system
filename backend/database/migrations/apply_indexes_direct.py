"""
直接应用索引优化（绕过事务限制）
"""
import sys
from pathlib import Path

# 添加后端目录到路径
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))

from store.postgres_client import db_client
import psycopg2.extensions

def apply_indexes():
    """应用索引优化"""

    # 读取 SQL 文件
    sql_file = Path(__file__).parent / '002_add_performance_indexes_nonconcurrent.sql'
    with open(sql_file, 'r') as f:
        full_sql = f.read()

    # 分割成单独的语句
    statements = []
    current = []
    for line in full_sql.split('\n'):
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        current.append(line)
        if line.endswith(';'):
            statements.append(' '.join(current))
            current = []

    # 获取连接并设置 autocommit
    conn = db_client._pool.getconn()
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        print(f"Applying {len(statements)} SQL statements...")

        with conn.cursor() as cur:
            for i, stmt in enumerate(statements, 1):
                try:
                    # 只显示 CREATE INDEX 和 ANALYZE 语句
                    if 'CREATE INDEX' in stmt or 'ANALYZE' in stmt:
                        # 提取表名或索引名
                        if 'CREATE INDEX' in stmt:
                            idx_name = stmt.split('idx_')[1].split()[0] if 'idx_' in stmt else 'unknown'
                            print(f"  [{i}/{len(statements)}] Creating index: idx_{idx_name}")
                        elif 'ANALYZE' in stmt:
                            table_name = stmt.split('ANALYZE')[1].strip().rstrip(';')
                            print(f"  [{i}/{len(statements)}] Analyzing table: {table_name}")

                    cur.execute(stmt)

                except Exception as e:
                    # 如果索引已存在，忽略错误
                    if 'already exists' in str(e):
                        print(f"    (already exists, skipping)")
                    else:
                        print(f"    Error: {e}")
                        raise

        print("\n✓ All indexes created successfully!")

        # 记录到迁移历史
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        with conn.cursor() as cur:
            cur.execute('''
                INSERT INTO migration_history (migration_name, execution_time_ms, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (migration_name) DO NOTHING
            ''', ('002_add_performance_indexes_nonconcurrent', 0, 'success'))
        conn.commit()

        print("✓ Migration recorded in history")

    finally:
        db_client._pool.putconn(conn)

if __name__ == '__main__':
    apply_indexes()
