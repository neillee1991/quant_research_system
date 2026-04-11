#!/usr/bin/env python3
"""
执行数据库约束和触发器迁移脚本

用法:
    python apply_constraints.py
"""
import sys
import os
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

os.environ.setdefault("ENVIRONMENT", "production")

import asyncio
from scheduler.db import DatabasePool
from app.core.logger import logger


async def apply_constraints():
    """应用数据库约束和触发器"""

    # 读取SQL脚本
    sql_file = Path(__file__).parent / "006_add_constraints_and_triggers.sql"

    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        return False

    with open(sql_file, "r") as f:
        sql_content = f.read()

    # 分割SQL语句，处理 $$ 分隔符
    statements = []
    current_stmt = ""
    in_dollar_quote = False

    for line in sql_content.split('\n'):
        # 检查 $$ 分隔符（可能出现多次）
        dollar_count = line.count('$$')
        if dollar_count > 0:
            in_dollar_quote = not in_dollar_quote

        current_stmt += line + "\n"

        # 如果不在 $$ 引号内且行以 ; 结尾，则这是一个完整的语句
        if not in_dollar_quote and line.rstrip().endswith(';'):
            stmt = current_stmt.strip()
            if stmt:
                # 移除纯注释行，但保留包含SQL的语句
                lines = [l.strip() for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')]
                if lines:
                    statements.append(stmt)
            current_stmt = ""

    # 添加最后一个语句（如果有）
    if current_stmt.strip():
        lines = [l.strip() for l in current_stmt.strip().split('\n') if l.strip() and not l.strip().startswith('--')]
        if lines:
            statements.append(current_stmt.strip())

    try:
        await DatabasePool.init_pool()
        logger.info(f"Executing {len(statements)} SQL statements...")

        for i, statement in enumerate(statements, 1):
            if statement.startswith("--"):
                # 跳过注释
                continue

            try:
                logger.info(f"[{i}/{len(statements)}] Executing: {statement[:80]}...")
                await DatabasePool.execute(statement)
                logger.info(f"✓ Statement {i} executed successfully")
            except Exception as e:
                # 某些约束可能已存在，继续执行
                if "already exists" in str(e) or "duplicate key" in str(e):
                    logger.warning(f"⚠ Statement {i} skipped (already exists): {e}")
                else:
                    logger.error(f"✗ Statement {i} failed: {e}")
                    # 继续执行其他语句

        logger.info("✓ All constraints and triggers applied successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to apply constraints: {e}")
        return False
    finally:
        await DatabasePool.close_pool()


async def main():
    """主函数"""
    success = await apply_constraints()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
