#!/usr/bin/env python3
"""
将现有 Python 文件中的因子迁移到数据库
作为系统初始化时的示例因子
"""
import sys
import os
import json
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import polars as pl
from store.dolphindb_client import db_client
from engine.production.registry import discover_factors, get_registry


def read_factor_code(factor_id: str, factors_dir: str) -> str:
    """从 Python 文件中读取因子代码"""
    for fname in os.listdir(factors_dir):
        if not fname.endswith(".py") or fname.startswith("__"):
            continue

        fpath = os.path.join(factors_dir, fname)
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                content = f.read()

            # 检查文件中是否包含该因子ID
            if f'"{factor_id}"' in content or f"'{factor_id}'" in content:
                return content
        except Exception:
            continue

    return ""


def migrate_factors():
    """迁移因子到数据库"""
    print("=" * 60)
    print("开始迁移因子到数据库")
    print("=" * 60)

    # 1. 从 Python 文件加载因子定义
    factors_dir = os.path.join(os.path.dirname(__file__), '..', 'engine', 'production', 'factors')
    factors_dir = os.path.normpath(factors_dir)

    print(f"\n1. 从 {factors_dir} 加载因子定义...")
    discover_factors(factors_dir=factors_dir)

    registry = get_registry()
    print(f"   找到 {len(registry)} 个因子")

    if not registry:
        print("   没有找到因子，退出")
        return

    # 2. 准备数据
    print("\n2. 准备迁移数据...")
    rows = []
    now = datetime.now()

    for factor_id, definition in registry.items():
        print(f"   处理因子: {factor_id}")

        # 读取因子代码
        code = read_factor_code(factor_id, factors_dir)
        if not code:
            print(f"     警告: 未找到因子 {factor_id} 的代码文件")
            continue

        rows.append({
            "factor_id": factor_id,
            "description": definition.description or "",
            "category": definition.category or "custom",
            "compute_mode": definition.compute_mode or "incremental",
            "storage_target": definition.storage.target or "factor_values",
            "depends_on": json.dumps(definition.depends_on or []),
            "params": json.dumps(definition.params or {}),
            "code": code,
            "last_computed_date": "",
            "last_computed_at": None,
            "created_at": now,
            "updated_at": now,
        })

    if not rows:
        print("   没有可迁移的因子")
        return

    # 3. 写入数据库
    print(f"\n3. 写入 {len(rows)} 个因子到数据库...")
    df = pl.DataFrame(rows)

    # 确保 last_computed_at 是正确的 TIMESTAMP 类型
    df = df.with_columns([
        pl.col("last_computed_at").cast(pl.Datetime("ns"))
    ])

    try:
        db_client.upsert("factor_metadata", df, ["factor_id"])
        print("   ✓ 迁移成功")
    except Exception as e:
        print(f"   ✗ 迁移失败: {e}")
        raise

    # 4. 验证
    print("\n4. 验证迁移结果...")
    verify_df = db_client.query("""
        SELECT factor_id, description, category, compute_mode,
               strlen(code) as code_length
        FROM factor_metadata
        WHERE code IS NOT NULL AND code != ''
        ORDER BY factor_id
    """)

    if not verify_df.is_empty():
        print(f"   数据库中共有 {len(verify_df)} 个因子:")
        for row in verify_df.to_dicts():
            print(f"     - {row['factor_id']:30s} [{row['category']:10s}] {row['description'][:40]} (代码: {row['code_length']} 字符)")
    else:
        print("   ✗ 验证失败: 数据库中没有因子")

    print("\n" + "=" * 60)
    print("迁移完成")
    print("=" * 60)


if __name__ == "__main__":
    migrate_factors()
