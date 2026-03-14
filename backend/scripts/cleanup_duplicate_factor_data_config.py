"""
清理 factor_data_config 表中的重复配置

问题：
- factor_data_config 表中同一个 field_key 有多个配置
- 导致前端页面显示重复、数据加载混乱

解决方案：
1. 保留 ETL 数据源（etl_stock_daily_info）的配置
2. 删除旧的原始数据源配置
3. 为 field_key 添加唯一约束（需要修改表结构）

使用方法：
python scripts/cleanup_duplicate_factor_data_config.py
"""
import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from store.dolphindb_client import db_client
import polars as pl
from datetime import datetime

def main():
    print("=== 清理 factor_data_config 重复配置 ===\n")

    # 1. 查询所有配置
    all_configs = db_client.query(
        "SELECT field_key, description, table_name, column_name, extra_config, updated_at "
        "FROM factor_data_config ORDER BY field_key, updated_at DESC"
    )

    print(f"当前配置总数: {len(all_configs)}")

    # 2. 找出重复的 field_key
    duplicates = all_configs.group_by("field_key").agg(pl.count().alias("count")).filter(pl.col("count") > 1)

    if duplicates.is_empty():
        print("✅ 没有发现重复配置")
        return

    print(f"\n发现 {len(duplicates)} 个重复的 field_key:")
    print(duplicates)

    # 3. 策略：删除所有重复的，然后重新插入保留的配置
    print("\n开始清理...")

    # 收集要保留的配置
    configs_to_keep = []

    for field_key in duplicates["field_key"].to_list():
        # 获取该 field_key 的所有配置
        configs = all_configs.filter(pl.col("field_key") == field_key).sort("updated_at", descending=True)

        print(f"\n处理 {field_key}:")
        print(f"  - 共有 {len(configs)} 个配置")

        # 保留第一个（最新的）
        keep = configs[0]
        print(f"  - 保留: table_name={keep['table_name'][0]}, updated_at={keep['updated_at'][0]}")
        configs_to_keep.append(keep.to_dicts()[0])

        # 显示要删除的
        for i in range(1, len(configs)):
            remove = configs[i]
            table_name = remove["table_name"][0]
            updated_at = remove["updated_at"][0]
            print(f"  - 删除: table_name={table_name}, updated_at={updated_at}")

    # 4. 批量删除所有重复的 field_key
    print("\n执行批量删除...")
    for field_key in duplicates["field_key"].to_list():
        delete_sql = (
            f"delete from loadTable('dfs://quant', 'factor_data_config') "
            f"where field_key = '{field_key}'"
        )
        try:
            db_client.session.run(delete_sql)
            print(f"  ✓ 已删除所有 {field_key} 配置")
        except Exception as e:
            print(f"  ✗ 删除 {field_key} 失败: {e}")

    # 5. 重新插入保留的配置
    print("\n重新插入保留的配置...")
    if configs_to_keep:
        keep_df = pl.DataFrame(configs_to_keep)
        pdf = keep_df.to_pandas()

        db_client.session.upload({"tmp_keep": pdf})
        db_client.session.run(
            "handle = loadTable('dfs://quant', 'factor_data_config');"
            "tableInsert(handle, tmp_keep);"
            "undef('tmp_keep')"
        )
        print(f"  ✓ 已重新插入 {len(configs_to_keep)} 条配置")

    # 4. 验证结果
    print("\n=== 清理后的配置 ===")
    final_configs = db_client.query(
        "SELECT field_key, description, table_name, column_name "
        "FROM factor_data_config ORDER BY field_key"
    )
    print(final_configs)

    # 检查是否还有重复
    final_duplicates = final_configs.group_by("field_key").agg(pl.count().alias("count")).filter(pl.col("count") > 1)

    if final_duplicates.is_empty():
        print("\n✅ 清理完成，所有 field_key 现在都是唯一的")
    else:
        print(f"\n⚠️  仍有 {len(final_duplicates)} 个重复的 field_key，需要手动处理")
        print(final_duplicates)

if __name__ == "__main__":
    main()
