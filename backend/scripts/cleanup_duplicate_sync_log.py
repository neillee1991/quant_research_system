"""
清理 sync_log 表中的重复记录

问题：
- sync_log 表应该是维度表，每个任务只保留一条最新记录
- 但实际存在多条记录，导致查询时返回旧数据

解决方案：
- 对于每个 (source, data_type) 组合，只保留 updated_at 最新的一条记录
- 删除其他旧记录

使用方法：
python scripts/cleanup_duplicate_sync_log.py
"""
import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from store.dolphindb_client import db_client
import polars as pl

def main():
    print("=== 清理 sync_log 表重复记录 ===\n")

    # 1. 查询所有记录
    all_logs = db_client.query(
        "SELECT source, data_type, last_date, updated_at "
        "FROM sync_log ORDER BY source, data_type, updated_at DESC"
    )

    print(f"当前记录总数: {len(all_logs)}")

    # 2. 找出重复的 (source, data_type) 组合
    duplicates = all_logs.group_by(["source", "data_type"]).agg(
        pl.len().alias("count")
    ).filter(pl.col("count") > 1)

    if duplicates.is_empty():
        print("✅ 没有发现重复记录")
        return

    print(f"\n发现 {len(duplicates)} 个任务有重复记录:")
    print(duplicates)

    # 3. 对于每个重复的任务，保留最新的记录
    print("\n开始清理...")

    logs_to_keep = []
    total_deleted = 0

    for row in duplicates.to_dicts():
        source = row["source"]
        data_type = row["data_type"]
        count = row["count"]

        # 获取该任务的所有记录
        task_logs = all_logs.filter(
            (pl.col("source") == source) & (pl.col("data_type") == data_type)
        ).sort("updated_at", descending=True)

        print(f"\n处理 {source}/{data_type}:")
        print(f"  - 共有 {len(task_logs)} 条记录")

        # 保留第一条（最新的）
        keep = task_logs[0]
        print(f"  - 保留: updated_at={keep['updated_at'][0]}")
        logs_to_keep.append(keep.to_dicts()[0])

        # 显示要删除的
        for i in range(1, len(task_logs)):
            remove = task_logs[i]
            updated_at = remove["updated_at"][0]
            print(f"  - 删除: updated_at={updated_at}")
            total_deleted += 1

    # 4. 批量删除重复记录
    print(f"\n执行批量删除（共 {total_deleted} 条旧记录）...")
    for row in duplicates.to_dicts():
        source = row["source"]
        data_type = row["data_type"]

        # 删除该任务的所有记录
        delete_sql = (
            f"delete from loadTable('dfs://quant', 'sync_log') "
            f"where source = '{source}' and data_type = '{data_type}'"
        )
        try:
            db_client.session.run(delete_sql)
            print(f"  ✓ 已删除 {source}/{data_type} 的所有记录")
        except Exception as e:
            print(f"  ✗ 删除 {source}/{data_type} 失败: {e}")

    # 5. 重新插入保留的记录
    print("\n重新插入保留的记录...")
    if logs_to_keep:
        keep_df = pl.DataFrame(logs_to_keep)
        pdf = keep_df.to_pandas()

        db_client.session.upload({"tmp_keep": pdf})
        db_client.session.run(
            "handle = loadTable('dfs://quant', 'sync_log');"
            "tableInsert(handle, tmp_keep);"
            "undef('tmp_keep')"
        )
        print(f"  ✓ 已重新插入 {len(logs_to_keep)} 条记录")

    # 6. 验证结果
    print("\n=== 清理后的记录 ===")
    final_logs = db_client.query(
        "SELECT source, data_type, last_date, updated_at "
        "FROM sync_log ORDER BY source, data_type"
    )
    print(f"总记录数: {len(final_logs)}")

    # 检查是否还有重复
    final_duplicates = final_logs.group_by(["source", "data_type"]).agg(
        pl.len().alias("count")
    ).filter(pl.col("count") > 1)

    if final_duplicates.is_empty():
        print("\n✅ 清理完成，所有任务现在都只有一条记录")
    else:
        print(f"\n⚠️  仍有 {len(final_duplicates)} 个任务有重复记录，需要手动处理")
        print(final_duplicates)

if __name__ == "__main__":
    main()
