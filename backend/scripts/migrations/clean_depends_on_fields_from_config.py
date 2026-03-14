"""
清理 factor_data_config 中不再需要的 depends_on 表字段配置

架构优化后，depends_on 表的字段会自动可用，无需在 factor_data_config 中配置。
此脚本删除以下字段的配置：
- adj_factor (来自 sync_adj_factor)
- market_cap (来自 sync_daily_basic)
- 以及其他所有来自 sync_daily_data、sync_adj_factor、sync_daily_basic、sync_moneyflow 的字段

保留的字段（需要特殊处理）：
- list_date (来自 sync_stock_basic，用于过滤新股)
- is_st, is_suspend, is_limit (需要特殊计算或配置)
- industry_l1, industry_l2 (需要跨表关联)
"""

import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from store.dolphindb_client import db_client
from app.core.logger import logger


# 需要删除的字段（来自 depends_on 表，不再需要配置）
FIELDS_TO_REMOVE = [
    'adj_factor',      # sync_adj_factor.adj_factor
    'market_cap',      # sync_daily_basic.total_mv
    # 可以根据实际情况添加更多字段
]

# 需要保留的字段（需要特殊处理）
FIELDS_TO_KEEP = [
    'list_date',       # sync_stock_basic.list_date (过滤新股)
    'is_st',           # 需要特殊计算
    'is_suspend',      # 需要特殊计算
    'is_limit',        # 需要特殊计算
    'industry_l1',     # 需要跨表关联
    'industry_l2',     # 需要跨表关联
]


def main():
    """执行清理"""
    print("=" * 60)
    print("清理 factor_data_config 中的 depends_on 表字段配置")
    print("=" * 60)

    # 1. 查看当前配置
    print("\n1. 查看当前配置...")
    try:
        df = db_client.query("SELECT field_key, table_name, column_name FROM factor_data_config ORDER BY field_key")
        if df.is_empty():
            print("   factor_data_config 表为空")
            return

        print(f"   当前共有 {len(df)} 条配置:")
        for row in df.to_dicts():
            field_key = row['field_key']
            table_name = row.get('table_name', '')
            column_name = row.get('column_name', '')
            status = "保留" if field_key in FIELDS_TO_KEEP else ("删除" if field_key in FIELDS_TO_REMOVE else "检查")
            print(f"   - {field_key}: {table_name}.{column_name} [{status}]")
    except Exception as e:
        logger.error(f"查询配置失败: {e}")
        return

    # 2. 确认删除
    print(f"\n2. 准备删除以下字段配置:")
    for field in FIELDS_TO_REMOVE:
        print(f"   - {field}")

    confirm = input("\n是否继续? (yes/no): ").strip().lower()
    if confirm not in ['yes', 'y']:
        print("取消操作")
        return

    # 3. 执行删除
    print("\n3. 执行删除...")
    deleted_count = 0
    for field_key in FIELDS_TO_REMOVE:
        try:
            db_client.execute(
                "DELETE FROM factor_data_config WHERE field_key = %s",
                (field_key,)
            )
            print(f"   ✓ 已删除: {field_key}")
            deleted_count += 1
        except Exception as e:
            logger.error(f"删除 {field_key} 失败: {e}")

    # 4. 验证结果
    print(f"\n4. 验证结果...")
    try:
        df = db_client.query("SELECT field_key, table_name, column_name FROM factor_data_config ORDER BY field_key")
        print(f"   清理后剩余 {len(df)} 条配置:")
        for row in df.to_dicts():
            field_key = row['field_key']
            table_name = row.get('table_name', '')
            column_name = row.get('column_name', '')
            print(f"   - {field_key}: {table_name}.{column_name}")
    except Exception as e:
        logger.error(f"验证失败: {e}")

    print(f"\n✓ 清理完成！共删除 {deleted_count} 条配置")
    print("\n说明:")
    print("- depends_on 表的字段现在会自动可用，无需配置")
    print("- factor_data_config 只用于配置需要特殊处理的字段")
    print("- 如需恢复，可以在数据配置页面重新添加")


if __name__ == "__main__":
    main()
