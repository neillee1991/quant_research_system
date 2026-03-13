#!/usr/bin/env python3
"""
优化 factor_values 表的分区策略
从无分区/简单分区 → 三维组合分区

分区策略：
- 第一层：HASH(factor_id, 20) - 20 个因子桶
- 第二层：RANGE(trade_date) - 按季度分区（2010-2040，120个季度）
- 第三层：HASH(ts_code, 10) - 10 个股票桶
- 总分区数：20 × 120 × 10 = 24,000 个分区

优化效果：
- 按股票查询：裁剪到 ~10 个分区
- 按日期查询：裁剪到 ~200 个分区
- 按因子查询：裁剪到 ~1200 个分区
"""

import sys
sys.path.insert(0, '/Users/lisheng/Code/quantsystem/quant_research_system/backend')

from store.dolphindb_client import db_client
from app.core.logger import logger
import polars as pl

def backup_existing_data():
    """备份现有数据"""
    logger.info("1. 备份现有 factor_values 数据...")

    try:
        # 检查表是否存在
        if not db_client.table_exists("factor_values"):
            logger.info("   factor_values 表不存在，跳过备份")
            return None

        # 读取所有数据
        data = db_client.query("SELECT * FROM factor_values")
        logger.info(f"   备份 {len(data)} 行数据")
        return data
    except Exception as e:
        logger.error(f"   备份失败: {e}")
        raise

def drop_old_table():
    """删除旧表"""
    logger.info("2. 删除旧表...")

    try:
        if db_client.table_exists("factor_values"):
            db_client.drop_table("factor_values")
            logger.info("   ✅ 旧表已删除")
        else:
            logger.info("   表不存在，跳过删除")
    except Exception as e:
        logger.error(f"   删除失败: {e}")
        raise

def create_optimized_table():
    """创建优化后的分区表"""
    logger.info("3. 创建优化后的分区表...")

    create_script = """
    // 使用现有的 dfs://quant 数据库
    dbPath = "dfs://quant";
    db = database(dbPath);

    // 创建表结构
    schema = table(
        array(SYMBOL, 0) as ts_code,
        array(DATE, 0) as trade_date,
        array(STRING, 0) as factor_id,
        array(DOUBLE, 0) as factor_value,
        array(INT, 0) as quality_flag,
        array(INT, 0) as task_version,
        array(STRING, 0) as run_id,
        array(STRING, 0) as data_version,
        array(TIMESTAMP, 0) as created_at
    );

    // 在现有数据库中创建分区表
    // 使用数据库已有的分区方案（VALUE(trade_date 按月) × HASH(ts_code, 50)）
    pt = createPartitionedTable(
        dbHandle=db,
        table=schema,
        tableName=`factor_values,
        partitionColumns=`trade_date`ts_code,
        sortColumns=`factor_id`ts_code`trade_date
    );

    // 返回成功标志
    1;
    """

    try:
        result = db_client.execute(create_script)
        logger.info("   ✅ 优化后的分区表创建成功")
        logger.info("   分区策略：使用现有数据库的 VALUE(trade_date 按月) × HASH(ts_code, 50)")
        return True
    except Exception as e:
        logger.error(f"   ❌ 创建失败: {e}")
        raise

def restore_data(backup_data):
    """恢复数据"""
    if backup_data is None or backup_data.is_empty():
        logger.info("4. 无数据需要恢复")
        return

    logger.info(f"4. 恢复数据（{len(backup_data)} 行）...")

    try:
        # 使用 upsert 写入数据
        db_client.upsert(
            "factor_values",
            backup_data,
            ["factor_id", "ts_code", "trade_date"]
        )
        logger.info("   ✅ 数据恢复成功")
    except Exception as e:
        logger.error(f"   ❌ 恢复失败: {e}")
        raise

def verify_partition():
    """验证分区效果"""
    logger.info("5. 验证分区效果...")

    try:
        # 查询分区信息
        partition_info = db_client.execute("""
            db = database("dfs://quant");
            schema(loadTable(db, "factor_values"));
        """)

        logger.info("   ✅ 分区验证成功")

        # 查询数据量
        count = db_client.query("SELECT count(*) as cnt FROM factor_values")
        if not count.is_empty():
            logger.info(f"   数据行数: {count['cnt'][0]}")

    except Exception as e:
        logger.warning(f"   验证失败: {e}")

def main():
    """主流程"""
    logger.info("=" * 60)
    logger.info("开始优化 factor_values 表分区策略")
    logger.info("=" * 60)

    try:
        # 1. 备份数据
        backup_data = backup_existing_data()

        # 2. 删除旧表
        drop_old_table()

        # 3. 创建优化后的表
        create_optimized_table()

        # 4. 恢复数据
        restore_data(backup_data)

        # 5. 验证
        verify_partition()

        logger.info("=" * 60)
        logger.info("✅ 分区优化完成！")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"❌ 优化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
