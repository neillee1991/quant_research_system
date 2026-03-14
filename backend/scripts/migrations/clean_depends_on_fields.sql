/*
清理 factor_data_config 中不再需要的 depends_on 表字段配置

架构优化说明：
1. depends_on 表的字段会自动可用，无需在 factor_data_config 中配置
2. factor_data_config 只用于配置需要特殊处理的字段

需要删除的字段（来自 depends_on 表）：
- adj_factor (来自 sync_adj_factor)
- market_cap (来自 sync_daily_basic)

需要保留的字段（需要特殊处理）：
- list_date (来自 sync_stock_basic，用于过滤新股)
- is_st, is_suspend, is_limit (需要特殊计算)
- industry_l1, industry_l2 (需要跨表关联)
*/

-- 查看当前配置
SELECT field_key, table_name, column_name, description
FROM factor_data_config
ORDER BY field_key;

-- 删除不再需要的字段配置
DELETE FROM factor_data_config WHERE field_key = 'adj_factor';
DELETE FROM factor_data_config WHERE field_key = 'market_cap';

-- 验证结果
SELECT field_key, table_name, column_name, description
FROM factor_data_config
ORDER BY field_key;
