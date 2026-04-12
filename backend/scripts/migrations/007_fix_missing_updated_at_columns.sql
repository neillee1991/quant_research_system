-- Migration 007: 为缺少 updated_at 列的表添加该列
-- 修复触发器报错 "record 'new' has no field 'updated_at'"

-- 为 task_runs 表添加 updated_at 列
ALTER TABLE task_runs
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- 为 flow_runs 表添加 updated_at 列（如果缺少）
ALTER TABLE flow_runs
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- 为 etl_task_configs 表添加 schema_json 和 source_tables 列（如果缺少）
ALTER TABLE etl_task_configs
  ADD COLUMN IF NOT EXISTS schema_json TEXT DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS source_tables TEXT DEFAULT '[]';

-- 为 sync_task_configs 表添加 source 列（如果缺少）
ALTER TABLE sync_task_configs
  ADD COLUMN IF NOT EXISTS source VARCHAR(255) DEFAULT 'tushare';

-- 为 factor_field_mappings 表添加 created_at 列（如果缺少）
ALTER TABLE factor_field_mappings
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

-- 验证所有表都有 updated_at 列
SELECT
  table_name,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND column_name = 'updated_at'
ORDER BY table_name;
