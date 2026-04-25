-- 迁移脚本: 去除 _json 后缀列名
-- 适用于已存在的生产数据库
-- 幂等：使用 IF EXISTS，可重复执行

-- sync_task_configs
ALTER TABLE sync_task_configs RENAME COLUMN params_json       TO params;
ALTER TABLE sync_task_configs RENAME COLUMN primary_keys_json TO primary_keys;
ALTER TABLE sync_task_configs RENAME COLUMN schema_json       TO schema;

-- 补充 column_mapping 列（旧表可能没有此列）
ALTER TABLE sync_task_configs
  ADD COLUMN IF NOT EXISTS column_mapping JSONB DEFAULT NULL;

-- etl_task_configs
ALTER TABLE etl_task_configs RENAME COLUMN params_json       TO params;
ALTER TABLE etl_task_configs RENAME COLUMN primary_keys_json TO primary_keys;
ALTER TABLE etl_task_configs RENAME COLUMN schema_json       TO schema;

-- etl_task_configs: primary_keys / schema 从 TEXT 升级为 JSONB
ALTER TABLE etl_task_configs
  ALTER COLUMN primary_keys TYPE JSONB USING primary_keys::jsonb;
ALTER TABLE etl_task_configs
  ALTER COLUMN schema       TYPE JSONB USING schema::jsonb;
