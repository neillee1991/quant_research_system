-- 010: 创建迁移版本追踪表 + 将配置表 JSON 字段从 TEXT 改为 JSONB

-- 1. 创建迁移版本追踪表
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(20) PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);

-- 补录已执行的历史迁移版本
INSERT INTO schema_migrations (version) VALUES
    ('001'), ('002'), ('003'), ('004'), ('005'),
    ('006'), ('007'), ('008'), ('009')
ON CONFLICT (version) DO NOTHING;

-- 2. sync_task_configs: TEXT -> JSONB（保留原字段名，兼容现有代码）
ALTER TABLE sync_task_configs
    ALTER COLUMN params_json TYPE JSONB USING params_json::jsonb,
    ALTER COLUMN primary_keys_json TYPE JSONB USING primary_keys_json::jsonb,
    ALTER COLUMN schema_json TYPE JSONB USING schema_json::jsonb;

-- 3. etl_task_configs: TEXT -> JSONB
ALTER TABLE etl_task_configs
    ALTER COLUMN params_json TYPE JSONB USING params_json::jsonb;

-- 4. 记录本次迁移
INSERT INTO schema_migrations (version) VALUES ('010')
ON CONFLICT (version) DO NOTHING;
