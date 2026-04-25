-- PostgreSQL 建表脚本（最终 schema，幂等）
-- 合并自 migrations 001-010，可直接用于全新部署
-- 替代 init_postgres.py 中的分批迁移逻辑

-- ============================================================
-- 触发器函数：自动更新 updated_at
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 调度器核心表
-- ============================================================

CREATE TABLE IF NOT EXISTS flow_configs (
  id               SERIAL       PRIMARY KEY,
  name             VARCHAR(255) UNIQUE NOT NULL,
  description      TEXT,
  cron             VARCHAR(100),
  timezone         VARCHAR(50)  DEFAULT 'Asia/Shanghai',
  tags             JSONB        DEFAULT '[]',
  tasks            JSONB        NOT NULL,
  date_offset_days INTEGER      DEFAULT 0,
  enabled          BOOLEAN      DEFAULT TRUE,
  version          INTEGER      DEFAULT 1,
  created_at       TIMESTAMPTZ  DEFAULT NOW(),
  updated_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flow_runs (
  id                 SERIAL       PRIMARY KEY,
  flow_name          VARCHAR(255) NOT NULL,
  parent_flow_run_id INTEGER      REFERENCES flow_runs(id),
  status             VARCHAR(20)  NOT NULL,
  trigger_type       VARCHAR(20)  NOT NULL,
  target_date        VARCHAR(8),
  scheduled_at       TIMESTAMPTZ,
  started_at         TIMESTAMPTZ,
  ended_at           TIMESTAMPTZ,
  error_message      TEXT,
  run_id             VARCHAR(64)  UNIQUE,
  created_at         TIMESTAMPTZ  DEFAULT NOW(),
  updated_at         TIMESTAMPTZ  DEFAULT NOW(),
  CONSTRAINT check_flow_runs_status
    CHECK (status IN ('pending', 'running', 'success', 'failed', 'cancelled')),
  CONSTRAINT fk_flow_runs_flow_name
    FOREIGN KEY (flow_name) REFERENCES flow_configs(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_runs (
  id            SERIAL       PRIMARY KEY,
  flow_run_id   INTEGER      REFERENCES flow_runs(id) ON DELETE CASCADE,
  task_id       VARCHAR(255) NOT NULL,
  task_type     VARCHAR(20)  NOT NULL,
  status        VARCHAR(20)  NOT NULL,
  started_at    TIMESTAMPTZ,
  ended_at      TIMESTAMPTZ,
  error_message TEXT,
  target_date   VARCHAR(8),
  run_id        VARCHAR(255),
  task_name     TEXT         DEFAULT '',
  rows          INT          DEFAULT 0,
  elapsed_sec   FLOAT,
  params        TEXT         DEFAULT '',
  extra         TEXT         DEFAULT '',
  finished_at   TIMESTAMPTZ,
  created_at    TIMESTAMPTZ  DEFAULT NOW(),
  updated_at    TIMESTAMPTZ  DEFAULT NOW(),
  CONSTRAINT check_task_runs_status
    CHECK (status IN ('pending', 'running', 'success', 'failed')),
  CONSTRAINT check_task_runs_task_type
    CHECK (task_type IN ('sync', 'etl', 'factor', 'flow'))
);

-- ============================================================
-- 配置表
-- ============================================================

CREATE TABLE IF NOT EXISTS sync_task_configs (
  task_id        VARCHAR(255) PRIMARY KEY,
  api_name       VARCHAR(255) NOT NULL DEFAULT '',
  description    TEXT         DEFAULT '',
  sync_type      VARCHAR(50)  NOT NULL DEFAULT 'incremental',
  params         JSONB        DEFAULT '{}',
  date_field     VARCHAR(100) DEFAULT '',
  primary_keys   JSONB        DEFAULT '[]',
  table_name     VARCHAR(255) NOT NULL DEFAULT '',
  schema         JSONB        DEFAULT '{}',
  column_mapping JSONB        DEFAULT NULL,
  enabled        BOOLEAN      DEFAULT TRUE,
  api_limit      INT          DEFAULT 5000,
  source         VARCHAR(255) DEFAULT 'tushare',
  created_at     TIMESTAMPTZ  DEFAULT NOW(),
  updated_at     TIMESTAMPTZ  DEFAULT NOW(),
  CONSTRAINT check_sync_task_configs_sync_type
    CHECK (sync_type IN ('incremental', 'full'))
);

CREATE TABLE IF NOT EXISTS etl_task_configs (
  task_id      VARCHAR(255) PRIMARY KEY,
  description  TEXT         DEFAULT '',
  script       TEXT         DEFAULT '',
  sync_type    VARCHAR(50)  DEFAULT 'incremental',
  date_field   VARCHAR(100) DEFAULT '',
  primary_keys JSONB        DEFAULT '[]',
  table_name   VARCHAR(255) DEFAULT '',
  schema       JSONB        DEFAULT '{}',
  source_tables TEXT        DEFAULT '[]',
  params       JSONB        DEFAULT '{}',
  enabled      BOOLEAN      DEFAULT TRUE,
  created_at   TIMESTAMPTZ  DEFAULT NOW(),
  updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS factor_configs (
  factor_id      VARCHAR(255) PRIMARY KEY,
  description    TEXT         DEFAULT '',
  category       VARCHAR(100) DEFAULT 'custom',
  compute_mode   VARCHAR(50)  DEFAULT 'incremental',
  storage_target VARCHAR(255) DEFAULT 'factor_values',
  depends_on     TEXT         DEFAULT '[]',
  params         TEXT         DEFAULT '{}',
  code           TEXT         DEFAULT '',
  enabled        BOOLEAN      DEFAULT TRUE,
  align_calendar BOOLEAN      DEFAULT FALSE,
  created_at     TIMESTAMPTZ  DEFAULT NOW(),
  updated_at     TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS factor_field_mappings (
  field_key    VARCHAR(255) PRIMARY KEY,
  description  TEXT         DEFAULT '',
  table_name   VARCHAR(255) DEFAULT '',
  column_name  VARCHAR(255) DEFAULT '',
  extra_config TEXT         DEFAULT '{}',
  created_at   TIMESTAMPTZ  DEFAULT NOW(),
  updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- 参考数据表
-- ============================================================

CREATE TABLE IF NOT EXISTS index_configs (
  index_code  VARCHAR(50)  PRIMARY KEY,
  index_name  VARCHAR(255) DEFAULT '',
  description TEXT         DEFAULT '',
  stock_count INT          DEFAULT 0,
  latest_date DATE,
  created_at  TIMESTAMPTZ  DEFAULT NOW(),
  updated_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_preferences (
  user_id       VARCHAR(255) PRIMARY KEY,
  index_table   VARCHAR(255) DEFAULT '',
  filter_config TEXT         DEFAULT '{}',
  created_at    TIMESTAMPTZ  DEFAULT NOW(),
  updated_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- 结果表
-- ============================================================

CREATE TABLE IF NOT EXISTS factor_analysis_results (
  id            BIGSERIAL    PRIMARY KEY,
  factor_id     VARCHAR(255) NOT NULL,
  analysis_date TIMESTAMPTZ  NOT NULL,
  start_date    VARCHAR(8)   DEFAULT '',
  end_date      VARCHAR(8)   DEFAULT '',
  periods       TEXT         DEFAULT '[]',
  ic_mean       FLOAT,
  ic_std        FLOAT,
  rank_ic_mean  FLOAT,
  rank_ic_std   FLOAT,
  ic_ir         FLOAT,
  turnover_mean FLOAT,
  ic_summary    TEXT         DEFAULT '{}',
  ic_by_period  TEXT         DEFAULT '{}',
  config        TEXT         DEFAULT '{}',
  report_path   TEXT         DEFAULT '',
  task_status   VARCHAR(20)  DEFAULT '',
  task_id       VARCHAR(255) DEFAULT '',
  error_message TEXT         DEFAULT '',
  created_at    TIMESTAMPTZ  DEFAULT NOW(),
  UNIQUE (factor_id, analysis_date),
  CONSTRAINT fk_factor_analysis_results_factor_id
    FOREIGN KEY (factor_id) REFERENCES factor_configs(factor_id) ON DELETE CASCADE
);

-- ============================================================
-- 迁移版本追踪表
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_migrations (
  version    VARCHAR(20) PRIMARY KEY,
  applied_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 索引
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_flow_configs_enabled        ON flow_configs(enabled);
CREATE INDEX IF NOT EXISTS idx_flow_configs_updated_at     ON flow_configs(updated_at);

CREATE INDEX IF NOT EXISTS idx_flow_runs_flow_name         ON flow_runs(flow_name);
CREATE INDEX IF NOT EXISTS idx_flow_runs_status            ON flow_runs(status);
CREATE INDEX IF NOT EXISTS idx_flow_runs_created_at        ON flow_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_flow_runs_parent_flow_run_id ON flow_runs(parent_flow_run_id);
CREATE INDEX IF NOT EXISTS idx_flow_runs_run_id            ON flow_runs(run_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_run_id     ON task_runs(run_id) WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_runs_flow_run_id       ON task_runs(flow_run_id);
CREATE INDEX IF NOT EXISTS idx_task_runs_status            ON task_runs(status);
CREATE INDEX IF NOT EXISTS idx_task_runs_task_id           ON task_runs(task_id);

CREATE INDEX IF NOT EXISTS idx_sync_task_configs_enabled    ON sync_task_configs(enabled);
CREATE INDEX IF NOT EXISTS idx_sync_task_configs_table_name ON sync_task_configs(table_name);

CREATE INDEX IF NOT EXISTS idx_etl_task_configs_enabled    ON etl_task_configs(enabled);

CREATE INDEX IF NOT EXISTS idx_factor_configs_enabled      ON factor_configs(enabled);
CREATE INDEX IF NOT EXISTS idx_factor_configs_category     ON factor_configs(category);

CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_factor_id
  ON factor_analysis_results(factor_id);
CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_date
  ON factor_analysis_results(analysis_date DESC);

-- ============================================================
-- updated_at 自动更新触发器
-- ============================================================

DROP TRIGGER IF EXISTS update_flow_configs_updated_at ON flow_configs;
CREATE TRIGGER update_flow_configs_updated_at
  BEFORE UPDATE ON flow_configs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_flow_runs_updated_at ON flow_runs;
CREATE TRIGGER update_flow_runs_updated_at
  BEFORE UPDATE ON flow_runs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_task_runs_updated_at ON task_runs;
CREATE TRIGGER update_task_runs_updated_at
  BEFORE UPDATE ON task_runs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_sync_task_configs_updated_at ON sync_task_configs;
CREATE TRIGGER update_sync_task_configs_updated_at
  BEFORE UPDATE ON sync_task_configs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_etl_task_configs_updated_at ON etl_task_configs;
CREATE TRIGGER update_etl_task_configs_updated_at
  BEFORE UPDATE ON etl_task_configs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_factor_configs_updated_at ON factor_configs;
CREATE TRIGGER update_factor_configs_updated_at
  BEFORE UPDATE ON factor_configs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_factor_analysis_results_updated_at ON factor_analysis_results;
CREATE TRIGGER update_factor_analysis_results_updated_at
  BEFORE UPDATE ON factor_analysis_results
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
