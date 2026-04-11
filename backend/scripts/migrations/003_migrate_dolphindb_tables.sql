-- Migration 003: Rename scheduler tables to plural + add fields + create config tables
-- Run ONCE before deploying new code.
-- Prerequisites: 001 and 002 must have been applied.

-- ============================================================
-- Part 1: Rename existing scheduler tables to plural form
-- ============================================================

ALTER TABLE flow_config RENAME TO flow_configs;
ALTER TABLE flow_run    RENAME TO flow_runs;
ALTER TABLE task_run    RENAME TO task_runs;

ALTER SEQUENCE flow_config_id_seq RENAME TO flow_configs_id_seq;
ALTER SEQUENCE flow_run_id_seq    RENAME TO flow_runs_id_seq;
ALTER SEQUENCE task_run_id_seq    RENAME TO task_runs_id_seq;

-- Update self-referencing FK on flow_runs
ALTER TABLE flow_runs
  DROP CONSTRAINT IF EXISTS flow_run_parent_flow_run_id_fkey,
  ADD CONSTRAINT flow_runs_parent_flow_run_id_fkey
    FOREIGN KEY (parent_flow_run_id) REFERENCES flow_runs(id);

-- Update FK on task_runs
ALTER TABLE task_runs
  DROP CONSTRAINT IF EXISTS task_run_flow_run_id_fkey,
  ADD CONSTRAINT task_runs_flow_run_id_fkey
    FOREIGN KEY (flow_run_id) REFERENCES flow_runs(id) ON DELETE CASCADE;

-- Rename indexes
ALTER INDEX IF EXISTS idx_flow_config_enabled    RENAME TO idx_flow_configs_enabled;
ALTER INDEX IF EXISTS idx_flow_config_updated_at RENAME TO idx_flow_configs_updated_at;
ALTER INDEX IF EXISTS idx_flow_run_flow_name     RENAME TO idx_flow_runs_flow_name;
ALTER INDEX IF EXISTS idx_flow_run_status        RENAME TO idx_flow_runs_status;
ALTER INDEX IF EXISTS idx_flow_run_created_at    RENAME TO idx_flow_runs_created_at;
ALTER INDEX IF EXISTS idx_task_run_flow_run_id   RENAME TO idx_task_runs_flow_run_id;
ALTER INDEX IF EXISTS idx_task_run_status        RENAME TO idx_task_runs_status;

-- ============================================================
-- Part 2: Extend task_runs with monitoring fields (merge DolphinDB task_runs)
-- ============================================================

ALTER TABLE task_runs
  ADD COLUMN IF NOT EXISTS run_id      VARCHAR(255),
  ADD COLUMN IF NOT EXISTS task_name   TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS rows        INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS elapsed_sec FLOAT,
  ADD COLUMN IF NOT EXISTS params      TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS extra       TEXT DEFAULT '',
  ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

-- target_date was added in migration 002, this is idempotent
ALTER TABLE task_runs
  ADD COLUMN IF NOT EXISTS target_date VARCHAR(8);

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_run_id
  ON task_runs(run_id) WHERE run_id IS NOT NULL;

-- ============================================================
-- Part 3: Config tables (migrated from DolphinDB)
-- ============================================================

CREATE TABLE IF NOT EXISTS sync_task_configs (
  task_id           VARCHAR(255) PRIMARY KEY,
  api_name          VARCHAR(255) NOT NULL DEFAULT '',
  description       TEXT         DEFAULT '',
  sync_type         VARCHAR(50)  NOT NULL DEFAULT 'incremental',
  params_json       TEXT         DEFAULT '{}',
  date_field        VARCHAR(100) DEFAULT '',
  primary_keys_json TEXT         DEFAULT '[]',
  table_name        VARCHAR(255) NOT NULL DEFAULT '',
  schema_json       TEXT         DEFAULT '{}',
  enabled           BOOLEAN      DEFAULT TRUE,
  api_limit         INT          DEFAULT 5000,
  created_at        TIMESTAMPTZ  DEFAULT NOW(),
  updated_at        TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sync_task_configs_enabled    ON sync_task_configs(enabled);
CREATE INDEX IF NOT EXISTS idx_sync_task_configs_table_name ON sync_task_configs(table_name);

CREATE TABLE IF NOT EXISTS etl_task_configs (
  task_id           VARCHAR(255) PRIMARY KEY,
  description       TEXT         DEFAULT '',
  script            TEXT         DEFAULT '',
  sync_type         VARCHAR(50)  DEFAULT 'incremental',
  date_field        VARCHAR(100) DEFAULT '',
  primary_keys_json TEXT         DEFAULT '[]',
  table_name        VARCHAR(255) DEFAULT '',
  enabled           BOOLEAN      DEFAULT TRUE,
  created_at        TIMESTAMPTZ  DEFAULT NOW(),
  updated_at        TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_etl_task_configs_enabled ON etl_task_configs(enabled);

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
CREATE INDEX IF NOT EXISTS idx_factor_configs_enabled  ON factor_configs(enabled);
CREATE INDEX IF NOT EXISTS idx_factor_configs_category ON factor_configs(category);

CREATE TABLE IF NOT EXISTS factor_field_mappings (
  field_key    VARCHAR(255) PRIMARY KEY,
  description  TEXT         DEFAULT '',
  table_name   VARCHAR(255) DEFAULT '',
  column_name  VARCHAR(255) DEFAULT '',
  extra_config TEXT         DEFAULT '{}',
  updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- Part 4: Reference data tables (migrated from DolphinDB)
-- ============================================================

CREATE TABLE IF NOT EXISTS stocks (
  ts_code     VARCHAR(20) PRIMARY KEY,
  symbol      VARCHAR(20)  DEFAULT '',
  name        VARCHAR(100) DEFAULT '',
  area        VARCHAR(100) DEFAULT '',
  industry    VARCHAR(100) DEFAULT '',
  market      VARCHAR(50)  DEFAULT '',
  list_date   DATE,
  list_status VARCHAR(10)  DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_stocks_list_status ON stocks(list_status);
CREATE INDEX IF NOT EXISTS idx_stocks_industry    ON stocks(industry);

CREATE TABLE IF NOT EXISTS trading_calendar (
  exchange      VARCHAR(20) NOT NULL,
  cal_date      DATE        NOT NULL,
  is_open       SMALLINT    DEFAULT 0,
  pretrade_date DATE,
  PRIMARY KEY (exchange, cal_date)
);
CREATE INDEX IF NOT EXISTS idx_trading_calendar_open
  ON trading_calendar(exchange, is_open, cal_date);

CREATE TABLE IF NOT EXISTS index_configs (
  index_code  VARCHAR(50) PRIMARY KEY,
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
-- Part 5: Results tables (migrated from DolphinDB)
-- ============================================================

-- Merges factor_analysis + factor_analysis_extended
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
  UNIQUE (factor_id, analysis_date)
);
CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_factor_id
  ON factor_analysis_results(factor_id);
CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_date
  ON factor_analysis_results(analysis_date DESC);

CREATE TABLE IF NOT EXISTS backtest_results (
  run_id            VARCHAR(255) PRIMARY KEY,
  task_id           VARCHAR(255) DEFAULT '',
  task_name         TEXT         DEFAULT '',
  metrics_json      TEXT         DEFAULT '{}',
  equity_curve_json TEXT         DEFAULT '[]',
  trades_json       TEXT         DEFAULT '[]',
  created_at        TIMESTAMPTZ  DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_backtest_results_task_id
  ON backtest_results(task_id);
CREATE INDEX IF NOT EXISTS idx_backtest_results_created_at
  ON backtest_results(created_at DESC);
