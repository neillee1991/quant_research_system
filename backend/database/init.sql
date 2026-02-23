-- 量化研究系统数据库初始化脚本
-- PostgreSQL 16+
-- 只创建系统必需的表，业务表由 sync_config.json 动态创建

-- 创建扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- 设置时区
SET timezone = 'Asia/Shanghai';

-- 创建同步日志表
CREATE TABLE IF NOT EXISTS sync_log (
    source VARCHAR(100) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    last_date VARCHAR(8),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, data_type)
);

CREATE INDEX idx_sync_log_updated ON sync_log(updated_at DESC);

COMMENT ON TABLE sync_log IS '数据同步日志表';
COMMENT ON COLUMN sync_log.source IS '数据源';
COMMENT ON COLUMN sync_log.data_type IS '数据类型';
COMMENT ON COLUMN sync_log.last_date IS '最后同步日期 YYYYMMDD';
COMMENT ON COLUMN sync_log.updated_at IS '更新时间';

-- 创建同步日志历史表
CREATE TABLE IF NOT EXISTS sync_log_history (
    id SERIAL PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    data_type VARCHAR(100) NOT NULL,
    last_date VARCHAR(8),
    sync_date VARCHAR(8),
    rows_synced INTEGER,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sync_log_history_source ON sync_log_history(source, data_type);
CREATE INDEX idx_sync_log_history_date ON sync_log_history(sync_date DESC);
CREATE INDEX idx_sync_log_history_created ON sync_log_history(created_at DESC);

COMMENT ON TABLE sync_log_history IS '数据同步历史记录表';
COMMENT ON COLUMN sync_log_history.source IS '数据源';
COMMENT ON COLUMN sync_log_history.data_type IS '数据类型';
COMMENT ON COLUMN sync_log_history.last_date IS '最后同步日期 YYYYMMDD';
COMMENT ON COLUMN sync_log_history.sync_date IS '本次同步日期 YYYYMMDD';
COMMENT ON COLUMN sync_log_history.rows_synced IS '同步行数';
COMMENT ON COLUMN sync_log_history.status IS '同步状态';
COMMENT ON COLUMN sync_log_history.created_at IS '创建时间';

-- ==================== DAG 系统表 ====================

-- DAG 执行日志
CREATE TABLE IF NOT EXISTS dag_run_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(200) NOT NULL UNIQUE,
    status VARCHAR(20) DEFAULT 'pending',
    target_date VARCHAR(8),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    trigger_type VARCHAR(20) DEFAULT 'manual',
    run_type VARCHAR(20) DEFAULT 'today',
    backfill_id VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN dag_run_log.run_type IS '运行类型: today=当日, single=指定单日, backfill=周期回溯';
COMMENT ON COLUMN dag_run_log.backfill_id IS '回溯批次ID，同一次回溯操作共享';

CREATE INDEX idx_dag_run_dag_id ON dag_run_log(dag_id);
CREATE INDEX idx_dag_run_status ON dag_run_log(status);
CREATE INDEX idx_dag_run_created ON dag_run_log(created_at DESC);
CREATE INDEX idx_dag_run_type ON dag_run_log(run_type);
CREATE INDEX idx_dag_run_backfill ON dag_run_log(backfill_id);

-- DAG 任务执行日志
CREATE TABLE IF NOT EXISTS dag_task_log (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(200) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    task_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    rows_affected INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dag_task_run_id ON dag_task_log(run_id);
CREATE INDEX idx_dag_task_status ON dag_task_log(status);

-- ==================== 因子系统表 ====================

-- 因子元数据
CREATE TABLE IF NOT EXISTS factor_metadata (
    factor_id VARCHAR(100) PRIMARY KEY,
    description TEXT,
    category VARCHAR(50),
    compute_mode VARCHAR(20) DEFAULT 'incremental',
    storage_target VARCHAR(100) DEFAULT 'factor_values',
    params JSONB,
    last_computed_date VARCHAR(8),
    last_computed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 因子值主表（按月分区）
CREATE TABLE IF NOT EXISTS factor_values (
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    factor_id VARCHAR(100) NOT NULL,
    factor_value DOUBLE PRECISION,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ts_code, trade_date, factor_id)
) PARTITION BY RANGE (trade_date);

CREATE INDEX idx_fv_factor_date ON factor_values(factor_id, trade_date);
CREATE INDEX idx_fv_code_date ON factor_values(ts_code, trade_date);

-- 因子分析结果
CREATE TABLE IF NOT EXISTS factor_analysis (
    id SERIAL PRIMARY KEY,
    factor_id VARCHAR(100) NOT NULL,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    start_date VARCHAR(8),
    end_date VARCHAR(8),
    periods JSONB,
    ic_mean DOUBLE PRECISION,
    ic_std DOUBLE PRECISION,
    rank_ic_mean DOUBLE PRECISION,
    rank_ic_std DOUBLE PRECISION,
    ic_ir DOUBLE PRECISION,
    turnover_mean DOUBLE PRECISION,
    quantile_returns JSONB,
    ic_series JSONB
);

CREATE INDEX idx_fa_factor_id ON factor_analysis(factor_id);
CREATE INDEX idx_fa_date ON factor_analysis(analysis_date DESC);

-- 因子生产运行记录
CREATE TABLE IF NOT EXISTS production_task_run (
    id SERIAL PRIMARY KEY,
    factor_id VARCHAR(100) NOT NULL,
    mode VARCHAR(20),
    status VARCHAR(20) DEFAULT 'running',
    start_date VARCHAR(8),
    end_date VARCHAR(8),
    rows_affected INTEGER DEFAULT 0,
    duration_seconds DOUBLE PRECISION,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ptr_factor_id ON production_task_run(factor_id);
CREATE INDEX idx_ptr_created ON production_task_run(created_at DESC);

-- 创建股票基础信息表
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(20) PRIMARY KEY,
    symbol VARCHAR(10),
    name VARCHAR(100),
    area VARCHAR(50),
    industry VARCHAR(100),
    market VARCHAR(20),
    list_date VARCHAR(8)
);

CREATE INDEX idx_stock_basic_industry ON stock_basic(industry);
CREATE INDEX idx_stock_basic_market ON stock_basic(market);

COMMENT ON TABLE stock_basic IS '股票基础信息表';
COMMENT ON COLUMN stock_basic.ts_code IS '股票代码';
COMMENT ON COLUMN stock_basic.symbol IS '股票简称';
COMMENT ON COLUMN stock_basic.name IS '股票名称';
COMMENT ON COLUMN stock_basic.area IS '地域';
COMMENT ON COLUMN stock_basic.industry IS '行业';
COMMENT ON COLUMN stock_basic.market IS '市场类型';
COMMENT ON COLUMN stock_basic.list_date IS '上市日期';

-- 创建每日指标表
CREATE TABLE IF NOT EXISTS daily_basic (
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    close DOUBLE PRECISION,
    turnover_rate DOUBLE PRECISION,
    volume_ratio DOUBLE PRECISION,
    pe DOUBLE PRECISION,
    pb DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX idx_daily_basic_date ON daily_basic(trade_date DESC);
CREATE INDEX idx_daily_basic_code ON daily_basic(ts_code);

COMMENT ON TABLE daily_basic IS '每日指标表（市盈率、市净率等）';
COMMENT ON COLUMN daily_basic.ts_code IS '股票代码';
COMMENT ON COLUMN daily_basic.trade_date IS '交易日期';
COMMENT ON COLUMN daily_basic.close IS '收盘价';
COMMENT ON COLUMN daily_basic.turnover_rate IS '换手率(%)';
COMMENT ON COLUMN daily_basic.volume_ratio IS '量比';
COMMENT ON COLUMN daily_basic.pe IS '市盈率';
COMMENT ON COLUMN daily_basic.pb IS '市净率';

-- 创建复权因子表
CREATE TABLE IF NOT EXISTS adj_factor (
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    adj_factor DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX idx_adj_factor_date ON adj_factor(trade_date DESC);
CREATE INDEX idx_adj_factor_code ON adj_factor(ts_code);

COMMENT ON TABLE adj_factor IS '复权因子表';
COMMENT ON COLUMN adj_factor.ts_code IS '股票代码';
COMMENT ON COLUMN adj_factor.trade_date IS '交易日期';
COMMENT ON COLUMN adj_factor.adj_factor IS '复权因子';

-- 创建指数日线行情表
CREATE TABLE IF NOT EXISTS index_daily (
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    pre_close DOUBLE PRECISION,
    change DOUBLE PRECISION,
    pct_chg DOUBLE PRECISION,
    vol DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX idx_index_daily_date ON index_daily(trade_date DESC);
CREATE INDEX idx_index_daily_code ON index_daily(ts_code);

COMMENT ON TABLE index_daily IS '指数日线行情表';
COMMENT ON COLUMN index_daily.ts_code IS '指数代码';
COMMENT ON COLUMN index_daily.trade_date IS '交易日期';
COMMENT ON COLUMN index_daily.open IS '开盘价';
COMMENT ON COLUMN index_daily.high IS '最高价';
COMMENT ON COLUMN index_daily.low IS '最低价';
COMMENT ON COLUMN index_daily.close IS '收盘价';
COMMENT ON COLUMN index_daily.pre_close IS '昨收价';
COMMENT ON COLUMN index_daily.change IS '涨跌额';
COMMENT ON COLUMN index_daily.pct_chg IS '涨跌幅';
COMMENT ON COLUMN index_daily.vol IS '成交量';
COMMENT ON COLUMN index_daily.amount IS '成交额';

-- 创建个股资金流向表
CREATE TABLE IF NOT EXISTS moneyflow (
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    buy_sm_vol DOUBLE PRECISION,
    buy_sm_amount DOUBLE PRECISION,
    sell_sm_vol DOUBLE PRECISION,
    sell_sm_amount DOUBLE PRECISION,
    buy_md_vol DOUBLE PRECISION,
    buy_md_amount DOUBLE PRECISION,
    sell_md_vol DOUBLE PRECISION,
    sell_md_amount DOUBLE PRECISION,
    buy_lg_vol DOUBLE PRECISION,
    buy_lg_amount DOUBLE PRECISION,
    sell_lg_vol DOUBLE PRECISION,
    sell_lg_amount DOUBLE PRECISION,
    buy_elg_vol DOUBLE PRECISION,
    buy_elg_amount DOUBLE PRECISION,
    sell_elg_vol DOUBLE PRECISION,
    sell_elg_amount DOUBLE PRECISION,
    net_mf_vol DOUBLE PRECISION,
    net_mf_amount DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX idx_moneyflow_date ON moneyflow(trade_date DESC);
CREATE INDEX idx_moneyflow_code ON moneyflow(ts_code);

COMMENT ON TABLE moneyflow IS '个股资金流向表';
COMMENT ON COLUMN moneyflow.ts_code IS '股票代码';
COMMENT ON COLUMN moneyflow.trade_date IS '交易日期';
COMMENT ON COLUMN moneyflow.buy_sm_vol IS '小单买入量';
COMMENT ON COLUMN moneyflow.buy_sm_amount IS '小单买入额';
COMMENT ON COLUMN moneyflow.sell_sm_vol IS '小单卖出量';
COMMENT ON COLUMN moneyflow.sell_sm_amount IS '小单卖出额';
COMMENT ON COLUMN moneyflow.buy_md_vol IS '中单买入量';
COMMENT ON COLUMN moneyflow.buy_md_amount IS '中单买入额';
COMMENT ON COLUMN moneyflow.sell_md_vol IS '中单卖出量';
COMMENT ON COLUMN moneyflow.sell_md_amount IS '中单卖出额';
COMMENT ON COLUMN moneyflow.buy_lg_vol IS '大单买入量';
COMMENT ON COLUMN moneyflow.buy_lg_amount IS '大单买入额';
COMMENT ON COLUMN moneyflow.sell_lg_vol IS '大单卖出量';
COMMENT ON COLUMN moneyflow.sell_lg_amount IS '大单卖出额';
COMMENT ON COLUMN moneyflow.buy_elg_vol IS '特大单买入量';
COMMENT ON COLUMN moneyflow.buy_elg_amount IS '特大单买入额';
COMMENT ON COLUMN moneyflow.sell_elg_vol IS '特大单卖出量';
COMMENT ON COLUMN moneyflow.sell_elg_amount IS '特大单卖出额';
COMMENT ON COLUMN moneyflow.net_mf_vol IS '净流入量';
COMMENT ON COLUMN moneyflow.net_mf_amount IS '净流入额';

-- 创建日线行情表（兼容旧版）
CREATE TABLE IF NOT EXISTS daily_data (
    trade_date VARCHAR(8) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    vol DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    pct_chg DOUBLE PRECISION,
    PRIMARY KEY (trade_date, ts_code)
);

CREATE INDEX idx_daily_data_code ON daily_data(ts_code);
CREATE INDEX idx_daily_data_date ON daily_data(trade_date DESC);

COMMENT ON TABLE daily_data IS '日线行情表（OHLCV）';
COMMENT ON COLUMN daily_data.trade_date IS '交易日期';
COMMENT ON COLUMN daily_data.ts_code IS '股票代码';
COMMENT ON COLUMN daily_data.open IS '开盘价';
COMMENT ON COLUMN daily_data.high IS '最高价';
COMMENT ON COLUMN daily_data.low IS '最低价';
COMMENT ON COLUMN daily_data.close IS '收盘价';
COMMENT ON COLUMN daily_data.vol IS '成交量';
COMMENT ON COLUMN daily_data.amount IS '成交额';
COMMENT ON COLUMN daily_data.pct_chg IS '涨跌幅';
