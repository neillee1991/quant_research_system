-- 量化研究系统数据库初始化脚本
-- PostgreSQL 16+

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
