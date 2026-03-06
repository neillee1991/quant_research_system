-- Alphalens 集成数据库迁移
-- 创建日期: 2026-03-03
-- 说明: 添加指数股票池表、指数元数据表、因子分析结果扩展表

-- ==================== 1. 指数成分股表 ====================
CREATE TABLE IF NOT EXISTS index_constituents (
    trade_date VARCHAR(8) NOT NULL COMMENT '交易日期 YYYYMMDD',
    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    index_code VARCHAR(20) NOT NULL COMMENT '指数代码（用户自定义，如 HS300, ZZ500）',
    weight DECIMAL(10, 6) COMMENT '权重（可选）',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, ts_code, index_code),
    INDEX idx_date_index (trade_date, index_code),
    INDEX idx_ts_code (ts_code),
    INDEX idx_index_code (index_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数成分股表（用户自主配置）';

-- ==================== 2. 指数元数据表 ====================
CREATE TABLE IF NOT EXISTS index_metadata (
    index_code VARCHAR(20) PRIMARY KEY COMMENT '指数代码（用户自定义）',
    index_name VARCHAR(100) COMMENT '指数名称',
    description TEXT COMMENT '描述',
    stock_count INT COMMENT '成分股数量（最新）',
    latest_date VARCHAR(8) COMMENT '最新数据日期',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数元数据表';

-- ==================== 3. 因子分析结果扩展表 ====================
CREATE TABLE IF NOT EXISTS factor_analysis_extended (
    id INT AUTO_INCREMENT PRIMARY KEY,
    factor_id VARCHAR(100) NOT NULL COMMENT '因子ID',
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '分析时间',
    start_date VARCHAR(8) COMMENT '分析起始日期',
    end_date VARCHAR(8) COMMENT '分析结束日期',

    -- 分析配置（永久保留，用于复现）
    config JSON COMMENT '分析配置（股票池、周期、分组数、分组字段等）',

    -- Alphalens 核心指标
    ic_summary JSON COMMENT 'IC 汇总统计',
    ic_by_period JSON COMMENT '各持仓周期 IC',
    ic_ts JSON COMMENT 'IC 时间序列',

    -- 分层收益
    quantile_returns JSON COMMENT '分位数收益统计',
    cumulative_returns JSON COMMENT '累计收益曲线数据',

    -- 多维度分析（动态分组）
    ic_by_group JSON COMMENT '分组 IC（行业/市值等，字段名动态）',
    returns_by_group JSON COMMENT '分组收益',

    -- 其他指标
    turnover JSON COMMENT '换手率',
    decay_analysis JSON COMMENT '衰减分析',

    -- 图表数据（预计算，加速前端渲染）
    charts_data JSON COMMENT '图表配置数据',

    -- 任务状态（支持长时间运行和取消）
    task_status VARCHAR(20) DEFAULT 'completed' COMMENT '任务状态: running/completed/cancelled/failed',
    task_id VARCHAR(100) COMMENT '后台任务ID（用于取消）',
    error_message TEXT COMMENT '错误信息（如果失败）',

    INDEX idx_factor_date (factor_id, analysis_date),
    INDEX idx_task_status (task_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Alphalens 因子分析结果扩展表（永久保留）';

-- ==================== 4. 初始化数据配置 ====================
-- 确保 factor_data_config 表中有行业和市值配置项
INSERT IGNORE INTO factor_data_config (field_key, description, table_name, column_name, extra_config, updated_at)
VALUES
('industry', '行业分类', '', '', '{"mode": "to_be_configured", "note": "请配置行业字段的数据源（如申万行业、中信行业等）"}', NOW()),
('market_cap', '市值', '', '', '{"mode": "to_be_configured", "note": "请配置市值字段的数据源（总市值或流通市值）"}', NOW());

-- ==================== 5. 验证表创建 ====================
-- 查看创建的表
SHOW TABLES LIKE '%index%';
SHOW TABLES LIKE 'factor_analysis_extended';

-- 查看表结构
-- DESCRIBE index_constituents;
-- DESCRIBE index_metadata;
-- DESCRIBE factor_analysis_extended;
