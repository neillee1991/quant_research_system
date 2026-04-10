-- 自研调度系统 - 数据库初始化脚本
-- 创建时间: 2026-04-10

-- flow_config: Flow 配置表
CREATE TABLE IF NOT EXISTS flow_config (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(255) UNIQUE NOT NULL,
    description      TEXT,
    cron             VARCHAR(100),           -- 空表示只手动触发
    timezone         VARCHAR(50) DEFAULT 'Asia/Shanghai',
    tags             JSONB DEFAULT '[]',
    tasks            JSONB NOT NULL,         -- DAG 任务节点定义
    date_offset_days INTEGER DEFAULT 0,
    enabled          BOOLEAN DEFAULT TRUE,
    version          INTEGER DEFAULT 1,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- flow_run: Flow 执行记录表
CREATE TABLE IF NOT EXISTS flow_run (
    id                  SERIAL PRIMARY KEY,
    flow_name           VARCHAR(255) NOT NULL,
    parent_flow_run_id  INTEGER REFERENCES flow_run(id),  -- 支持嵌套
    status              VARCHAR(20) NOT NULL,  -- pending/running/success/failed/cancelled
    trigger_type        VARCHAR(20) NOT NULL,  -- cron/manual/parent_flow
    target_date         VARCHAR(8),            -- YYYYMMDD
    scheduled_at        TIMESTAMPTZ,
    started_at          TIMESTAMPTZ,
    ended_at            TIMESTAMPTZ,
    error_message       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- task_run: Task 执行记录表
CREATE TABLE IF NOT EXISTS task_run (
    id            SERIAL PRIMARY KEY,
    flow_run_id   INTEGER REFERENCES flow_run(id) ON DELETE CASCADE,
    task_id       VARCHAR(255) NOT NULL,
    task_type     VARCHAR(20) NOT NULL,  -- sync/etl/factor/flow
    status        VARCHAR(20) NOT NULL,  -- pending/running/success/failed
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,
    error_message TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_flow_config_enabled ON flow_config(enabled);
CREATE INDEX IF NOT EXISTS idx_flow_config_updated_at ON flow_config(updated_at);
CREATE INDEX IF NOT EXISTS idx_flow_run_flow_name ON flow_run(flow_name);
CREATE INDEX IF NOT EXISTS idx_flow_run_status ON flow_run(status);
CREATE INDEX IF NOT EXISTS idx_flow_run_created_at ON flow_run(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_run_flow_run_id ON task_run(flow_run_id);
CREATE INDEX IF NOT EXISTS idx_task_run_status ON task_run(status);
