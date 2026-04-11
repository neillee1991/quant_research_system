-- 为 flow_runs 表添加随机 run_id 字段，用于对外展示（替代自增整数 id）
ALTER TABLE flow_runs ADD COLUMN IF NOT EXISTS run_id VARCHAR(64) UNIQUE;

-- 为已有记录补充 run_id（使用 gen_random_uuid 或 md5 兜底）
UPDATE flow_runs SET run_id = encode(gen_random_bytes(16), 'hex') WHERE run_id IS NULL;

-- 后续新记录由应用层写入，此处仅补历史数据
CREATE INDEX IF NOT EXISTS idx_flow_runs_run_id ON flow_runs(run_id);
