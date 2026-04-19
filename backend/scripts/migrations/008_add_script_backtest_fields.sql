-- 008: 为脚本回测扩展 task_runs 和 backtest_results 表字段

-- task_runs: 区分 graph/script 模式
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS mode VARCHAR(10) DEFAULT 'graph';
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS script_hash VARCHAR(64);
ALTER TABLE task_runs ADD COLUMN IF NOT EXISTS ir_version VARCHAR(20);

-- backtest_results: 支持脚本回测结果
ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS mode VARCHAR(10) DEFAULT 'graph';
ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS script_hash VARCHAR(64);
ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS warnings_json TEXT DEFAULT '[]';

-- 索引：按模式查询
CREATE INDEX IF NOT EXISTS idx_task_runs_mode ON task_runs(mode);
CREATE INDEX IF NOT EXISTS idx_backtest_results_mode ON backtest_results(mode);
