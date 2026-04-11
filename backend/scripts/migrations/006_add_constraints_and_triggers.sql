-- 数据库约束添加脚本
-- 添加外键约束、CHECK约束和updated_at触发器

-- ==================== 1. 添加外键约束 ====================

-- flow_runs.flow_name -> flow_configs.name
ALTER TABLE flow_runs
ADD CONSTRAINT fk_flow_runs_flow_name
FOREIGN KEY (flow_name) REFERENCES flow_configs(name)
ON DELETE CASCADE;

-- factor_analysis_results.factor_id -> factor_configs.factor_id
ALTER TABLE factor_analysis_results
ADD CONSTRAINT fk_factor_analysis_results_factor_id
FOREIGN KEY (factor_id) REFERENCES factor_configs(factor_id)
ON DELETE CASCADE;

-- ==================== 2. 添加CHECK约束 ====================

-- flow_runs.status 约束
ALTER TABLE flow_runs
ADD CONSTRAINT check_flow_runs_status
CHECK (status IN ('pending', 'running', 'success', 'failed', 'cancelled'));

-- task_runs.status 约束
ALTER TABLE task_runs
ADD CONSTRAINT check_task_runs_status
CHECK (status IN ('pending', 'running', 'success', 'failed'));

-- task_runs.task_type 约束
ALTER TABLE task_runs
ADD CONSTRAINT check_task_runs_task_type
CHECK (task_type IN ('sync', 'etl', 'factor', 'flow'));

-- sync_task_configs.sync_type 约束
ALTER TABLE sync_task_configs
ADD CONSTRAINT check_sync_task_configs_sync_type
CHECK (sync_type IN ('incremental', 'full'));

-- trading_calendar.is_open 约束
ALTER TABLE trading_calendar
ADD CONSTRAINT check_trading_calendar_is_open
CHECK (is_open IN (0, 1));

-- ==================== 3. 添加updated_at自动更新触发器 ====================

-- 创建触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ language 'plpgsql';

-- 为flow_configs表添加触发器
DROP TRIGGER IF EXISTS update_flow_configs_updated_at ON flow_configs;
CREATE TRIGGER update_flow_configs_updated_at
BEFORE UPDATE ON flow_configs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为flow_runs表添加触发器
DROP TRIGGER IF EXISTS update_flow_runs_updated_at ON flow_runs;
CREATE TRIGGER update_flow_runs_updated_at
BEFORE UPDATE ON flow_runs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为task_runs表添加触发器
DROP TRIGGER IF EXISTS update_task_runs_updated_at ON task_runs;
CREATE TRIGGER update_task_runs_updated_at
BEFORE UPDATE ON task_runs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为sync_task_configs表添加触发器
DROP TRIGGER IF EXISTS update_sync_task_configs_updated_at ON sync_task_configs;
CREATE TRIGGER update_sync_task_configs_updated_at
BEFORE UPDATE ON sync_task_configs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为etl_task_configs表添加触发器
DROP TRIGGER IF EXISTS update_etl_task_configs_updated_at ON etl_task_configs;
CREATE TRIGGER update_etl_task_configs_updated_at
BEFORE UPDATE ON etl_task_configs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为factor_configs表添加触发器
DROP TRIGGER IF EXISTS update_factor_configs_updated_at ON factor_configs;
CREATE TRIGGER update_factor_configs_updated_at
BEFORE UPDATE ON factor_configs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为factor_analysis_results表添加触发器
DROP TRIGGER IF EXISTS update_factor_analysis_results_updated_at ON factor_analysis_results;
CREATE TRIGGER update_factor_analysis_results_updated_at
BEFORE UPDATE ON factor_analysis_results
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- 为backtest_results表添加触发器
DROP TRIGGER IF EXISTS update_backtest_results_updated_at ON backtest_results;
CREATE TRIGGER update_backtest_results_updated_at
BEFORE UPDATE ON backtest_results
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- ==================== 4. 添加缺失的索引 ====================

-- flow_runs.parent_flow_run_id 索引
CREATE INDEX IF NOT EXISTS idx_flow_runs_parent_flow_run_id
ON flow_runs(parent_flow_run_id);

-- factor_analysis_results.factor_id 索引
CREATE INDEX IF NOT EXISTS idx_factor_analysis_results_factor_id
ON factor_analysis_results(factor_id);

-- backtest_results.task_id 索引
CREATE INDEX IF NOT EXISTS idx_backtest_results_task_id
ON backtest_results(task_id);

-- task_runs.task_id 索引
CREATE INDEX IF NOT EXISTS idx_task_runs_task_id
ON task_runs(task_id);

-- task_runs.flow_run_id 索引
CREATE INDEX IF NOT EXISTS idx_task_runs_flow_run_id
ON task_runs(flow_run_id);

-- ==================== 5. 验证约束 ====================

-- 显示所有约束
SELECT constraint_name, table_name, constraint_type
FROM information_schema.table_constraints
WHERE table_schema = 'public'
ORDER BY table_name, constraint_name;

-- 显示所有触发器
SELECT trigger_name, event_object_table, event_manipulation
FROM information_schema.triggers
WHERE trigger_schema = 'public'
ORDER BY event_object_table, trigger_name;

-- 显示所有索引
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
