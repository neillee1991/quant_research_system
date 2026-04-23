-- 009: 创建脚本版本表和批量回测结果表
-- Phase 3, Task 3B-6

-- ============================================================
-- Part 1: 脚本版本表 (script_versions)
-- 存储策略脚本的完整历史版本，通过 script_hash 唯一标识
-- ============================================================

CREATE TABLE IF NOT EXISTS script_versions (
  id           BIGSERIAL    PRIMARY KEY,
  script_hash  VARCHAR(64)  UNIQUE NOT NULL,
  script_text  TEXT         NOT NULL,
  name         VARCHAR(255) DEFAULT '',
  description  TEXT         DEFAULT '',
  language     VARCHAR(20)  DEFAULT 'python',
  created_at   TIMESTAMPTZ  DEFAULT NOW()
);

-- 索引：按哈希快速查找
CREATE INDEX IF NOT EXISTS idx_script_versions_hash
  ON script_versions(script_hash);

-- 索引：按创建时间排序
CREATE INDEX IF NOT EXISTS idx_script_versions_created_at
  ON script_versions(created_at DESC);

-- ============================================================
-- Part 2: 批量回测结果表 (script_batch_results)
-- 存储同脚本不同参数组合的批量回测结果
-- ============================================================

CREATE TABLE IF NOT EXISTS script_batch_results (
  id          BIGSERIAL    PRIMARY KEY,
  batch_id    VARCHAR(255) NOT NULL,
  run_id      VARCHAR(255),
  script_hash VARCHAR(64),
  params_json JSONB        NOT NULL DEFAULT '{}',
  metrics_json JSONB       DEFAULT '{}',
  status      VARCHAR(20)  DEFAULT 'pending',
  error_msg   TEXT         DEFAULT '',
  created_at  TIMESTAMPTZ  DEFAULT NOW(),
  updated_at  TIMESTAMPTZ  DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

-- 索引：按批次查询
CREATE INDEX IF NOT EXISTS idx_script_batch_results_batch_id
  ON script_batch_results(batch_id);

-- 索引：按脚本哈希查询
CREATE INDEX IF NOT EXISTS idx_script_batch_results_script_hash
  ON script_batch_results(script_hash);

-- 索引：按状态查询
CREATE INDEX IF NOT EXISTS idx_script_batch_results_status
  ON script_batch_results(status);

-- 索引：按创建时间排序
CREATE INDEX IF NOT EXISTS idx_script_batch_results_created_at
  ON script_batch_results(created_at DESC);

-- 外键约束：关联到 script_versions
ALTER TABLE script_batch_results
  ADD CONSTRAINT fk_script_batch_results_script_hash
  FOREIGN KEY (script_hash) REFERENCES script_versions(script_hash)
  ON DELETE SET NULL;

-- ============================================================
-- Part 3: 添加 updated_at 自动更新触发器
-- ============================================================

-- 为 script_batch_results 表添加触发器
DROP TRIGGER IF EXISTS update_script_batch_results_updated_at ON script_batch_results;
CREATE TRIGGER update_script_batch_results_updated_at
BEFORE UPDATE ON script_batch_results
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
