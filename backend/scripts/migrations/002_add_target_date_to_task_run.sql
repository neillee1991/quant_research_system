-- 迁移: 为 task_run 表添加 target_date 列
-- 创建时间: 2026-04-11

ALTER TABLE task_run
    ADD COLUMN IF NOT EXISTS target_date VARCHAR(8);
