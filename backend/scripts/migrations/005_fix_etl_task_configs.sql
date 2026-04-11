-- Migration 005: Fix etl_task_configs missing columns
-- The seed manager inserts schema_json and source_tables but the table was created without them.

ALTER TABLE etl_task_configs
  ADD COLUMN IF NOT EXISTS schema_json   TEXT DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS source_tables TEXT DEFAULT '[]';
