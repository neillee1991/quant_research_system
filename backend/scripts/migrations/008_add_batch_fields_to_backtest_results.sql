-- Migration 008: Add batch-related fields to backtest_results
-- Add support for script batch backtest functionality

ALTER TABLE backtest_results
  ADD COLUMN IF NOT EXISTS mode           VARCHAR(50)  DEFAULT '',
  ADD COLUMN IF NOT EXISTS script_hash    VARCHAR(255) DEFAULT '',
  ADD COLUMN IF NOT EXISTS warnings_json  TEXT         DEFAULT '[]',
  ADD COLUMN IF NOT EXISTS params_json    TEXT         DEFAULT '{}';
