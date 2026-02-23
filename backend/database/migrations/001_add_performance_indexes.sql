-- Migration: 001_add_performance_indexes
-- Description: Add indexes to improve query performance for high-frequency queries
-- Created: 2026-02-23

-- ============================================================================
-- Daily Data Indexes
-- ============================================================================

-- Composite index for stock code + date queries (most common pattern)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_data_ts_code_trade_date
ON daily_data(ts_code, trade_date DESC);

-- Index for date range queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_data_trade_date_desc
ON daily_data(trade_date DESC);

-- ============================================================================
-- Daily Basic Indexes
-- ============================================================================

-- Composite index for stock code + date queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_basic_ts_code_trade_date
ON daily_basic(ts_code, trade_date DESC);

-- Index for date range queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_daily_basic_trade_date_desc
ON daily_basic(trade_date DESC);

-- ============================================================================
-- Factor Values Indexes
-- ============================================================================

-- Composite index for factor + date queries (factor analysis)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_values_factor_id_trade_date
ON factor_values(factor_id, trade_date DESC);

-- Composite index for stock + date queries (stock factor history)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_values_ts_code_trade_date
ON factor_values(ts_code, trade_date DESC);

-- Composite index for factor + stock + date (most specific queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_values_factor_ts_code_date
ON factor_values(factor_id, ts_code, trade_date DESC);

-- ============================================================================
-- Factor Metadata Indexes
-- ============================================================================

-- Index for factor_id lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_metadata_factor_id
ON factor_metadata(factor_id);

-- Index for enabled factors
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_metadata_enabled
ON factor_metadata(enabled) WHERE enabled = true;

-- ============================================================================
-- Factor Analysis Indexes
-- ============================================================================

-- Index for factor analysis lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_analysis_factor_id
ON factor_analysis(factor_id);

-- Index for date-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_analysis_analysis_date
ON factor_analysis(analysis_date DESC);

-- Composite index for factor + date
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factor_analysis_factor_date
ON factor_analysis(factor_id, analysis_date DESC);

-- ============================================================================
-- Sync Log Indexes
-- ============================================================================

-- Composite index for sync status queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sync_log_source_data_type
ON sync_log(source, data_type);

-- Index for last sync time queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sync_log_last_sync_time
ON sync_log(last_sync_time DESC);

-- ============================================================================
-- Production Task Run Indexes
-- ============================================================================

-- Index for task status queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_production_task_run_status
ON production_task_run(status);

-- Index for factor task queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_production_task_run_factor_id
ON production_task_run(factor_id);

-- Index for date-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_production_task_run_created_at
ON production_task_run(created_at DESC);

-- ============================================================================
-- DAG Run Log Indexes
-- ============================================================================

-- Index for DAG status queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dag_run_log_status
ON dag_run_log(status);

-- Index for DAG ID queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dag_run_log_dag_id
ON dag_run_log(dag_id);

-- Index for date-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dag_run_log_start_time
ON dag_run_log(start_time DESC);

-- ============================================================================
-- Stock Basic Indexes
-- ============================================================================

-- Index for stock code lookups (if not already primary key)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_ts_code
ON stock_basic(ts_code);

-- Index for active stocks
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_basic_list_status
ON stock_basic(list_status) WHERE list_status = 'L';

-- ============================================================================
-- Analyze tables to update statistics
-- ============================================================================

ANALYZE daily_data;
ANALYZE daily_basic;
ANALYZE factor_values;
ANALYZE factor_metadata;
ANALYZE factor_analysis;
ANALYZE sync_log;
ANALYZE production_task_run;
ANALYZE dag_run_log;
ANALYZE stock_basic;
