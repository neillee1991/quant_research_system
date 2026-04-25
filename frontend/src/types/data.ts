/**
 * Data Management Types
 */

export interface StockInfo {
  ts_code: string;
  symbol: string;
  name: string;
  area?: string;
  industry?: string;
  market?: string;
  list_date?: string;
}

export interface DailyData {
  ts_code: string;
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount: number;
  pre_close?: number;
  change?: number;
  pct_chg?: number;
  [key: string]: string | number | undefined;
}

// 前端运行时类型：任务列表展示 + Drawer 使用
// JSONB 迁移后 *_json 字段直接是 dict/list，params/schema/primary_keys 为 parseJsonFields 填充的别名
export interface SyncTask {
  task_id: string;
  description: string;
  sync_type: string;
  table_name: string;
  source?: string;
  enabled?: boolean;
  params_json?: Record<string, unknown>;
  schema_json?: Record<string, unknown>;
  primary_keys_json?: string[];
  // parseJsonFields 填充的运行时别名
  params?: Record<string, unknown>;
  schema?: Record<string, unknown>;
  primary_keys?: string[];
  api_name?: string;
  date_field?: string;
  api_limit?: number;
}

export interface TaskStatus {
  task_id: string;
  description: string;
  sync_type: string;
  last_sync_date: string | null;
  last_sync_time: string | null;
  table_name: string;
  table_latest_date?: string | null;
  source?: string;
  enabled?: boolean;
  schedule?: string;
  [key: string]: unknown;
}

export interface SyncLog {
  id: number;
  task_id?: string;
  source: string;
  data_type: string;
  last_date: string;
  sync_date: string;
  rows_synced: number;
  status: 'success' | 'failed' | 'running';
  error_message?: string;
  created_at: string;
}

export interface TableInfo {
  table_name: string;
  row_count: number;
  column_count: number;
  columns: string[];
  latest_date?: string;
  earliest_date?: string;
}

export interface QueryResult {
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  execution_time?: number;
}

export interface ScheduleInfo {
  task_id: string;
  enabled: boolean;
  schedule?: string;
  cron_expression?: string;
  next_run_time?: string;
  last_run_time?: string;
}

// 前端运行时类型：ETL 任务列表展示用
export interface ETLTask {
  task_id: string;
  description: string;
  table_name: string;
  script: string;
  sync_type?: 'full' | 'incremental';
  date_field?: string;
  enabled?: boolean;
  schema_json?: Record<string, unknown>;
  primary_keys_json?: string[];
  // parseJsonFields 填充的运行时别名
  schema?: Record<string, unknown>;
  primary_keys?: string[];
  created_at?: string;
  updated_at?: string;
}

// ETL 字段定义（前端专用，后端无对应 schema）
export interface ETLFieldDefinition {
  name: string;
  type: string;
  description?: string;
}

export interface ETLTestResult {
  success: boolean;
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  error?: string;
}

