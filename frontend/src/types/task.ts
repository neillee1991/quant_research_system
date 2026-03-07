/**
 * Task Management Abstraction Types
 *
 * Provides unified type definitions for all task types (sync, etl, factor)
 * with shared version control fields.
 */

// Task type discriminator
export type TaskType = 'sync' | 'etl' | 'factor';

// Base configuration interface with 8 version control fields
export interface BaseTaskConfig {
  // Version control fields (shared by all task types)
  version_number: number;
  is_current: boolean;
  changed_by: string;
  change_reason: string;
  created_at?: string;
  updated_at?: string;
  description: string;
  enabled: boolean;
}

// Sync task configuration
export interface SyncTaskConfig extends BaseTaskConfig {
  task_id: string;
  api_name: string;
  api_limit?: number;
  fields?: string;
  start_date?: string;
  end_date?: string;
  sync_type?: string;
  table_name?: string;
  source?: string;
  schedule?: string;
  cron_expression?: string;
}

// ETL task configuration
export interface ETLTaskConfig extends BaseTaskConfig {
  task_id: string;
  source_table: string;
  target_table: string;
  script: string;
  schedule?: string;
  table_name?: string;
}

// Factor configuration
export interface FactorConfig extends BaseTaskConfig {
  factor_id: string;
  code: string;
  depends_on?: string;
  params?: string;
  lookback_days?: number;
  category?: string;
}

// Generic task type union
export type TaskConfig = SyncTaskConfig | ETLTaskConfig | FactorConfig;

// API response types
export interface TaskListResponse<T extends BaseTaskConfig> {
  tasks: T[];
  total: number;
}

export interface TaskDetailResponse<T extends BaseTaskConfig> {
  task: T;
}

export interface TaskCreateRequest<T extends BaseTaskConfig> {
  config: Omit<T, 'version_number' | 'is_current' | 'created_at' | 'updated_at'>;
}

export interface TaskUpdateRequest<T extends BaseTaskConfig> {
  config: Partial<Omit<T, 'version_number' | 'is_current' | 'created_at' | 'updated_at'>>;
  changed_by: string;
  change_reason: string;
}

export interface TaskDeleteResponse {
  success: boolean;
  message: string;
}

// Helper type to extract ID field name based on task type
export type TaskIdField<T extends TaskType> =
  T extends 'sync' ? 'task_id' :
  T extends 'etl' ? 'task_id' :
  T extends 'factor' ? 'factor_id' :
  never;

// Helper type to map task type to config interface
export type TaskConfigMap = {
  sync: SyncTaskConfig;
  etl: ETLTaskConfig;
  factor: FactorConfig;
};
