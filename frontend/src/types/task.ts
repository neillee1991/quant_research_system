/**
 * Task Management Abstraction Types
 *
 * SyncTaskConfig/ETLTaskConfig/FactorConfig 与后端 base_task.py 保持一致。
 * 后端 TaskListResponse.tasks 是 Dict[str, Any]，无法从 generated.ts 推断，故手写。
 */

// Task type discriminator
export type TaskType = 'sync' | 'etl' | 'factor';

// Base configuration interface
export interface BaseTaskConfig {
  created_at?: string;
  updated_at?: string;
  description?: string;
  enabled?: boolean;
}

// 与后端 SyncTaskConfig (base_task.py) 保持一致
// JSONB 迁移后 *_json 字段直接是 dict/list（不再是字符串）
export interface SyncTaskConfig extends BaseTaskConfig {
  task_id: string;
  api_name: string;
  api_limit?: number;
  sync_type?: string;
  params_json?: Record<string, unknown>;
  date_field?: string;
  primary_keys_json?: string[];
  table_name: string;
  schema_json?: Record<string, unknown>;
  column_mapping_json?: Record<string, string> | null;
  source?: string;
  // 前端运行时别名（parseJsonFields 填充，供可视化编辑器使用）
  params?: Record<string, unknown>;
  schema?: Record<string, unknown>;
  primary_keys?: string[];
  column_mapping?: Record<string, string> | null;
}

// 与后端 ETLTaskConfig (base_task.py) 保持一致
export interface ETLTaskConfig extends BaseTaskConfig {
  task_id: string;
  script: string;
  sync_type?: string;
  date_field?: string | null;
  primary_keys_json?: string[];
  schema_json?: Record<string, unknown>;
  table_name: string;
  // 前端运行时别名
  primary_keys?: string[];
  schema?: Record<string, unknown>;
}

// 与后端 FactorConfig (base_task.py) 保持一致
export interface FactorConfig extends BaseTaskConfig {
  factor_id: string;
  code: string;
  depends_on?: string;
  params?: Record<string, unknown>;
  lookback_days?: number;
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
  config: Omit<T, 'created_at' | 'updated_at'>;
}

export interface TaskUpdateRequest<T extends BaseTaskConfig> {
  config: Partial<Omit<T, 'created_at' | 'updated_at'>>;
}

export interface TaskDeleteResponse {
  success: boolean;
  message: string;
}

export type TaskIdField<T extends TaskType> =
  T extends 'sync' ? 'task_id' :
  T extends 'etl' ? 'task_id' :
  T extends 'factor' ? 'factor_id' :
  never;

export type TaskConfigMap = {
  sync: SyncTaskConfig;
  etl: ETLTaskConfig;
  factor: FactorConfig;
};

