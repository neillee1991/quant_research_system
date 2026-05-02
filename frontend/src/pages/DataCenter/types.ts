/**
 * DataCenter Local Types
 * 扩展全局类型定义，添加本地特定类型
 */

export interface LogFilters {
  source?: string;
  dataType?: string;
  startDate?: string;
  endDate?: string;
}

export interface ETLLogFilters {
  taskId?: string;
  startDate?: string;
  endDate?: string;
}

export interface ETLTaskStatus {
  task_id: string;
  last_date: string | null;
  last_sync_time: string | null;
  table_name: string;
  table_latest_date?: string | null;
  [key: string]: unknown;
}

export interface CopyTaskConfig {
  source: any;
  newId: string;
  newTableName: string;
  step: 'input' | 'table_not_exists';
  config: any | null;
}

export interface SyncModalState {
  visible: boolean;
  task: any | null;
  targetDate: string;
  startDate: string;
  endDate: string;
}

export interface BatchSyncModalState {
  visible: boolean;
  startDate: string;
  endDate: string;
}

export interface ETLBackfillModalState {
  visible: boolean;
  taskId: string;
  startDate: string;
  endDate: string;
}

export interface DeleteConfirmState {
  visible: boolean;
  taskId: string;
  type: 'sync' | 'etl';
}
