/**
 * Version Control Type Definitions
 */

export type TaskType = 'sync' | 'factor' | 'etl';

export interface VersionRecord {
  version: number;
  changed_by: string;
  change_reason: string;
  created_at: string;
  config?: any;
}

export interface VersionHistoryResponse {
  task_id: string;
  task_type: TaskType;
  current_version: number;
  versions: VersionRecord[];
}

export interface VersionDiffResponse {
  v1: number;
  v2: number;
  diff: {
    added?: Record<string, any>;
    removed?: Record<string, any>;
    modified?: Record<string, any>;
  };
}

export interface RollbackRequest {
  reason?: string;
}

export interface RollbackResponse {
  success: boolean;
  new_version: number;
  message: string;
}

export interface VersionMetadata {
  version: number;
  changed_by?: string;
  change_reason?: string;
  created_at?: string;
  updated_at?: string;
}

export interface TaskWithVersion {
  task_id: string;
  version: number;
  changed_by?: string;
  change_reason?: string;
  updated_at?: string;
  config?: any;
}
