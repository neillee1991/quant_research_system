/**
 * Task Management Abstraction Types
 *
 * Provides unified type definitions for all task types (sync, etl, factor).
 * SyncTaskConfig, ETLTaskConfig, FactorConfig are sourced from generated.ts (backend Pydantic models).
 */
import type { components } from './generated';

// Task type discriminator
export type TaskType = 'sync' | 'etl' | 'factor';

// Base configuration interface
export interface BaseTaskConfig {
  created_at?: string;
  updated_at?: string;
  description?: string;
  enabled?: boolean;
}

// Backend model types — single source of truth from generated.ts
export type SyncTaskConfig = components['schemas']['SyncTaskConfig'];
export type ETLTaskConfig = components['schemas']['ETLTaskConfig'];
export type FactorConfig = components['schemas']['FactorConfig'];

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

