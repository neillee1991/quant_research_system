export enum ConfigType {
  SYNC_TASKS = 'sync_tasks',
  ETL_TASKS = 'etl_tasks',
  FACTOR_METADATA = 'factor_metadata',
  FACTOR_DATA_CONFIG = 'factor_data_config'
}

export enum ImportMode {
  FAST = 'fast',
  SAFE = 'safe'
}

export interface ConfigTypeOption {
  value: ConfigType;
  label: string;
}

export interface ExportRequest {
  config_types: ConfigType[];
}

export interface ExportResponse {
  filename: string;
  content: string;
}

export interface ConfigItemDiff {
  item_id: string;
  status: 'new' | 'modified' | 'unchanged' | 'deleted';
  current: Record<string, any> | null;
  imported: Record<string, any> | null;
}

export interface ConfigTypeDiff {
  config_type: ConfigType;
  items: ConfigItemDiff[];
  summary: {
    new: number;
    modified: number;
    unchanged: number;
    deleted: number;
  };
}

export interface ImportVerifyRequest {
  content: string;
  mode: ImportMode;
}

export interface ImportVerifyResponse {
  valid: boolean;
  errors: string[];
  diffs: ConfigTypeDiff[] | null;
}

export interface ImportApplyRequest {
  content: string;
  mode: ImportMode;
  selections?: Record<ConfigType, string[]>;
}

export interface ImportResultSummary {
  created: number;
  updated: number;
  skipped: number;
}

export interface ImportApplyResponse {
  success: boolean;
  summary: Record<ConfigType, ImportResultSummary>;
  errors: string[];
}
