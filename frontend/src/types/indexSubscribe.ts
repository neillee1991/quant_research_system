/**
 * 临时类型扩展文件 - 修复指数订阅功能的类型问题
 */

import type { SyncTask as OriginalSyncTask } from './data';

// 扩展 SyncTask 类型，添加指数订阅需要的字段
export interface ExtendedSyncTask extends OriginalSyncTask {
  params_json?: string;
  schema_json?: string;
  primary_keys_json?: string;
  column_mapping_json?: string;
  params?: Record<string, unknown>;
  schema?: Record<string, unknown>;
  primary_keys?: string[];
  column_mapping?: Record<string, string>;
  api_name?: string;
  date_field?: string;
  api_limit?: number;
}

// 指数信息类型
export interface IndexInfo {
  ts_code: string;
  name: string;
  market: string;
  publisher?: string;
  list_date?: string;
  weight_rule?: string;
  desc?: string;
  exp_date?: string;
  is_subscribed: boolean;
  subscribed_task_id?: string;
}

// 筛选字段配置类型
export interface FilterFieldConfig {
  field: string;
  label: string;
  enabled: boolean;
  default_value: string | null;
}

// 用户偏好配置类型
export interface UserPreference {
  index_basic_table: string;
  filter_config?: FilterFieldConfig[];
}

