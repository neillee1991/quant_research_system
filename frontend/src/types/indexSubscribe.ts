/**
 * 临时类型扩展文件 - 修复指数订阅功能的类型问题
 */

import type { SyncTask as OriginalSyncTask } from './data';

// SyncTask 已包含所有字段，ExtendedSyncTask 仅补充 column_mapping 相关字段
export interface ExtendedSyncTask extends OriginalSyncTask {
  column_mapping_json?: Record<string, string> | null;
  column_mapping?: Record<string, string> | null;
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
  has_daily: boolean;
  has_weight: boolean;
  subscribed_tasks?: string[];
}

export interface IndexTaskInfo {
  task_id: string;
  task_type: string; // "daily" or "weight"
  enabled: boolean;
  status: string;
  last_sync?: string;
}

export interface IndexSubscriptionStatus {
  index_code: string;
  name?: string;
  has_daily: boolean;
  has_weight: boolean;
  daily_task?: IndexTaskInfo;
  weight_task?: IndexTaskInfo;
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

