export type PoolType = 'static' | 'dynamic' | 'index' | 'composite';
export type PoolStatus = 'draft' | 'active' | 'paused' | 'error' | 'archived';
export type SyncStatus = 'idle' | 'syncing' | 'success' | 'failed';
export type WeightMethod = 'equal' | 'market_cap' | 'custom' | 'index_native';

export interface PoolMetadata {
  pool_id: string;
  pool_type: PoolType;
  pool_name: string;
  description: string;
  status: PoolStatus;
  version: number;
  weight_method: WeightMethod;
  definition?: Record<string, any>;
  created_at: string;
  updated_at: string;
}

export interface PoolSyncStatus {
  status: SyncStatus;
  last_sync_date?: string;
  last_sync_time?: string;
  error_message?: string;
}

export interface ConstituentItem {
  trade_date: string;
  ts_code: string;
  weight?: number;
  rank?: number;
}

export interface PoolDetail {
  metadata: PoolMetadata;
  constituents?: ConstituentItem[];
  sync_status?: PoolSyncStatus;
  available_dates?: string[];
}

export interface AvailableIndex {
  ts_code: string;
  name: string;
  market?: string;
  publisher?: string;
  list_date?: string;
  desc?: string;
  is_subscribed: boolean;
  pool_id?: string;
  pool_status?: string;
}

export interface SubscribeResult {
  pool_id: string;
  pool_name: string;
  status: PoolStatus;
  index_code: string;
  sync_task_id?: string;
  created_at: string;
}

export interface StockPoolApiResponse<T> {
  success: boolean;
  data?: T;
  error?: { code: string; message: string };
}

export interface StockPoolListResponse<T> {
  items: T[];
  total?: number;
}
