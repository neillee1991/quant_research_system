/**
 * Factor-related Types
 */

export interface PreprocessOptions {
  adjust_price: 'none' | 'forward' | 'backward';
  filter_st: boolean;
  filter_new_stock: boolean;
  new_stock_days: number;
  mark_limit: boolean;
}

export interface DataFieldMapping {
  field_key: string;
  field_name?: string;
  table_name: string;
  column_name: string;
  description?: string;
  extra_config?: string;
}

export interface FactorDefinition {
  factor_id: string;
  factor_name: string;
  category: string;
  description?: string;
  code?: string;
  compute_mode?: string;
  params?: Record<string, unknown>;
  depends_on?: string[];
  lookback_days?: number;
  align_calendar?: boolean;
  created_at?: string;
  updated_at?: string;
  enabled?: boolean;
  latest_date?: string;
  source?: 'code' | 'db';
}

export interface FactorValue {
  ts_code: string;
  trade_date: string;
  factor_id: string;
  factor_value: number | null;
  quality_flag?: number;
  created_at?: string;
}

export interface FactorAnalysisResult {
  factor_id: string;
  ic_mean?: number;
  ic_std?: number;
  ic_ir?: number;
  rank_ic_mean?: number;
  rank_ic_std?: number;
  rank_ic_ir?: number;
  turnover?: number;
  coverage?: number;
  total_rows?: number;
  stock_count?: number;
  min_date?: string;
  max_date?: string;
  mean_val?: number;
  std_val?: number;
  min_val?: number;
  max_val?: number;
  [key: string]: unknown;
}

export interface FactorComputeRequest {
  factor_id: string;
  start_date?: string;
  end_date?: string;
  mode?: 'full' | 'incremental';
  preprocess?: PreprocessOptions;
}

export interface FactorListResponse {
  factors: FactorDefinition[];
  total: number;
}

export interface FactorRunResponse {
  run_id: string;
  factor_id: string;
  status: string;
  message?: string;
}

export interface FactorDataConfigItem {
  field_name: string;
  source_table: string;
  source_field: string;
  description?: string;
}
