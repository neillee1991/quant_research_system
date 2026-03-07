/**
 * FactorCenter 内部类型定义
 */

import type {
  PreprocessOptions,
  FactorDefinition,
  FactorRunRecord,
} from '../../types';

export interface FactorCodeInfo {
  filename: string;
  code: string;
}

export interface DataConfigLabel {
  source_label: string;
  values?: Record<string, string>;
}

export interface FactorDrawerProps {
  factor: FactorDefinition | null;
  open: boolean;
  initialTab?: string;
  onClose: () => void;
  onSaved: () => void;
}

export interface TestLog {
  level: string;
  phase: string;
  message: string;
}

export interface TestStats {
  total_rows?: number;
  stock_count?: number;
  factor_mean?: number;
  factor_std?: number;
  null_count?: number;
}

export interface TestResultData {
  ts_code: string;
  trade_date: string;
  factor_value: number | null;
  [key: string]: unknown;
}

export interface TestResult {
  stats?: TestStats;
  preview?: TestResultData[];
  truncated?: boolean;
  stocks?: string[];
  dates?: string[];
}

export interface FactorManageState {
  factors: FactorDefinition[];
  history: FactorRunRecord[];
  loading: boolean;
  runLoading: string | null;
  selectedFactor: string | null;
  selectedRowKeys: string[];
  batchLoading: boolean;
}

export interface AnalysisState {
  factors: any[];
  indexPools: any[];
  selectedFactor: string;
  periods: number[];
  quantiles: number;
  startDate: string;
  endDate: string;
  indexPool: string;
  groupbyField: string;
  useAlphalens: boolean;
  analysisResult: any;
  loading: boolean;
  runLoading: boolean;
  analysisHistory: any[];
  historyLoading: boolean;
}

export const CODE_TEMPLATE = `"""自定义因子"""
import polars as pl
from engine.production.registry import factor


@factor(
    "factor_custom_01",
    description="自定义因子",
    depends_on=["sync_daily_data"],
    category="custom",
    params={"window": 20, "lookback_days": 40},
)
def compute_custom(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 20)
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close").rolling_mean(window_size=w).over("ts_code").alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
    )
`;

export const formatRunParams = (record: FactorRunRecord): string => {
  const parts: string[] = [];
  const start = record.start_date || '';
  const end = record.end_date || '';
  if (start || end) parts.push(`${start}~${end}`);
  if (record.preprocess) {
    try {
      const pp = typeof record.preprocess === 'string' ? JSON.parse(record.preprocess) : record.preprocess;
      const adjMap: Record<string, string> = { forward: '前复权', backward: '后复权', none: '不复权' };
      if (pp.adjust_price) parts.push(adjMap[pp.adjust_price] || pp.adjust_price);
      if (pp.filter_st === false) parts.push('含ST');
      if (pp.filter_new_stock === false) parts.push('含次新');
      if (pp.handle_suspension === false) parts.push('含停牌');
      if (pp.mark_limit === false) parts.push('不标涨跌停');
    } catch {
      // Ignore parse errors
    }
  }
  return parts.join(' | ') || '-';
};
