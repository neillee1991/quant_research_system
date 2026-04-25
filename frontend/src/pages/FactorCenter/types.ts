/**
 * FactorCenter 内部类型定义
 */

import type {
  PreprocessOptions,
  FactorDefinition,
} from '../../types';
import type { TaskRun } from '../../api';

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
  count?: number;
  null_count?: number;
  null_ratio?: number;
  min?: number;
  max?: number;
  mean?: number;
  std?: number;
  median?: number;
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
  history: TaskRun[];
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

// 格式化日期：YYYYMMDD -> YYYY-MM-DD
const formatDate = (date: string | null | undefined): string => {
  if (!date) return '';
  const str = String(date);
  if (str.length === 8) {
    return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
  }
  return str;
};

export const formatRunParams = (record: TaskRun): string => {
  const parts: string[] = [];
  try {
    let p;
    if (typeof record.params === 'string') {
      // 如果是字符串，尝试解析为 JSON 对象
      p = record.params ? JSON.parse(record.params) : {};
    } else if (typeof record.params === 'object') {
      // 如果是对象，直接使用
      p = record.params || {};
    } else {
      // 如果是其他类型，使用空对象
      p = {};
    }
    const start = formatDate(p.start_date);
    const end = formatDate(p.end_date);
    if (start || end) parts.push(`${start}~${end}`);
    if (p.mode) parts.push(p.mode === 'incremental' ? '增量' : '全量');
  } catch {
    // Ignore parse errors
  }
  return parts.join(' | ') || '-';
};
