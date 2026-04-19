/**
 * Strategy and Backtest Types
 */

export interface BacktestMetrics {
  total_return: number;
  annual_return: number;
  sharpe_ratio: number;
  max_drawdown: number;
  win_rate: number;
  total_trades: number;
  profit_factor?: number;
  calmar_ratio?: number;
  sortino_ratio?: number;
  [key: string]: number | undefined;
}

export interface EquityPoint {
  date: string;
  equity: number;
  drawdown?: number;
  benchmark?: number;
}

export interface BacktestResult {
  metrics: BacktestMetrics;
  equity_curve: EquityPoint[];
  trades?: Trade[];
  positions?: Position[];
  start_date: string;
  end_date: string;
  initial_capital: number;
}

export interface Trade {
  trade_id: string;
  ts_code: string;
  direction: 'long' | 'short';
  entry_date: string;
  entry_price: number;
  exit_date?: string;
  exit_price?: number;
  quantity: number;
  pnl?: number;
  pnl_pct?: number;
  status: 'open' | 'closed';
}

export interface Position {
  ts_code: string;
  quantity: number;
  avg_price: number;
  current_price: number;
  market_value: number;
  pnl: number;
  pnl_pct: number;
  weight: number;
}

export interface FlowNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: FlowNodeData;
}

export interface FlowNodeData {
  label: string;
  config?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
}

export interface FlowDefinition {
  nodes: FlowNode[];
  edges: FlowEdge[];
  name?: string;
  description?: string;
}

export type StrategyMode = 'graph' | 'code';

export interface StrategyPlaceholderIR {
  source_type: 'script';
  language: string;
  entry_point: string;
  pipeline_version: string;
}

export interface StrategyScriptValidateResponse {
  valid: boolean;
  language: string;
  script_hash: string;
  warnings: string[];
  errors: string[];
}

export interface StrategyScriptCompileResponse {
  status: 'compiled';
  script_hash: string;
  ir: StrategyPlaceholderIR;
  warnings: string[];
}

export interface StrategyScriptBacktestRequest {
  script: string;
  name?: string;
  language?: string;
  entry_point?: string;
  params?: Record<string, unknown>;
}

export interface MLTrainRequest {
  ts_code: string;
  task: 'full' | 'incremental';
  start_date?: string;
  end_date?: string;
  model_type?: string;
  params?: Record<string, unknown>;
}

export interface MLJobStatus {
  job_id: string;
  status: 'queued' | 'running' | 'done' | 'failed';
  progress?: number;
  message?: string;
  error?: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
}

export interface MLWeights {
  [factor_id: string]: number;
}

export interface MLTrainResponse {
  job_id: string;
  status: string;
  message?: string;
}

export interface MLStatusResponse {
  job_id: string;
  status: 'queued' | 'running' | 'done' | 'failed';
  progress?: number;
  message?: string;
  error?: string;
}

export interface MLWeightsResponse {
  weights: MLWeights;
  model_type?: string;
  trained_at?: string;
}
