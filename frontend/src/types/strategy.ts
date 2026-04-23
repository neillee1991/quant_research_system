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
    direction: "long" | "short";
    entry_date: string;
    entry_price: number;
    exit_date?: string;
    exit_price?: number;
    quantity: number;
    pnl?: number;
    pnl_pct?: number;
    status: "open" | "closed";
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

export type ScriptRunStatus =
    | "idle"
    | "validating"
    | "compiling"
    | "submitting"
    | "running"
    | "success"
    | "failed";

interface PipelineStage {
    type: string;
    op?: string;
    output_col?: string;
    params?: Record<string, unknown>;
}

interface DataSource {
    ts_code?: string;
    start_date?: string;
    end_date?: string;
}

export interface StrategyPlaceholderIR {
    version: string;
    source_type: "script";
    language: string;
    entry_point: string;
    pipeline_version?: string;
    pipeline?: PipelineStage[];
    data_source?: DataSource;
}

export interface StrategyScriptValidateResponse {
    valid: boolean;
    language: string;
    script_hash: string;
    warnings: string[];
    errors: string[];
}

export interface StrategyScriptCompileSuccessResponse {
    status: "compiled";
    script_hash: string;
    ir: StrategyPlaceholderIR;
    warnings: string[];
}

export interface StrategyScriptCompileFailureResponse {
    status: "failed";
    script_hash: string;
    errors: string[];
}

export type StrategyScriptCompileResponse = StrategyScriptCompileSuccessResponse | StrategyScriptCompileFailureResponse;

export interface StrategyScriptBacktestRequest {
    script: string;
    name?: string;
    language?: string;
    entry_point?: string;
    params?: Record<string, unknown>;
}

export interface MLTrainRequest {
    ts_code: string;
    task: "full" | "incremental";
    start_date?: string;
    end_date?: string;
    model_type?: string;
    params?: Record<string, unknown>;
}

export interface MLJobStatus {
    job_id: string;
    status: "queued" | "running" | "done" | "failed";
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
    status: "queued" | "running" | "done" | "failed";
    progress?: number;
    message?: string;
    error?: string;
}

export interface MLWeightsResponse {
    weights: MLWeights;
    model_type?: string;
    trained_at?: string;
}
