/**
 * Strategy Types
 */

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
