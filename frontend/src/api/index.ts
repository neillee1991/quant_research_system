import axios from 'axios';
import type { DataFieldMapping } from '../types/factor';
import type {
  ConfigType,
  ImportMode,
  ConfigTypeOption,
  ExportRequest,
  ExportResponse,
  ImportVerifyRequest,
  ImportVerifyResponse,
  ImportApplyRequest,
  ImportApplyResponse,
} from '../pages/ConfigManagement/types';

// 因子计算预处理选项
export interface PreprocessOptions {
  adjust_price: 'none' | 'forward' | 'backward';  // 复权方式
  filter_st: boolean;           // 过滤 ST/*ST
  filter_new_stock: boolean;    // 过滤新股
  new_stock_days: number;       // 新股排除天数
  mark_limit: boolean;          // 标记涨跌停
}

export const DEFAULT_PREPROCESS: PreprocessOptions = {
  adjust_price: 'forward',
  filter_st: true,
  filter_new_stock: true,
  new_stock_days: 60,
  mark_limit: true,
};

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 60000, // 增加到 60 秒
});

// 为长时间运行的操作创建单独的实例
const longRunningApi = axios.create({
  baseURL: '/api/v1',
  timeout: 300000, // 5 分钟，用于数据同步等长时间操作
});

export const dataApi = {
  listStocks: () => api.get('/data/stocks'),
  getDaily: (tsCode: string, startDate?: string, endDate?: string, limit = 500) =>
    api.get('/data/daily', { params: { ts_code: tsCode, start_date: startDate, end_date: endDate, limit } }),

  // 同步任务管理
  listSyncTasks: () => api.get('/tasks/sync'),
  syncTask: (taskId: string, targetDate?: string, startDate?: string, endDate?: string) =>
    longRunningApi.post(`/tasks/sync/${taskId}/execute`, {
      start_date: startDate || targetDate,
      end_date: endDate,
    }),
  getTaskConfig: (taskId: string) => api.get(`/tasks/sync/${taskId}`),
  getSyncTaskStatus: (taskId: string) => api.get(`/tasks/sync/${taskId}/status`),
  createSyncTask: (config: any) => api.post('/tasks/sync', { config_data: config }),
  updateSyncTask: (taskId: string, config: any) => api.put(`/tasks/sync/${taskId}`, { config_data: config }),
  deleteTask: (taskId: string, dropTable?: boolean) => api.delete(`/tasks/sync/${taskId}`, { params: { drop_table: dropTable } }),

  // ETL 任务管理
  listEtlTasks: () => api.get('/tasks/etl'),
  createEtlTask: (config: any) => api.post('/data/etl/tasks', config),
  updateEtlTask: (taskId: string, config: any) => api.put(`/data/etl/task/${taskId}`, config),
  deleteEtlTask: (taskId: string, dropTable?: boolean) => api.delete(`/tasks/etl/${taskId}`, { params: { drop_table: dropTable } }),
  getEtlTaskStatus: (taskId: string) => api.get(`/tasks/etl/${taskId}/status`),
  getEtlTableSchema: (taskId: string) => api.get(`/tasks/etl/${taskId}/schema`),
  runEtlTask: (taskId: string) => longRunningApi.post(`/tasks/etl/${taskId}/execute`),
  testEtlScript: (script: string, date?: string) => api.post('/tasks/etl/test', { script, date }),
  backfillEtlTask: (taskId: string, startDate: string, endDate: string) =>
    longRunningApi.post(`/tasks/etl/${taskId}/backfill`, null, {
      params: { start_date: startDate, end_date: endDate }
    }),
  createEtlTable: (taskId: string, tableName: string, fields: any[]) =>
    api.post(`/tasks/etl/${taskId}/create-table`, { table_name: tableName, fields }),

  // 数据库管理
  listTables: () => api.get('/data/tables'),
  getTableInfo: (tableName: string) => api.get(`/data/tables/${tableName}/info`),
  truncateTable: (tableName: string) => api.delete(`/data/tables/${tableName}`),
  executeQuery: (sql: string, limit = 1000) =>
    api.post('/data/query', null, { params: { sql, limit } }),
};

export const strategyApi = {
  backtest: (graph: object) => longRunningApi.post('/strategy/backtest', { graph }),
  backtestAsync: (name: string, graph: object) =>
    api.post('/strategy/backtest/async', { name, graph }),
  getBacktestResult: (runId: string) =>
    api.get(`/strategy/backtest/${runId}/result`),
};

export const mlApi = {
  train: (payload: {
    ts_code: string;
    start_date?: string;
    end_date?: string;
    feature_cols?: string[];
    task?: string;
  }) => longRunningApi.post('/ml/train', payload), // 模型训练耗时长
  getStatus: (jobId: string) => api.get(`/ml/status/${jobId}`),
  getWeights: () => api.get('/ml/weights'),
};

export const productionApi = {
  // 因子 CRUD
  listFactors: () => api.get('/factor/factors'),
  createFactor: (data: { factor_id: string; description?: string; category?: string; compute_mode?: string; depends_on?: string[]; storage_target?: string; params?: Record<string, any>; code?: string; align_calendar?: boolean }) =>
    api.post('/factor/factors', data),
  updateFactor: (factorId: string, data: { description?: string; category?: string; compute_mode?: string; depends_on?: string[]; storage_target?: string; params?: Record<string, any>; align_calendar?: boolean }) =>
    api.put(`/factor/factors/${factorId}`, data),
  deleteFactor: (factorId: string, deleteData = false) =>
    api.delete(`/factor/factors/${factorId}`, { params: { delete_data: deleteData } }),

  // 生产任务
  runProduction: (factorId: string, mode = 'incremental', targetDate?: string, startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/factor/run', { factor_id: factorId, mode, target_date: targetDate, start_date: startDate, end_date: endDate, preprocess }),
  batchRunFactors: (factorIds: string[], mode = 'incremental', startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/factor/batch-run', { factor_ids: factorIds, mode, start_date: startDate, end_date: endDate, preprocess }),

  // 因子代码查看/编辑
  getFactorCode: (factorId: string) => api.get(`/factor/factors/${factorId}/code`),
  updateFactorCode: (factorId: string, filename: string, code: string) =>
    api.put(`/factor/factors/${factorId}/code`, { filename, code }),

  // DataFrame schema 预览
  getDataFrameSchema: (dependsOn: string[]) =>
    api.post('/factor/dataframe-schema', { depends_on: dependsOn }),

  // 因子代码测试
  testFactorCode: (data: { code: string; start_date: string; end_date: string; depends_on?: string[]; params?: Record<string, any>; preprocess?: PreprocessOptions; lookback_days?: number }) =>
    longRunningApi.post('/factor/factors/test', data),

  // 因子数据探查
  getFactorData: (factorId: string, params?: { start_date?: string; end_date?: string; ts_code?: string; limit?: number }) =>
    api.get(`/factor/factors/${factorId}/data`, { params }),
  getFactorStats: (factorId: string) => api.get(`/factor/factors/${factorId}/stats`),

  // 数据配置
  getDataConfig: () => api.get('/factor/data-config'),
  updateDataConfig: (mappings: DataFieldMapping[]) => api.put('/factor/data-config', { mappings }),
  getResolvedDataConfig: () => api.get('/factor/data-config/resolved'),
  getAvailableTables: () => api.get('/factor/available-tables'),

  // 指数股票池管理
  listIndexPools: () => api.get('/factor/index-pool/list'),
  getIndexPool: (indexCode: string, tradeDate?: string) =>
    api.get(`/factor/index-pool/${indexCode}`, { params: { trade_date: tradeDate } }),
  batchUploadIndexPool: (data: { index_code: string; index_name?: string; description?: string; data: any[] }) =>
    api.post('/factor/index-pool/batch-upload', data),
  csvUploadIndexPool: (data: { index_code: string; index_name?: string; description?: string; csv_content: string }) =>
    api.post('/factor/index-pool/csv-upload', data),
  deleteIndexPool: (indexCode: string) => api.delete(`/factor/index-pool/${indexCode}`),
  downloadIndexPoolTemplate: () => api.get('/factor/index-pool/template', { responseType: 'text' }),

  // Alphalens 分析 API
  runAlphalensAnalysis: (data: {
    factor_id: string;
    start_date: string;
    end_date: string;
    periods?: number[];
    quantiles?: number;
    index_pool?: string;
    groupby_field?: string;
    next_day_entry?: boolean;
    entry_price?: string;
    neutralize?: boolean;
    neutralize_controls?: string[];
    industry_level?: string;
    winsorize?: boolean;
    winsorize_lower?: number;
    winsorize_upper?: number;
  }) => longRunningApi.post('/factor/analysis/alphalens', data),
  getLatestAlphalensAnalysis: (factorId: string) => api.get(`/factor/analysis/${factorId}/latest`),
  getAlphalensAnalysisById: (factorId: string, analysisId: string) => api.get(`/factor/analysis/${factorId}/detail/${analysisId}`),
  deleteAlphalensAnalysisById: (factorId: string, analysisId: string) => api.delete(`/factor/analysis/${factorId}/detail/${analysisId}`),
  getAnalysisTaskStatus: (taskId: string) =>
    api.get(`/factor/analysis/status/${taskId}`),
  getTradingDays: (start: string, end: string) =>
    api.get('/factor/analysis/trading-days', { params: { start, end } }),
};

// Data API - 指数订阅管理
export const indexApi = {
  // 获取可订阅的指数列表
  listAvailableIndices: (params?: {
    page?: number;
    limit?: number;
    search?: string;
    filters?: Record<string, string>;
    show_subscribed_only?: boolean;
  }) => {
    const { filters, ...rest } = params || {};
    return api.get('/data/index/available', {
      params: {
        ...rest,
        filters: filters ? JSON.stringify(filters) : undefined,
      },
    });
  },

  // 订阅指数
  subscribeIndex: (data: { index_code: string }) =>
    api.post('/data/index/subscribe', data),

  // 取消订阅指数
  unsubscribeIndex: (indexCode: string) =>
    api.delete(`/data/index/subscribe/${indexCode}`),

  // 获取用户偏好配置
  getUserPreference: () =>
    api.get('/data/index/preference'),

  // 保存用户偏好配置
  saveUserPreference: (data: { index_basic_table: string; filter_config?: Array<{ field: string; label: string; enabled: boolean; default_value: string | null }> }) =>
    api.post('/data/index/preference', data),
};

// Flow 配置管理
export interface TaskConfig {
  id: string;
  type: 'sync' | 'etl' | 'factor' | 'flow';
  depends_on: string[];
  flow_name?: string;  // 当 type='flow' 时，指定嵌套的 flow 名称
}

export interface FlowConfig {
  name: string;
  description: string;
  cron?: string;  // 现在是可选的
  timezone?: string;
  tags: string[];
  enabled: boolean;
  date_offset_days: number;
  tasks: TaskConfig[];
  version?: number;
}

export interface FlowListItem {
  name: string;
  description: string;
  cron?: string;
  tags: string[];
  enabled: boolean;
  date_offset_days: number;
  task_count: number;
  updated_at: string;
  version?: number;
}

// Flow Run 相关类型
export interface FlowRun {
  flow_run_id: string;
  flow_name: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'cancelled';
  trigger_type: 'scheduled' | 'manual';
  target_date: string;
  started_at?: string;
  finished_at?: string;
  duration_sec?: number;
  error?: string;
}

export interface FlowTaskRun {
  task_run_id: string;
  flow_run_id: string;
  task_id: string;
  task_type: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'skipped';
  started_at?: string;
  finished_at?: string;
  duration_sec?: number;
  error?: string;
}

export interface FlowRunDetail {
  flow_run: {
    id: number;
    flow_name: string;
    status: string;
    trigger_type: string;
    target_date?: string;
    started_at?: string;
    ended_at?: string;
    duration_sec?: number;
    error_message?: string;
  };
  tasks: {
    run_id: string;
    task_id: string;
    task_type: string;
    status: string;
    started_at?: string;
    finished_at?: string;
    elapsed_sec?: number;
    rows?: number;
    params?: string;
    extra?: string;
    error?: string;
  }[];
}

export const flowApi = {
  list: (enabledOnly = false) => api.get<FlowListItem[]>('/flows', { params: { enabled_only: enabledOnly } }),
  get: (name: string) => api.get<FlowConfig>(`/flows/${name}`),
  create: (config: FlowConfig) => api.post<FlowConfig>('/flows', config),
  update: (name: string, config: Partial<FlowConfig>) => api.put<FlowConfig>(`/flows/${name}`, config),
  delete: (name: string, hard = false) => api.delete(`/flows/${name}`, { params: { hard } }),
  trigger: (name: string, targetDate?: string) =>
    api.post<{ status: string; flow_run_id: string }>(`/flows/${name}/trigger`, null, { params: { target_date: targetDate } }),
  backfill: (name: string, startDate: string, endDate: string) =>
    api.post(`/flows/${name}/backfill`, { start_date: startDate, end_date: endDate }),
  listRuns: (name: string, limit = 50) =>
    api.get<{ status: string; data: FlowRun[] }>(`/flows/${name}/runs`, { params: { limit } }),
  getRunDetail: (name: string, flowRunId: string) =>
    api.get<FlowRunDetail>(`/flows/${name}/runs/${flowRunId}`),
  inferDependencies: (tasks: TaskConfig[]) =>
    api.post<{ status: string; tasks: TaskConfig[] }>('/flows/infer-dependencies', tasks),
};

// 任务监控 API
export interface RunningTask {
  run_id: string;
  task_id: string;
  task_type: 'factor' | 'sync' | 'etl' | 'analysis' | 'backtest';
  task_name: string;
  status: 'running' | 'success' | 'failed' | string;
  started_at?: string;
  finished_at?: string | null;
  elapsed_sec?: number | null;
  rows?: number | null;
  error?: string | null;
  params?: string;
}

export interface TaskRun {
  run_id: string;
  task_type: 'factor' | 'sync' | 'etl' | 'analysis' | 'backtest';
  task_id: string;
  task_name: string;
  status: 'running' | 'success' | 'failed';
  started_at: string;
  finished_at: string | null;
  elapsed_sec: number | null;
  rows: number | null;
  error: string | null;
  params: string;
  extra?: string | null;
}

export interface RunningTasksResponse {
  tasks: RunningTask[];
  total: number;
}

export interface TaskHistoryResponse {
  tasks: TaskRun[];
  total: number;
}

export const taskMonitorApi = {
  getRunningTasks: (taskType?: string, taskId?: string) =>
    api.get<RunningTasksResponse>('/tasks/running', { params: { task_type: taskType, task_id: taskId } }),
  getTaskHistory: (limit = 50, taskType?: string, taskId?: string, startDate?: string, endDate?: string) =>
    api.get<TaskHistoryResponse>('/tasks/history', { params: { limit, task_type: taskType, task_id: taskId, start_date: startDate, end_date: endDate } }),
  cleanupStale: (timeoutMinutes = 0) =>
    api.post('/tasks/cleanup', null, { params: { timeout_minutes: timeoutMinutes } }),
  getTaskStatus: (taskType: string, runId: string) =>
    api.get(`/tasks/${taskType}/status/${runId}`),
};

// 配置管理 API
export const configApi = {
  getConfigTypes: () => api.get<ConfigTypeOption[]>('/config/types'),
  exportConfigs: (data: ExportRequest) => api.post<ExportResponse>('/config/export', data),
  verifyImport: (data: ImportVerifyRequest) => api.post<ImportVerifyResponse>('/config/import/verify', data),
  applyImport: (data: ImportApplyRequest) => api.post<ImportApplyResponse>('/config/import/apply', data),
};

export default api;
