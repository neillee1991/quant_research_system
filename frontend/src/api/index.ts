import axios from 'axios';
import type { DataFieldMapping } from '../types/factor';

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
  triggerSync: (tsCode?: string, source = 'tushare') =>
    longRunningApi.post('/data/sync', null, { params: { ts_code: tsCode, source } }), // 使用长超时

  // 同步任务管理
  listSyncTasks: () => api.get('/data/sync/tasks'),
  syncTask: (taskId: string, targetDate?: string, startDate?: string, endDate?: string) =>
    longRunningApi.post(`/data/sync/task/${taskId}`, null, {
      params: {
        target_date: targetDate,
        start_date: startDate,
        end_date: endDate
      }
    }),
  syncAllTasks: (targetDate?: string) =>
    longRunningApi.post('/data/sync/all', null, { params: { target_date: targetDate } }),
  getTaskConfig: (taskId: string) => api.get(`/data/sync/task/${taskId}/config`),
  updateTaskConfig: (taskId: string, config: any) => api.put(`/data/sync/task/${taskId}/config`, config),
  createTask: (config: any) => api.post('/data/sync/tasks', config),
  createSyncTask: (config: any) => api.post('/data/sync/tasks', config),
  updateSyncTask: (taskId: string, config: any) => api.put(`/data/sync/task/${taskId}/config`, config),
  createSyncTaskTable: (taskId: string) => api.post(`/data/sync/task/${taskId}/create-table`),
  deleteTask: (taskId: string, dropTable?: boolean) => api.delete(`/data/sync/tasks/${taskId}`, { params: { drop_table: dropTable } }),

  // ETL 任务管理
  listEtlTasks: () => api.get('/data/etl/tasks'),
  createEtlTask: (config: any) => api.post('/data/etl/tasks', config),
  updateEtlTask: (taskId: string, config: any) => api.put(`/data/etl/task/${taskId}`, config),
  deleteEtlTask: (taskId: string, dropTable?: boolean) => api.delete(`/data/etl/task/${taskId}`, { params: { drop_table: dropTable } }),
  getEtlTaskStatus: (taskId: string) => api.get(`/data/etl/task/${taskId}/status`),
  getEtlTableSchema: (taskId: string) => api.get(`/data/etl/task/${taskId}/schema`),
  runEtlTask: (taskId: string) => longRunningApi.post(`/data/etl/task/${taskId}/run`),
  testEtlScript: (script: string, date?: string) => api.post('/data/etl/test', { script, date }),
  backfillEtlTask: (taskId: string, startDate: string, endDate: string) =>
    longRunningApi.post(`/data/etl/task/${taskId}/backfill`, null, {
      params: { start_date: startDate, end_date: endDate }
    }),
  createEtlTable: (taskId: string, tableName: string, fields: any[]) =>
    api.post(`/data/etl/task/${taskId}/create-table`, { table_name: tableName, fields }),

  // 数据库管理
  listTables: () => api.get('/data/tables'),
  getTableInfo: (tableName: string) => api.get(`/data/tables/${tableName}/info`),
  truncateTable: (tableName: string) => api.delete(`/data/tables/${tableName}`),
  executeQuery: (sql: string, limit = 1000) =>
    api.post('/data/query', null, { params: { sql, limit } }),

  // 调度管理
  startScheduler: () => api.post('/data/sync/scheduler/start'),
  stopScheduler: () => api.post('/data/sync/scheduler/stop'),
  loadSchedules: () => api.post('/data/sync/scheduler/load'),
  getAllSchedules: () => api.get('/data/sync/scheduler/schedules'),
  enableTaskSchedule: (taskId: string, schedule: string, cronExpression?: string) =>
    api.post(`/data/sync/scheduler/task/${taskId}/enable`, null, {
      params: { schedule, cron_expression: cronExpression }
    }),
  disableTaskSchedule: (taskId: string) => api.post(`/data/sync/scheduler/task/${taskId}/disable`),
  getTaskScheduleInfo: (taskId: string) => api.get(`/data/sync/scheduler/task/${taskId}`),
};

export const factorApi = {
  compute: (payload: { ts_code: string; start_date?: string; end_date?: string; factors: string[] }) =>
    longRunningApi.post('/factor/compute', payload), // 因子计算可能耗时
  ic: (payload: { ts_code: string; start_date?: string; end_date?: string; factors: string[] }) =>
    api.post('/factor/ic', payload),
};

export const strategyApi = {
  backtest: (graph: object) => longRunningApi.post('/strategy/backtest', { graph }), // 回测可能耗时
  listOperators: () => api.get('/strategy/operators'),
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
  getProductionHistory: (factorId?: string, limit = 20, startDate?: string, endDate?: string) =>
    api.get('/factor/history', { params: { factor_id: factorId, limit, start_date: startDate, end_date: endDate } }),

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
  getFactorMissingDates: (factorId: string) => api.get(`/factor/factors/${factorId}/missing-dates`),

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
  getAlphalensAnalysisHistory: (factorId: string, limit = 20, offset = 0) =>
    api.get(`/factor/analysis/${factorId}/history`, { params: { limit, offset } }),
  getAnalysisTaskStatus: (taskId: string) =>
    api.get(`/factor/analysis/status/${taskId}`),
  getTradingDays: (start: string, end: string) =>
    api.get('/factor/analysis/trading-days', { params: { start, end } }),
};

export const stockPoolApi = {
  // Pool CRUD
  listPools: (params?: { pool_type?: string; status?: string; page?: number; limit?: number }) =>
    api.get('/stock-pool/pools', { params }),
  getPool: (poolId: string) =>
    api.get(`/stock-pool/pools/${poolId}`),
  createPool: (data: any) =>
    api.post('/stock-pool/pools', data),
  updatePool: (poolId: string, data: any) =>
    api.put(`/stock-pool/pools/${poolId}`, data),
  setPoolStatus: (poolId: string, status: string, reason?: string) =>
    api.post(`/stock-pool/pools/${poolId}/status`, { status, reason }),
  archivePool: (poolId: string) =>
    api.delete(`/stock-pool/pools/${poolId}`),

  // Index subscription (legacy - keep for compatibility)
  listAvailableIndexes: (params?: { search?: string; market?: string; publisher?: string; page?: number; limit?: number }) =>
    api.get('/stock-pool/index/available', { params }),
  getIndexFilterOptions: () =>
    api.get('/stock-pool/index/filter-options'),
  subscribeIndex: (data: { index_code: string; pool_name?: string; auto_sync?: boolean }) =>
    api.post('/stock-pool/pools/index-subscribe', data),

  // Sync & constituents (legacy - keep for compatibility)
  syncPool: (poolId: string, tradeDate?: string) =>
    longRunningApi.post(`/stock-pool/pools/${poolId}/sync`, { trade_date: tradeDate }),
  getConstituents: (poolId: string, tradeDate?: string) =>
    api.get(`/stock-pool/pools/${poolId}/constituents`, { params: { trade_date: tradeDate } }),
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

  // 获取筛选选项
  getFilterOptions: () =>
    api.get('/data/index/filter-options'),

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
  type: 'sync' | 'factor';
  depends_on?: string[];
}

export interface FlowConfig {
  name: string;
  description?: string;
  cron: string;
  tags?: string[];
  enabled?: boolean;
  tasks: TaskConfig[];
}

export interface FlowListItem {
  name: string;
  description: string;
  cron: string;
  tags: string[];
  enabled: boolean;
  task_count: number;
}

export const flowApi = {
  list: () => api.get<FlowListItem[]>('/flows'),
  get: (name: string) => api.get<FlowConfig>(`/flows/${name}`),
  create: (config: FlowConfig) => api.post<FlowConfig>('/flows', config),
  update: (name: string, config: FlowConfig) => api.put<FlowConfig>(`/flows/${name}`, config),
  delete: (name: string) => api.delete(`/flows/${name}`),
  run: (name: string, targetDate?: string) =>
    longRunningApi.post(`/flows/${name}/run`, null, { params: { target_date: targetDate } }),
};

// 任务监控 API
export interface RunningTask {
  run_id: string;
  task_id: string;
  task_type: 'factor' | 'sync' | 'etl' | 'analysis';
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
  task_type: 'factor' | 'sync' | 'etl' | 'analysis';
  task_id: string;
  task_name: string;
  status: 'running' | 'success' | 'failed';
  started_at: string;
  finished_at: string | null;
  elapsed_sec: number | null;
  rows: number | null;
  error: string | null;
  params: string;
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
  getTaskHistory: (limit = 50, taskType?: string, taskId?: string) =>
    api.get<TaskHistoryResponse>('/tasks/history', { params: { limit, task_type: taskType, task_id: taskId } }),
  cleanupStale: (timeoutMinutes = 0) =>
    api.post('/tasks/cleanup', null, { params: { timeout_minutes: timeoutMinutes } }),
  getTaskStatus: (taskType: string, runId: string) =>
    api.get(`/tasks/${taskType}/status/${runId}`),
};

export default api;
