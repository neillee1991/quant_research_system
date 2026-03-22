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
  getSyncStatus: (source?: string, dataType?: string, startDate?: string, endDate?: string, limit = 1000) =>
    api.get('/data/sync/status', { params: { source, data_type: dataType, start_date: startDate, end_date: endDate, limit } }),

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
  getTaskStatus: (taskId: string) => api.get(`/data/sync/status/${taskId}`),
  getTaskStatusBatch: () => api.get('/data/sync/tasks/status-batch'),
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
  getEtlLogs: (taskId?: string, startDate?: string, endDate?: string, limit = 1000) =>
    api.get('/data/etl/logs', { params: { task_id: taskId, start_date: startDate, end_date: endDate, limit } }),

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
  listFactors: () => api.get('/production/factors'),
  createFactor: (data: { factor_id: string; description?: string; category?: string; compute_mode?: string; depends_on?: string[]; storage_target?: string; params?: Record<string, any>; code?: string; align_calendar?: boolean }) =>
    api.post('/production/factors', data),
  updateFactor: (factorId: string, data: { description?: string; category?: string; compute_mode?: string; depends_on?: string[]; storage_target?: string; params?: Record<string, any>; align_calendar?: boolean }) =>
    api.put(`/production/factors/${factorId}`, data),
  deleteFactor: (factorId: string, deleteData = false) =>
    api.delete(`/production/factors/${factorId}`, { params: { delete_data: deleteData } }),

  // 生产任务
  runProduction: (factorId: string, mode = 'incremental', targetDate?: string, startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/production/run', { factor_id: factorId, mode, target_date: targetDate, start_date: startDate, end_date: endDate, preprocess }),
  batchRunFactors: (factorIds: string[], mode = 'incremental', startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/production/batch-run', { factor_ids: factorIds, mode, start_date: startDate, end_date: endDate, preprocess }),
  getProductionHistory: (factorId?: string, limit = 20, startDate?: string, endDate?: string) =>
    api.get('/production/history', { params: { factor_id: factorId, limit, start_date: startDate, end_date: endDate } }),

  // 因子代码查看/编辑
  getFactorCode: (factorId: string) => api.get(`/production/factors/${factorId}/code`),
  updateFactorCode: (factorId: string, filename: string, code: string) =>
    api.put(`/production/factors/${factorId}/code`, { filename, code }),

  // DataFrame schema 预览
  getDataFrameSchema: (dependsOn: string[]) =>
    api.post('/production/dataframe-schema', { depends_on: dependsOn }),

  // 因子代码测试
  testFactorCode: (data: { code: string; start_date: string; end_date: string; depends_on?: string[]; params?: Record<string, any>; preprocess?: PreprocessOptions; lookback_days?: number }) =>
    longRunningApi.post('/production/factors/test', data),

  // 因子数据探查
  getFactorData: (factorId: string, params?: { start_date?: string; end_date?: string; ts_code?: string; limit?: number }) =>
    api.get(`/production/factors/${factorId}/data`, { params }),
  getFactorStats: (factorId: string) => api.get(`/production/factors/${factorId}/stats`),
  getFactorMissingDates: (factorId: string) => api.get(`/production/factors/${factorId}/missing-dates`),

  // 数据配置
  getDataConfig: () => api.get('/production/data-config'),
  updateDataConfig: (mappings: DataFieldMapping[]) => api.put('/production/data-config', { mappings }),
  getResolvedDataConfig: () => api.get('/production/data-config/resolved'),
  getAvailableTables: () => api.get('/production/available-tables'),

  // 指数股票池管理
  listIndexPools: () => api.get('/index-pool/list'),
  getIndexPool: (indexCode: string, tradeDate?: string) =>
    api.get(`/index-pool/${indexCode}`, { params: { trade_date: tradeDate } }),
  batchUploadIndexPool: (data: { index_code: string; index_name?: string; description?: string; data: any[] }) =>
    api.post('/index-pool/batch-upload', data),
  csvUploadIndexPool: (data: { index_code: string; index_name?: string; description?: string; csv_content: string }) =>
    api.post('/index-pool/csv-upload', data),
  deleteIndexPool: (indexCode: string) => api.delete(`/index-pool/${indexCode}`),
  downloadIndexPoolTemplate: () => api.get('/index-pool/template', { responseType: 'text' }),

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
  }) => longRunningApi.post('/analysis/alphalens', data),
  getLatestAlphalensAnalysis: (factorId: string) => api.get(`/analysis/alphalens/${factorId}/latest`),
  getAlphalensAnalysisById: (factorId: string, analysisId: string) => api.get(`/analysis/alphalens/${factorId}/detail/${analysisId}`),
  deleteAlphalensAnalysisById: (factorId: string, analysisId: string) => api.delete(`/analysis/alphalens/${factorId}/detail/${analysisId}`),
  getAlphalensAnalysisHistory: (factorId: string, limit = 20, offset = 0) =>
    api.get(`/analysis/alphalens/${factorId}/history`, { params: { limit, offset } }),
  getAnalysisTaskStatus: (taskId: string) =>
    api.get(`/analysis/alphalens/status/${taskId}`),
  getTradingDays: (start: string, end: string) =>
    api.get('/analysis/trading-days', { params: { start, end } }),
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

export default api;
