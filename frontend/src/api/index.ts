import axios from 'axios';

// 因子计算预处理选项
export interface PreprocessOptions {
  adjust_price: 'none' | 'forward' | 'backward';  // 复权方式
  filter_st: boolean;           // 过滤 ST/*ST
  filter_new_stock: boolean;    // 过滤新股
  new_stock_days: number;       // 新股排除天数
  handle_suspension: boolean;   // 停牌复牌处理
  mark_limit: boolean;          // 标记涨跌停
}

export const DEFAULT_PREPROCESS: PreprocessOptions = {
  adjust_price: 'forward',
  filter_st: true,
  filter_new_stock: true,
  new_stock_days: 60,
  handle_suspension: true,
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
  getTaskConfig: (taskId: string) => api.get(`/data/sync/task/${taskId}/config`),
  updateTaskConfig: (taskId: string, config: any) => api.put(`/data/sync/task/${taskId}/config`, config),
  createTask: (config: any) => api.post('/data/sync/tasks', config),
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
  createFactor: (data: { factor_id: string; description?: string; category?: string; compute_mode?: string; depends_on?: string[]; storage_target?: string; params?: Record<string, any>; code?: string }) =>
    api.post('/production/factors', data),
  updateFactor: (factorId: string, data: { description?: string; category?: string; compute_mode?: string; depends_on?: string[]; storage_target?: string; params?: Record<string, any> }) =>
    api.put(`/production/factors/${factorId}`, data),
  deleteFactor: (factorId: string, deleteData = false) =>
    api.delete(`/production/factors/${factorId}`, { params: { delete_data: deleteData } }),

  // 生产任务
  runProduction: (factorId: string, mode = 'incremental', targetDate?: string, startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/production/run', { factor_id: factorId, mode, target_date: targetDate, start_date: startDate, end_date: endDate, preprocess }),
  batchRunFactors: (factorIds: string[], mode = 'incremental', startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/production/batch-run', { factor_ids: factorIds, mode, start_date: startDate, end_date: endDate, preprocess }),
  getProductionHistory: (factorId?: string, limit = 20) =>
    api.get('/production/history', { params: { factor_id: factorId, limit } }),

  // 因子代码查看/编辑
  getFactorCode: (factorId: string) => api.get(`/production/factors/${factorId}/code`),
  updateFactorCode: (factorId: string, filename: string, code: string) =>
    api.put(`/production/factors/${factorId}/code`, { filename, code }),

  // 因子代码测试
  testFactorCode: (data: { code: string; start_date: string; end_date: string; depends_on?: string[]; params?: Record<string, any> }) =>
    longRunningApi.post('/production/factors/test', data),

  // 因子数据探查
  getFactorData: (factorId: string, params?: { start_date?: string; end_date?: string; ts_code?: string; limit?: number }) =>
    api.get(`/production/factors/${factorId}/data`, { params }),
  getFactorStats: (factorId: string) => api.get(`/production/factors/${factorId}/stats`),

  // 因子分析
  runAnalysis: (factorId: string, startDate?: string, endDate?: string, periods = [1, 5, 10], quantiles = 5) =>
    longRunningApi.post('/analysis/run', { factor_id: factorId, start_date: startDate, end_date: endDate, periods, quantiles }),
  getAnalysis: (factorId: string) => api.get(`/analysis/${factorId}`),
  getAnalysisHistory: (factorId: string, limit = 10) =>
    api.get(`/analysis/${factorId}/history`, { params: { limit } }),
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
