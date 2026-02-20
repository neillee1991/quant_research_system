import axios from 'axios';

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
  getSyncStatus: () => api.get('/data/sync/status'),

  // 同步任务管理
  listSyncTasks: () => api.get('/data/sync/tasks'),
  syncTask: (taskId: string, targetDate?: string) =>
    longRunningApi.post(`/data/sync/task/${taskId}`, null, { params: { target_date: targetDate } }),
  syncAllTasks: (targetDate?: string) =>
    longRunningApi.post('/data/sync/all', null, { params: { target_date: targetDate } }),
  getTaskStatus: (taskId: string) => api.get(`/data/sync/status/${taskId}`),

  // 数据库管理
  listTables: () => api.get('/data/tables'),
  executeQuery: (sql: string, limit = 1000) =>
    api.post('/data/query', null, { params: { sql, limit } }),
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

export default api;
