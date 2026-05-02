import type { DataFieldMapping, PreprocessOptions } from '../types/factor';
import { api, longRunningApi } from './client';

export type { PreprocessOptions };

export const DEFAULT_PREPROCESS: PreprocessOptions = {
  adjust_price: 'forward',
  filter_st: true,
  filter_new_stock: true,
  new_stock_days: 60,
  mark_limit: true,
};

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
  getTaskRunStatus: (taskType: string, runId: string) => api.get(`/tasks/${taskType}/status/${runId}`),
  createSyncTask: (config: any) => api.post('/tasks/sync', { config_data: config }),
  updateSyncTask: (taskId: string, config: any) => api.put(`/tasks/sync/${taskId}`, { config_data: config }),
  deleteTask: (taskId: string, dropTable?: boolean) => api.delete(`/tasks/sync/${taskId}`, { params: { drop_table: dropTable } }),

  // ETL 任务管理
  listEtlTasks: () => api.get('/tasks/etl'),
  createEtlTask: (config: any) => api.post('/tasks/etl', { config_data: config }),
  updateEtlTask: (taskId: string, config: any) => api.put(`/tasks/etl/${taskId}`, { config_data: config }),
  deleteEtlTask: (taskId: string, dropTable?: boolean) => api.delete(`/tasks/etl/${taskId}`, { params: { drop_table: dropTable } }),
  getEtlTaskStatus: (taskId: string) => api.get(`/tasks/etl/${taskId}/status`),
  getEtlTableSchema: (taskId: string) => api.get(`/tasks/etl/${taskId}/schema`),
  runEtlTask: (taskId: string, startDate?: string, endDate?: string) =>
    longRunningApi.post(`/tasks/etl/${taskId}/execute`, { start_date: startDate, end_date: endDate }),
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
