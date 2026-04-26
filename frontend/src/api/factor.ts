import type { PreprocessOptions } from '../types/factor';
import { api, longRunningApi } from './client';

export const productionApi = {
  listFactors: () => api.get('/factor/factors'),
  createFactor: (data: {
    factor_id: string; description?: string; category?: string; compute_mode?: string;
    depends_on?: string[]; storage_target?: string; params?: Record<string, any>;
    code?: string; align_calendar?: boolean;
  }) => api.post('/factor/factors', data),
  updateFactor: (factorId: string, data: {
    description?: string; category?: string; compute_mode?: string;
    depends_on?: string[]; storage_target?: string; params?: Record<string, any>; align_calendar?: boolean;
  }) => api.put(`/factor/factors/${factorId}`, data),
  deleteFactor: (factorId: string, deleteData = false) =>
    api.delete(`/factor/factors/${factorId}`, { params: { delete_data: deleteData } }),

  runProduction: (factorId: string, mode = 'incremental', targetDate?: string, startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/factor/run', { factor_id: factorId, mode, target_date: targetDate, start_date: startDate, end_date: endDate, preprocess }),
  batchRunFactors: (factorIds: string[], mode = 'incremental', startDate?: string, endDate?: string, preprocess?: PreprocessOptions) =>
    longRunningApi.post('/factor/batch-run', { factor_ids: factorIds, mode, start_date: startDate, end_date: endDate, preprocess }),

  getFactorCode: (factorId: string) => api.get(`/factor/factors/${factorId}/code`),
  updateFactorCode: (factorId: string, filename: string, code: string) =>
    api.put(`/factor/factors/${factorId}/code`, { filename, code }),
  testFactorCode: (data: {
    code: string; start_date: string; end_date: string; depends_on?: string[];
    params?: Record<string, any>; preprocess?: PreprocessOptions; lookback_days?: number;
  }) => longRunningApi.post('/factor/factors/test', data),

  getFactorData: (factorId: string, params?: { start_date?: string; end_date?: string; ts_code?: string; limit?: number }) =>
    api.get(`/factor/factors/${factorId}/data`, { params }),
  getFactorStats: (factorId: string) => api.get(`/factor/factors/${factorId}/stats`),

  getDataConfig: () => api.get('/config/data-mappings'),
  updateDataConfig: (mappings: any[]) => api.put('/config/data-mappings', { mappings }),
  getResolvedDataConfig: () => api.get('/config/data-mappings/resolved'),
  getAvailableTables: () => api.get('/config/available-tables'),
  getTableColumns: (tableName: string) => api.get(`/config/table-columns/${tableName}`),

  listIndexPools: () => api.get('/config/index-pool/list'),

  runAlphalensAnalysis: (data: {
    factor_id: string; start_date: string; end_date: string; periods?: number[];
    quantiles?: number; index_pool?: string; groupby_field?: string; next_day_entry?: boolean;
    entry_price?: string; neutralize?: boolean; neutralize_controls?: string[];
    industry_level?: string; winsorize?: boolean; winsorize_lower?: number; winsorize_upper?: number;
  }) => longRunningApi.post('/factor/analysis/alphalens', data),
  getLatestAlphalensAnalysis: (factorId: string) => api.get(`/factor/analysis/${factorId}/latest`),
  getAlphalensAnalysisById: (factorId: string, analysisId: string) => api.get(`/factor/analysis/${factorId}/detail/${analysisId}`),
  deleteAlphalensAnalysisById: (factorId: string, analysisId: string) => api.delete(`/factor/analysis/${factorId}/detail/${analysisId}`),
  getAnalysisTaskStatus: (taskId: string) => api.get(`/factor/analysis/status/${taskId}`),
  getTradingDays: (start: string, end: string) =>
    api.get('/factor/analysis/trading-days', { params: { start, end } }),
};
