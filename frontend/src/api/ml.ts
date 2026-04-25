import { api, longRunningApi } from './client';

export const mlApi = {
  train: (payload: {
    ts_code: string;
    start_date?: string;
    end_date?: string;
    feature_cols?: string[];
    task?: string;
  }) => longRunningApi.post('/ml/train', payload),
  getStatus: (jobId: string) => api.get(`/ml/status/${jobId}`),
  getWeights: () => api.get('/ml/weights'),
};
