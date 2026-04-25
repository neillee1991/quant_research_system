import { api } from './client';
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
} from '../pages/ConfigCenter/types';

export const indexApi = {
  listAvailableIndices: (params?: {
    page?: number; limit?: number; search?: string;
    filters?: Record<string, string>; show_subscribed_only?: boolean;
  }) => {
    const { filters, ...rest } = params || {};
    return api.get('/config/index/available', {
      params: { ...rest, filters: filters ? JSON.stringify(filters) : undefined },
    });
  },
  subscribeIndex: (data: { index_code: string }) => api.post('/config/index/subscribe', data),
  unsubscribeIndex: (indexCode: string) => api.delete(`/config/index/subscribe/${indexCode}`),
  getUserPreference: () => api.get('/config/index/preference'),
  saveUserPreference: (data: {
    index_basic_table: string;
    filter_config?: Array<{ field: string; label: string; enabled: boolean; default_value: string | null }>;
  }) => api.post('/config/index/preference', data),
};

export const configApi = {
  getConfigTypes: () => api.get<ConfigTypeOption[]>('/config/types'),
  exportConfigs: (data: ExportRequest) => api.post<ExportResponse>('/config/export', data),
  verifyImport: (data: ImportVerifyRequest) => api.post<ImportVerifyResponse>('/config/import/verify', data),
  applyImport: (data: ImportApplyRequest) => api.post<ImportApplyResponse>('/config/import/apply', data),
};
