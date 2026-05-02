export interface TaskConfig {
  id: string;
  type: 'sync' | 'etl' | 'factor' | 'flow';
  depends_on: string[];
  flow_name?: string;
}

export interface FlowConfig {
  name: string;
  description: string;
  cron?: string;
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

import { api } from './client';

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
