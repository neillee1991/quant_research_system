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

import { api } from './client';

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
