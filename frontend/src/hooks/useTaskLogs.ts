import { useState, useCallback } from 'react';
import { taskMonitorApi, type TaskRun } from '../api';

export interface TaskLogFilter {
  taskId?: string;
  startDate?: string;
  endDate?: string;
}

export const useTaskLogs = (taskType: string, limit = 100) => {
  const [logs, setLogs] = useState<TaskRun[]>([]);
  const [loading, setLoading] = useState(false);

  const loadLogs = useCallback(async (filter?: TaskLogFilter) => {
    setLoading(true);
    try {
      const res = await taskMonitorApi.getTaskHistory(
        limit,
        taskType,
        filter?.taskId,
        filter?.startDate,
        filter?.endDate,
      );
      setLogs(res.data.tasks || []);
    } catch (err) {
      console.error(`Failed to load ${taskType} logs:`, err);
    } finally {
      setLoading(false);
    }
  }, [taskType, limit]);

  return { logs, loading, loadLogs };
};
