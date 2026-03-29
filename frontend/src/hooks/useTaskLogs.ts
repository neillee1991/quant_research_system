import { useState, useCallback } from 'react';
import { taskMonitorApi, type TaskRun } from '../api';

export const useTaskLogs = (taskType: string, taskId?: string, limit = 100) => {
  const [logs, setLogs] = useState<TaskRun[]>([]);
  const [loading, setLoading] = useState(false);

  const loadLogs = useCallback(async () => {
    setLoading(true);
    try {
      const res = await taskMonitorApi.getTaskHistory(limit, taskType, taskId);
      setLogs(res.data.tasks || []);
    } catch (err) {
      console.error(`Failed to load ${taskType} logs:`, err);
    } finally {
      setLoading(false);
    }
  }, [taskType, taskId, limit]);

  return { logs, loading, loadLogs };
};
