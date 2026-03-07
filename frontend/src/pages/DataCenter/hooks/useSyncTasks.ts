/**
 * 同步任务管理 Hook
 */
import { useState, useCallback } from 'react';
import { Toast } from '@douyinfe/semi-ui';
import { dataApi } from '../../../api';
import type { SyncTask, TaskStatus, SyncLog, ScheduleInfo } from '../../../types';

export const useSyncTasks = () => {
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [syncLogs, setSyncLogs] = useState<SyncLog[]>([]);
  const [syncingTasks, setSyncingTasks] = useState<Set<string>>(new Set());
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);
  const [scheduleInfo, setScheduleInfo] = useState<Record<string, ScheduleInfo>>({});

  const loadSyncTasks = useCallback(async () => {
    try {
      const res = await dataApi.listSyncTasks();
      setSyncTasks(res.data.tasks || []);
    } catch (error) {
      console.error('Failed to load sync tasks:', error);
      Toast.error('加载同步任务失败');
    }
  }, []);

  const loadTaskStatus = useCallback(async (taskId: string) => {
    try {
      const res = await dataApi.getTaskStatus(taskId);
      setTaskStatuses((prev) => ({ ...prev, [taskId]: res.data }));
    } catch (error) {
      console.error(`Failed to load status for ${taskId}:`, error);
    }
  }, []);

  const loadTaskScheduleInfo = useCallback(async (taskId: string) => {
    try {
      const res = await dataApi.getTaskScheduleInfo(taskId);
      setScheduleInfo((prev) => ({ ...prev, [taskId]: res.data }));
    } catch (error) {
      console.error(`Failed to load schedule info for ${taskId}:`, error);
    }
  }, []);

  const loadSyncLogs = useCallback(async (
    source?: string,
    dataType?: string,
    startDate?: string,
    endDate?: string
  ) => {
    try {
      const res = await dataApi.getSyncStatus(source, dataType, startDate, endDate);
      setSyncLogs(res.data.logs || []);
    } catch (error) {
      console.error('Failed to load sync logs:', error);
    }
  }, []);

  const syncTask = useCallback(async (
    taskId: string,
    targetDate?: string,
    startDate?: string,
    endDate?: string
  ) => {
    setSyncingTasks((prev) => new Set(prev).add(taskId));
    try {
      await dataApi.syncTask(taskId, targetDate, startDate, endDate);
      Toast.success(`任务 ${taskId} 同步已启动`);
      setTimeout(() => {
        loadTaskStatus(taskId);
        loadSyncLogs();
      }, 2000);
    } catch (error: any) {
      Toast.error(`任务 ${taskId} 同步失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    } finally {
      setSyncingTasks((prev) => {
        const newSet = new Set(prev);
        newSet.delete(taskId);
        return newSet;
      });
    }
  }, [loadTaskStatus, loadSyncLogs]);

  const batchSyncTasks = useCallback(async (
    taskIds: string[],
    startDate?: string,
    endDate?: string
  ) => {
    const fullIds = taskIds.filter(id => {
      const t = syncTasks.find(t => t.task_id === id);
      return t?.sync_type === 'full';
    });
    const incrementalIds = taskIds.filter(id => !fullIds.includes(id));

    if (incrementalIds.length > 0 && (!startDate || !endDate)) {
      Toast.warning('存在增量任务，请选择日期范围');
      return false;
    }

    taskIds.forEach(taskId => {
      setSyncingTasks((prev) => new Set(prev).add(taskId));
    });

    Toast.info(`开始同步 ${taskIds.length} 个任务`);

    for (const taskId of taskIds) {
      const isFull = fullIds.includes(taskId);
      try {
        if (isFull) {
          await dataApi.syncTask(taskId);
        } else {
          await dataApi.syncTask(taskId, undefined, startDate, endDate);
        }
      } catch (error: any) {
        Toast.error(`任务 ${taskId} 同步失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    setTimeout(() => {
      taskIds.forEach((taskId) => {
        loadTaskStatus(taskId);
        setSyncingTasks((prev) => {
          const newSet = new Set(prev);
          newSet.delete(taskId);
          return newSet;
        });
      });
      loadSyncLogs();
      setSelectedTaskIds([]);
    }, 2000);

    return true;
  }, [syncTasks, loadTaskStatus, loadSyncLogs]);

  const deleteTask = useCallback(async (taskId: string, dropTable = true) => {
    try {
      await dataApi.deleteTask(taskId, dropTable);
      Toast.success(`同步任务 ${taskId} 已删除`);
      await loadSyncTasks();
    } catch (error: any) {
      Toast.error(`删除任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [loadSyncTasks]);

  const createTask = useCallback(async (config: any) => {
    try {
      await dataApi.createSyncTask(config);
      Toast.success(`同步任务 ${config.task_id} 创建成功`);
      await loadSyncTasks();
    } catch (error: any) {
      Toast.error(`创建任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [loadSyncTasks]);

  const updateTask = useCallback(async (config: any) => {
    try {
      await dataApi.updateSyncTask(config.task_id, config);
      Toast.success(`同步任务 ${config.task_id} 更新成功`);
      await loadSyncTasks();
    } catch (error: any) {
      Toast.error(`更新任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [loadSyncTasks]);

  const toggleSchedule = useCallback(async (
    taskId: string,
    enabled: boolean,
    schedule?: string,
    cronExpression?: string
  ) => {
    try {
      if (enabled) {
        if (!schedule) {
          Toast.warning('请先配置调度规则');
          return false;
        }
        await dataApi.enableTaskSchedule(taskId, schedule, cronExpression);
        Toast.success(`任务 ${taskId} 调度已启用`);
      } else {
        await dataApi.disableTaskSchedule(taskId);
        Toast.success(`任务 ${taskId} 调度已禁用`);
      }
      await loadTaskScheduleInfo(taskId);
      return true;
    } catch (error: any) {
      Toast.error(`调度设置失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, [loadTaskScheduleInfo]);

  return {
    syncTasks,
    taskStatuses,
    syncLogs,
    syncingTasks,
    selectedTaskIds,
    scheduleInfo,
    setSelectedTaskIds,
    loadSyncTasks,
    loadTaskStatus,
    loadTaskScheduleInfo,
    loadSyncLogs,
    syncTask,
    batchSyncTasks,
    deleteTask,
    createTask,
    updateTask,
    toggleSchedule,
  };
};
