/**
 * 同步任务管理 Hook
 */
import { useState, useCallback } from 'react';
import { notify } from '../../../utils/notify';
import { dataApi } from '../../../api';
import type { SyncTask, TaskStatus, ScheduleInfo } from '../../../types';

export const useSyncTasks = () => {
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [syncingTasks, setSyncingTasks] = useState<Set<string>>(new Set());
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);
  const [scheduleInfo, setScheduleInfo] = useState<Record<string, ScheduleInfo>>({});

  const loadSyncTasks = useCallback(async () => {
    try {
      const res = await dataApi.listSyncTasks();
      const tasks = res.data.tasks || [];
      setSyncTasks(tasks);

      // 加载所有任务状态
      const statuses: Record<string, TaskStatus> = {};
      for (const task of tasks) {
        try {
          const statusRes = await dataApi.getSyncTaskStatus(task.task_id);
          if (statusRes.data?.data) {
            statuses[task.task_id] = statusRes.data.data;
          }
        } catch (err) {
          console.error(`Failed to load status for ${task.task_id}:`, err);
        }
      }
      setTaskStatuses(statuses);
    } catch (error) {
      console.error('Failed to load sync tasks:', error);
    }
  }, []);

  const loadTaskStatus = useCallback(async (taskId: string) => {
    try {
      const res = await dataApi.getSyncTaskStatus(taskId);
      if (res.data?.data) {
        setTaskStatuses((prev) => ({ ...prev, [taskId]: res.data.data }));
      }
    } catch (error) {
      console.error(`Failed to load status for ${taskId}:`, error);
    }
  }, []);

  const setBatchTaskStatuses = useCallback((statuses: Record<string, TaskStatus>) => {
    setTaskStatuses(statuses);
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
      notify.success(`任务 ${taskId} 同步已启动`);
      setTimeout(() => {
        loadTaskStatus(taskId);
      }, 2000);
    } catch (error: any) {
      notify.error(`任务 ${taskId} 同步失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    } finally {
      setSyncingTasks((prev) => {
        const newSet = new Set(prev);
        newSet.delete(taskId);
        return newSet;
      });
    }
  }, [loadTaskStatus]);

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
      notify.warning('存在增量任务，请选择日期范围');
      return false;
    }

    taskIds.forEach(taskId => {
      setSyncingTasks((prev) => new Set(prev).add(taskId));
    });

    notify.info(`开始同步 ${taskIds.length} 个任务`);

    for (const taskId of taskIds) {
      const isFull = fullIds.includes(taskId);
      try {
        if (isFull) {
          await dataApi.syncTask(taskId);
        } else {
          await dataApi.syncTask(taskId, undefined, startDate, endDate);
        }
      } catch (error: any) {
        notify.error(`任务 ${taskId} 同步失败: ${error.response?.data?.detail || error.message}`);
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
      setSelectedTaskIds([]);
    }, 2000);

    return true;
  }, [syncTasks, loadTaskStatus]);

  const deleteTask = useCallback(async (taskId: string, dropTable = true) => {
    try {
      await dataApi.deleteTask(taskId, dropTable);
      notify.success(`同步任务 ${taskId} 已删除`);
      await loadSyncTasks();
    } catch (error: any) {
      notify.error(`删除任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [loadSyncTasks]);

  const createTask = useCallback(async (config: any) => {
    try {
      await dataApi.createSyncTask(config);
      notify.success(`同步任务 ${config.task_id} 创建成功`);
      await loadSyncTasks();
    } catch (error: any) {
      notify.error(`创建任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [loadSyncTasks]);

  const updateTask = useCallback(async (config: any) => {
    try {
      await dataApi.updateSyncTask(config.task_id, config);
      notify.success(`同步任务 ${config.task_id} 更新成功`);
      await loadSyncTasks();
    } catch (error: any) {
      notify.error(`更新任务失败: ${error.response?.data?.detail || error.message}`);
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
      // 调度管理已迁移到 SchedulerCenter
      // 此处保留接口以保持兼容性
      notify.info('调度管理已迁移到调度中心，请在调度中心配置');
      return false;
    } catch (error: any) {
      notify.error(`调度设置失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, []);

  return {
    syncTasks,
    taskStatuses,
    syncingTasks,
    selectedTaskIds,
    scheduleInfo,
    setSelectedTaskIds,
    loadSyncTasks,
    loadTaskStatus,
    setBatchTaskStatuses,
    syncTask,
    batchSyncTasks,
    deleteTask,
    createTask,
    updateTask,
    toggleSchedule,
  };
};
