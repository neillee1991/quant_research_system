/**
 * 同步任务管理 Hook
 */
import { useState, useCallback, useRef, useEffect } from 'react';
import { notify } from '../../../utils/notify';
import { dataApi } from '../../../api';
import type { SyncTask, TaskStatus } from '../../../types';

// 跟踪正在执行的任务
interface RunningTaskInfo {
  runId: string;
  pollTimer: ReturnType<typeof setInterval>;
}

export const useSyncTasks = () => {
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [syncingTasks, setSyncingTasks] = useState<Set<string>>(new Set());
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);

  // 跟踪每个任务的 runId 和轮询定时器
  const runningTasksRef = useRef<Map<string, RunningTaskInfo>>(new Map());

  // 组件卸载时清除所有定时器
  useEffect(() => {
    return () => {
      runningTasksRef.current.forEach((info) => {
        clearInterval(info.pollTimer);
      });
    };
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

  // 轮询任务执行状态
  const pollTaskStatus = useCallback((taskId: string, runId: string) => {
    const pollTimer = setInterval(async () => {
      try {
        const res = await dataApi.getTaskRunStatus('sync', runId);
        const status = res.data?.data?.status;

        if (status === 'success' || status === 'failed') {
          // 任务完成，清除定时器
          const info = runningTasksRef.current.get(taskId);
          if (info) {
            clearInterval(info.pollTimer);
            runningTasksRef.current.delete(taskId);
          }

          // 更新同步状态
          setSyncingTasks((prev) => {
            const newSet = new Set(prev);
            newSet.delete(taskId);
            return newSet;
          });

          // 刷新任务数据状态
          loadTaskStatus(taskId);

          if (status === 'success') {
            notify.success(`任务 ${taskId} 同步完成`);
          } else {
            const error = res.data?.data?.error || '未知错误';
            notify.error(`任务 ${taskId} 同步失败: ${error}`);
          }
        }
      } catch (error) {
        console.error(`Failed to poll status for ${taskId}:`, error);
      }
    }, 2000); // 每2秒轮询一次

    return pollTimer;
  }, [loadTaskStatus]);

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
      const res = await dataApi.syncTask(taskId, targetDate, startDate, endDate);
      const runId = res.data?.result?.run_id;

      notify.success(`任务 ${taskId} 同步已启动`);

      // 保存 runId 并开始轮询
      if (runId) {
        const pollTimer = pollTaskStatus(taskId, runId);
        runningTasksRef.current.set(taskId, { runId, pollTimer });
      } else {
        // 如果没有返回 runId，2秒后刷新状态并清除 syncing 状态
        setTimeout(() => {
          loadTaskStatus(taskId);
          setSyncingTasks((prev) => {
            const newSet = new Set(prev);
            newSet.delete(taskId);
            return newSet;
          });
        }, 2000);
      }
    } catch (error: any) {
      notify.error(`任务 ${taskId} 同步失败: ${error.response?.data?.detail || error.message}`);
      setSyncingTasks((prev) => {
        const newSet = new Set(prev);
        newSet.delete(taskId);
        return newSet;
      });
      throw error;
    }
  }, [loadTaskStatus, pollTaskStatus]);

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

    // 标记所有任务为同步中
    taskIds.forEach(taskId => {
      setSyncingTasks((prev) => new Set(prev).add(taskId));
    });

    notify.info(`开始同步 ${taskIds.length} 个任务`);

    // 并行执行所有同步任务
    const syncPromises = taskIds.map(async (taskId) => {
      const isFull = fullIds.includes(taskId);
      try {
        const res = isFull
          ? await dataApi.syncTask(taskId)
          : await dataApi.syncTask(taskId, undefined, startDate, endDate);

        const runId = res.data?.result?.run_id;
        if (runId) {
          const pollTimer = pollTaskStatus(taskId, runId);
          runningTasksRef.current.set(taskId, { runId, pollTimer });
        } else {
          // 如果没有 runId，稍后清除 syncing 状态
          setTimeout(() => {
            loadTaskStatus(taskId);
            setSyncingTasks((prev) => {
              const newSet = new Set(prev);
              newSet.delete(taskId);
              return newSet;
            });
          }, 2000);
        }
      } catch (error: any) {
        notify.error(`任务 ${taskId} 同步失败: ${error.response?.data?.detail || error.message}`);
        setSyncingTasks((prev) => {
          const newSet = new Set(prev);
          newSet.delete(taskId);
          return newSet;
        });
      }
    });

    await Promise.all(syncPromises);
    setSelectedTaskIds([]);

    return true;
  }, [syncTasks, loadTaskStatus, pollTaskStatus]);

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

  return {
    syncTasks,
    taskStatuses,
    syncingTasks,
    selectedTaskIds,
    setSelectedTaskIds,
    loadSyncTasks,
    loadTaskStatus,
    setBatchTaskStatuses,
    syncTask,
    batchSyncTasks,
    deleteTask,
    createTask,
    updateTask,
  };
};
