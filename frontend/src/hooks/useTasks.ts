/**
 * 通用任务管理 Hook
 * 为所有任务类型提供统一的状态管理和操作接口
 */
import { useState, useCallback, useMemo } from 'react';
import { notify } from '../utils/notify';
import type { TaskTypeConfig, GenericTaskStatus } from '../config/taskTypes';
import { useTaskLogs } from './useTaskLogs';
import { useTaskMonitorStore } from '../store';

export interface UseTasksOptions<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>> {
  config: TaskTypeConfig<TTask, TStatus, TRunParams>;
  autoLoad?: boolean;
}

export interface UseTasksResult<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>> {
  // 数据状态
  tasks: TTask[];
  taskStatuses: Record<string, TStatus>;
  runningTasks: Set<string>;
  selectedTaskIds: string[];

  // 日志相关
  logs: ReturnType<typeof useTaskLogs>['logs'];
  loading: ReturnType<typeof useTaskLogs>['loading'];
  loadLogs: ReturnType<typeof useTaskLogs>['loadLogs'];

  // 操作函数
  setSelectedTaskIds: (ids: string[]) => void;
  loadTasks: () => Promise<void>;
  loadTaskStatus: (taskId: string) => Promise<void>;
  runTask: (taskId: string, params?: TRunParams) => Promise<boolean>;
  batchRunTasks: (taskIds: string[], params?: TRunParams) => Promise<boolean>;
  deleteTask: (taskId: string, dropTable?: boolean) => Promise<void>;

  // 衍生状态
  selectedTasks: TTask[];
  isLoading: boolean;
}

export function useTasks<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>>({
  config,
  autoLoad = true,
}: UseTasksOptions<TTask, TStatus, TRunParams>): UseTasksResult<TTask, TStatus, TRunParams> {
  const [tasks, setTasks] = useState<TTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TStatus>>({});
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);

  // 使用 TaskMonitorStore 的 runningTasks，保持同步
  const monitorRunningTasks = useTaskMonitorStore((state) => state.runningTasks);

  // 将 monitorRunningTasks 转换为 Set<string>，只包含当前任务类型的任务
  const runningTasks = useMemo(() => {
    const taskIds = new Set<string>();
    monitorRunningTasks.forEach((task) => {
      if (task.task_type === config.type) {
        taskIds.add(task.task_id);
      }
    });
    return taskIds;
  }, [monitorRunningTasks, config.type]);

  const { logs, loading: logsLoading, loadLogs } = useTaskLogs(config.type);

  // 衍生状态：选中的任务
  const selectedTasks = useMemo(() => {
    const taskMap = new Map(tasks.map((task) => [config.utils.getTaskId(task), task]));
    return selectedTaskIds.map((id) => taskMap.get(id)).filter(Boolean) as TTask[];
  }, [tasks, selectedTaskIds, config.utils]);

  // 加载所有任务
  const loadTasks = useCallback(async () => {
    setIsLoading(true);
    try {
      const res = await config.api.listTasks();
      const taskList = res.data?.tasks || [];
      setTasks(taskList);

      // 加载所有任务状态
      const statuses: Record<string, TStatus> = {};
      for (const task of taskList) {
        try {
          const taskId = config.utils.getTaskId(task);
          const statusRes = await config.api.getTaskStatus(taskId);
          if (statusRes.data?.data) {
            statuses[taskId] = statusRes.data.data;
          }
        } catch (err) {
          console.error(`Failed to load status for ${config.type} task ${config.utils.getTaskId(task)}:`, err);
        }
      }
      setTaskStatuses(statuses);
    } catch (error) {
      console.error(`Failed to load ${config.type} tasks:`, error);
    } finally {
      setIsLoading(false);
    }
  }, [config]);

  // 加载单个任务状态
  const loadTaskStatus = useCallback(async (taskId: string) => {
    try {
      const res = await config.api.getTaskStatus(taskId);
      if (res.data?.data) {
        setTaskStatuses((prev) => ({ ...prev, [taskId]: res.data!.data! }));
      }
    } catch (error) {
      console.error(`Failed to load status for ${config.type} task ${taskId}:`, error);
    }
  }, [config]);

  // 运行单个任务
  const runTask = useCallback(async (taskId: string, params?: TRunParams): Promise<boolean> => {
    try {
      await config.api.runTask(taskId, params);
      notify.success(`${config.label} ${taskId} 已启动`);
      // TaskMonitor 会通过轮询 /tasks/running 来更新状态
      // 这里只在2秒后刷新任务状态和日志
      setTimeout(() => {
        loadLogs();
        loadTaskStatus(taskId);
      }, 2000);
      return true;
    } catch (error: any) {
      notify.error(`${config.label} ${taskId} 启动失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, [config, loadLogs, loadTaskStatus]);

  // 批量运行任务
  const batchRunTasks = useCallback(async (taskIds: string[], params?: TRunParams): Promise<boolean> => {
    // 区分全量和增量任务（如果需要）
    const fullIds: string[] = [];
    const incrementalIds: string[] = [];

    taskIds.forEach((taskId) => {
      const task = tasks.find((t) => config.utils.getTaskId(t) === taskId);
      if (task && config.utils.isIncremental(task)) {
        incrementalIds.push(taskId);
      } else {
        fullIds.push(taskId);
      }
    });

    // 增量任务可能需要特殊参数验证
    if (incrementalIds.length > 0) {
      // 这里可以根据任务类型添加特定的参数验证
      // 例如：同步任务需要日期范围，ETL 任务也需要日期范围
    }

    notify.info(`开始执行 ${taskIds.length} 个 ${config.label}`);

    // 执行全量任务
    for (const taskId of fullIds) {
      try {
        await config.api.runTask(taskId, params);
        notify.success(`${config.label} ${taskId} 执行成功`);
      } catch (error: any) {
        notify.error(`${config.label} ${taskId} 执行失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    // 执行增量任务
    for (const taskId of incrementalIds) {
      try {
        await config.api.runTask(taskId, params);
        notify.success(`${config.label} ${taskId} 执行成功`);
      } catch (error: any) {
        notify.error(`${config.label} ${taskId} 执行失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    // TaskMonitor 会通过轮询 /tasks/running 来更新状态
    // 这里只在2秒后刷新任务状态和日志
    setTimeout(() => {
      taskIds.forEach((taskId) => {
        loadTaskStatus(taskId);
      });
      loadLogs();
      setSelectedTaskIds([]);
    }, 2000);

    return true;
  }, [config, tasks, loadLogs, loadTaskStatus]);

  // 删除任务
  const deleteTask = useCallback(async (taskId: string, dropTable = true) => {
    if (!config.api.deleteTask) {
      notify.warning('删除功能未实现');
      return;
    }
    try {
      await config.api.deleteTask(taskId, dropTable);
      notify.success(`${config.label} ${taskId} 已删除`);
      await loadTasks();
    } catch (error: any) {
      notify.error(`删除任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [config, loadTasks]);

  return {
    // 数据状态
    tasks,
    taskStatuses,
    runningTasks,
    selectedTaskIds,

    // 日志相关
    logs,
    loading: logsLoading,
    loadLogs,

    // 操作函数
    setSelectedTaskIds,
    loadTasks,
    loadTaskStatus,
    runTask,
    batchRunTasks,
    deleteTask,

    // 衍生状态
    selectedTasks,
    isLoading,
  };
}
