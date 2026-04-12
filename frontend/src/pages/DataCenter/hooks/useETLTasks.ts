/**
 * ETL 任务管理 Hook
 */
import { useState, useCallback } from 'react';
import { notify } from '../../../utils/notify';
import { dataApi } from '../../../api';
import { useTaskLogs } from '../../../hooks/useTaskLogs';
import type { ETLTask, ETLTestResult, ETLFieldDefinition } from '../../../types';
import type { ETLTaskStatus } from '../types';

export const useETLTasks = () => {
  const [etlTasks, setEtlTasks] = useState<ETLTask[]>([]);
  const [etlTaskStatuses, setEtlTaskStatuses] = useState<Record<string, ETLTaskStatus>>({});
  const [runningEtlTasks, setRunningEtlTasks] = useState<Set<string>>(new Set());
  const [selectedEtlTaskIds, setSelectedEtlTaskIds] = useState<string[]>([]);

  const loadEtlTasks = useCallback(async () => {
    try {
      const res = await dataApi.listEtlTasks();
      const tasks = res.data.tasks || [];
      setEtlTasks(tasks);

      // 加载所有 ETL 任务状态
      const statuses: Record<string, ETLTaskStatus> = {};
      for (const task of tasks) {
        try {
          const statusRes = await dataApi.getEtlTaskStatus(task.task_id);
          if (statusRes.data?.data) {
            statuses[task.task_id] = statusRes.data.data;
          }
        } catch (err) {
          console.error(`Failed to load status for ETL ${task.task_id}:`, err);
        }
      }
      setEtlTaskStatuses(statuses);
    } catch (error) {
      console.error('Failed to load ETL tasks:', error);
    }
  }, []);

  const { logs: etlLogs, loading: etlLogsLoading, loadLogs: loadEtlLogs } = useTaskLogs('etl');

  const testEtlScript = useCallback(async (
    script: string,
    date?: string
  ): Promise<ETLTestResult | null> => {
    try {
      const res = await dataApi.testEtlScript(script, date);
      const result: ETLTestResult = {
        success: true,
        columns: res.data.columns || [],
        data: res.data.data || [],
        row_count: res.data.count || 0,
      };
      notify.success(`测试成功，返回 ${result.row_count} 行数据`);
      return result;
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '脚本测试失败';
      notify.error(errorMsg);
      return {
        success: false,
        columns: [],
        data: [],
        row_count: 0,
        error: errorMsg,
      };
    }
  }, []);

  const createEtlTask = useCallback(async (config: any) => {
    try {
      await dataApi.createEtlTask(config);
      notify.success(`ETL 任务 ${config.task_id} 创建成功`);
      await loadEtlTasks();
      return true;
    } catch (error: any) {
      notify.error(`创建任务失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, [loadEtlTasks]);

  const updateEtlTask = useCallback(async (config: any) => {
    try {
      await dataApi.updateEtlTask(config.task_id, config);
      notify.success(`ETL 任务 ${config.task_id} 更新成功`);
      await loadEtlTasks();
      return true;
    } catch (error: any) {
      notify.error(`更新任务失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, [loadEtlTasks]);

  const deleteEtlTask = useCallback(async (taskId: string, dropTable = true) => {
    try {
      await dataApi.deleteEtlTask(taskId, dropTable);
      notify.success(`ETL 任务 ${taskId} 已删除`);
      await loadEtlTasks();
    } catch (error: any) {
      notify.error(`删除任务失败: ${error.response?.data?.detail || error.message}`);
      throw error;
    }
  }, [loadEtlTasks]);

  const backfillEtlTask = useCallback(async (
    taskId: string,
    startDate: string,
    endDate: string
  ) => {
    // 检查任务类型
    const task = etlTasks.find(t => t.task_id === taskId);
    const isFullTask = task?.sync_type === 'full';

    // 增量任务需要日期范围
    if (!isFullTask && (!startDate || !endDate)) {
      notify.warning('请选择回溯日期范围');
      return false;
    }

    setRunningEtlTasks((prev) => new Set(prev).add(taskId));
    try {
      // 全量任务不传日期参数，增量任务传日期范围
      await dataApi.runEtlTask(taskId, isFullTask ? undefined : startDate, isFullTask ? undefined : endDate);
      notify.success(`任务 ${taskId} 回溯已启动`);
      setTimeout(() => {
        loadEtlLogs();
        loadEtlTaskStatus(taskId);
      }, 2000);
      return true;
    } catch (error: any) {
      notify.error(`回溯失败: ${error.response?.data?.detail || error.message}`);
      return false;
    } finally {
      setRunningEtlTasks((prev) => {
        const newSet = new Set(prev);
        newSet.delete(taskId);
        return newSet;
      });
    }
  }, [etlTasks, loadEtlLogs]);

  const batchBackfillEtlTasks = useCallback(async (
    taskIds: string[],
    startDate: string,
    endDate: string
  ) => {
    // 区分全量任务和增量任务
    const fullIds = taskIds.filter(id => {
      const t = etlTasks.find(t => t.task_id === id);
      return t?.sync_type === 'full';
    });
    const incrementalIds = taskIds.filter(id => !fullIds.includes(id));

    // 增量任务需要日期范围
    if (incrementalIds.length > 0 && (!startDate || !endDate)) {
      notify.warning('存在增量任务，请选择回溯日期范围');
      return false;
    }

    taskIds.forEach(taskId => {
      setRunningEtlTasks((prev) => new Set(prev).add(taskId));
    });

    notify.info(`开始回溯 ${taskIds.length} 个任务`);

    // 先执行全量任务（不传日期参数）
    for (const taskId of fullIds) {
      try {
        await dataApi.runEtlTask(taskId);
        notify.success(`全量任务 ${taskId} 回溯成功`);
      } catch (error: any) {
        notify.error(`任务 ${taskId} 回溯失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    // 再执行增量任务（按日期范围执行）
    for (const taskId of incrementalIds) {
      try {
        await dataApi.runEtlTask(taskId, startDate, endDate);
        notify.success(`增量任务 ${taskId} 回溯成功`);
      } catch (error: any) {
        notify.error(`任务 ${taskId} 回溯失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    setTimeout(() => {
      taskIds.forEach((taskId) => {
        setRunningEtlTasks((prev) => {
          const newSet = new Set(prev);
          newSet.delete(taskId);
          return newSet;
        });
        loadEtlTaskStatus(taskId);
      });
      loadEtlLogs();
      setSelectedEtlTaskIds([]);
    }, 2000);

    return true;
  }, [etlTasks, loadEtlLogs]);

  const createEtlTable = useCallback(async (
    taskId: string,
    tableName: string,
    fields: ETLFieldDefinition[]
  ) => {
    try {
      await dataApi.createEtlTable(taskId, tableName, fields);
      notify.success(`目标表 ${tableName} 已创建`);
      return true;
    } catch (error: any) {
      if (!error.response?.data?.detail?.includes('已存在')) {
        notify.warning(error.response?.data?.detail || '建表失败');
      }
      return false;
    }
  }, []);

  const getEtlTaskStatus = useCallback(async (taskId: string): Promise<ETLTaskStatus | null> => {
    try {
      const res = await dataApi.getEtlTaskStatus(taskId);
      return res.data;
    } catch (error) {
      console.error(`Failed to load ETL task status for ${taskId}:`, error);
      return null;
    }
  }, []);

  const getEtlTableSchema = useCallback(async (taskId: string): Promise<ETLFieldDefinition[]> => {
    try {
      const res = await dataApi.getEtlTableSchema(taskId);
      return res.data.fields || [];
    } catch (error) {
      console.error(`Failed to load ETL table schema for ${taskId}:`, error);
      return [];
    }
  }, []);

  const loadEtlTaskStatus = useCallback(async (taskId: string) => {
    try {
      const res = await dataApi.getEtlTaskStatus(taskId);
      if (res.data?.data) {
        setEtlTaskStatuses((prev) => ({ ...prev, [taskId]: res.data.data }));
      }
    } catch (error) {
      console.error(`Failed to load status for ETL ${taskId}:`, error);
    }
  }, []);

  return {
    etlTasks,
    etlTaskStatuses,
    runningEtlTasks,
    selectedEtlTaskIds,
    etlLogs,
    etlLogsLoading,
    setSelectedEtlTaskIds,
    loadEtlTasks,
    loadEtlLogs,
    loadEtlTaskStatus,
    testEtlScript,
    createEtlTask,
    updateEtlTask,
    deleteEtlTask,
    backfillEtlTask,
    batchBackfillEtlTasks,
    createEtlTable,
    getEtlTaskStatus,
    getEtlTableSchema,
  };
};
