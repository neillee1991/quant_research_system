/**
 * ETL 任务管理 Hook
 */
import { useState, useCallback } from 'react';
import { message } from 'antd';
import { dataApi } from '../../../api';
import type { ETLTask, ETLTestResult, ETLFieldDefinition } from '../../../types';
import type { ETLTaskStatus } from '../types';

export const useETLTasks = () => {
  const [etlTasks, setEtlTasks] = useState<ETLTask[]>([]);
  const [runningEtlTasks, setRunningEtlTasks] = useState<Set<string>>(new Set());
  const [selectedEtlTaskIds, setSelectedEtlTaskIds] = useState<string[]>([]);
  const [etlLogs, setEtlLogs] = useState<any[]>([]);

  const loadEtlTasks = useCallback(async () => {
    try {
      const res = await dataApi.listEtlTasks();
      setEtlTasks(res.data.tasks || []);
    } catch (error) {
      console.error('Failed to load ETL tasks:', error);
      message.error('加载 ETL 任务失败');
    }
  }, []);

  const loadEtlLogs = useCallback(async (
    taskId?: string,
    startDate?: string,
    endDate?: string
  ) => {
    try {
      const res = await dataApi.getEtlLogs(taskId, startDate, endDate);
      setEtlLogs(res.data.logs || []);
    } catch (error) {
      console.error('Failed to load ETL logs:', error);
    }
  }, []);

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
      message.success(`测试成功，返回 ${result.row_count} 行数据`);
      return result;
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '脚本测试失败';
      message.error(errorMsg);
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
      message.success(`ETL 任务 ${config.task_id} 创建成功`);
      await loadEtlTasks();
      return true;
    } catch (error: any) {
      message.error(`创建任务失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, [loadEtlTasks]);

  const updateEtlTask = useCallback(async (config: any) => {
    try {
      await dataApi.updateEtlTask(config.task_id, config);
      message.success(`ETL 任务 ${config.task_id} 更新成功`);
      await loadEtlTasks();
      return true;
    } catch (error: any) {
      message.error(`更新任务失败: ${error.response?.data?.detail || error.message}`);
      return false;
    }
  }, [loadEtlTasks]);

  const deleteEtlTask = useCallback(async (taskId: string, dropTable = true) => {
    try {
      await dataApi.deleteEtlTask(taskId, dropTable);
      message.success(`ETL 任务 ${taskId} 已删除`);
      await loadEtlTasks();
    } catch (error: any) {
      message.error(`删除任务失败: ${error.response?.data?.detail || error.message}`);
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
      message.warning('请选择回溯日期范围');
      return false;
    }

    setRunningEtlTasks((prev) => new Set(prev).add(taskId));
    try {
      // 全量任务不传日期参数
      if (isFullTask) {
        await dataApi.backfillEtlTask(taskId, '', '');
      } else {
        await dataApi.backfillEtlTask(taskId, startDate, endDate);
      }
      message.success(`任务 ${taskId} 回溯已启动`);
      setTimeout(() => {
        loadEtlLogs(taskId);
      }, 2000);
      return true;
    } catch (error: any) {
      message.error(`回溯失败: ${error.response?.data?.detail || error.message}`);
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
      message.warning('存在增量任务，请选择回溯日期范围');
      return false;
    }

    taskIds.forEach(taskId => {
      setRunningEtlTasks((prev) => new Set(prev).add(taskId));
    });

    message.info(`开始回溯 ${taskIds.length} 个任务`);

    // 先执行全量任务（只执行一次，不传日期参数）
    for (const taskId of fullIds) {
      try {
        await dataApi.backfillEtlTask(taskId, '', '');
        message.success(`全量任务 ${taskId} 回溯成功`);
      } catch (error: any) {
        message.error(`任务 ${taskId} 回溯失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    // 再执行增量任务（按日期范围执行）
    for (const taskId of incrementalIds) {
      try {
        await dataApi.backfillEtlTask(taskId, startDate, endDate);
        message.success(`增量任务 ${taskId} 回溯成功`);
      } catch (error: any) {
        message.error(`任务 ${taskId} 回溯失败: ${error.response?.data?.detail || error.message}`);
      }
    }

    setTimeout(() => {
      taskIds.forEach((taskId) => {
        setRunningEtlTasks((prev) => {
          const newSet = new Set(prev);
          newSet.delete(taskId);
          return newSet;
        });
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
      message.success(`目标表 ${tableName} 已创建`);
      return true;
    } catch (error: any) {
      if (!error.response?.data?.detail?.includes('已存在')) {
        message.warning(error.response?.data?.detail || '建表失败');
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

  return {
    etlTasks,
    runningEtlTasks,
    selectedEtlTaskIds,
    etlLogs,
    setSelectedEtlTaskIds,
    loadEtlTasks,
    loadEtlLogs,
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
