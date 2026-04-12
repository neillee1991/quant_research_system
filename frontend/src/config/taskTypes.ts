/**
 * 任务类型配置体系
 * 定义所有任务类型的通用配置和特定配置
 */
import React from 'react';
import { SyncOutlined, PlayCircleOutlined, DeleteOutlined, HistoryOutlined } from '@ant-design/icons';
import type { SyncTask, ETLTask, TaskStatus, FactorDefinition } from '../types';
import type { ETLTaskStatus } from '../pages/DataCenter/types';
import { dataApi, productionApi } from '../api';
import { SyncTaskDrawer } from '../pages/DataCenter/SyncTaskDrawer';
import { ETLTaskDrawer } from '../pages/DataCenter/ETLTaskDrawer';
import { FactorTaskDrawer } from '../pages/FactorCenter/FactorTaskDrawer';

// 通用任务状态类型
export interface GenericTaskStatus {
  last_date?: string | null;
  last_sync_time?: string | null;
  table_latest_date?: string | null;
  [key: string]: unknown;
}

// 通用表格列配置
export interface TaskColumnConfig<TTask, TStatus> {
  key: string;
  title: string;
  width?: number;
  fixed?: 'left' | 'right';
  render?: (value: unknown, record: TTask & { status?: TStatus }, index: number) => React.ReactNode;
  dataIndex?: string;
}

// 任务操作配置
export interface TaskActionConfig<TTask> {
  key: string;
  label: string;
  icon?: React.ReactNode;
  type?: 'primary' | 'default' | 'dashed' | 'text' | 'link';
  danger?: boolean;
  disabled?: (task: TTask, runningTasks: Set<string>) => boolean;
  loading?: (task: TTask, runningTasks: Set<string>) => boolean;
  onClick: (task: TTask) => void;
}

// 任务类型配置接口
export interface TaskTypeConfig<TTask, TStatus extends GenericTaskStatus, TRunParams = Record<string, unknown>> {
  // 基础信息
  type: 'sync' | 'etl' | 'factor';
  label: string;
  icon?: React.ReactNode;

  // API 接口
  api: {
    listTasks: () => Promise<{ data: { tasks: TTask[] } }>;
    getTaskStatus: (taskId: string) => Promise<{ data?: { data?: TStatus } }>;
    createTask?: (config: unknown) => Promise<unknown>;
    updateTask?: (taskId: string, config: unknown) => Promise<unknown>;
    deleteTask?: (taskId: string, dropTable?: boolean) => Promise<unknown>;
    runTask: (taskId: string, params?: TRunParams) => Promise<unknown>;
  };

  // 表格列配置
  columns: Array<TaskColumnConfig<TTask, TStatus>>;

  // 操作按钮配置
  actions: Array<TaskActionConfig<TTask>>;

  // 批量操作配置
  batchActions?: {
    key: string;
    label: string;
    icon?: React.ReactNode;
    type?: 'primary' | 'default' | 'dashed' | 'text' | 'link';
    onClick: (taskIds: string[]) => void;
  }[];

  // 抽屉配置
  drawer: {
    component: React.ComponentType<{
      visible: boolean;
      task: TTask | null;
      isNew: boolean;
      onClose: () => void;
      onSave: () => void;
    }>;
  };

  // 任务运行参数模态框配置
  runParamsModal?: {
    component: React.ComponentType<{
      visible: boolean;
      task: TTask;
      onOk: (params: TRunParams) => void;
      onCancel: () => void;
    }>;
  };

  // 工具函数
  utils: {
    getTaskId: (task: TTask) => string;
    getTaskName: (task: TTask) => string;
    getTableName: (task: TTask) => string;
    isIncremental: (task: TTask) => boolean;
  };
}

// ========== Sync 任务配置 ==========

interface SyncTaskActions {
  onSyncTask: (taskId: string) => void;
  onDeleteTask: (taskId: string) => void;
  onBatchSync: () => void;
  onNewIndexSubscribe: () => void;
}

export const createSyncTaskConfig = (
  actions: SyncTaskActions
): TaskTypeConfig<SyncTask, TaskStatus, { targetDate?: string; startDate?: string; endDate?: string }> => ({
  type: 'sync',
  label: '同步任务',
  icon: React.createElement(SyncOutlined),

  api: {
    listTasks: dataApi.listSyncTasks,
    getTaskStatus: dataApi.getSyncTaskStatus,
    createTask: dataApi.createSyncTask,
    updateTask: dataApi.updateSyncTask,
    deleteTask: dataApi.deleteTask,
    runTask: (taskId, params) =>
      dataApi.syncTask(taskId, params?.targetDate, params?.startDate, params?.endDate),
  },

  columns: [
    {
      key: 'task_id',
      title: '任务ID',
      dataIndex: 'task_id',
      width: 120,
      fixed: 'left',
    },
    {
      key: 'description',
      title: '描述',
      dataIndex: 'description',
      width: 180,
    },
    {
      key: 'sync_type',
      title: '类型',
      width: 60,
    },
    {
      key: 'table_name',
      title: '数据表',
      dataIndex: 'table_name',
      width: 120,
    },
    {
      key: 'latest_data',
      title: '最新数据',
      width: 90,
    },
    {
      key: 'last_sync',
      title: '上次同步',
      width: 130,
    },
  ],

  actions: [
    {
      key: 'sync',
      label: '同步',
      icon: React.createElement(SyncOutlined),
      type: 'default',
      disabled: (_task, runningTasks) => runningTasks.has(_task.task_id),
      loading: (_task, runningTasks) => runningTasks.has(_task.task_id),
      onClick: (task) => actions.onSyncTask(task.task_id),
    },
    {
      key: 'delete',
      label: '删除',
      icon: React.createElement(DeleteOutlined),
      type: 'default',
      danger: true,
      onClick: (task) => actions.onDeleteTask(task.task_id),
    },
  ],

  batchActions: [
    {
      key: 'batch_sync',
      label: '批量同步',
      icon: React.createElement(SyncOutlined),
      type: 'primary',
      onClick: actions.onBatchSync,
    },
  ],

  drawer: {
    component: SyncTaskDrawer,
  },

  utils: {
    getTaskId: (task) => task.task_id,
    getTaskName: (task) => task.description,
    getTableName: (task) => task.table_name,
    isIncremental: (task) => task.sync_type === 'incremental',
  },
});

// ========== ETL 任务配置 ==========

interface ETLTaskActions {
  onBackfillTask: (taskId: string) => void;
  onDeleteTask: (taskId: string) => void;
  onBatchBackfill: () => void;
}

export const createEtlTaskConfig = (
  actions: ETLTaskActions
): TaskTypeConfig<ETLTask, ETLTaskStatus, { startDate?: string; endDate?: string }> => ({
  type: 'etl',
  label: 'ETL 任务',
  icon: React.createElement(PlayCircleOutlined),

  api: {
    listTasks: dataApi.listEtlTasks,
    getTaskStatus: dataApi.getEtlTaskStatus,
    createTask: dataApi.createEtlTask,
    updateTask: dataApi.updateEtlTask,
    deleteTask: dataApi.deleteEtlTask,
    runTask: (taskId, params) =>
      dataApi.backfillEtlTask(taskId, params?.startDate || '', params?.endDate || ''),
  },

  columns: [
    {
      key: 'task_id',
      title: '任务ID',
      dataIndex: 'task_id',
      width: 120,
      fixed: 'left',
    },
    {
      key: 'description',
      title: '描述',
      dataIndex: 'description',
      width: 180,
    },
    {
      key: 'sync_type',
      title: '类型',
      width: 60,
    },
    {
      key: 'table_name',
      title: '数据表',
      dataIndex: 'table_name',
      width: 120,
    },
    {
      key: 'latest_data',
      title: '最新数据',
      width: 90,
    },
    {
      key: 'last_sync',
      title: '上次同步',
      width: 130,
    },
  ],

  actions: [
    {
      key: 'backfill',
      label: '回溯',
      icon: React.createElement(HistoryOutlined),
      type: 'default',
      disabled: (_task, runningTasks) => runningTasks.has(_task.task_id),
      loading: (_task, runningTasks) => runningTasks.has(_task.task_id),
      onClick: (task) => actions.onBackfillTask(task.task_id),
    },
    {
      key: 'delete',
      label: '删除',
      icon: React.createElement(DeleteOutlined),
      type: 'default',
      danger: true,
      onClick: (task) => actions.onDeleteTask(task.task_id),
    },
  ],

  batchActions: [
    {
      key: 'batch_backfill',
      label: '批量回溯',
      icon: React.createElement(HistoryOutlined),
      type: 'primary',
      onClick: actions.onBatchBackfill,
    },
  ],

  drawer: {
    component: ETLTaskDrawer,
  },

  utils: {
    getTaskId: (task) => task.task_id,
    getTaskName: (task) => task.description,
    getTableName: (task) => task.table_name,
    isIncremental: (task) => task.sync_type === 'incremental',
  },
});

// ========== Factor 任务配置 ==========

interface FactorTaskActions {
  onRunFactor: (factorId: string) => void;
  onDeleteFactor: (factorId: string) => void;
}

export const createFactorTaskConfig = (
  actions: FactorTaskActions
): TaskTypeConfig<FactorDefinition, GenericTaskStatus, { startDate?: string; endDate?: string; mode?: string }> => ({
  type: 'factor',
  label: '因子任务',
  icon: React.createElement(PlayCircleOutlined),

  api: {
    listTasks: () =>
      productionApi.listFactors().then((res) => ({
        data: { tasks: res.data?.data || [] },
      })),
    getTaskStatus: (_taskId: string) =>
      Promise.resolve({ data: { data: {} as GenericTaskStatus } }),
    deleteTask: (taskId: string) => productionApi.deleteFactor(taskId),
    runTask: (taskId: string, params) =>
      productionApi.runProduction(taskId, params?.mode || 'incremental', undefined, params?.startDate, params?.endDate),
  },

  columns: [
    {
      key: 'factor_id',
      title: '因子ID',
      dataIndex: 'factor_id',
      width: 180,
      fixed: 'left',
    },
    {
      key: 'description',
      title: '描述',
      dataIndex: 'description',
      width: 180,
    },
    {
      key: 'compute_mode',
      title: '模式',
      dataIndex: 'compute_mode',
      width: 80,
    },
    {
      key: 'latest_date',
      title: '最新数据',
      dataIndex: 'latest_date',
      width: 100,
    },
  ],

  actions: [
    {
      key: 'run',
      label: '计算',
      icon: React.createElement(PlayCircleOutlined),
      type: 'default',
      disabled: (task, runningTasks) => runningTasks.has(task.factor_id),
      loading: (task, runningTasks) => runningTasks.has(task.factor_id),
      onClick: (task) => actions.onRunFactor(task.factor_id),
    },
    {
      key: 'delete',
      label: '删除',
      icon: React.createElement(DeleteOutlined),
      type: 'default',
      danger: true,
      onClick: (task) => actions.onDeleteFactor(task.factor_id),
    },
  ],

  drawer: {
    component: FactorTaskDrawer,
  },

  utils: {
    getTaskId: (task) => task.factor_id,
    getTaskName: (task) => task.description || task.factor_id,
    getTableName: (_task) => 'factor_values',
    isIncremental: (task) => task.compute_mode === 'incremental',
  },
});

// 任务类型配置映射
// 注意：实际使用时需要通过工厂函数创建配置，传入 actions
export const taskTypeConfigFactories = {
  sync: createSyncTaskConfig,
  etl: createEtlTaskConfig,
  factor: createFactorTaskConfig,
} as const;

// 导出类型
export type TaskType = keyof typeof taskTypeConfigFactories;
