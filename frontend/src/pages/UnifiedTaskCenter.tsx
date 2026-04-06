/**
 * 统一任务管理示例页面
 * 展示如何使用新的统一任务管理架构
 */
import React, { useState, useEffect, useCallback } from 'react';
import { Tabs, Button } from 'antd';
import { SyncOutlined, PlayCircleOutlined, DatabaseOutlined } from '@ant-design/icons';
import { useTasks } from '../hooks/useTasks';
import { TaskPanel } from '../components/TaskPanel';
import {
  createSyncTaskConfig,
  createEtlTaskConfig,
} from '../config/taskTypes';
import type { SyncTask, ETLTask } from '../types';
import {
  SyncTaskDrawer,
  ETLTaskDrawer,
  SyncModal,
  BatchSyncModal,
  ETLBackfillModal,
  DeleteConfirmModal,
  IndexSubscribeDrawer,
} from './DataCenter/index.components';

const UnifiedTaskCenter: React.FC = () => {
  // ========== 模态框状态 ==========
  // 同步任务模态框
  const [syncModalVisible, setSyncModalVisible] = useState(false);
  const [syncModalTask, setSyncModalTask] = useState<SyncTask | null>(null);
  const [syncTargetDate, setSyncTargetDate] = useState<string>('');
  const [syncStartDate, setSyncStartDate] = useState<string>('');
  const [syncEndDate, setSyncEndDate] = useState<string>('');

  // 批量同步模态框
  const [batchSyncModalVisible, setBatchSyncModalVisible] = useState(false);
  const [batchSyncStartDate, setBatchSyncStartDate] = useState<string>('');
  const [batchSyncEndDate, setBatchSyncEndDate] = useState<string>('');

  // ETL 回溯模态框
  const [etlBackfillModalVisible, setEtlBackfillModalVisible] = useState(false);
  const [etlBackfillTask, setEtlBackfillTask] = useState<ETLTask | null>(null);
  const [etlBackfillTaskId, setEtlBackfillTaskId] = useState<string>('');
  const [etlBackfillStartDate, setEtlBackfillStartDate] = useState<string>('');
  const [etlBackfillEndDate, setEtlBackfillEndDate] = useState<string>('');

  // 批量 ETL 回溯模态框
  const [batchEtlBackfillModalVisible, setBatchEtlBackfillModalVisible] = useState(false);
  const [batchEtlBackfillStartDate, setBatchEtlBackfillStartDate] = useState<string>('');
  const [batchEtlBackfillEndDate, setBatchEtlBackfillEndDate] = useState<string>('');

  // 删除确认模态框
  const [deleteConfirmVisible, setDeleteConfirmVisible] = useState(false);
  const [deleteConfirmTaskId, setDeleteConfirmTaskId] = useState<string>('');
  const [deleteConfirmType, setDeleteConfirmType] = useState<'sync' | 'etl'>('sync');

  // 任务配置抽屉状态
  const [syncDrawerVisible, setSyncDrawerVisible] = useState(false);
  const [syncDrawerTask, setSyncDrawerTask] = useState<SyncTask | null>(null);
  const [syncDrawerIsNew, setSyncDrawerIsNew] = useState(false);

  // ETL 任务配置抽屉状态
  const [etlDrawerVisible, setEtlDrawerVisible] = useState(false);
  const [etlDrawerTask, setEtlDrawerTask] = useState<ETLTask | null>(null);
  const [etlDrawerIsNew, setEtlDrawerIsNew] = useState(false);

  // 指数订阅抽屉状态
  const [indexSubscribeDrawerVisible, setIndexSubscribeDrawerVisible] = useState(false);

  // ========== Sync 任务 Actions ==========
  const syncActions = {
    onSyncTask: (taskId: string) => {
      const task = syncTasksHook.tasks.find((t) => t.task_id === taskId);
      if (!task) return;
      setSyncModalTask(task);
      setSyncTargetDate('');
      setSyncStartDate('');
      setSyncEndDate('');
      setSyncModalVisible(true);
    },
    onDeleteTask: (taskId: string) => {
      setDeleteConfirmTaskId(taskId);
      setDeleteConfirmType('sync');
      setDeleteConfirmVisible(true);
    },
    onBatchSync: () => setBatchSyncModalVisible(true),
    onNewIndexSubscribe: () => setIndexSubscribeDrawerVisible(true),
  };

  // ========== ETL 任务 Actions ==========
  const etlActions = {
    onBackfillTask: (taskId: string) => {
      const task = etlTasksHook.tasks.find((t) => t.task_id === taskId);
      if (!task) return;
      setEtlBackfillTask(task);
      setEtlBackfillTaskId(taskId);
      setEtlBackfillStartDate('');
      setEtlBackfillEndDate('');
      setEtlBackfillModalVisible(true);
    },
    onDeleteTask: (taskId: string) => {
      setDeleteConfirmTaskId(taskId);
      setDeleteConfirmType('etl');
      setDeleteConfirmVisible(true);
    },
    onBatchBackfill: () => setBatchEtlBackfillModalVisible(true),
  };

  // ========== 创建配置 ==========
  const syncConfig = createSyncTaskConfig(syncActions);
  const etlConfig = createEtlTaskConfig(etlActions);

  // ========== 使用通用 Hook ==========
  const syncTasksHook = useTasks({ config: syncConfig, autoLoad: false });
  const etlTasksHook = useTasks({ config: etlConfig, autoLoad: false });

  // ========== 初始化加载 ==========
  useEffect(() => {
    syncTasksHook.loadTasks();
    etlTasksHook.loadTasks();
  }, []);

  // ========== Sync 任务处理函数 ==========
  const executeSyncTask = async () => {
    if (!syncModalTask) return;
    setSyncModalVisible(false);
    await syncTasksHook.runTask(syncModalTask.task_id, {
      targetDate: syncTargetDate || undefined,
      startDate: syncStartDate || undefined,
      endDate: syncEndDate || undefined,
    });
  };

  const executeBatchSync = async () => {
    setBatchSyncModalVisible(false);
    await syncTasksHook.batchRunTasks(syncTasksHook.selectedTaskIds, {
      startDate: batchSyncStartDate || undefined,
      endDate: batchSyncEndDate || undefined,
    });
  };

  const handleNewSyncTask = () => {
    setSyncDrawerTask(null);
    setSyncDrawerIsNew(true);
    setSyncDrawerVisible(true);
  };

  const handleOpenSyncDrawer = (task: SyncTask) => {
    setSyncDrawerTask(task);
    setSyncDrawerIsNew(false);
    setSyncDrawerVisible(true);
  };

  const handleSaveSyncTask = async () => {
    await syncTasksHook.loadTasks();
  };

  // ========== ETL 任务处理函数 ==========
  const executeEtlBackfill = async () => {
    setEtlBackfillModalVisible(false);
    await etlTasksHook.runTask(etlBackfillTaskId, {
      startDate: etlBackfillStartDate,
      endDate: etlBackfillEndDate,
    });
  };

  const executeBatchEtlBackfill = async () => {
    setBatchEtlBackfillModalVisible(false);
    await etlTasksHook.batchRunTasks(etlTasksHook.selectedTaskIds, {
      startDate: batchEtlBackfillStartDate,
      endDate: batchEtlBackfillEndDate,
    });
  };

  const handleNewEtlTask = () => {
    setEtlDrawerTask(null);
    setEtlDrawerIsNew(true);
    setEtlDrawerVisible(true);
  };

  const handleOpenEtlDrawer = (task: ETLTask) => {
    setEtlDrawerTask(task);
    setEtlDrawerIsNew(false);
    setEtlDrawerVisible(true);
  };

  const handleSaveEtlTask = async () => {
    await etlTasksHook.loadTasks();
  };

  // ========== 删除确认处理 ==========
  const executeDelete = async () => {
    setDeleteConfirmVisible(false);
    try {
      if (deleteConfirmType === 'sync') {
        await syncTasksHook.deleteTask(deleteConfirmTaskId, true);
      } else {
        await etlTasksHook.deleteTask(deleteConfirmTaskId, true);
      }
    } catch {
      // Error already handled in hook
    }
  };

  // ========== 指数订阅处理 ==========
  const handleIndexSubscribeSuccess = () => {
    syncTasksHook.loadTasks();
  };

  return (
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{ marginBottom: '16px' }}>
        <h1
          style={{
            color: 'var(--color-primary)',
            fontSize: '24px',
            fontWeight: 700,
            margin: 0,
            letterSpacing: '1px',
          }}
        >
          <DatabaseOutlined style={{ marginRight: '8px' }} />
          统一任务管理中心 (示例)
        </h1>
        <p
          style={{
            color: 'var(--text-secondary)',
            margin: '4px 0 0 0',
            fontSize: '12px',
          }}
        >
          使用新的统一任务管理架构
        </p>
      </div>

      <Tabs
        defaultActiveKey="1"
        items={[
          {
            key: '1',
            label: <span><SyncOutlined /> 同步任务</span>,
            children: (
              <TaskPanel
                config={syncConfig}
                tasksHook={syncTasksHook}
                onNewTask={handleNewSyncTask}
                onEditTask={handleOpenSyncDrawer}
                extraActions={
                  <Button onClick={syncActions.onNewIndexSubscribe}>
                    新增指数同步
                  </Button>
                }
              />
            ),
          },
          {
            key: '2',
            label: <span><PlayCircleOutlined /> ETL 任务</span>,
            children: (
              <TaskPanel
                config={etlConfig}
                tasksHook={etlTasksHook}
                onNewTask={handleNewEtlTask}
                onEditTask={handleOpenEtlDrawer}
              />
            ),
          },
        ]}
      />

      {/* ========== 模态框 ========== */}
      <SyncModal
        visible={syncModalVisible}
        task={syncModalTask}
        targetDate={syncTargetDate}
        startDate={syncStartDate}
        endDate={syncEndDate}
        onTargetDateChange={setSyncTargetDate}
        onStartDateChange={setSyncStartDate}
        onEndDateChange={setSyncEndDate}
        onOk={executeSyncTask}
        onCancel={() => setSyncModalVisible(false)}
      />

      <BatchSyncModal
        visible={batchSyncModalVisible}
        selectedTaskIds={syncTasksHook.selectedTaskIds}
        syncTasks={syncTasksHook.tasks}
        startDate={batchSyncStartDate}
        endDate={batchSyncEndDate}
        onStartDateChange={setBatchSyncStartDate}
        onEndDateChange={setBatchSyncEndDate}
        onOk={executeBatchSync}
        onCancel={() => setBatchSyncModalVisible(false)}
      />

      <ETLBackfillModal
        visible={etlBackfillModalVisible}
        task={etlBackfillTask}
        taskId={etlBackfillTaskId}
        startDate={etlBackfillStartDate}
        endDate={etlBackfillEndDate}
        onStartDateChange={setEtlBackfillStartDate}
        onEndDateChange={setEtlBackfillEndDate}
        onOk={executeEtlBackfill}
        onCancel={() => setEtlBackfillModalVisible(false)}
      />

      <ETLBackfillModal
        visible={batchEtlBackfillModalVisible}
        taskId={`批量回溯 (${etlTasksHook.selectedTaskIds.length} 个任务)`}
        startDate={batchEtlBackfillStartDate}
        endDate={batchEtlBackfillEndDate}
        onStartDateChange={setBatchEtlBackfillStartDate}
        onEndDateChange={setBatchEtlBackfillEndDate}
        onOk={executeBatchEtlBackfill}
        onCancel={() => setBatchEtlBackfillModalVisible(false)}
        selectedTaskIds={etlTasksHook.selectedTaskIds}
        etlTasks={etlTasksHook.tasks}
      />

      <DeleteConfirmModal
        visible={deleteConfirmVisible}
        taskId={deleteConfirmTaskId}
        type={deleteConfirmType}
        onOk={executeDelete}
        onCancel={() => setDeleteConfirmVisible(false)}
      />

      {/* ========== 抽屉 ========== */}
      <SyncTaskDrawer
        visible={syncDrawerVisible}
        task={syncDrawerTask}
        isNew={syncDrawerIsNew}
        onClose={() => setSyncDrawerVisible(false)}
        onSave={handleSaveSyncTask}
      />

      <ETLTaskDrawer
        visible={etlDrawerVisible}
        task={etlDrawerTask}
        isNew={etlDrawerIsNew}
        onClose={() => setEtlDrawerVisible(false)}
        onSave={handleSaveEtlTask}
      />

      <IndexSubscribeDrawer
        visible={indexSubscribeDrawerVisible}
        onClose={() => setIndexSubscribeDrawerVisible(false)}
        onSubscribeSuccess={handleIndexSubscribeSuccess}
        onUnsubscribeSuccess={handleIndexSubscribeSuccess}
        onSubscribe={() => {}}
      />
    </div>
  );
};

export default UnifiedTaskCenter;
