/**
 * 数据中心主页面
 * 重构后的版本 - 组件化、模块化
 */
import React, { useEffect, useState } from 'react';
import { Tabs, Modal } from 'antd';
import { SyncOutlined, PlayCircleOutlined, CodeOutlined, DatabaseOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import { dataApi } from '../../api';
import { useThemeStore } from '../../store';
import { SyncPanel } from './SyncPanel';
import { ETLPanel } from './ETLPanel';
import { DataTable } from './DataTable';
import { useSyncTasks } from './hooks/useSyncTasks';
import { useETLTasks } from './hooks/useETLTasks';
import { useDataQuery } from './hooks/useDataQuery';
import {
  SyncModal,
  BatchSyncModal,
  ETLBackfillModal,
  DeleteConfirmModal,
  SchemaChangeConfirmModal,
} from './Modals';
import { SyncTaskDrawer } from './SyncTaskDrawer';
import { ETLTaskDrawer } from './ETLTaskDrawer';
import type { SyncTask, ETLTask } from '@/types';

const DataCenter: React.FC = () => {
  const { mode } = useThemeStore();
  const message = useMessage();

  // 使用自定义 Hooks
  const syncTasksHook = useSyncTasks();
  const etlTasksHook = useETLTasks();
  const dataQueryHook = useDataQuery();

  // 同步任务模态框状态
  const [syncModalVisible, setSyncModalVisible] = useState(false);
  const [syncModalTask, setSyncModalTask] = useState<SyncTask | null>(null);
  const [syncTargetDate, setSyncTargetDate] = useState<string>('');
  const [syncStartDate, setSyncStartDate] = useState<string>('');
  const [syncEndDate, setSyncEndDate] = useState<string>('');

  // 批量同步模态框状态
  const [batchSyncModalVisible, setBatchSyncModalVisible] = useState(false);
  const [batchSyncStartDate, setBatchSyncStartDate] = useState<string>('');
  const [batchSyncEndDate, setBatchSyncEndDate] = useState<string>('');

  // ETL 回溯模态框状态
  const [etlBackfillModalVisible, setEtlBackfillModalVisible] = useState(false);
  const [etlBackfillTask, setEtlBackfillTask] = useState<ETLTask | null>(null);
  const [etlBackfillTaskId, setEtlBackfillTaskId] = useState<string>('');
  const [etlBackfillStartDate, setEtlBackfillStartDate] = useState<string>('');
  const [etlBackfillEndDate, setEtlBackfillEndDate] = useState<string>('');

  // 批量 ETL 回溯模态框状态
  const [batchEtlBackfillModalVisible, setBatchEtlBackfillModalVisible] = useState(false);
  const [batchEtlBackfillStartDate, setBatchEtlBackfillStartDate] = useState<string>('');
  const [batchEtlBackfillEndDate, setBatchEtlBackfillEndDate] = useState<string>('');

  // 删除确认模态框状态
  const [deleteConfirmVisible, setDeleteConfirmVisible] = useState(false);
  const [deleteConfirmTaskId, setDeleteConfirmTaskId] = useState<string>('');
  const [deleteConfirmType, setDeleteConfirmType] = useState<'sync' | 'etl'>('sync');

  // 任务配置抽屉状态
  const [taskDrawerVisible, setTaskDrawerVisible] = useState(false);
  const [taskDrawerTask, setTaskDrawerTask] = useState<SyncTask | null>(null);
  const [taskDrawerIsNew, setTaskDrawerIsNew] = useState(false);

  // ETL 任务配置抽屉状态
  const [etlDrawerVisible, setEtlDrawerVisible] = useState(false);
  const [etlDrawerTask, setEtlDrawerTask] = useState<ETLTask | null>(null);
  const [etlDrawerIsNew, setEtlDrawerIsNew] = useState(false);

  // 初始化数据加载
  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      const [statusBatchRes] = await Promise.all([
        dataApi.getTaskStatusBatch().catch(() => ({ data: { data: {} } })),
        syncTasksHook.loadSyncTasks(),
        etlTasksHook.loadEtlTasks(),
        dataQueryHook.loadTables(),
        syncTasksHook.loadSyncLogs(),
      ]);

      // 批量填充状态和调度信息
      const batchData: Record<string, any> = statusBatchRes.data?.data || {};
      if (Object.keys(batchData).length > 0) {
        for (const taskId of Object.keys(batchData)) {
          await syncTasksHook.loadTaskStatus(taskId);
          await syncTasksHook.loadTaskScheduleInfo(taskId);
        }
      }
    } catch (error) {
      console.error('Failed to load initial data:', error);
      message.error('加载初始数据失败');
    }
  };

  // 同步任务处理函数
  const handleSyncTask = (taskId: string) => {
    const task = syncTasksHook.syncTasks.find((t) => t.task_id === taskId);
    if (!task) return;
    setSyncModalTask(task);
    setSyncTargetDate('');
    setSyncStartDate('');
    setSyncEndDate('');
    setSyncModalVisible(true);
  };

  const executeSyncTask = async () => {
    if (!syncModalTask) return;
    setSyncModalVisible(false);
    try {
      await syncTasksHook.syncTask(
        syncModalTask.task_id,
        syncTargetDate || undefined,
        syncStartDate || undefined,
        syncEndDate || undefined
      );
    } catch (error) {
      // Error already handled in hook
    }
  };

  const handleBatchSync = () => {
    if (syncTasksHook.selectedTaskIds.length === 0) {
      message.warning('请先选择要同步的任务');
      return;
    }
    setBatchSyncStartDate('');
    setBatchSyncEndDate('');
    setBatchSyncModalVisible(true);
  };

  const executeBatchSync = async () => {
    setBatchSyncModalVisible(false);
    await syncTasksHook.batchSyncTasks(
      syncTasksHook.selectedTaskIds,
      batchSyncStartDate || undefined,
      batchSyncEndDate || undefined
    );
  };

  const handleNewTask = () => {
    setTaskDrawerTask(null);
    setTaskDrawerIsNew(true);
    setTaskDrawerVisible(true);
  };

  const handleDeleteTask = (taskId: string) => {
    setDeleteConfirmTaskId(taskId);
    setDeleteConfirmType('sync');
    setDeleteConfirmVisible(true);
  };

  const handleOpenTaskDrawer = (task: SyncTask) => {
    setTaskDrawerTask(task);
    setTaskDrawerIsNew(false);
    setTaskDrawerVisible(true);
  };

  // ETL 任务处理函数
  const handleNewEtlTask = () => {
    setEtlDrawerTask(null);
    setEtlDrawerIsNew(true);
    setEtlDrawerVisible(true);
  };

  const handleEditEtlTask = (task: ETLTask) => {
    setEtlDrawerTask(task);
    setEtlDrawerIsNew(false);
    setEtlDrawerVisible(true);
  };

  const handleDeleteEtlTask = (taskId: string) => {
    setDeleteConfirmTaskId(taskId);
    setDeleteConfirmType('etl');
    setDeleteConfirmVisible(true);
  };

  const handleOpenEtlBackfillModal = (taskId: string) => {
    const task = etlTasksHook.etlTasks.find((t) => t.task_id === taskId);
    if (!task) return;
    setEtlBackfillTask(task);
    setEtlBackfillTaskId(taskId);
    setEtlBackfillStartDate('');
    setEtlBackfillEndDate('');
    setEtlBackfillModalVisible(true);
  };

  const executeEtlBackfill = async () => {
    setEtlBackfillModalVisible(false);
    await etlTasksHook.backfillEtlTask(
      etlBackfillTaskId,
      etlBackfillStartDate,
      etlBackfillEndDate
    );
  };

  const handleBatchEtlBackfill = () => {
    if (etlTasksHook.selectedEtlTaskIds.length === 0) {
      message.warning('请先选择要回溯的任务');
      return;
    }
    setBatchEtlBackfillStartDate('');
    setBatchEtlBackfillEndDate('');
    setBatchEtlBackfillModalVisible(true);
  };

  const executeBatchEtlBackfill = async () => {
    setBatchEtlBackfillModalVisible(false);
    await etlTasksHook.batchBackfillEtlTasks(
      etlTasksHook.selectedEtlTaskIds,
      batchEtlBackfillStartDate,
      batchEtlBackfillEndDate
    );
  };

  // 删除确认处理
  const executeDelete = async () => {
    setDeleteConfirmVisible(false);
    try {
      if (deleteConfirmType === 'sync') {
        await syncTasksHook.deleteTask(deleteConfirmTaskId, true);
      } else {
        await etlTasksHook.deleteEtlTask(deleteConfirmTaskId, true);
      }
    } catch (error) {
      // Error already handled in hook
    }
  };

  // 任务配置保存处理
  const handleSaveTask = async () => {
    await syncTasksHook.loadSyncTasks();
    await loadInitialData();
  };

  const handleSaveEtlTask = async () => {
    await etlTasksHook.loadEtlTasks();
    await loadInitialData();
  };

  // 数据表处理函数
  const handleTruncateTable = (tableName: string) => {
    Modal.confirm({
      title: '清空表数据',
      content: `确定要清空表 "${tableName}" 的所有数据吗？此操作不可撤销。`,
      okText: '确定清空',
      cancelText: '取消',
      onOk: async () => {
        try {
          await dataQueryHook.truncateTable(tableName);
        } catch (error) {
          // Error already handled in hook
        }
      },
    });
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
          数据中心
        </h1>
        <p
          style={{
            color: 'var(--text-secondary)',
            margin: '4px 0 0 0',
            fontSize: '12px',
          }}
        >
          数据同步管理与 SQL 查询
        </p>
      </div>

      <Tabs
        defaultActiveKey="1"
        onChange={(activeKey) => {
          if (activeKey === '1.5') {
            etlTasksHook.loadEtlLogs();
          }
        }}
        items={[
          {
            key: '1',
            label: <span><SyncOutlined /> 同步任务</span>,
            children: (
              <SyncPanel
                syncTasks={syncTasksHook.syncTasks}
                taskStatuses={syncTasksHook.taskStatuses}
                syncLogs={syncTasksHook.syncLogs}
                syncingTasks={syncTasksHook.syncingTasks}
                selectedTaskIds={syncTasksHook.selectedTaskIds}
                scheduleInfo={syncTasksHook.scheduleInfo}
                onSelectedTaskIdsChange={syncTasksHook.setSelectedTaskIds}
                onRefreshStatus={() => {
                  syncTasksHook.syncTasks.forEach((task) =>
                    syncTasksHook.loadTaskStatus(task.task_id)
                  );
                  message.success('任务状态已刷新');
                }}
                onNewTask={handleNewTask}
                onBatchSync={handleBatchSync}
                onSyncTask={handleSyncTask}
                onDeleteTask={handleDeleteTask}
                onOpenTaskDrawer={handleOpenTaskDrawer}
                onLoadSyncLogs={syncTasksHook.loadSyncLogs}
              />
            ),
          },
          {
            key: '1.5',
            label: <span><PlayCircleOutlined /> ETL 任务</span>,
            children: (
              <ETLPanel
                etlTasks={etlTasksHook.etlTasks}
                etlLogs={etlTasksHook.etlLogs}
                runningEtlTasks={etlTasksHook.runningEtlTasks}
                selectedEtlTaskIds={etlTasksHook.selectedEtlTaskIds}
                onSelectedEtlTaskIdsChange={etlTasksHook.setSelectedEtlTaskIds}
                onRefresh={etlTasksHook.loadEtlTasks}
                onNewTask={handleNewEtlTask}
                onBatchBackfill={handleBatchEtlBackfill}
                onEditTask={handleEditEtlTask}
                onDeleteTask={handleDeleteEtlTask}
                onOpenBackfillModal={handleOpenEtlBackfillModal}
                onLoadLogs={etlTasksHook.loadEtlLogs}
              />
            ),
          },
          {
            key: '3',
            label: <span><CodeOutlined /> SQL 查询</span>,
            children: (
              <DataTable
                tables={dataQueryHook.tables}
                sqlQuery={dataQueryHook.sqlQuery}
                queryResult={dataQueryHook.queryResult}
                queryColumns={dataQueryHook.queryColumns}
                queryLoading={dataQueryHook.queryLoading}
                onSqlQueryChange={dataQueryHook.setSqlQuery}
                onExecuteQuery={dataQueryHook.executeQuery}
                onTruncateTable={handleTruncateTable}
                onRefreshTables={dataQueryHook.loadTables}
                theme={mode}
              />
            ),
          },
        ]}
      />

      {/* 模态框 */}
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
        syncTasks={syncTasksHook.syncTasks}
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
        taskId={`批量回溯 (${etlTasksHook.selectedEtlTaskIds.length} 个任务)`}
        startDate={batchEtlBackfillStartDate}
        endDate={batchEtlBackfillEndDate}
        onStartDateChange={setBatchEtlBackfillStartDate}
        onEndDateChange={setBatchEtlBackfillEndDate}
        onOk={executeBatchEtlBackfill}
        onCancel={() => setBatchEtlBackfillModalVisible(false)}
        selectedTaskIds={etlTasksHook.selectedEtlTaskIds}
        etlTasks={etlTasksHook.etlTasks}
      />

      <DeleteConfirmModal
        visible={deleteConfirmVisible}
        taskId={deleteConfirmTaskId}
        type={deleteConfirmType}
        onOk={executeDelete}
        onCancel={() => setDeleteConfirmVisible(false)}
      />

      {/* 同步任务配置抽屉 */}
      <SyncTaskDrawer
        visible={taskDrawerVisible}
        task={taskDrawerTask}
        isNew={taskDrawerIsNew}
        onClose={() => setTaskDrawerVisible(false)}
        onSave={handleSaveTask}
      />

      {/* ETL 任务配置抽屉 */}
      <ETLTaskDrawer
        visible={etlDrawerVisible}
        task={etlDrawerTask}
        isNew={etlDrawerIsNew}
        onClose={() => setEtlDrawerVisible(false)}
        onSave={handleSaveEtlTask}
      />
    </div>
  );
};

export default DataCenter;
