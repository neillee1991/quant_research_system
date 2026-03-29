/**
 * 同步任务面板组件
 */
import React, { useEffect } from 'react';
import {
  Card,
  Table,
  Button,
  Tag,
  Tooltip,
} from 'antd';
import {
  SyncOutlined,
  ReloadOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { useTaskLogs } from '../../hooks/useTaskLogs';
import { TaskLogTable } from '../../components/TaskLogTable';
import type { SyncTask, TaskStatus, ScheduleInfo } from '../../types';

interface SyncPanelProps {
  syncTasks: SyncTask[];
  taskStatuses: Record<string, TaskStatus>;
  syncingTasks: Set<string>;
  selectedTaskIds: string[];
  scheduleInfo: Record<string, ScheduleInfo>;
  onSelectedTaskIdsChange: (ids: string[]) => void;
  onRefreshStatus: () => void;
  onNewTask: () => void;
  onNewIndexSubscribe: () => void;
  onBatchSync: () => void;
  onSyncTask: (taskId: string) => void;
  onDeleteTask: (taskId: string) => void;
  onOpenTaskDrawer: (task: SyncTask) => void;
}

export const SyncPanel: React.FC<SyncPanelProps> = ({
  syncTasks,
  taskStatuses,
  syncingTasks,
  selectedTaskIds,
  scheduleInfo,
  onSelectedTaskIdsChange,
  onRefreshStatus,
  onNewTask,
  onNewIndexSubscribe,
  onBatchSync,
  onSyncTask,
  onDeleteTask,
  onOpenTaskDrawer,
}) => {
  const { logs: syncLogs, loading: syncLogsLoading, loadLogs: loadSyncLogs } = useTaskLogs('sync');

  useEffect(() => {
    loadSyncLogs();
  }, [loadSyncLogs]);

  const formatDate = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '-';
    if (typeof dateStr === 'string' && dateStr.length === 8) {
      return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
    }
    return String(dateStr).slice(0, 10);
  };

  const syncTaskColumns = [
    {
      title: '任务ID',
      dataIndex: 'task_id',
      key: 'task_id',
      width: 120,
      fixed: 'left' as const,
      render: (v: string, r: any) => (
        <Tooltip title={v}>
          <span
            style={{
              cursor: 'pointer',
              display: 'block',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
            onClick={() => onOpenTaskDrawer(r)}
          >
            <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
          </span>
        </Tooltip>
      ),
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'desc',
      width: 180,
      render: (v: string) => (
        <Tooltip title={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {v}
          </div>
        </Tooltip>
      ),
    },
    {
      title: '类型',
      key: 'sync_type',
      width: 60,
      render: (_: any, r: any) => (
        <Tag color={r.sync_type === 'incremental' ? 'blue' : 'green'}>
          {r.sync_type === 'incremental' ? '增量' : '全量'}
        </Tag>
      ),
    },
    {
      title: '数据表',
      dataIndex: 'table_name',
      key: 'table_name',
      width: 120,
      render: (v: string) => (
        <Tooltip title={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v}</code>
          </div>
        </Tooltip>
      ),
    },
    {
      title: '最新数据',
      key: 'latest',
      width: 90,
      render: (_: any, r: any) => {
        const dateStr = r.status?.table_latest_date;
        const formatted = formatDate(dateStr);
        return (
          <Tooltip title={formatted}>
            <div
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: formatted === '-' ? 'var(--text-muted)' : 'var(--color-gain)',
              }}
            >
              {formatted}
            </div>
          </Tooltip>
        );
      },
    },
    {
      title: '上次同步',
      key: 'last_sync',
      width: 130,
      render: (_: any, r: any) => {
        const syncTime = r.status?.last_sync_time;
        if (!syncTime) return '-';
        return (
          <Tooltip title={syncTime}>
            <div
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'var(--text-secondary)',
                fontSize: '12px',
              }}
            >
              {syncTime.slice(0, 16)}
            </div>
          </Tooltip>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 160,
      fixed: 'right' as const,
      render: (_: any, r: any) => {
        const isSyncing = syncingTasks.has(r.task_id);
        return (
          <div style={{ display: 'flex', gap: 4 }}>
            <Button

              icon={
                <SyncOutlined
                  style={isSyncing ? { animation: 'spin 1s linear infinite' } : undefined}
                />
              }
              onClick={() => onSyncTask(r.task_id)}
              disabled={isSyncing}
              loading={isSyncing}
            >
              同步
            </Button>
            <Button

              danger
              icon={<DeleteOutlined />}
              onClick={() => onDeleteTask(r.task_id)}
            />
          </div>
        );
      },
    },
  ];

  return (
    <>
      <Card
        className="content-card"
        style={{ marginBottom: '12px' }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            同步任务管理
          </span>
        }
        extra={
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {selectedTaskIds.length > 0 && (
              <Button
                type="primary"
                icon={<SyncOutlined />}
                onClick={onBatchSync}

              >
                批量同步 ({selectedTaskIds.length})
              </Button>
            )}
            <Button icon={<ReloadOutlined />} onClick={onRefreshStatus}>
              刷新
            </Button>
            <Button onClick={onNewIndexSubscribe}>
              新增指数同步
            </Button>
            <Button onClick={onNewTask}>
              新建任务
            </Button>
          </div>
        }
      >
        <Table
          dataSource={syncTasks.map((task) => {
            const status = taskStatuses[task.task_id];
            const taskScheduleInfo = scheduleInfo[task.task_id];
            return { ...task, status, taskScheduleInfo };
          })}
          rowKey="task_id"

          pagination={false}
          rowSelection={{
            selectedRowKeys: selectedTaskIds,
            onChange: (selectedRowKeys) => onSelectedTaskIdsChange(selectedRowKeys as string[]),
          }}
          columns={syncTaskColumns}
        />
      </Card>

      <Card
        className="content-card"
        style={{ marginTop: 12 }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            同步日志
          </span>
        }
        extra={
          <Button
            icon={<ReloadOutlined />}
            onClick={loadSyncLogs}
            type="text"
          >
            刷新
          </Button>
        }
      >
        <TaskLogTable logs={syncLogs} loading={syncLogsLoading} />
      </Card>
    </>
  );
};
