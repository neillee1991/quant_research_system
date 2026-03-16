/**
 * 同步任务面板组件
 */
import React, { useState } from 'react';
import {
  Card,
  Table,
  Button,
  Tag,
  Tooltip,
  Select,
  Toast,
} from '@douyinfe/semi-ui';
import {
  IconSync,
  IconRefresh,
  IconDelete,
  IconHistory,
  IconClock,
} from '@douyinfe/semi-icons';
import QuantDatePicker from '../../components/QuantDatePicker';
import type { SyncTask, TaskStatus, SyncLog, ScheduleInfo } from '../../types';

interface SyncPanelProps {
  syncTasks: SyncTask[];
  taskStatuses: Record<string, TaskStatus>;
  syncLogs: SyncLog[];
  syncingTasks: Set<string>;
  selectedTaskIds: string[];
  scheduleInfo: Record<string, ScheduleInfo>;
  onSelectedTaskIdsChange: (ids: string[]) => void;
  onRefreshStatus: () => void;
  onNewTask: () => void;
  onBatchSync: () => void;
  onSyncTask: (taskId: string) => void;
  onDeleteTask: (taskId: string) => void;
  onOpenTaskDrawer: (task: SyncTask) => void;
  onLoadSyncLogs: (source?: string, dataType?: string, startDate?: string, endDate?: string) => void;
}

export const SyncPanel: React.FC<SyncPanelProps> = ({
  syncTasks,
  taskStatuses,
  syncLogs,
  syncingTasks,
  selectedTaskIds,
  scheduleInfo,
  onSelectedTaskIdsChange,
  onRefreshStatus,
  onNewTask,
  onBatchSync,
  onSyncTask,
  onDeleteTask,
  onOpenTaskDrawer,
  onLoadSyncLogs,
}) => {
  const [filterSource, setFilterSource] = useState<string | undefined>(undefined);
  const [filterDataType, setFilterDataType] = useState<string | undefined>(undefined);
  const [filterStartDate, setFilterStartDate] = useState<string>('');
  const [filterEndDate, setFilterEndDate] = useState<string>('');

  const handleFilterChange = () => {
    onLoadSyncLogs(filterSource, filterDataType, filterStartDate || undefined, filterEndDate || undefined);
  };

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
        <Tooltip content={v}>
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
        <Tooltip content={v}>
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
        <Tag size="small" color={r.sync_type === 'incremental' ? 'blue' : 'green'}>
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
        <Tooltip content={v}>
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
          <Tooltip content={formatted}>
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
          <Tooltip content={syncTime}>
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
              size="small"
              icon={
                <IconSync
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
              size="small"
              type="danger"
              icon={<IconDelete />}
              onClick={() => onDeleteTask(r.task_id)}
            />
          </div>
        );
      },
    },
  ];

  const syncLogColumns = [
    {
      title: '任务ID',
      dataIndex: 'data_type',
      key: 'data_type',
      width: 150,
      render: (v: string) => (
        <Tooltip content={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
          </div>
        </Tooltip>
      ),
    },
    {
      title: '同步日期',
      dataIndex: 'sync_date',
      key: 'sync_date',
      width: 100,
      render: (v: string) => (
        <Tooltip content={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {v}
          </div>
        </Tooltip>
      ),
    },
    {
      title: '参数',
      dataIndex: 'params',
      key: 'params',
      width: 200,
      render: (v: string) =>
        v ? (
          <Tooltip content={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              <code style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>{v}</code>
            </div>
          </Tooltip>
        ) : (
          '-'
        ),
    },
    {
      title: '同步行数',
      dataIndex: 'rows_synced',
      key: 'rows_synced',
      width: 90,
      render: (text: number) => (
        <span style={{ color: 'var(--color-primary)', fontWeight: 500 }}>
          {text?.toLocaleString() || 0}
        </span>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (v: string) => {
        const colorMap: Record<string, string> = {
          success: 'green',
          failed: 'red',
          running: 'blue',
        };
        return <Tag color={(colorMap[v] || 'grey') as any}>{v}</Tag>;
      },
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 150,
      render: (v: string) => (
        <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
          {v?.slice(0, 19) || '-'}
        </span>
      ),
    },
    {
      title: '错误信息',
      dataIndex: 'error_message',
      key: 'error_message',
      width: 200,
      render: (v: string) =>
        v ? (
          <Tooltip content={v}>
            <div
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                display: '-webkit-box',
                WebkitLineClamp: 2,
                WebkitBoxOrient: 'vertical',
                color: 'var(--color-danger)',
                fontSize: '12px',
                lineHeight: '1.4',
              }}
            >
              {v}
            </div>
          </Tooltip>
        ) : (
          '-'
        ),
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
        headerExtraContent={
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {selectedTaskIds.length > 0 && (
              <Button
                theme="solid"
                type="primary"
                icon={<IconSync />}
                onClick={onBatchSync}
                size="small"
              >
                批量同步 ({selectedTaskIds.length})
              </Button>
            )}
            <Button icon={<IconRefresh />} onClick={onRefreshStatus} size="small">
              刷新
            </Button>
            <Button onClick={onNewTask} size="small">
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
          size="small"
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
        headerExtraContent={
          <Button
            icon={<IconRefresh />}
            onClick={() => handleFilterChange()}
            size="small"
            theme="borderless"
          >
            刷新
          </Button>
        }
      >
        <div
          style={{
            display: 'flex',
            gap: 8,
            flexWrap: 'wrap',
            marginBottom: 12,
            alignItems: 'center',
          }}
        >
          <Select
            placeholder="按任务筛选"
            style={{ width: 150 }}
            showClear
            size="small"
            optionList={syncTasks.map((task) => ({ label: task.task_id, value: task.task_id }))}
            onChange={(value) => setFilterDataType(value as string | undefined)}
          />
          <QuantDatePicker
            value={[filterStartDate, filterEndDate]}
            style={{ width: 280 }}
            onChange={(s, e) => { setFilterStartDate(s); setFilterEndDate(e); }}
          />
          <Button theme="solid" type="primary" onClick={handleFilterChange} size="small">
            筛选
          </Button>
          <span style={{ fontSize: 11, color: 'var(--semi-color-text-2)', whiteSpace: 'nowrap' }}>
            按任务完成日期筛选
          </span>
        </div>
        <Table
          dataSource={syncLogs}
          columns={syncLogColumns}
          rowKey={(record: any) => `${record.id || ''}-${record.created_at || ''}`}
          size="small"
          pagination={{ pageSize: 10 }}
          scroll={{ x: '100%' }}
        />
      </Card>
    </>
  );
};
