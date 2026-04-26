/**
 * ETL 任务面板组件
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
  PlayCircleOutlined,
  ReloadOutlined,
  DeleteOutlined,
  HistoryOutlined,
} from '@ant-design/icons';
import { useTaskLogs } from '../../hooks/useTaskLogs';
import { TaskLogTable } from '../../components/TaskLogTable';
import type { ETLTask } from '../../types';
import type { ETLTaskStatus } from './types';

interface ETLPanelProps {
  etlTasks: ETLTask[];
  etlTaskStatuses: Record<string, ETLTaskStatus>;
  runningEtlTasks: Set<string>;
  selectedEtlTaskIds: string[];
  onSelectedEtlTaskIdsChange: (ids: string[]) => void;
  onRefresh: () => void;
  onNewTask: () => void;
  onBatchBackfill: () => void;
  onEditTask: (task: ETLTask) => void;
  onDeleteTask: (taskId: string) => void;
  onOpenBackfillModal: (taskId: string) => void;
}

export const ETLPanel: React.FC<ETLPanelProps> = ({
  etlTasks,
  etlTaskStatuses,
  runningEtlTasks,
  selectedEtlTaskIds,
  onSelectedEtlTaskIdsChange,
  onRefresh,
  onNewTask,
  onBatchBackfill,
  onEditTask,
  onDeleteTask,
  onOpenBackfillModal,
}) => {
  const { logs: etlLogs, loading: etlLogsLoading, loadLogs: loadEtlLogs } = useTaskLogs('etl');

  useEffect(() => {
    loadEtlLogs();
  }, [loadEtlLogs]);

  const formatDate = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '-';
    const dateString = String(dateStr);
    if (typeof dateString === 'string' && dateString.length === 8) {
      return `${dateString.slice(0, 4)}-${dateString.slice(4, 6)}-${dateString.slice(6, 8)}`;
    }
    return dateString.slice(0, 10);
  };

  const etlTaskColumns = [
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
            onClick={() => onEditTask(r)}
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
      dataIndex: 'sync_type',
      key: 'sync_type',
      width: 60,
      render: (v: string) => (
        <Tag color={v === 'incremental' ? 'blue' : 'green'}>
          {v === 'incremental' ? '增量' : '全量'}
        </Tag>
      ),
    },
    {
      title: '数据表',
      dataIndex: 'table_name',
      key: 'table_name',
      width: 120,
      render: (v: string) => (
        <Tooltip title={v || '-'}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v || '-'}</code>
          </div>
        </Tooltip>
      ),
    },
    {
      title: '最新数据',
      dataIndex: 'last_date',
      key: 'last_date',
      width: 90,
      render: (v: string) => {
        const formatted = formatDate(v);
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
      dataIndex: 'last_sync_time',
      key: 'last_sync_time',
      width: 130,
      render: (v: string) => {
        if (!v) return '-';
        return (
          <Tooltip title={v}>
            <div
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'var(--text-secondary)',
                fontSize: '12px',
              }}
            >
              {(() => {
                const normalized = String(v).replace(' ', 'T');
                const d = new Date(normalized);
                return isNaN(d.getTime()) ? String(v).slice(0, 16) : d.toLocaleString('zh-CN', {
                  year: 'numeric', month: '2-digit', day: '2-digit',
                  hour: '2-digit', minute: '2-digit', hour12: false,
                });
              })()}
            </div>
          </Tooltip>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 200,
      fixed: 'right' as const,
      render: (_: any, r: any) => {
        return (
          <div style={{ display: 'flex', gap: 4 }}>
            <Button icon={<HistoryOutlined />} onClick={() => onOpenBackfillModal(r.task_id)}>
              回溯
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
            ETL 任务管理
          </span>
        }
        extra={
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {selectedEtlTaskIds.length > 0 && (
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={onBatchBackfill}

              >
                批量回溯 ({selectedEtlTaskIds.length})
              </Button>
            )}
            <Button icon={<ReloadOutlined />} onClick={onRefresh}>
              刷新
            </Button>
            <Button onClick={onNewTask}>
              新建任务
            </Button>
          </div>
        }
      >
        <Table
          dataSource={etlTasks.map((task) => {
            const status = etlTaskStatuses[task.task_id];
            return { ...task, ...status };
          })}
          rowKey="task_id"

          pagination={false}
          rowSelection={{
            selectedRowKeys: selectedEtlTaskIds,
            onChange: (selectedRowKeys) => onSelectedEtlTaskIdsChange(selectedRowKeys as string[]),
          }}
          locale={{ emptyText: <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无 ETL 任务</div> }}
          columns={etlTaskColumns}
        />
      </Card>

      <Card
        className="content-card"
        style={{ marginTop: 12 }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            ETL 日志
          </span>
        }
        extra={
          <Button
            icon={<ReloadOutlined />}
            onClick={() => loadEtlLogs()}
            type="text"
          >
            刷新
          </Button>
        }
      >
        <TaskLogTable logs={etlLogs} loading={etlLogsLoading} onFilter={loadEtlLogs} />
      </Card>
    </>
  );
};
