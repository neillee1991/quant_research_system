/**
 * ETL 任务面板组件
 */
import React, { useState } from 'react';
import {
  Card,
  Table,
  Button,
  Tag,
  Tooltip,
  Select,
  DatePicker,
} from '@douyinfe/semi-ui';
import {
  IconPlay,
  IconRefresh,
  IconDelete,
  IconCopy,
  IconHistory,
} from '@douyinfe/semi-icons';
import dayjs from 'dayjs';
import type { ETLTask } from '../../types';
import type { ETLLogFilters } from './types';

interface ETLPanelProps {
  etlTasks: ETLTask[];
  etlLogs: any[];
  runningEtlTasks: Set<string>;
  selectedEtlTaskIds: string[];
  onSelectedEtlTaskIdsChange: (ids: string[]) => void;
  onRefresh: () => void;
  onNewTask: () => void;
  onBatchBackfill: () => void;
  onEditTask: (task: ETLTask) => void;
  onCopyTask: (task: ETLTask) => void;
  onDeleteTask: (taskId: string) => void;
  onOpenBackfillModal: (taskId: string) => void;
  onLoadLogs: (taskId?: string, startDate?: string, endDate?: string) => void;
}

export const ETLPanel: React.FC<ETLPanelProps> = ({
  etlTasks,
  etlLogs,
  runningEtlTasks,
  selectedEtlTaskIds,
  onSelectedEtlTaskIdsChange,
  onRefresh,
  onNewTask,
  onBatchBackfill,
  onEditTask,
  onCopyTask,
  onDeleteTask,
  onOpenBackfillModal,
  onLoadLogs,
}) => {
  const [logFilters, setLogFilters] = useState<ETLLogFilters>({
    taskId: undefined,
    startDate: undefined,
    endDate: undefined,
  });

  const handleFilterChange = () => {
    onLoadLogs(logFilters.taskId, logFilters.startDate, logFilters.endDate);
  };

  const formatDate = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '-';
    if (typeof dateStr === 'string' && dateStr.length === 8) {
      return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
    }
    return String(dateStr).slice(0, 10);
  };

  const etlTaskColumns = [
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
        <Tooltip content={v}>
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
        <Tag size="small" color={v === 'incremental' ? 'blue' : 'green'}>
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
        <Tooltip content={v || '-'}>
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
      dataIndex: 'last_sync_time',
      key: 'last_sync_time',
      width: 130,
      render: (v: string) => {
        if (!v) return '-';
        return (
          <Tooltip content={v}>
            <div
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'var(--text-secondary)',
                fontSize: '12px',
              }}
            >
              {v.slice(0, 16)}
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
        return (
          <div style={{ display: 'flex', gap: 4 }}>
            <Button size="small" icon={<IconHistory />} onClick={() => onOpenBackfillModal(r.task_id)}>
              回溯
            </Button>
            <Tooltip content="复制任务">
              <Button size="small" icon={<IconCopy />} onClick={() => onCopyTask(r)} />
            </Tooltip>
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

  const etlLogColumns = [
    {
      title: '任务ID',
      dataIndex: 'task_id',
      key: 'task_id',
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
      title: '执行日期',
      dataIndex: 'exec_date',
      key: 'exec_date',
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
      title: '处理行数',
      dataIndex: 'rows_processed',
      key: 'rows_processed',
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
      title: '执行时间',
      dataIndex: 'execution_time',
      key: 'execution_time',
      width: 100,
      render: (v: number) => (v ? `${v.toFixed(2)}s` : '-'),
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
      render: (v: string) =>
        v ? (
          <Tooltip content={v}>
            <div
              style={{
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'var(--color-danger)',
                fontSize: '12px',
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
            ETL 任务管理
          </span>
        }
        headerExtraContent={
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {selectedEtlTaskIds.length > 0 && (
              <Button
                theme="solid"
                type="primary"
                icon={<IconPlay />}
                onClick={onBatchBackfill}
                size="small"
              >
                批量回溯 ({selectedEtlTaskIds.length})
              </Button>
            )}
            <Button icon={<IconRefresh />} onClick={onRefresh} size="small">
              刷新
            </Button>
            <Button onClick={onNewTask} size="small">
              新建任务
            </Button>
          </div>
        }
      >
        <Table
          dataSource={etlTasks}
          rowKey="task_id"
          size="small"
          pagination={false}
          rowSelection={{
            selectedRowKeys: selectedEtlTaskIds,
            onChange: (selectedRowKeys) => onSelectedEtlTaskIdsChange(selectedRowKeys as string[]),
          }}
          empty={
            <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>
              暂无 ETL 任务
            </div>
          }
          columns={etlTaskColumns}
        />
      </Card>

      <Card
        className="content-card"
        style={{ marginTop: 12 }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            ETL 任务日志
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
            optionList={etlTasks.map((t) => ({ label: t.task_id, value: t.task_id }))}
            onChange={(value) =>
              setLogFilters({ ...logFilters, taskId: value as string | undefined })
            }
          />
          <DatePicker
            type="dateRange"
            placeholder={['开始日期', '结束日期']}
            style={{ width: 280 }}
            size="small"
            defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
            onChange={(date: any, dateStr: any) => {
              const strs = dateStr as unknown as string[];
              if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                setLogFilters({
                  ...logFilters,
                  startDate: strs[0].replace(/-/g, ''),
                  endDate: strs[1].replace(/-/g, ''),
                });
              } else {
                setLogFilters({ ...logFilters, startDate: undefined, endDate: undefined });
              }
            }}
          />
          <Button theme="solid" type="primary" onClick={handleFilterChange} size="small">
            筛选
          </Button>
        </div>
        <Table
          dataSource={etlLogs}
          columns={etlLogColumns}
          rowKey={(record: any) => `${record.id || ''}-${record.created_at || ''}`}
          size="small"
          pagination={{ pageSize: 50 }}
          scroll={{ x: 'max-content' }}
        />
      </Card>
    </>
  );
};
