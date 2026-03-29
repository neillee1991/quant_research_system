import React from 'react';
import { Table, Tag, Tooltip, Typography } from 'antd';
import type { TaskRun } from '../../api';

const { Text } = Typography;

interface TaskLogTableProps {
  logs: TaskRun[];
  loading?: boolean;
  taskIdLabel?: string;
}

const statusColorMap: Record<string, string> = {
  success: 'green',
  failed: 'red',
  running: 'blue',
};

export const TaskLogTable: React.FC<TaskLogTableProps> = ({
  logs,
  loading = false,
  taskIdLabel = '任务ID',
}) => {
  const columns = [
    {
      title: taskIdLabel,
      dataIndex: 'task_id',
      key: 'task_id',
      width: 150,
      render: (v: string) => (
        <Tooltip title={v}>
          <Text code style={{ fontSize: '12px' }} ellipsis>
            {v}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: '任务名称',
      dataIndex: 'task_name',
      key: 'task_name',
      width: 160,
      render: (v: string) => (
        <Tooltip title={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {v || '-'}
          </div>
        </Tooltip>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (v: string) => (
        <Tag color={statusColorMap[v] || 'default'}>{v}</Tag>
      ),
    },
    {
      title: '行数',
      dataIndex: 'rows',
      key: 'rows',
      width: 80,
      render: (v: number | null) => (
        <span style={{ color: 'var(--color-primary)', fontWeight: 500 }}>
          {v != null && v > 0 ? v.toLocaleString() : '-'}
        </span>
      ),
    },
    {
      title: '耗时(s)',
      dataIndex: 'elapsed_sec',
      key: 'elapsed_sec',
      width: 80,
      render: (v: number | null) => (v != null ? v.toFixed(1) : '-'),
    },
    {
      title: '开始时间',
      dataIndex: 'started_at',
      key: 'started_at',
      width: 150,
      render: (v: string) => (
        <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
          {v ? v.slice(0, 19) : '-'}
        </span>
      ),
    },
    {
      title: '错误信息',
      dataIndex: 'error',
      key: 'error',
      width: 200,
      render: (v: string | null) =>
        v ? (
          <Tooltip title={v}>
            <Text type="danger" style={{ fontSize: '12px' }} ellipsis>
              {v}
            </Text>
          </Tooltip>
        ) : (
          '-'
        ),
    },
  ];

  return (
    <Table
      dataSource={logs}
      columns={columns}
      rowKey="run_id"
      loading={loading}
      pagination={{ pageSize: 10 }}
      scroll={{ x: '100%' }}
      size="small"
    />
  );
};

export default TaskLogTable;
