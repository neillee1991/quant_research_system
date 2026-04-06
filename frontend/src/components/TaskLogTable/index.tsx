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

function renderJsonCell(v: string | null) {
  if (!v) return '-';
  try {
    const obj = JSON.parse(v);
    const text = Object.entries(obj)
      .filter(([, val]) => val != null && val !== '')
      .map(([k, val]) => `${k}: ${val}`)
      .join(', ');
    return (
      <Tooltip title={<pre style={{ margin: 0, fontSize: 11 }}>{JSON.stringify(obj, null, 2)}</pre>}>
        <Text style={{ fontSize: '12px' }} ellipsis>{text || '-'}</Text>
      </Tooltip>
    );
  } catch {
    return <Text style={{ fontSize: '12px' }} ellipsis>{v}</Text>;
  }
}

function renderTime(v: string | null) {
  return (
    <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
      {v ? v.slice(0, 19) : '-'}
    </span>
  );
}

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
          <Text code style={{ fontSize: '12px' }} ellipsis>{v}</Text>
        </Tooltip>
      ),
    },
    {
      title: '运行ID',
      dataIndex: 'run_id',
      key: 'run_id',
      width: 120,
      render: (v: string) => (
        <Tooltip title={v}>
          <Text code style={{ fontSize: '12px' }} ellipsis>{v.slice(-8)}</Text>
        </Tooltip>
      ),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (v: string) => <Tag color={statusColorMap[v] || 'default'}>{v}</Tag>,
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
      render: renderTime,
    },
    {
      title: '结束时间',
      dataIndex: 'finished_at',
      key: 'finished_at',
      width: 150,
      render: renderTime,
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
      title: '参数',
      dataIndex: 'params',
      key: 'params',
      width: 180,
      render: renderJsonCell,
    },
    {
      title: '附加信息',
      dataIndex: 'extra',
      key: 'extra',
      width: 160,
      render: renderJsonCell,
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
