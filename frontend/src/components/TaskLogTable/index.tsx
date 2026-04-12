import React, { useState, useMemo } from 'react';
import { Table, Tag, Tooltip, Typography, Select, DatePicker, Button, Space } from 'antd';
import { SearchOutlined, ReloadOutlined } from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import type { TaskRun } from '../../api';
import type { TaskLogFilter } from '../../hooks/useTaskLogs';

const { Text } = Typography;
const { RangePicker } = DatePicker;

interface TaskLogTableProps {
  logs: TaskRun[];
  loading?: boolean;
  taskIdLabel?: string;
  onFilter?: (filter: TaskLogFilter) => void;
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
  if (!v) return <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>-</span>;
  const d = new Date(v);
  const formatted = isNaN(d.getTime()) ? v.slice(0, 19) : d.toLocaleString('zh-CN', {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  });
  return (
    <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
      {formatted}
    </span>
  );
}

export const TaskLogTable: React.FC<TaskLogTableProps> = ({
  logs,
  loading = false,
  taskIdLabel = '任务ID',
  onFilter,
}) => {
  const [selectedTaskId, setSelectedTaskId] = useState<string | undefined>();
  const [dateRange, setDateRange] = useState<[Dayjs | null, Dayjs | null] | null>(null);
  const [pageSize, setPageSize] = useState(10);

  const taskIdOptions = useMemo(() => {
    const ids = Array.from(new Set(logs.map(l => l.task_id).filter(Boolean)));
    return ids.map(id => ({ label: id, value: id }));
  }, [logs]);

  const handleFilter = () => {
    if (!onFilter) return;
    onFilter({
      taskId: selectedTaskId,
      startDate: dateRange?.[0]?.format('YYYYMMDD'),
      endDate: dateRange?.[1]?.format('YYYYMMDD'),
    });
  };

  const handleReset = () => {
    setSelectedTaskId(undefined);
    setDateRange(null);
    onFilter?.({});
  };

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
          <Text code style={{ fontSize: '12px' }} ellipsis>{v ? v.slice(-8) : '-'}</Text>
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
    <div>
      {onFilter && (
        <Space style={{ marginBottom: 12 }} wrap>
          <Select
            allowClear
            placeholder={`筛选${taskIdLabel}`}
            style={{ minWidth: 180 }}
            options={taskIdOptions}
            value={selectedTaskId}
            onChange={setSelectedTaskId}
            showSearch
            filterOption={(input, opt) =>
              (opt?.label as string)?.toLowerCase().includes(input.toLowerCase())
            }
          />
          <RangePicker
            value={dateRange}
            onChange={v => setDateRange(v as [Dayjs | null, Dayjs | null] | null)}
            format="YYYY-MM-DD"
            placeholder={['开始日期', '结束日期']}
          />
          <Button type="primary" icon={<SearchOutlined />} onClick={handleFilter}>
            筛选
          </Button>
          <Button icon={<ReloadOutlined />} onClick={handleReset}>
            重置
          </Button>
        </Space>
      )}
      <Table
        dataSource={logs}
        columns={columns}
        rowKey="run_id"
        loading={loading}
        pagination={{
          pageSize,
          showSizeChanger: true,
          pageSizeOptions: ['10', '20', '50', '100'],
          onShowSizeChange: (_, size) => setPageSize(size),
        }}
        scroll={{ x: '100%' }}
        size="small"
      />
    </div>
  );
};

export default TaskLogTable;
