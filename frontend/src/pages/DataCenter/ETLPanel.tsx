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
} from 'antd';
import {
  PlayCircleOutlined,
  ReloadOutlined,
  DeleteOutlined,
  HistoryOutlined,
} from '@ant-design/icons';
import QuantDatePicker from '../../components/QuantDatePicker';
import type { ETLTask } from '../../types';

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
  onDeleteTask,
  onOpenBackfillModal,
  onLoadLogs,
}) => {
  const [filterTaskId, setFilterTaskId] = useState<string | undefined>(undefined);
  const [filterStartDate, setFilterStartDate] = useState<string>('');
  const [filterEndDate, setFilterEndDate] = useState<string>('');

  const handleFilterChange = () => {
    onLoadLogs(filterTaskId, filterStartDate || undefined, filterEndDate || undefined);
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
              {v.slice(0, 16)}
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

  const etlLogColumns = [
    {
      title: '任务ID',
      dataIndex: 'data_type',
      key: 'data_type',
      width: 180,
      render: (v: string) => (
        <Tooltip title={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
          </div>
        </Tooltip>
      ),
    },
    {
      title: '执行日期',
      dataIndex: 'sync_date',
      key: 'sync_date',
      width: 100,
      render: (v: string) => (
        <Tooltip title={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {v}
          </div>
        </Tooltip>
      ),
    },
    {
      title: '处理行数',
      dataIndex: 'rows_synced',
      key: 'rows_synced',
      width: 100,
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
      title: '参数',
      dataIndex: 'params',
      key: 'params',
      width: 200,
      render: (v: string) =>
        v ? (
          <Tooltip title={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              <code style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>{v}</code>
            </div>
          </Tooltip>
        ) : (
          '-'
        ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 160,
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
      width: 300,
      render: (v: string) =>
        v ? (
          <Tooltip title={v}>
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
          dataSource={etlTasks}
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
            ETL 任务日志
          </span>
        }
        extra={
          <Button
            icon={<ReloadOutlined />}
            onClick={() => handleFilterChange()}
           
            type="text"
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
            allowClear
           
            options={etlTasks.map((t) => ({ label: t.task_id, value: t.task_id }))}
            onChange={(value) => setFilterTaskId(value as string | undefined)}
          />
          <QuantDatePicker
            value={[filterStartDate, filterEndDate]}
            style={{ width: 280 }}
            onChange={(s, e) => { setFilterStartDate(s); setFilterEndDate(e); }}
          />
          <Button type="primary" onClick={handleFilterChange}>
            筛选
          </Button>
          <span style={{ fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>
            按任务完成日期筛选
          </span>
        </div>
        <Table
          dataSource={etlLogs}
          columns={etlLogColumns}
          rowKey={(record: any) => `${record.data_type || ''}-${record.sync_date || ''}-${record.created_at || ''}`}
         
          pagination={{ pageSize: 50 }}
          scroll={{ x: 'max-content' }}
          style={{ width: '100%' }}
        />
      </Card>
    </>
  );
};
