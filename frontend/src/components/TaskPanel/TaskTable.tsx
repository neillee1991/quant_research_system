/**
 * 通用任务表格组件
 */
import React from 'react';
import { Table, Button, Tag, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { TaskTypeConfig, GenericTaskStatus, TaskActionConfig } from '../../config/taskTypes';

interface TaskTableProps<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>> {
  config: TaskTypeConfig<TTask, TStatus, TRunParams>;
  tasks: TTask[];
  taskStatuses: Record<string, TStatus>;
  runningTasks: Set<string>;
  selectedTaskIds: string[];
  onSelectedTaskIdsChange: (ids: string[]) => void;
  onEditTask?: (task: TTask) => void;
  formatDate?: (dateStr: string | null | undefined) => string;
}

export function TaskTable<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>>({
  config,
  tasks,
  taskStatuses,
  runningTasks,
  selectedTaskIds,
  onSelectedTaskIdsChange,
  onEditTask,
  formatDate,
}: TaskTableProps<TTask, TStatus, TRunParams>) {
  const defaultFormatDate = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '-';
    if (typeof dateStr === 'string' && dateStr.length === 8) {
      return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
    }
    return String(dateStr).slice(0, 10);
  };

  const formatDateFn = formatDate || defaultFormatDate;

  // 构建表格列
  const columns: ColumnsType<TTask & { status?: TStatus }> = config.columns.map((col) => {
    const baseColumn: any = {
      title: col.title,
      key: col.key,
      width: col.width,
      fixed: col.fixed,
    };

    if (col.render) {
      baseColumn.render = col.render;
    } else if (col.dataIndex) {
      baseColumn.dataIndex = col.dataIndex;
    }

    // 为特定列提供默认渲染
    switch (col.key) {
      case 'task_id':
        baseColumn.render = (v: string, r: any) => (
          <Tooltip title={v}>
            <span
              style={{
                cursor: onEditTask ? 'pointer' : 'default',
                display: 'block',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
              onClick={() => onEditTask?.(r)}
            >
              <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
            </span>
          </Tooltip>
        );
        break;

      case 'description':
        baseColumn.render = (v: string) => (
          <Tooltip title={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {v}
            </div>
          </Tooltip>
        );
        break;

      case 'sync_type':
        baseColumn.render = (_: any, r: any) => {
          const syncType = (r as any).sync_type;
          return (
            <Tag color={syncType === 'incremental' ? 'blue' : 'green'}>
              {syncType === 'incremental' ? '增量' : '全量'}
            </Tag>
          );
        };
        break;

      case 'table_name':
        baseColumn.render = (v: string) => (
          <Tooltip title={v || '-'}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v || '-'}</code>
            </div>
          </Tooltip>
        );
        break;

      case 'latest_data':
        baseColumn.render = (_: any, r: any) => {
          const dateStr = r.status?.table_latest_date || r.status?.last_date;
          const formatted = formatDateFn(dateStr);
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
        };
        break;

      case 'last_sync':
        baseColumn.render = (_: any, r: any) => {
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
        };
        break;
    }

    return baseColumn;
  });

  // 添加操作列
  columns.push({
    title: '操作',
    key: 'action',
    width: 200,
    fixed: 'right',
    render: (_: any, record: any) => {
      const taskId = config.utils.getTaskId(record);
      return (
        <div style={{ display: 'flex', gap: 4 }}>
          {config.actions.map((action) => {
            const isDisabled = action.disabled?.(record, runningTasks) ?? false;
            const isLoading = action.loading?.(record, runningTasks) ?? false;
            return (
              <Button
                key={action.key}
                type={action.type || 'default'}
                danger={action.danger}
                icon={action.icon}
                onClick={() => action.onClick(record)}
                disabled={isDisabled}
                loading={isLoading}
              >
                {action.label}
              </Button>
            );
          })}
        </div>
      );
    },
  });

  return (
    <Table
      dataSource={tasks.map((task) => {
        const taskId = config.utils.getTaskId(task);
        const status = taskStatuses[taskId];
        return { ...task, status, key: taskId };
      })}
      rowKey={(record) => config.utils.getTaskId(record)}
      pagination={false}
      rowSelection={{
        selectedRowKeys: selectedTaskIds,
        onChange: (selectedRowKeys) => onSelectedTaskIdsChange(selectedRowKeys as string[]),
      }}
      columns={columns}
      locale={{ emptyText: <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无 {config.label}</div> }}
      scroll={{ x: 'max-content' }}
    />
  );
}
