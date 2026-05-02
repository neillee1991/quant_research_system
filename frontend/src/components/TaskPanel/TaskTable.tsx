/**
 * 通用任务表格组件
 * 指数订阅任务（sync_index_daily_* / sync_index_weight_*）合并为一行，可展开查看子任务
 * 其他任务保持原样
 */
import React, { useMemo } from 'react';
import { Table, Button, Tag, Tooltip, Space } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { RiseOutlined, BarChartOutlined } from '@ant-design/icons';
import type { TaskTypeConfig, GenericTaskStatus } from '../../config/taskTypes';

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

// 虚拟分组行（不是真实任务）
interface IndexGroupRow {
  _isGroup: true;
  key: string;
  indexCode: string;
  groupTitle: string;
  hasDaily: boolean;
  hasWeight: boolean;
  children: any[];
}

type TableRow = (any & { _isGroup?: false }) | IndexGroupRow;

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
    const dateString = String(dateStr);
    if (dateString.length === 8) {
      return `${dateString.slice(0, 4)}-${dateString.slice(4, 6)}-${dateString.slice(6, 8)}`;
    }
    return dateString.slice(0, 10);
  };

  const formatDateFn = formatDate || defaultFormatDate;

  // 将任务列表转换为树形数据，指数任务合并为分组行（children 字段供 Ant Design 原生树形展开）
  const tableData: TableRow[] = useMemo(() => {
    const indexGroups: Record<string, { daily?: any; weight?: any; indexCode: string; name: string }> = {};
    const normalRows: any[] = [];

    tasks.forEach((task: any) => {
      const taskId = config.utils.getTaskId(task);
      const status = taskStatuses[taskId];
      const row = { ...task, status, key: taskId };

      if (taskId.startsWith('sync_index_daily_') && !taskId.endsWith('_template')) {
        const code = taskId.replace('sync_index_daily_', '');
        if (!indexGroups[code]) indexGroups[code] = { indexCode: code, name: code };
        indexGroups[code].daily = row;
      } else if (taskId.startsWith('sync_index_weight_') && !taskId.endsWith('_template')) {
        const code = taskId.replace('sync_index_weight_', '');
        if (!indexGroups[code]) indexGroups[code] = { indexCode: code, name: code };
        indexGroups[code].weight = row;
      } else {
        normalRows.push(row);
      }
    });

    const groupRows: IndexGroupRow[] = Object.values(indexGroups).map((g) => {
      const desc = g.daily?.description || g.weight?.description || '';
      const match = desc.match(/指数 ([^\s]+)/);
      const displayCode = match ? match[1] : g.indexCode;
      const children = [g.daily, g.weight].filter(Boolean);

      return {
        _isGroup: true,
        key: `__group_${g.indexCode}`,
        indexCode: g.indexCode,
        groupTitle: `${displayCode} 指数同步任务`,
        hasDaily: !!g.daily,
        hasWeight: !!g.weight,
        children,
      };
    });

    return [...groupRows.sort((a, b) => a.indexCode.localeCompare(b.indexCode)), ...normalRows];
  }, [tasks, taskStatuses, config]);

  // 构建列定义
  const columns: ColumnsType<TableRow> = config.columns.map((col) => {
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

    switch (col.key) {
      case 'task_id':
        // expandIcon 由 Ant Design 自动注入到第一列（task_id），无需手动处理
        baseColumn.render = (v: string, r: any) => {
          if (r._isGroup) {
            return (
              <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>
                {r.indexCode}
              </code>
            );
          }
          return (
            <Tooltip title={v}>
              <span
                style={{ cursor: onEditTask ? 'pointer' : 'default', display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                onClick={() => onEditTask?.(r)}
              >
                <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
              </span>
            </Tooltip>
          );
        };
        break;

      case 'description':
        baseColumn.render = (v: string, r: any) => {
          if (r._isGroup) {
            return (
              <Space size={4}>
                <span style={{ fontWeight: 500 }}>{r.groupTitle}</span>
                {r.hasDaily && <Tag color="blue" icon={<RiseOutlined />}>日线</Tag>}
                {r.hasWeight && <Tag color="green" icon={<BarChartOutlined />}>成分股</Tag>}
              </Space>
            );
          }
          return (
            <Tooltip title={v}>
              <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{v}</div>
            </Tooltip>
          );
        };
        break;

      case 'sync_type':
        baseColumn.render = (_: any, r: any) => {
          if (r._isGroup) return null;
          const syncType = r.sync_type;
          return <Tag color={syncType === 'incremental' ? 'blue' : 'green'}>{syncType === 'incremental' ? '增量' : '全量'}</Tag>;
        };
        break;

      case 'table_name':
        baseColumn.render = (v: string, r: any) => {
          if (r._isGroup) return null;
          return (
            <Tooltip title={v || '-'}>
              <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v || '-'}</code>
              </div>
            </Tooltip>
          );
        };
        break;

      case 'latest_data':
        baseColumn.render = (_: any, r: any) => {
          if (r._isGroup) return null;
          const dateStr = r.status?.table_latest_date || r.status?.last_date;
          const formatted = formatDateFn(dateStr);
          return (
            <div style={{ color: formatted === '-' ? 'var(--text-muted)' : 'var(--color-gain)' }}>
              {formatted}
            </div>
          );
        };
        break;

      case 'last_sync':
        baseColumn.render = (_: any, r: any) => {
          if (r._isGroup) return null;
          const syncTime = r.status?.last_sync_time;
          if (!syncTime) return '-';
          const normalized = String(syncTime).replace(' ', 'T');
          const d = new Date(normalized);
          const display = isNaN(d.getTime()) ? String(syncTime).slice(0, 16) : d.toLocaleString('zh-CN', {
            year: 'numeric', month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', hour12: false,
          });
          return (
            <div style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>
              {display}
            </div>
          );
        };
        break;
    }

    return baseColumn;
  });

  // 操作列
  columns.push({
    title: '操作',
    key: 'action',
    width: 200,
    fixed: 'right',
    render: (_: any, record: any) => {
      if (record._isGroup) return null;
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
      dataSource={tableData}
      rowKey="key"
      pagination={false}
      rowSelection={{
        selectedRowKeys: selectedTaskIds,
        // checkStrictly=false：勾选父行时自动勾选子行，列自动对齐
        checkStrictly: false,
        onChange: (selectedRowKeys: React.Key[]) => {
          // 过滤掉虚拟分组行的 key，只保留真实任务 key
          const taskKeys = (selectedRowKeys as string[]).filter((k) => !k.startsWith('__group_'));
          onSelectedTaskIdsChange(taskKeys);
        },
        onSelectAll: (selected, _selectedRows, _changeRows) => {
          if (selected) {
            // 全选：收集所有真实任务 key（普通行 + 所有子任务）
            const allTaskKeys: string[] = [];
            tableData.forEach((row: any) => {
              if (!row._isGroup) {
                allTaskKeys.push(row.key);
              } else if (row.children) {
                row.children.forEach((child: any) => allTaskKeys.push(child.key));
              }
            });
            onSelectedTaskIdsChange(allTaskKeys);
          } else {
            onSelectedTaskIdsChange([]);
          }
        },
        getCheckboxProps: (record: any) => ({
          disabled: record._isGroup,
          style: record._isGroup ? { display: 'none' } : undefined,
        }),
      }}
      columns={columns}
      // 使用原生树形数据展开，expandIcon 自动注入到第一列（task_id），列宽自动对齐
      expandable={{
        rowExpandable: (record) => !!(record as any)._isGroup,
      }}
      onRow={(record: any) => {
        const isSubtask = !record._isGroup && (
          record.key?.startsWith('sync_index_daily_') || record.key?.startsWith('sync_index_weight_')
        );
        return isSubtask ? { style: { background: 'var(--bg-tertiary)' } } : {};
      }}
      indentSize={24}
      locale={{ emptyText: <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无 {config.label}</div> }}
      scroll={{ x: 'max-content' }}
    />
  );
}
