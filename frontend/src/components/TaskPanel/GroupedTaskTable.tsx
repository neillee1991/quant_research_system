/**
 * 支持分组显示的任务表格组件
 * 将相同指数的任务合并显示
 */
import React, { useState } from 'react';
import { Table, Button, Tag, Tooltip, Collapse, Space } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { SyncOutlined, DeleteOutlined, RiseOutlined, BarChartOutlined } from '@ant-design/icons';
import type { TaskTypeConfig, GenericTaskStatus, TaskActionConfig } from '../../config/taskTypes';

const { Panel } = Collapse;

// 任务类型显示映射
const taskTypeMap: Record<string, { label: string; icon: React.ReactNode; color: string }> = {
  daily: { label: '日线', icon: <RiseOutlined />, color: 'blue' },
  weight: { label: '成分股', icon: <BarChartOutlined />, color: 'green' }
};

interface GroupedTaskTableProps<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>> {
  config: TaskTypeConfig<TTask, TStatus, TRunParams>;
  tasks: TTask[];
  taskStatuses: Record<string, TStatus>;
  runningTasks: Set<string>;
  selectedTaskIds: string[];
  onSelectedTaskIdsChange: (ids: string[]) => void;
  onEditTask?: (task: TTask) => void;
  formatDate?: (dateStr: string | null | undefined) => string;
}

interface TaskGroup {
  groupKey: string;
  groupTitle: string;
  isIndexGroup: boolean;
  indexCode?: string;
  indexName?: string;
  tasks: any[];
  hasDaily: boolean;
  hasWeight: boolean;
}

export function GroupedTaskTable<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>>({
  config,
  tasks,
  taskStatuses,
  runningTasks,
  selectedTaskIds,
  onSelectedTaskIdsChange,
  onEditTask,
  formatDate,
}: GroupedTaskTableProps<TTask, TStatus, TRunParams>) {
  const [expandedGroups, setExpandedGroups] = useState<string[]>([]);

  const defaultFormatDate = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '-';
    const dateString = String(dateStr);
    if (typeof dateString === 'string' && dateString.length === 8) {
      return `${dateString.slice(0, 4)}-${dateString.slice(4, 6)}-${dateString.slice(6, 8)}`;
    }
    return dateString.slice(0, 10);
  };

  const formatDateFn = formatDate || defaultFormatDate;

  // 将任务分组
  const groupTasks = (): (TaskGroup | TTask)[] => {
    const indexGroups: Record<string, TaskGroup> = {};
    const nonIndexTasks: TTask[] = [];

    tasks.forEach((task: any) => {
      const taskId = config.utils.getTaskId(task);

      // 判断是否是指数同步任务
      if (taskId.startsWith('sync_index_')) {
        // 解析指数代码
        let indexCode = '';
        let taskType = '';

        if (taskId.startsWith('sync_index_daily_')) {
          indexCode = taskId.replace('sync_index_daily_', '');
          taskType = 'daily';
        } else if (taskId.startsWith('sync_index_weight_')) {
          indexCode = taskId.replace('sync_index_weight_', '');
          taskType = 'weight';
        }

        if (indexCode) {
          // 尝试从任务描述中提取指数名称
          const description = task.description || '';
          const nameMatch = description.match(/指数 ([^\s]+)/);
          const indexName = nameMatch ? nameMatch[1] : indexCode;

          const groupKey = `index_${indexCode}`;

          if (!indexGroups[groupKey]) {
            indexGroups[groupKey] = {
              groupKey,
              groupTitle: `${indexName} 指数同步任务`,
              isIndexGroup: true,
              indexCode,
              indexName,
              tasks: [],
              hasDaily: false,
              hasWeight: false
            };
          }

          indexGroups[groupKey].tasks.push(task);

          if (taskType === 'daily') {
            indexGroups[groupKey].hasDaily = true;
          } else if (taskType === 'weight') {
            indexGroups[groupKey].hasWeight = true;
          }

          return;
        }
      }

      nonIndexTasks.push(task);
    });

    const result: (TaskGroup | TTask)[] = [];

    // 添加指数分组（按指数代码排序）
    Object.values(indexGroups)
      .sort((a, b) => (a.indexCode || '').localeCompare(b.indexCode || ''))
      .forEach(group => result.push(group));

    // 添加非指数任务
    result.push(...nonIndexTasks);

    return result;
  };

  // 切换分组展开/折叠
  const toggleGroup = (groupKey: string) => {
    setExpandedGroups(prev => {
      if (prev.includes(groupKey)) {
        return prev.filter(k => k !== groupKey);
      } else {
        return [...prev, groupKey];
      }
    });
  };

  // 构建单个任务的列
  const buildTaskColumns = (groupKey?: string): ColumnsType<any> => {
    const columns: ColumnsType<any> = config.columns.map((col) => {
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
                  {(() => {
                    const normalized = String(syncTime).replace(' ', 'T');
                    const d = new Date(normalized);
                    return isNaN(d.getTime()) ? String(syncTime).slice(0, 16) : d.toLocaleString('zh-CN', {
                      year: 'numeric', month: '2-digit', day: '2-digit',
                      hour: '2-digit', minute: '2-digit', hour12: false,
                    });
                  })()}
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

    return columns;
  };

  // 渲染分组标题
  const renderGroupTitle = (group: TaskGroup) => {
    const tags: React.ReactNode[] = [];

    if (group.hasDaily) {
      tags.push(
        <Tag key="daily" color="blue" icon={<RiseOutlined />}>
          日线
        </Tag>
      );
    }

    if (group.hasWeight) {
      tags.push(
        <Tag key="weight" color="green" icon={<BarChartOutlined />}>
          成分股
        </Tag>
      );
    }

    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <span style={{ fontWeight: 500 }}>{group.groupTitle}</span>
        <span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>
          ({group.tasks.length} 个任务)
        </span>
        <Space size={[4, 4]} wrap>
          {tags}
        </Space>
      </div>
    );
  };

  const groupedData = groupTasks();

  // 构建可选择的任务列表（用于行选择）
  const allSelectableTasks: any[] = [];
  groupedData.forEach((item) => {
    if ((item as TaskGroup).isIndexGroup) {
      allSelectableTasks.push(...(item as TaskGroup).tasks);
    } else {
      allSelectableTasks.push(item);
    }
  });

  // 处理行选择
  const handleRowSelectionChange = (selectedRowKeys: React.Key[]) => {
    onSelectedTaskIdsChange(selectedRowKeys as string[]);
  };

  return (
    <div>
      {groupedData.map((item, index) => {
        const group = item as TaskGroup;
        if (group.isIndexGroup) {
          // 渲染分组
          const taskIds = group.tasks.map((t) => config.utils.getTaskId(t));
          const allSelected = taskIds.every((id) => selectedTaskIds.includes(id));
          const someSelected = taskIds.some((id) => selectedTaskIds.includes(id));

          return (
            <Collapse
              key={group.groupKey}
              activeKey={expandedGroups}
              onChange={(keys) => {
                if (Array.isArray(keys)) {
                  setExpandedGroups(keys as string[]);
                } else {
                  setExpandedGroups(keys ? [keys] : []);
                }
              }}
              style={{ marginBottom: '8px' }}
            >
              <Panel
                header={renderGroupTitle(group)}
                key={group.groupKey}
                extra={
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <Button
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        if (allSelected) {
                          // 取消选择分组中的所有任务
                          onSelectedTaskIdsChange(selectedTaskIds.filter(id => !taskIds.includes(id)));
                        } else {
                          // 选择分组中的所有任务
                          const newSelected = new Set(selectedTaskIds);
                          taskIds.forEach(id => newSelected.add(id));
                          onSelectedTaskIdsChange(Array.from(newSelected));
                        }
                      }}
                    >
                      {allSelected ? '取消全选' : '全选'}
                    </Button>
                  </div>
                }
              >
                <Table
                  dataSource={group.tasks.map((task) => {
                    const taskId = config.utils.getTaskId(task);
                    const status = taskStatuses[taskId];
                    return { ...task, status, key: taskId };
                  })}
                  rowKey={(record) => config.utils.getTaskId(record)}
                  pagination={false}
                  rowSelection={{
                    selectedRowKeys: selectedTaskIds,
                    onChange: handleRowSelectionChange,
                  }}
                  columns={buildTaskColumns(group.groupKey)}
                  locale={{ emptyText: <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无任务</div> }}
                  scroll={{ x: 'max-content' }}
                  size="small"
                />
              </Panel>
            </Collapse>
          );
        } else {
          // 渲染单个任务（非分组）
          const task = item as any;
          const taskId = config.utils.getTaskId(task);
          const status = taskStatuses[taskId];

          return (
            <div key={taskId} style={{ marginBottom: '8px' }}>
              <Table
                dataSource={[{ ...task, status, key: taskId }]}
                rowKey={(record) => config.utils.getTaskId(record)}
                pagination={false}
                rowSelection={{
                  selectedRowKeys: selectedTaskIds,
                  onChange: handleRowSelectionChange,
                }}
                columns={buildTaskColumns()}
                locale={{ emptyText: <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无任务</div> }}
                scroll={{ x: 'max-content' }}
                size="small"
              />
            </div>
          );
        }
      })}
    </div>
  );
}
