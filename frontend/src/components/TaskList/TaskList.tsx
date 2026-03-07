/**
 * Generic Task List Component
 *
 * Reusable component for displaying and managing tasks with version control.
 * Supports sync, etl, and factor task types.
 */

import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Switch,
  Tooltip,
  Popconfirm,
  Tag,
  Space,
  Input,
  Toast,
  Spin,
} from '@douyinfe/semi-ui';
import {
  IconEdit,
  IconDelete,
  IconHistory,
  IconSearch,
  IconPlus,
} from '@douyinfe/semi-icons';
import { VersionHistory } from '../VersionHistory';
import type { BaseTaskConfig, TaskType } from '../../types/task';
import type { TaskService } from '../../services/taskService';
import type { ColumnProps } from '@douyinfe/semi-ui/lib/es/table';

interface TaskListProps<T extends BaseTaskConfig> {
  taskType: TaskType;
  service: TaskService<T>;
  columns: ColumnProps<T>[];
  onEdit?: (task: T) => void;
  onCreate?: () => void;
  onRefresh?: () => void;
  idField: 'task_id' | 'factor_id';
  showSearch?: boolean;
  showCreate?: boolean;
  extraActions?: (task: T) => React.ReactNode;
}

export function TaskList<T extends BaseTaskConfig>({
  taskType,
  service,
  columns,
  onEdit,
  onCreate,
  onRefresh,
  idField,
  showSearch = true,
  showCreate = true,
  extraActions,
}: TaskListProps<T>) {
  const [tasks, setTasks] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [versionHistoryVisible, setVersionHistoryVisible] = useState(false);
  const [selectedTaskId, setSelectedTaskId] = useState<string>('');
  const [selectedVersion, setSelectedVersion] = useState<number | undefined>();

  useEffect(() => {
    loadTasks();
  }, []);

  const loadTasks = async () => {
    setLoading(true);
    try {
      const data = await service.listTasks();
      setTasks(data);
      onRefresh?.();
    } catch (error) {
      // Error already handled by service
    } finally {
      setLoading(false);
    }
  };

  const handleToggleEnabled = async (task: T, enabled: boolean) => {
    try {
      const taskId = (task as any)[idField] as string;
      await service.toggleEnabled(taskId, enabled);
      await loadTasks();
    } catch (error) {
      // Error already handled by service
    }
  };

  const handleDelete = async (task: T) => {
    try {
      const taskId = (task as any)[idField] as string;
      await service.deleteTask(taskId);
      await loadTasks();
    } catch (error) {
      // Error already handled by service
    }
  };

  const handleShowVersionHistory = (task: T) => {
    const taskId = (task as any)[idField] as string;
    setSelectedTaskId(taskId);
    setSelectedVersion(task.version_number);
    setVersionHistoryVisible(true);
  };

  const handleVersionRollback = async () => {
    await loadTasks();
  };

  // Filter tasks based on search text
  const filteredTasks = tasks.filter((task) => {
    if (!searchText) return true;
    const searchLower = searchText.toLowerCase();
    const taskId = (task as any)[idField] as string;
    return (
      taskId.toLowerCase().includes(searchLower) ||
      task.description?.toLowerCase().includes(searchLower)
    );
  });

  // Build table columns with actions
  const tableColumns: ColumnProps<T>[] = [
    ...columns,
    {
      title: '状态',
      dataIndex: 'enabled',
      width: 80,
      render: (enabled: boolean, record: T) => (
        <Switch
          checked={enabled}
          onChange={(checked) => handleToggleEnabled(record, checked)}
        />
      ),
    },
    {
      title: '版本',
      dataIndex: 'version_number',
      width: 80,
      render: (version: number, record: T) => (
        <Tag color={record.is_current ? 'blue' : 'grey'}>
          v{version}
        </Tag>
      ),
    },
    {
      title: '操作',
      width: 200,
      render: (_: any, record: T) => (
        <Space>
          {onEdit && (
            <Tooltip content="编辑">
              <Button
                icon={<IconEdit />}
                size="small"
                onClick={() => onEdit(record)}
              />
            </Tooltip>
          )}
          <Tooltip content="版本历史">
            <Button
              icon={<IconHistory />}
              size="small"
              onClick={() => handleShowVersionHistory(record)}
            />
          </Tooltip>
          {extraActions?.(record)}
          <Popconfirm
            title="确认删除？"
            content="删除后无法恢复"
            onConfirm={() => handleDelete(record)}
          >
            <Tooltip content="删除">
              <Button
                icon={<IconDelete />}
                size="small"
                type="danger"
              />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', gap: 8 }}>
        {showSearch && (
          <Input
            prefix={<IconSearch />}
            placeholder="搜索任务ID或描述"
            value={searchText}
            onChange={setSearchText}
            style={{ width: 300 }}
          />
        )}
        {showCreate && onCreate && (
          <Button
            icon={<IconPlus />}
            theme="solid"
            onClick={onCreate}
          >
            创建任务
          </Button>
        )}
        <Button onClick={loadTasks}>刷新</Button>
      </div>

      <Spin spinning={loading}>
        <Table
          columns={tableColumns}
          dataSource={filteredTasks}
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            formatPageText: ({ currentStart, currentEnd, total }: any) => `${currentStart}-${currentEnd} 共 ${total} 条`,
          }}
          rowKey={idField}
        />
      </Spin>

      <VersionHistory
        visible={versionHistoryVisible}
        onClose={() => setVersionHistoryVisible(false)}
        taskId={selectedTaskId}
        taskType={taskType}
        currentVersion={selectedVersion}
        onRollback={handleVersionRollback}
      />
    </div>
  );
}

export default TaskList;
