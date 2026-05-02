/**
 * Generic Task List Component
 *
 * Reusable component for displaying and managing tasks.
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
  Spin,
} from 'antd';
import type { ColumnType } from 'antd/es/table';
import {
  EditOutlined,
  DeleteOutlined,
  SearchOutlined,
  PlusOutlined,
} from '@ant-design/icons';
import type { BaseTaskConfig, TaskType } from '../../types/task';

interface TaskListProps<T extends BaseTaskConfig> {
  taskType: TaskType;
  columns: ColumnType<T>[];
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

  useEffect(() => {
    loadTasks();
  }, []);

  const loadTasks = async () => {
    setLoading(true);
    try {
      // Tasks are now managed through the API directly
      // This component is a generic display component
      onRefresh?.();
    } catch (error) {
      // Error handling
    } finally {
      setLoading(false);
    }
  };

  const handleToggleEnabled = async (task: T, enabled: boolean) => {
    try {
      // Toggle enabled state through parent component
      onRefresh?.();
    } catch (error) {
      // Error handling
    }
  };

  const handleDelete = async (task: T) => {
    try {
      // Delete through parent component
      onRefresh?.();
    } catch (error) {
      // Error handling
    }
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
  const tableColumns: ColumnType<T>[] = [
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
      title: '操作',
      width: 150,
      render: (_: any, record: T) => (
        <Space>
          {onEdit && (
            <Tooltip title="编辑">
              <Button
                icon={<EditOutlined />}
                size="middle"
                onClick={() => onEdit(record)}
              />
            </Tooltip>
          )}
          {extraActions?.(record)}
          <Popconfirm
            title="确认删除？"
            description="删除后无法恢复"
            onConfirm={() => handleDelete(record)}
          >
            <Tooltip title="删除">
              <Button
                icon={<DeleteOutlined />}
                size="middle"
                danger
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
            prefix={<SearchOutlined />}
            placeholder="搜索任务ID或描述"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 300 }}
          />
        )}
        {showCreate && onCreate && (
          <Button
            icon={<PlusOutlined />}
            type="primary"
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
            showTotal: (total, range) => `${range[0]}-${range[1]} 共 ${total} 条`,
          }}
          rowKey={idField}
        />
      </Spin>
    </div>
  );
}

export default TaskList;
