import { notify } from '../utils/notify';
/**
 * Task Management Example Page
 *
 * Demonstrates how to use the task management abstraction layer
 * with sync, etl, and factor services.
 */

import React, { useState } from 'react';
import {
  Card,
  Tabs,
  Button,
  Modal,
  Form,
  Input,
  Select,
  Space,
  Tag,
  Descriptions,
} from 'antd';
import { SyncOutlined, CodeOutlined } from '@ant-design/icons';
import { useMessage } from '../hooks/useMessage';
import { TaskList } from '../components/TaskList';
import { syncService, etlService, factorService } from '../services/taskService';
import type { SyncTaskConfig, ETLTaskConfig, FactorConfig } from '../types/task';
import type { ColumnType } from 'antd/es/table';

const TaskManagementExample: React.FC = () => {
  const message = useMessage();
  const [activeTab, setActiveTab] = useState('sync');
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingTask, setEditingTask] = useState<any>(null);
  const [isNewTask, setIsNewTask] = useState(false);
  const [form] = Form.useForm();

  // Sync task columns
  const syncColumns: ColumnType<SyncTaskConfig>[] = [
    {
      title: '任务ID',
      dataIndex: 'task_id',
      width: 150,
      render: (text: string) => <Tag color="blue">{text}</Tag>,
    },
    {
      title: '描述',
      dataIndex: 'description',
      ellipsis: true,
    },
    {
      title: 'API名称',
      dataIndex: 'api_name',
      width: 150,
    },
    {
      title: '表名',
      dataIndex: 'table_name',
      width: 150,
    },
    {
      title: '同步类型',
      dataIndex: 'sync_type',
      width: 100,
      render: (type: string) => (
        <Tag color={type === 'incremental' ? 'green' : 'orange'}>
          {type === 'incremental' ? '增量' : '全量'}
        </Tag>
      ),
    },
  ];

  // ETL task columns
  const etlColumns: ColumnType<ETLTaskConfig>[] = [
    {
      title: '任务ID',
      dataIndex: 'task_id',
      width: 150,
      render: (text: string) => <Tag color="purple">{text}</Tag>,
    },
    {
      title: '描述',
      dataIndex: 'description',
      ellipsis: true,
    },
    {
      title: '源表',
      dataIndex: 'source_table',
      width: 150,
    },
    {
      title: '目标表',
      dataIndex: 'target_table',
      width: 150,
    },
    {
      title: '调度',
      dataIndex: 'schedule',
      width: 120,
    },
  ];

  // Factor columns
  const factorColumns: ColumnType<FactorConfig>[] = [
    {
      title: '因子ID',
      dataIndex: 'factor_id',
      width: 150,
      render: (text: string) => <Tag color="cyan">{text}</Tag>,
    },
    {
      title: '描述',
      dataIndex: 'description',
      ellipsis: true,
    },
    {
      title: '分类',
      dataIndex: 'category',
      width: 120,
    },
    {
      title: '依赖',
      dataIndex: 'depends_on',
      width: 150,
      ellipsis: true,
    },
    {
      title: '回溯天数',
      dataIndex: 'lookback_days',
      width: 100,
    },
  ];

  // Handle edit task
  const handleEdit = (task: any) => {
    setEditingTask(task);
    setIsNewTask(false);
    setEditModalVisible(true);
    form.setFieldsValue(task);
  };

  // Handle create task
  const handleCreate = () => {
    setEditingTask(null);
    setIsNewTask(true);
    setEditModalVisible(true);
    form.resetFields();
  };

  // Handle save task
  const handleSave = async (values: any) => {
    try {
      if (activeTab === 'sync') {
        if (isNewTask) {
          await syncService.createTask(values);
        } else {
          await syncService.updateTask(
            editingTask.task_id,
            values
          );
        }
      } else if (activeTab === 'etl') {
        if (isNewTask) {
          await etlService.createTask(values);
        } else {
          await etlService.updateTask(
            editingTask.task_id,
            values
          );
        }
      } else if (activeTab === 'factor') {
        if (isNewTask) {
          await factorService.createTask(values);
        } else {
          await factorService.updateTask(
            editingTask.factor_id,
            values
          );
        }
      }
      setEditModalVisible(false);
      notify.success('保存成功');
    } catch (error) {
      // Error already handled by service
    }
  };

  return (
    <div style={{ padding: 24 }}>
      <Card
        title="任务管理示例"
        extra={
          <Space>
            <Tag color="blue">使用任务抽象层</Tag>
            <Tag color="green">统一版本控制</Tag>
          </Space>
        }
      >
        <Descriptions
          style={{ marginBottom: 24 }}
          items={[
            { key: '说明', label: '说明', children: '本页面展示如何使用任务管理抽象层统一管理不同类型的任务' },
            { key: '特性', label: '特性', children: '统一的CRUD接口、版本控制、类型安全、错误处理' },
            { key: '支持任务', label: '支持任务', children: '同步任务 (Sync)、ETL任务、因子 (Factor)' },
          ]}
        />

        <Tabs activeKey={activeTab} onChange={setActiveTab} items={[
          {
            key: 'sync',
            label: <span><SyncOutlined /> 同步任务</span>,
            children: (
              <TaskList
                taskType="sync"
                service={syncService}
                columns={syncColumns}
                onEdit={handleEdit}
                onCreate={handleCreate}
                idField="task_id"
                extraActions={(task) => (
                  <Button
                    size="middle"
                    onClick={() => {
                      Modal.info({
                        title: '任务详情',
                        content: <pre>{JSON.stringify(task, null, 2)}</pre>,
                        width: 600,
                      });
                    }}
                  >
                    详情
                  </Button>
                )}
              />
            ),
          },
          {
            key: 'etl',
            label: <span><CodeOutlined /> ETL任务</span>,
            children: (
              <TaskList
                taskType="etl"
                service={etlService}
                columns={etlColumns}
                onEdit={handleEdit}
                onCreate={handleCreate}
                idField="task_id"
              />
            ),
          },
          {
            key: 'factor',
            label: <span><CodeOutlined /> 因子</span>,
            children: (
              <TaskList
                taskType="factor"
                service={factorService}
                columns={factorColumns}
                onEdit={handleEdit}
                onCreate={handleCreate}
                idField="factor_id"
              />
            ),
          },
        ]} />
      </Card>

      {/* Edit/Create Modal */}
      <Modal
        title={isNewTask ? '创建任务' : '编辑任务'}
        open={editModalVisible}
        onCancel={() => setEditModalVisible(false)}
        footer={null}
        width={800}
      >
        <Form
          form={form}
          onFinish={handleSave}
          labelCol={{ span: 5 }}
          wrapperCol={{ span: 19 }}
        >
          {activeTab === 'sync' && (
            <>
              <Form.Item
                name="task_id"
                label="任务ID"
                rules={[{ required: true, message: '请输入任务ID' }]}
              >
                <Input disabled={!isNewTask} />
              </Form.Item>
              <Form.Item
                name="description"
                label="描述"
                rules={[{ required: true, message: '请输入描述' }]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                name="api_name"
                label="API名称"
                rules={[{ required: true, message: '请输入API名称' }]}
              >
                <Input />
              </Form.Item>
              <Form.Item name="table_name" label="表名">
                <Input />
              </Form.Item>
              <Form.Item name="sync_type" label="同步类型">
                <Select options={[
                  { label: '增量', value: 'incremental' },
                  { label: '全量', value: 'full' },
                ]} />
              </Form.Item>
              <Form.Item name="enabled" label="启用" valuePropName="checked">
                <input type="checkbox" />
              </Form.Item>
            </>
          )}

          {activeTab === 'etl' && (
            <>
              <Form.Item
                name="task_id"
                label="任务ID"
                rules={[{ required: true, message: '请输入任务ID' }]}
              >
                <Input disabled={!isNewTask} />
              </Form.Item>
              <Form.Item
                name="description"
                label="描述"
                rules={[{ required: true, message: '请输入描述' }]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                name="source_table"
                label="源表"
                rules={[{ required: true, message: '请输入源表' }]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                name="target_table"
                label="目标表"
                rules={[{ required: true, message: '请输入目标表' }]}
              >
                <Input />
              </Form.Item>
              <Form.Item
                name="script"
                label="SQL脚本"
                rules={[{ required: true, message: '请输入SQL脚本' }]}
              >
                <Input.TextArea rows={6} />
              </Form.Item>
              <Form.Item name="schedule" label="调度表达式">
                <Input />
              </Form.Item>
              <Form.Item name="enabled" label="启用" valuePropName="checked">
                <input type="checkbox" />
              </Form.Item>
            </>
          )}

          {activeTab === 'factor' && (
            <>
              <Form.Item
                name="factor_id"
                label="因子ID"
                rules={[{ required: true, message: '请输入因子ID' }]}
              >
                <Input disabled={!isNewTask} />
              </Form.Item>
              <Form.Item
                name="description"
                label="描述"
                rules={[{ required: true, message: '请输入描述' }]}
              >
                <Input />
              </Form.Item>
              <Form.Item name="category" label="分类">
                <Input />
              </Form.Item>
              <Form.Item
                name="code"
                label="因子代码"
                rules={[{ required: true, message: '请输入因子代码' }]}
              >
                <Input.TextArea rows={8} />
              </Form.Item>
              <Form.Item name="depends_on" label="依赖字段">
                <Input />
              </Form.Item>
              <Form.Item name="lookback_days" label="回溯天数">
                <Input type="number" defaultValue={250} />
              </Form.Item>
              <Form.Item name="enabled" label="启用" valuePropName="checked">
                <input type="checkbox" />
              </Form.Item>
            </>
          )}

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 16 }}>
            <Button onClick={() => setEditModalVisible(false)}>取消</Button>
            <Button type="primary" htmlType="submit">
              保存
            </Button>
          </div>
        </Form>
      </Modal>
    </div>
  );
};

export default TaskManagementExample;
