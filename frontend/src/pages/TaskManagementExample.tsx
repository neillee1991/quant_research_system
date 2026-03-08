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
  TabPane,
  Button,
  Modal,
  Form,
  Input,
  TextArea,
  Select,
  Toast,
  Space,
  Tag,
  Descriptions,
} from '@douyinfe/semi-ui';
import { IconSync, IconCode } from '@douyinfe/semi-icons';
import { TaskList } from '../components/TaskList';
import { syncService, etlService, factorService } from '../services/taskService';
import type { SyncTaskConfig, ETLTaskConfig, FactorConfig } from '../types/task';
import type { ColumnProps } from '@douyinfe/semi-ui/lib/es/table';

const TaskManagementExample: React.FC = () => {
  const [activeTab, setActiveTab] = useState('sync');
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingTask, setEditingTask] = useState<any>(null);
  const [isNewTask, setIsNewTask] = useState(false);

  // Sync task columns
  const syncColumns: ColumnProps<SyncTaskConfig>[] = [
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
  const etlColumns: ColumnProps<ETLTaskConfig>[] = [
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
  const factorColumns: ColumnProps<FactorConfig>[] = [
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
  };

  // Handle create task
  const handleCreate = () => {
    setEditingTask(null);
    setIsNewTask(true);
    setEditModalVisible(true);
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
      Toast.success('保存成功');
    } catch (error) {
      // Error already handled by service
    }
  };

  return (
    <div style={{ padding: 24 }}>
      <Card
        title="任务管理示例"
        headerExtraContent={
          <Space>
            <Tag color="blue">使用任务抽象层</Tag>
            <Tag color="green">统一版本控制</Tag>
          </Space>
        }
      >
        <Descriptions
          data={[
            { key: '说明', value: '本页面展示如何使用任务管理抽象层统一管理不同类型的任务' },
            { key: '特性', value: '统一的CRUD接口、版本控制、类型安全、错误处理' },
            { key: '支持任务', value: '同步任务 (Sync)、ETL任务、因子 (Factor)' },
          ]}
          style={{ marginBottom: 24 }}
        />

        <Tabs activeKey={activeTab} onChange={setActiveTab}>
          <TabPane
            tab={
              <span>
                <IconSync /> 同步任务
              </span>
            }
            itemKey="sync"
          >
            <TaskList
              taskType="sync"
              service={syncService}
              columns={syncColumns}
              onEdit={handleEdit}
              onCreate={handleCreate}
              idField="task_id"
              extraActions={(task) => (
                <Button
                  size="small"
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
          </TabPane>

          <TabPane
            tab={
              <span>
                <IconCode /> ETL任务
              </span>
            }
            itemKey="etl"
          >
            <TaskList
              taskType="etl"
              service={etlService}
              columns={etlColumns}
              onEdit={handleEdit}
              onCreate={handleCreate}
              idField="task_id"
            />
          </TabPane>

          <TabPane
            tab={
              <span>
                <IconCode /> 因子
              </span>
            }
            itemKey="factor"
          >
            <TaskList
              taskType="factor"
              service={factorService}
              columns={factorColumns}
              onEdit={handleEdit}
              onCreate={handleCreate}
              idField="factor_id"
            />
          </TabPane>
        </Tabs>
      </Card>

      {/* Edit/Create Modal */}
      <Modal
        title={isNewTask ? '创建任务' : '编辑任务'}
        visible={editModalVisible}
        onCancel={() => setEditModalVisible(false)}
        footer={null}
        width={800}
      >
        <Form
          initValues={editingTask || {}}
          onSubmit={handleSave}
          labelPosition="left"
          labelWidth={120}
        >
          {activeTab === 'sync' && (
            <>
              <Form.Input
                field="task_id"
                label="任务ID"
                rules={[{ required: true, message: '请输入任务ID' }]}
                disabled={!isNewTask}
              />
              <Form.Input
                field="description"
                label="描述"
                rules={[{ required: true, message: '请输入描述' }]}
              />
              <Form.Input
                field="api_name"
                label="API名称"
                rules={[{ required: true, message: '请输入API名称' }]}
              />
              <Form.Input field="table_name" label="表名" />
              <Form.Select
                field="sync_type"
                label="同步类型"
                optionList={[
                  { label: '增量', value: 'incremental' },
                  { label: '全量', value: 'full' },
                ]}
              />
              <Form.Switch field="enabled" label="启用" />
            </>
          )}

          {activeTab === 'etl' && (
            <>
              <Form.Input
                field="task_id"
                label="任务ID"
                rules={[{ required: true, message: '请输入任务ID' }]}
                disabled={!isNewTask}
              />
              <Form.Input
                field="description"
                label="描述"
                rules={[{ required: true, message: '请输入描述' }]}
              />
              <Form.Input
                field="source_table"
                label="源表"
                rules={[{ required: true, message: '请输入源表' }]}
              />
              <Form.Input
                field="target_table"
                label="目标表"
                rules={[{ required: true, message: '请输入目标表' }]}
              />
              <Form.TextArea
                field="script"
                label="SQL脚本"
                rows={6}
                rules={[{ required: true, message: '请输入SQL脚本' }]}
              />
              <Form.Input field="schedule" label="调度表达式" />
              <Form.Switch field="enabled" label="启用" />
            </>
          )}

          {activeTab === 'factor' && (
            <>
              <Form.Input
                field="factor_id"
                label="因子ID"
                rules={[{ required: true, message: '请输入因子ID' }]}
                disabled={!isNewTask}
              />
              <Form.Input
                field="description"
                label="描述"
                rules={[{ required: true, message: '请输入描述' }]}
              />
              <Form.Input field="category" label="分类" />
              <Form.TextArea
                field="code"
                label="因子代码"
                rows={8}
                rules={[{ required: true, message: '请输入因子代码' }]}
              />
              <Form.Input field="depends_on" label="依赖字段" />
              <Form.InputNumber
                field="lookback_days"
                label="回溯天数"
                initValue={250}
              />
              <Form.Switch field="enabled" label="启用" />
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
