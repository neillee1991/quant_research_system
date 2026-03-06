import React, { useState, useEffect, useMemo } from 'react';
import {
  Button,
  Tooltip,
  Tabs,
  TabPane,
  Table,
  Tag,
  Space,
  Popconfirm,
  Toast,
  Empty,
} from '@douyinfe/semi-ui';
import {
  IconRefresh,
  IconExternalOpen,
  IconCalendarClock,
  IconPlus,
  IconEdit,
  IconDelete,
  IconPlay,
} from '@douyinfe/semi-icons';
import cronstrue from 'cronstrue/i18n';
import { flowApi, FlowListItem } from '../api';
import FlowEditor from '../components/SchedulerFlowEditor';

const SchedulerCenter: React.FC = () => {
  const prefectUrl = process.env.REACT_APP_PREFECT_URL || 'http://localhost:4200';

  const [flows, setFlows] = useState<FlowListItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [editorVisible, setEditorVisible] = useState(false);
  const [editingFlow, setEditingFlow] = useState<string | undefined>();
  const [runningFlow, setRunningFlow] = useState<string | null>(null);

  const fetchFlows = async () => {
    setLoading(true);
    try {
      const res = await flowApi.list();
      setFlows(res.data);
    } catch (e) {
      Toast.error({ content: '加载 Flow 列表失败' });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchFlows();
  }, []);

  const handleCreate = () => {
    setEditingFlow(undefined);
    setEditorVisible(true);
  };

  const handleEdit = (name: string) => {
    setEditingFlow(name);
    setEditorVisible(true);
  };

  const handleDelete = async (name: string) => {
    try {
      await flowApi.delete(name);
      Toast.success({ content: `Flow "${name}" 已删除` });
      fetchFlows();
    } catch (e: any) {
      Toast.error({ content: e?.response?.data?.detail || '删除失败' });
    }
  };

  const handleRun = async (name: string) => {
    setRunningFlow(name);
    try {
      await flowApi.run(name);
      Toast.success({ content: `Flow "${name}" 已开始执行` });
    } catch (e: any) {
      Toast.error({ content: e?.response?.data?.detail || '执行失败' });
    } finally {
      setRunningFlow(null);
    }
  };

  const columns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => (
        <span style={{ fontWeight: 500 }}>{name}</span>
      ),
    },
    {
      title: '调度',
      dataIndex: 'cron',
      key: 'cron',
      render: (cron: string) => {
        let desc = '';
        try {
          desc = cronstrue.toString(cron, { locale: 'zh_CN' });
        } catch {
          desc = '无效表达式';
        }
        return (
          <div>
            <code style={{ fontSize: 12, color: 'var(--semi-color-text-2)' }}>{cron}</code>
            <div style={{ fontSize: 12, color: 'var(--semi-color-text-1)', marginTop: 2 }}>
              {desc}
            </div>
          </div>
        );
      },
    },
    {
      title: '标签',
      dataIndex: 'tags',
      key: 'tags',
      render: (tags: string[]) => (
        <Space>
          {tags.map(tag => (
            <Tag key={tag} size="small" color="blue">{tag}</Tag>
          ))}
        </Space>
      ),
    },
    {
      title: '任务数',
      dataIndex: 'task_count',
      key: 'task_count',
      width: 80,
      align: 'center' as const,
    },
    {
      title: '状态',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 80,
      render: (enabled: boolean) => (
        <Tag color={enabled ? 'green' : 'grey'} size="small">
          {enabled ? '启用' : '禁用'}
        </Tag>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 160,
      render: (_: any, record: FlowListItem) => (
        <Space>
          <Tooltip content="立即执行">
            <Button
              theme="borderless"
              icon={<IconPlay />}
              size="small"
              loading={runningFlow === record.name}
              onClick={() => handleRun(record.name)}
            />
          </Tooltip>
          <Tooltip content="编辑">
            <Button
              theme="borderless"
              icon={<IconEdit />}
              size="small"
              onClick={() => handleEdit(record.name)}
            />
          </Tooltip>
          <Popconfirm
            title="确定删除此 Flow？"
            onConfirm={() => handleDelete(record.name)}
          >
            <Tooltip content="删除">
              <Button
                theme="borderless"
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
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto', height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div style={{ marginBottom: '16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div>
          <h1 style={{
            color: 'var(--color-primary)',
            fontSize: '24px',
            fontWeight: 700,
            margin: 0,
            letterSpacing: '1px'
          }}>
            <IconCalendarClock style={{ marginRight: '8px' }} />
            调度
          </h1>
          <p style={{
            color: 'var(--text-secondary)',
            margin: '4px 0 0 0',
            fontSize: '12px'
          }}>
            工作流调度管理
          </p>
        </div>
      </div>

      <Tabs type="line" style={{ flex: 1 }}>
        <TabPane tab="调度管理" itemKey="management">
          <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
            <Button
              theme="solid"
              icon={<IconPlus />}
              onClick={handleCreate}
            >
              新建 Flow
            </Button>
            <Button
              theme="borderless"
              icon={<IconRefresh />}
              onClick={fetchFlows}
            >
              刷新
            </Button>
          </div>

          <Table
            columns={columns}
            dataSource={flows}
            rowKey="name"
            loading={loading}
            pagination={false}
            empty={<Empty description="暂无 Flow 配置" />}
          />
        </TabPane>

        <TabPane tab="Prefect Dashboard" itemKey="prefect">
          <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Tooltip content="刷新">
              <Button theme="borderless" icon={<IconRefresh />} onClick={() => {
                const iframe = document.querySelector('iframe');
                if (iframe) iframe.src = iframe.src;
              }} />
            </Tooltip>
            <Tooltip content="在新标签页打开">
              <Button theme="borderless" icon={<IconExternalOpen />} onClick={() => window.open(prefectUrl, '_blank')} />
            </Tooltip>
          </div>

          <div style={{
            borderRadius: 8,
            overflow: 'hidden',
            border: '1px solid var(--border-color)',
            minHeight: 'calc(100vh - 240px)',
          }}>
            <iframe
              src={prefectUrl}
              style={{ width: '100%', height: 'calc(100vh - 240px)', border: 'none' }}
              title="Prefect Dashboard"
            />
          </div>
        </TabPane>
      </Tabs>

      <FlowEditor
        visible={editorVisible}
        flowName={editingFlow}
        onClose={() => setEditorVisible(false)}
        onSaved={fetchFlows}
      />
    </div>
  );
};

export default SchedulerCenter;
