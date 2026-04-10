import React, { useState, useEffect } from 'react';
import {
  Button,
  Tooltip,
  Tabs,
  Table,
  Tag,
  Space,
  Popconfirm,
  Empty,
} from 'antd';
import {
  ReloadOutlined,
  ScheduleOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  HistoryOutlined,
} from '@ant-design/icons';
import { useMessage } from '../hooks/useMessage';
import cronstrue from 'cronstrue/i18n';
import { flowApi, FlowListItem, FlowRun } from '../api';
import FlowEditor from '../components/SchedulerFlowEditor';

const SchedulerCenter: React.FC = () => {
  const message = useMessage();

  const [flows, setFlows] = useState<FlowListItem[]>([]);
  const [flowRuns, setFlowRuns] = useState<FlowRun[]>([]);
  const [loading, setLoading] = useState(false);
  const [runsLoading, setRunsLoading] = useState(false);
  const [editorVisible, setEditorVisible] = useState(false);
  const [editingFlow, setEditingFlow] = useState<string | undefined>();
  const [runningFlow, setRunningFlow] = useState<string | null>(null);

  const fetchFlows = async () => {
    setLoading(true);
    try {
      const res = await flowApi.list();
      setFlows(res.data);
    } catch (e) {
      message.error({ content: '加载 Flow 列表失败' });
    } finally {
      setLoading(false);
    }
  };

  const fetchFlowRuns = async () => {
    setRunsLoading(true);
    try {
      // 为了简单，这里获取所有 flow 的最近运行记录
      // 实际项目中可能需要单独的 API 来获取所有 flow runs
      const allRuns: FlowRun[] = [];
      for (const flow of flows) {
        try {
          const res = await flowApi.listRuns(flow.name, 10);
          if (res.data?.data) {
            allRuns.push(...res.data.data);
          }
        } catch (e) {
          // 忽略单个 flow 的错误
        }
      }
      // 按时间倒序排列
      allRuns.sort((a, b) => new Date(b.started_at || 0).getTime() - new Date(a.started_at || 0).getTime());
      setFlowRuns(allRuns.slice(0, 100));
    } catch (e) {
      message.error({ content: '加载 Flow 运行记录失败' });
    } finally {
      setRunsLoading(false);
    }
  };

  useEffect(() => {
    fetchFlows();
  }, []);

  useEffect(() => {
    if (flows.length > 0) {
      fetchFlowRuns();
    }
  }, [flows]);

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
      console.log('Deleting flow:', name);
      await flowApi.delete(name, true);
      message.success({ content: `Flow "${name}" 已删除` });
      console.log('Fetching flows again...');
      await fetchFlows();
    } catch (e: any) {
      console.error('Delete failed:', e);
      message.error({ content: e?.response?.data?.detail || '删除失败' });
    }
  };

  const handleRun = async (name: string) => {
    setRunningFlow(name);
    try {
      await flowApi.trigger(name);
      message.success({ content: `Flow "${name}" 已开始执行` });
      // 刷新运行记录
      setTimeout(fetchFlowRuns, 2000);
    } catch (e: any) {
      message.error({ content: e?.response?.data?.detail || '执行失败' });
    } finally {
      setRunningFlow(null);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success':
        return 'green';
      case 'running':
        return 'blue';
      case 'failed':
        return 'red';
      case 'cancelled':
        return 'grey';
      default:
        return 'orange';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'success':
        return '成功';
      case 'running':
        return '运行中';
      case 'failed':
        return '失败';
      case 'cancelled':
        return '已取消';
      case 'pending':
        return '等待中';
      default:
        return status;
    }
  };

  const flowColumns = [
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
      render: (cron?: string) => {
        if (!cron) {
          return <span style={{ color: 'var(--text-secondary)' }}>手动触发</span>;
        }
        let desc = '';
        try {
          desc = cronstrue.toString(cron, { locale: 'zh_CN' });
        } catch {
          desc = '无效表达式';
        }
        return (
          <div>
            <code style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{cron}</code>
            <div style={{ fontSize: 12, color: 'var(--text-primary)', marginTop: 2 }}>
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
            <Tag key={tag} color="blue">{tag}</Tag>
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
        <Tag color={enabled ? 'green' : 'grey'}>
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
          <Tooltip title="立即执行">
            <Button
              type="text"
              icon={<PlayCircleOutlined />}
              loading={runningFlow === record.name}
              onClick={() => handleRun(record.name)}
            />
          </Tooltip>
          <Tooltip title="编辑">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record.name)}
            />
          </Tooltip>
          <Tooltip title="删除">
            <Popconfirm
              title="确定删除此 Flow？"
              onConfirm={() => handleDelete(record.name)}
            >
              <Button
                type="text"
                icon={<DeleteOutlined />}
                danger
              />
            </Popconfirm>
          </Tooltip>
        </Space>
      ),
    },
  ];

  const runColumns = [
    {
      title: 'Flow 名称',
      dataIndex: 'flow_name',
      key: 'flow_name',
      render: (name: string) => (
        <span style={{ fontWeight: 500 }}>{name}</span>
      ),
    },
    {
      title: '运行 ID',
      dataIndex: 'flow_run_id',
      key: 'flow_run_id',
      render: (id: string) => (
        <code style={{ fontSize: 11 }}>{id.slice(0, 8)}...</code>
      ),
    },
    {
      title: '触发方式',
      dataIndex: 'trigger_type',
      key: 'trigger_type',
      render: (type: string) => (
        <Tag color={type === 'scheduled' ? 'blue' : 'orange'}>
          {type === 'scheduled' ? '定时' : '手动'}
        </Tag>
      ),
    },
    {
      title: '目标日期',
      dataIndex: 'target_date',
      key: 'target_date',
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={getStatusColor(status)}>
          {getStatusText(status)}
        </Tag>
      ),
    },
    {
      title: '开始时间',
      dataIndex: 'started_at',
      key: 'started_at',
      render: (time?: string) => time ? new Date(time).toLocaleString('zh-CN') : '-',
    },
    {
      title: '耗时',
      dataIndex: 'duration_sec',
      key: 'duration_sec',
      render: (sec?: number) => sec !== undefined ? `${sec.toFixed(1)}s` : '-',
    },
    {
      title: '错误',
      dataIndex: 'error',
      key: 'error',
      render: (error?: string) => error ? (
        <Tooltip title={error}>
          <span style={{ color: 'var(--error-color)' }}>查看</span>
        </Tooltip>
      ) : '-',
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
            <ScheduleOutlined style={{ marginRight: '8px' }} />
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

      <Tabs type="line" style={{ flex: 1 }} items={[
        {
          key: 'management',
          label: '调度管理',
          children: (
            <div>
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={handleCreate}
                >
                  新建 Flow
                </Button>
                <Button
                  type="text"
                  icon={<ReloadOutlined />}
                  onClick={fetchFlows}
                >
                  刷新
                </Button>
              </div>

              <Table
                columns={flowColumns}
                dataSource={flows}
                rowKey="name"
                loading={loading}
                pagination={false}
                locale={{ emptyText: <Empty description="暂无 Flow 配置" /> }}
              />
            </div>
          ),
        },
        {
          key: 'history',
          label: '执行历史',
          children: (
            <div>
              <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'flex-end' }}>
                <Button
                  type="text"
                  icon={<ReloadOutlined />}
                  onClick={fetchFlowRuns}
                >
                  刷新
                </Button>
              </div>

              <Table
                columns={runColumns}
                dataSource={flowRuns}
                rowKey="flow_run_id"
                loading={runsLoading}
                pagination={{ pageSize: 20 }}
                locale={{ emptyText: <Empty description="暂无执行记录" /> }}
              />
            </div>
          ),
        },
      ]} />

      <FlowEditor
        visible={editorVisible}
        flowName={editingFlow}
        onClose={() => setEditorVisible(false)}
        onSaved={() => {
          fetchFlows();
        }}
      />
    </div>
  );
};

export default SchedulerCenter;
