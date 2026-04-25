import React, { useState, useEffect } from 'react';
import {
  Button,
  Tooltip,
  Table,
  Tag,
  Space,
  Popconfirm,
  Empty,
  Modal,
  Spin,
  Typography,
  Divider,
} from 'antd';
import {
  ReloadOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { notify } from '../../utils/notify';
import cronstrue from 'cronstrue/i18n';
import { flowApi, FlowListItem, FlowRun, FlowRunDetail, TaskConfig } from '../../api';
import FlowEditor from '../../components/SchedulerFlowEditor';
import QuantDatePicker from '../../components/QuantDatePicker';

const { Text } = Typography;

const SchedulerPanel: React.FC = () => {
  const [flows, setFlows] = useState<FlowListItem[]>([]);
  const [flowRuns, setFlowRuns] = useState<FlowRun[]>([]);
  const [loading, setLoading] = useState(false);
  const [runsLoading, setRunsLoading] = useState(false);
  const [editorVisible, setEditorVisible] = useState(false);
  const [editingFlow, setEditingFlow] = useState<string | undefined>();
  const [runningFlow, setRunningFlow] = useState<string | null>(null);

  const [triggerModal, setTriggerModal] = useState<{ visible: boolean; flowName: string }>({ visible: false, flowName: '' });
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [flowTasks, setFlowTasks] = useState<TaskConfig[]>([]);
  const [tasksLoading, setTasksLoading] = useState(false);

  const [runDetails, setRunDetails] = useState<Record<string, FlowRunDetail>>({});
  const [expandLoading, setExpandLoading] = useState<Record<string, boolean>>({});

  const handleExpandRun = async (expanded: boolean, record: FlowRun) => {
    if (!expanded || !record.flow_run_id) return;
    const key = record.flow_run_id;
    if (runDetails[key]) return;
    setExpandLoading(prev => ({ ...prev, [key]: true }));
    try {
      const res = await flowApi.getRunDetail(record.flow_name, key);
      setRunDetails(prev => ({ ...prev, [key]: res.data }));
    } catch {
      // silent
    } finally {
      setExpandLoading(prev => ({ ...prev, [key]: false }));
    }
  };

  const fetchFlows = async () => {
    setLoading(true);
    try {
      const res = await flowApi.list();
      setFlows(res.data);
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  };

  const fetchFlowRuns = async () => {
    setRunsLoading(true);
    try {
      const allRuns: FlowRun[] = [];
      for (const flow of flows) {
        try {
          const res = await flowApi.listRuns(flow.name, 10);
          if (res.data?.data && Array.isArray(res.data.data)) {
            allRuns.push(...res.data.data);
          }
        } catch {
          // 忽略单个 flow 的错误
        }
      }
      allRuns.sort((a, b) => {
        const aTime = a.started_at ? new Date(a.started_at).getTime() : 0;
        const bTime = b.started_at ? new Date(b.started_at).getTime() : 0;
        return bTime - aTime;
      });
      setFlowRuns(allRuns.slice(0, 100));
    } catch {
      // silent
    } finally {
      setRunsLoading(false);
    }
  };

  useEffect(() => { fetchFlows(); }, []);
  useEffect(() => { if (flows.length > 0) fetchFlowRuns(); }, [flows]);

  const handleCreate = () => { setEditingFlow(undefined); setEditorVisible(true); };
  const handleEdit = (name: string) => { setEditingFlow(name); setEditorVisible(true); };

  const handleDelete = async (name: string) => {
    try {
      await flowApi.delete(name, true);
      await fetchFlows();
    } catch {
      // silent
    }
  };

  const handleRun = async (name: string) => {
    const today = dayjs().format('YYYYMMDD');
    setTriggerModal({ visible: true, flowName: name });
    setDateRange([today, today]);
    setFlowTasks([]);
    setTasksLoading(true);
    try {
      const res = await flowApi.get(name);
      setFlowTasks(res.data?.tasks ?? []);
    } catch {
      // 拿不到 tasks 也不影响执行
    } finally {
      setTasksLoading(false);
    }
  };

  const handleTriggerConfirm = async () => {
    const { flowName } = triggerModal;
    const [start, end] = dateRange;
    if (!start || !end) { notify.error('请选择日期范围'); return; }
    setRunningFlow(flowName);
    setTriggerModal({ visible: false, flowName: '' });
    try {
      await flowApi.backfill(flowName, start, end);
      const days = dayjs(end, 'YYYYMMDD').diff(dayjs(start, 'YYYYMMDD'), 'day') + 1;
      notify.success(`Flow "${flowName}" 已启动，共 ${days} 天`);
      setTimeout(fetchFlowRuns, 2000);
    } catch (e: any) {
      notify.error(e?.response?.data?.detail || '执行失败');
    } finally {
      setRunningFlow(null);
    }
  };

  const topoLayers = (tasks: TaskConfig[]): TaskConfig[][] => {
    const idMap = new Map(tasks.map(t => [t.id, t]));
    const inDegree = new Map(tasks.map(t => [t.id, 0]));
    const adj = new Map<string, string[]>(tasks.map(t => [t.id, []]));
    for (const t of tasks) {
      for (const dep of t.depends_on) {
        if (idMap.has(dep)) {
          adj.get(dep)!.push(t.id);
          inDegree.set(t.id, (inDegree.get(t.id) ?? 0) + 1);
        }
      }
    }
    const layers: TaskConfig[][] = [];
    let queue = tasks.filter(t => (inDegree.get(t.id) ?? 0) === 0);
    while (queue.length > 0) {
      layers.push(queue);
      const next: TaskConfig[] = [];
      for (const t of queue) {
        for (const nid of (adj.get(t.id) ?? [])) {
          inDegree.set(nid, (inDegree.get(nid) ?? 1) - 1);
          if (inDegree.get(nid) === 0) next.push(idMap.get(nid)!);
        }
      }
      queue = next;
    }
    return layers;
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success': return 'green';
      case 'running': return 'blue';
      case 'failed': return 'red';
      case 'cancelled': return 'grey';
      default: return 'orange';
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'success': return '成功';
      case 'running': return '运行中';
      case 'failed': return '失败';
      case 'cancelled': return '已取消';
      case 'pending': return '等待中';
      default: return status;
    }
  };

  const flowColumns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => <span style={{ fontWeight: 500 }}>{name}</span>,
    },
    {
      title: '调度',
      dataIndex: 'cron',
      key: 'cron',
      render: (cron?: string) => {
        if (!cron) return <span style={{ color: 'var(--text-secondary)' }}>手动触发</span>;
        let desc = '';
        try { desc = cronstrue.toString(cron, { locale: 'zh_CN' }); } catch { desc = '无效表达式'; }
        return (
          <div>
            <code style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{cron}</code>
            <div style={{ fontSize: 12, color: 'var(--text-primary)', marginTop: 2 }}>{desc}</div>
          </div>
        );
      },
    },
    {
      title: '标签',
      dataIndex: 'tags',
      key: 'tags',
      render: (tags: string[]) => (
        <Space>{tags.map(tag => <Tag key={tag} color="blue">{tag}</Tag>)}</Space>
      ),
    },
    { title: '任务数', dataIndex: 'task_count', key: 'task_count', width: 80, align: 'center' as const },
    {
      title: '状态',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 80,
      render: (enabled: boolean) => (
        <Tag color={enabled ? 'green' : 'grey'}>{enabled ? '启用' : '禁用'}</Tag>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 160,
      render: (_: any, record: FlowListItem) => (
        <Space>
          <Tooltip title="立即执行">
            <Button type="text" icon={<PlayCircleOutlined />} loading={runningFlow === record.name} onClick={() => handleRun(record.name)} />
          </Tooltip>
          <Tooltip title="编辑">
            <Button type="text" icon={<EditOutlined />} onClick={() => handleEdit(record.name)} />
          </Tooltip>
          <Tooltip title="删除">
            <Popconfirm title="确定删除此 Flow？" onConfirm={() => handleDelete(record.name)}>
              <Button type="text" icon={<DeleteOutlined />} danger />
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
      render: (name: string) => <span style={{ fontWeight: 500 }}>{name}</span>,
    },
    {
      title: '运行 ID',
      dataIndex: 'flow_run_id',
      key: 'flow_run_id',
      render: (id?: string) => id ? (
        <Tooltip title={id}>
          <Typography.Text code style={{ fontSize: 12 }} ellipsis>{id.slice(-8)}</Typography.Text>
        </Tooltip>
      ) : '-',
    },
    {
      title: '触发方式',
      dataIndex: 'trigger_type',
      key: 'trigger_type',
      render: (type: string) => (
        <Tag color={type === 'scheduled' ? 'blue' : 'orange'}>{type === 'scheduled' ? '定时' : '手动'}</Tag>
      ),
    },
    { title: '目标日期', dataIndex: 'target_date', key: 'target_date' },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => <Tag color={getStatusColor(status)}>{getStatusText(status)}</Tag>,
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
      render: (sec?: number) => sec != null ? `${sec.toFixed(1)}s` : '-',
    },
    {
      title: '错误',
      dataIndex: 'error',
      key: 'error',
      render: (error?: string) => error ? (
        <Tooltip title={error}><span style={{ color: 'var(--error-color)' }}>查看</span></Tooltip>
      ) : '-',
    },
  ];

  return (
    <div>
      {/* 调度管理 */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text style={{ fontSize: 13, fontWeight: 600 }}>调度管理</Text>
          <Space>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>新建 Flow</Button>
            <Button type="text" icon={<ReloadOutlined />} onClick={fetchFlows}>刷新</Button>
          </Space>
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

      <Divider style={{ margin: '0 0 16px' }} />

      {/* 执行历史 */}
      <div>
        <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text style={{ fontSize: 13, fontWeight: 600 }}>执行历史</Text>
          <Button type="text" icon={<ReloadOutlined />} onClick={fetchFlowRuns}>刷新</Button>
        </div>
        <Table
          columns={runColumns}
          dataSource={flowRuns}
          rowKey="flow_run_id"
          loading={runsLoading}
          pagination={{ pageSize: 20 }}
          locale={{ emptyText: <Empty description="暂无执行记录" /> }}
          expandable={{
            onExpand: handleExpandRun,
            expandedRowRender: (record: FlowRun) => {
              const key = record.flow_run_id;
              if (expandLoading[key]) return <Spin size="small" style={{ padding: 12 }} />;
              const detail = runDetails[key];
              if (!detail) return <span style={{ color: 'var(--text-secondary)', fontSize: 12, padding: 12 }}>暂无数据</span>;
              return (
                <Table
                  size="small"
                  dataSource={detail.tasks}
                  rowKey="run_id"
                  pagination={false}
                  style={{ margin: '4px 0' }}
                  columns={[
                    {
                      title: '运行 ID', dataIndex: 'run_id', key: 'run_id', width: 120,
                      render: (id: string) => id ? (
                        <Tooltip title={id}>
                          <Typography.Text code style={{ fontSize: 12 }} ellipsis>{id.slice(-8)}</Typography.Text>
                        </Tooltip>
                      ) : '-',
                    },
                    {
                      title: '任务', dataIndex: 'task_id', key: 'task_id',
                      render: (id: string, r: any) => (
                        <span>
                          <Tag color={r.task_type === 'sync' ? 'blue' : r.task_type === 'etl' ? 'cyan' : r.task_type === 'factor' ? 'purple' : 'orange'} style={{ marginRight: 6 }}>
                            {r.task_type}
                          </Tag>
                          {id}
                        </span>
                      ),
                    },
                    {
                      title: '状态', dataIndex: 'status', key: 'status', width: 80,
                      render: (s: string) => (
                        <Tag color={s === 'success' ? 'green' : s === 'running' ? 'blue' : s === 'failed' ? 'red' : 'orange'}>
                          {s === 'success' ? '成功' : s === 'running' ? '运行中' : s === 'failed' ? '失败' : s}
                        </Tag>
                      ),
                    },
                    { title: '耗时', dataIndex: 'elapsed_sec', key: 'elapsed_sec', width: 80, render: (v?: number) => v != null ? `${v.toFixed(1)}s` : '-' },
                    { title: '行数', dataIndex: 'rows', key: 'rows', width: 80, render: (v?: number) => v != null ? v.toLocaleString() : '-' },
                    {
                      title: '参数', dataIndex: 'params', key: 'params',
                      render: (v?: string | object) => {
                        if (!v || v === '{}' || v === '') return '-';
                        try {
                          let obj;
                          if (typeof v === 'string') {
                            obj = JSON.parse(v);
                          } else if (typeof v === 'object') {
                            obj = v;
                          } else {
                            return <Typography.Text type="secondary" style={{ fontSize: 11 }}>{String(v)}</Typography.Text>;
                          }
                          // 对于对象或数组，先转为字符串，避免 React 渲染错误
                          const text = Object.entries(obj)
                            .map(([k, val]) => {
                              const strVal = typeof val === 'object' ? JSON.stringify(val) : String(val);
                              return `${k}:${strVal}`;
                            })
                            .join(' ');
                          return <Typography.Text type="secondary" style={{ fontSize: 11 }}>{text}</Typography.Text>;
                        } catch { return <Typography.Text type="secondary" style={{ fontSize: 11 }}>{String(v)}</Typography.Text>; }
                      },
                    },
                    {
                      title: '错误', dataIndex: 'error', key: 'error',
                      render: (e?: string) => e ? <Tooltip title={e}><span style={{ color: 'var(--color-loss)' }}>查看</span></Tooltip> : '-',
                    },
                  ]}
                />
              );
            },
          }}
        />
      </div>

      <FlowEditor
        visible={editorVisible}
        flowName={editingFlow}
        onClose={() => setEditorVisible(false)}
        onSaved={fetchFlows}
      />

      <Modal
        title={`执行调度: ${triggerModal.flowName}`}
        open={triggerModal.visible}
        onOk={handleTriggerConfirm}
        onCancel={() => setTriggerModal({ visible: false, flowName: '' })}
        okText="确认执行"
        cancelText="取消"
        okButtonProps={{ loading: runningFlow === triggerModal.flowName }}
        width={480}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: 13 }}>日期范围</div>
            <QuantDatePicker value={dateRange} onChange={(s, e) => setDateRange([s, e])} style={{ width: '100%' }} />
            {dateRange[0] && dateRange[1] && (
              <div style={{ marginTop: 8, padding: '6px 10px', background: 'var(--color-primary-light-default)', borderRadius: 6 }}>
                <span style={{ color: 'var(--color-primary)', fontSize: 13, fontWeight: 500 }}>
                  共 {dayjs(dateRange[1], 'YYYYMMDD').diff(dayjs(dateRange[0], 'YYYYMMDD'), 'day') + 1} 天，每天串行执行一次
                </span>
              </div>
            )}
          </div>
          <div>
            <div style={{ marginBottom: 8, fontWeight: 500, fontSize: 13 }}>任务执行顺序</div>
            {tasksLoading ? (
              <Spin size="small" />
            ) : flowTasks.length === 0 ? (
              <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>暂无任务配置</span>
            ) : (() => {
              const layers = topoLayers(flowTasks);
              return (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {layers.map((layer, i) => (
                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ minWidth: 24, height: 24, borderRadius: '50%', background: 'var(--color-primary)', color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 600, flexShrink: 0 }}>
                        {i + 1}
                      </span>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                        {layer.map(t => (
                          <Tag key={t.id} color={t.type === 'sync' ? 'blue' : t.type === 'etl' ? 'cyan' : t.type === 'factor' ? 'purple' : 'orange'} style={{ margin: 0 }}>
                            {t.id}
                          </Tag>
                        ))}
                      </div>
                      {i < layers.length - 1 && (
                        <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 'auto', flexShrink: 0 }}>↓ 完成后</span>
                      )}
                    </div>
                  ))}
                </div>
              );
            })()}
          </div>
        </div>
      </Modal>
    </div>
  );
};

export default SchedulerPanel;
