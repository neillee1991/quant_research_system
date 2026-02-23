import React, { useEffect, useState, useCallback } from 'react';
import {
  Table,
  Button,
  Input,
  Select,
  Space,
  Tag,
  message,
  Card,
  Tabs,
  Modal,
  Descriptions,
  Badge,
  Tooltip,
  Row,
  Col,
  Collapse,
  Form,
  Popconfirm,
  DatePicker,
  Radio,
  Progress,
} from 'antd';
import {
  SyncOutlined,
  DatabaseOutlined,
  CodeOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  EditOutlined,
  DeleteOutlined,
  ClockCircleOutlined,
  CalendarOutlined,
  ApartmentOutlined,
  HistoryOutlined,
  PlusOutlined,
} from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import dayjs from 'dayjs';
import { dataApi, productionApi } from '../api';
import TradingViewChart from '../components/Charts/TradingViewChart';

const { Search, TextArea } = Input;
const { TabPane } = Tabs;
const { Panel } = Collapse;

interface SyncTask {
  task_id: string;
  description: string;
  sync_type: string;
  schedule: string;
  enabled: boolean;
  table_name: string;
}

interface TaskStatus {
  task_id: string;
  description: string;
  enabled: boolean;
  sync_type: string;
  schedule: string;
  last_sync_date: string | null;
  last_sync_time: string | null;
  table_name: string;
  table_latest_date?: string | null;
}

interface SyncLog {
  id: number;
  source: string;
  data_type: string;
  last_date: string;
  sync_date: string;
  rows_synced: number;
  status: string;
  created_at: string;
}

interface TableInfo {
  table_name: string;
  row_count: number;
  column_count: number;
  columns: string[];
}

// ==================== DAG 管理 Section ====================
const DAGManageSection: React.FC = () => {
  const [dags, setDags] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState<string | null>(null);
  const [historyModal, setHistoryModal] = useState<{ visible: boolean; dagId: string; data: any[]; filter: string }>({ visible: false, dagId: '', data: [], filter: '' });
  const [backfillDetail, setBackfillDetail] = useState<Record<string, any[]>>({});
  const [createModal, setCreateModal] = useState(false);
  const [editModal, setEditModal] = useState<{ visible: boolean; dag: any }>({ visible: false, dag: null });
  const [runModal, setRunModal] = useState<{ visible: boolean; dagId: string }>({ visible: false, dagId: '' });
  const [runMode, setRunMode] = useState<'today' | 'date' | 'range'>('today');
  const [runDate, setRunDate] = useState<dayjs.Dayjs | null>(null);
  const [runRange, setRunRange] = useState<[dayjs.Dayjs | null, dayjs.Dayjs | null]>([null, null]);
  const [backfillProgress, setBackfillProgress] = useState<{ visible: boolean; data: any } | null>(null);
  const [form] = Form.useForm();
  const [editForm] = Form.useForm();

  const loadDags = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.listDags();
      setDags(res.data?.dags || res.data?.data || []);
    } catch { message.error('加载 DAG 列表失败'); }
    setLoading(false);
  }, []);

  useEffect(() => { loadDags(); }, [loadDags]);

  const openRunModal = (dagId: string) => {
    setRunMode('today');
    setRunDate(null);
    setRunRange([null, null]);
    setRunModal({ visible: true, dagId });
  };

  const handleRunDag = async () => {
    const dagId = runModal.dagId;
    let params: any = {};

    if (runMode === 'date') {
      if (!runDate) { message.warning('请选择执行日期'); return; }
      params.target_date = runDate.format('YYYY-MM-DD');
      params.run_type = 'single';
    } else if (runMode === 'range') {
      if (!runRange[0] || !runRange[1]) { message.warning('请选择日期范围'); return; }
      params.start_date = runRange[0].format('YYYY-MM-DD');
      params.end_date = runRange[1].format('YYYY-MM-DD');
      params.run_type = 'backfill';
    } else {
      params.run_type = 'today';
    }

    setRunModal({ visible: false, dagId: '' });
    setRunLoading(dagId);
    try {
      const res = await productionApi.runDag(dagId, Object.keys(params).length ? params : undefined);
      const data = res.data?.data || res.data;

      // 回溯模式
      if (data?.mode === 'backfill') {
        setBackfillProgress({ visible: true, data });
        loadDags();
        setRunLoading(null);
        return;
      }

      // 单日模式
      const dagStatus = data?.status || 'unknown';
      const summary = data?.summary || '';
      const failedTasks: any[] = data?.failed_tasks || [];

      if (dagStatus === 'success') {
        message.success(`DAG ${dagId} 执行成功${summary ? ` (${summary})` : ''}`);
      } else {
        Modal.warning({
          title: `DAG ${dagId} 执行完成 - ${dagStatus}`,
          width: 520,
          content: (
            <div>
              <p>{summary}</p>
              {failedTasks.length > 0 && (
                <div style={{ marginTop: 8 }}>
                  <p style={{ fontWeight: 600, marginBottom: 4 }}>失败任务:</p>
                  {failedTasks.map((t: any) => (
                    <div key={t.task_id} style={{ marginBottom: 4, padding: '4px 8px', background: '#1e293b', borderRadius: 4, fontSize: 12 }}>
                      <Tag color={t.status === 'failed' ? 'red' : 'orange'}>{t.task_id}</Tag>
                      <span style={{ color: '#f87171' }}>{t.error_message || '无详细信息'}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ),
        });
      }
      loadDags();
    } catch (e: any) {
      message.error(e.response?.data?.detail || 'DAG 执行失败');
    }
    setRunLoading(null);
  };

  const showHistory = async (dagId: string, runType?: string) => {
    try {
      const res = await productionApi.getDagHistory(dagId, 20, runType || undefined);
      setHistoryModal({ visible: true, dagId, data: res.data?.runs || res.data?.data || [], filter: runType || '' });
      setBackfillDetail({});
    } catch { message.error('加载历史失败'); }
  };

  const loadBackfillDetail = async (backfillId: string) => {
    if (backfillDetail[backfillId]) return;
    try {
      const res = await productionApi.getBackfillDetail(backfillId);
      const runs = res.data?.data?.runs || res.data?.runs || [];
      setBackfillDetail(prev => ({ ...prev, [backfillId]: runs }));
    } catch { message.error('加载回溯详情失败'); }
  };

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const tasks = values.tasks_json ? JSON.parse(values.tasks_json) : [];
      await productionApi.createDag({ dag_id: values.dag_id, description: values.description, schedule: values.schedule, tasks });
      message.success(`DAG ${values.dag_id} 创建成功`);
      setCreateModal(false);
      form.resetFields();
      loadDags();
    } catch (e: any) {
      if (e.response) message.error(e.response?.data?.detail || '创建失败');
      else if (e instanceof SyntaxError) message.error('任务 JSON 格式错误');
    }
  };

  const handleEdit = async () => {
    try {
      const values = await editForm.validateFields();
      const tasks = values.tasks_json ? JSON.parse(values.tasks_json) : undefined;
      await productionApi.updateDag(editModal.dag.dag_id, { description: values.description, schedule: values.schedule, tasks });
      message.success('DAG 更新成功');
      setEditModal({ visible: false, dag: null });
      loadDags();
    } catch (e: any) {
      if (e.response) message.error(e.response?.data?.detail || '更新失败');
      else if (e instanceof SyntaxError) message.error('任务 JSON 格式错误');
    }
  };

  const handleDelete = async (dagId: string) => {
    try {
      await productionApi.deleteDag(dagId);
      message.success(`DAG ${dagId} 已删除`);
      loadDags();
    } catch (e: any) {
      message.error(e.response?.data?.detail || '删除失败');
    }
  };

  const openEdit = (record: any) => {
    editForm.setFieldsValue({
      description: record.description,
      schedule: record.schedule || '',
      tasks_json: JSON.stringify(record.tasks || [], null, 2),
    });
    setEditModal({ visible: true, dag: record });
  };

  const dagColumns = [
    { title: 'DAG ID', dataIndex: 'dag_id', key: 'dag_id', render: (v: string) => <Tag color="purple">{v}</Tag> },
    { title: '描述', dataIndex: 'description', key: 'desc' },
    { title: '调度', dataIndex: 'schedule', key: 'schedule', render: (v: string) => v ? <Tag color="cyan">{v}</Tag> : <Tag>手动</Tag> },
    { title: '任务数', dataIndex: 'tasks', key: 'tasks', render: (v: any[]) => v?.length || 0 },
    { title: '最近成功', dataIndex: 'last_success', key: 'last_success', render: (v: string) => v ? <span style={{ color: '#4ade80', fontSize: 12 }}>{v.slice(0, 19)}</span> : <span style={{ color: '#64748b' }}>-</span> },
    {
      title: '操作', key: 'action', width: 280, render: (_: any, record: any) => (
        <Space size={4}>
          <Button size="small" type="primary" icon={<PlayCircleOutlined />}
            loading={runLoading === record.dag_id}
            onClick={() => openRunModal(record.dag_id)}>执行</Button>
          <Button size="small" icon={<HistoryOutlined />}
            onClick={() => showHistory(record.dag_id)}>历史</Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => openEdit(record)} />
          <Popconfirm title="确认删除此 DAG?" onConfirm={() => handleDelete(record.dag_id)}>
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      )
    },
  ];

  const runTypeLabel: Record<string, { text: string; color: string }> = {
    today: { text: '今日', color: 'blue' },
    single: { text: '指定日期', color: 'cyan' },
    backfill: { text: '回溯', color: 'purple' },
  };

  const historyColumns = [
    { title: '运行ID', dataIndex: 'run_id', key: 'run_id', width: 100, render: (v: string) => <Tooltip title={v}><span>{v?.slice(0, 8)}...</span></Tooltip> },
    { title: '类型', dataIndex: 'run_type', key: 'run_type', width: 80, render: (v: string) => {
      const info = runTypeLabel[v] || { text: v || '-', color: 'default' };
      return <Tag color={info.color}>{info.text}</Tag>;
    }},
    { title: '目标日期', dataIndex: 'target_date', key: 'target_date', width: 110, render: (v: string) => v || '-' },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80, render: (v: string) => <Tag color={v === 'success' ? 'green' : v === 'running' ? 'blue' : 'red'}>{v}</Tag> },
    { title: '开始时间', dataIndex: 'started_at', key: 'start', width: 160, render: (v: string) => v?.slice(0, 19) },
    { title: '结束时间', dataIndex: 'finished_at', key: 'end', width: 160, render: (v: string) => v?.slice(0, 19) || '-' },
    { title: '任务详情', dataIndex: 'task_results', key: 'tasks', render: (v: any) => {
      if (!v) return '-';
      const tasks = typeof v === 'string' ? JSON.parse(v) : v;
      if (Array.isArray(tasks)) {
        return <Space wrap>{tasks.map((t: any) => (
          <Tooltip key={t.task_id} title={t.error_message || (t.status === 'success' ? '成功' : t.status)}>
            <Tag color={t.status === 'success' ? 'green' : t.status === 'skipped' ? 'orange' : 'red'}>{t.task_id}</Tag>
          </Tooltip>
        ))}</Space>;
      }
      return <Space wrap>{Object.entries(tasks).map(([k, s]: any) => <Tag key={k} color={s === 'success' ? 'green' : 'red'}>{k}</Tag>)}</Space>;
    }},
  ];

  const backfillSubColumns = [
    { title: '目标日期', dataIndex: 'target_date', key: 'date', width: 120 },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80, render: (v: string) => <Tag color={v === 'success' ? 'green' : v === 'running' ? 'blue' : 'red'}>{v}</Tag> },
    { title: '开始', dataIndex: 'started_at', key: 'start', width: 160, render: (v: string) => v?.slice(0, 19) },
    { title: '结束', dataIndex: 'finished_at', key: 'end', width: 160, render: (v: string) => v?.slice(0, 19) || '-' },
    { title: '任务', dataIndex: 'task_results', key: 'tasks', render: (v: any) => {
      if (!v) return '-';
      const tasks = typeof v === 'string' ? JSON.parse(v) : v;
      if (Array.isArray(tasks)) {
        return <Space wrap size={2}>{tasks.map((t: any) => (
          <Tooltip key={t.task_id} title={t.error_message || t.status}>
            <Tag color={t.status === 'success' ? 'green' : t.status === 'skipped' ? 'orange' : 'red'} style={{ fontSize: 11 }}>{t.task_id}</Tag>
          </Tooltip>
        ))}</Space>;
      }
      return '-';
    }},
  ];

  return (
    <div>
      <Card className="tech-card" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: '#7c3aed', fontWeight: 600, fontSize: 15 }}>🔗 DAG 列表</span>
          <Space>
            <Button size="small" icon={<PlusOutlined />} onClick={() => setCreateModal(true)}>新建 DAG</Button>
            <Button icon={<ReloadOutlined />} onClick={loadDags} size="small">刷新</Button>
          </Space>
        </div>
        <Table dataSource={dags} columns={dagColumns} rowKey="dag_id"
          loading={loading} size="small" pagination={false} className="tech-table"
          expandable={{
            expandedRowRender: (record) => {
              const tasks: any[] = record.tasks || [];
              if (!tasks.length) return <span style={{ color: '#94a3b8', fontSize: 12 }}>暂无任务</span>;

              // 拓扑分层：计算每个节点的层级
              const depMap: Record<string, string[]> = {};
              tasks.forEach(t => { depMap[t.task_id] = t.depends_on || []; });
              const layerOf: Record<string, number> = {};
              const calcLayer = (id: string, visited: Set<string> = new Set()): number => {
                if (layerOf[id] !== undefined) return layerOf[id];
                if (visited.has(id)) return 0;
                visited.add(id);
                const deps = depMap[id] || [];
                const layer = deps.length === 0 ? 0 : Math.max(...deps.map(d => calcLayer(d, visited))) + 1;
                layerOf[id] = layer;
                return layer;
              };
              tasks.forEach(t => calcLayer(t.task_id));

              // 同层节点垂直排列
              const layerGroups: Record<number, string[]> = {};
              Object.entries(layerOf).forEach(([id, l]) => {
                if (!layerGroups[l]) layerGroups[l] = [];
                layerGroups[l].push(id);
              });
              const maxLayer = Math.max(...Object.values(layerOf), 0);
              const xGap = 180;
              const yGap = 60;

              const nodes = tasks.map(t => {
                const layer = layerOf[t.task_id] || 0;
                const siblings = layerGroups[layer] || [t.task_id];
                const idx = siblings.indexOf(t.task_id);
                const yOffset = (idx - (siblings.length - 1) / 2) * yGap;
                return {
                  name: t.task_id,
                  x: layer * xGap,
                  y: yOffset,
                  symbolSize: 40,
                  label: { show: true, fontSize: 11, color: '#e2e8f0', formatter: (p: any) => p.name.length > 10 ? p.name.slice(0, 10) + '..' : p.name },
                  itemStyle: {
                    color: (t.depends_on?.length ? '#7c3aed' : '#00d4ff'),
                    borderColor: '#1e293b', borderWidth: 2,
                  },
                  tooltip: { formatter: `${t.task_id}<br/>Action: ${t.action || '-'}<br/>依赖: ${t.depends_on?.join(', ') || '无'}` },
                };
              });

              const edges = tasks.flatMap(t =>
                (t.depends_on || []).map((dep: string) => ({
                  source: dep, target: t.task_id,
                  lineStyle: { color: '#475569', width: 2, curveness: 0.1 },
                }))
              );

              const chartH = Math.max(120, Object.values(layerGroups).reduce((m, g) => Math.max(m, g.length), 0) * yGap + 40);

              const option = {
                backgroundColor: 'transparent',
                tooltip: { trigger: 'item' as const },
                series: [{
                  type: 'graph',
                  layout: 'none',
                  roam: false,
                  edgeSymbol: ['none', 'arrow'],
                  edgeSymbolSize: [0, 8],
                  data: nodes,
                  links: edges,
                  lineStyle: { opacity: 0.8 },
                  emphasis: { focus: 'adjacency' as const, lineStyle: { width: 3 } },
                }],
              };

              return (
                <div style={{ padding: '8px 0' }}>
                  <span style={{ color: '#94a3b8', fontSize: 12, marginBottom: 4, display: 'block' }}>
                    📊 任务依赖图 ({tasks.length} 个任务, {maxLayer + 1} 层)
                  </span>
                  <ReactECharts option={option} style={{ height: chartH, width: Math.max(300, (maxLayer + 1) * xGap + 80) }} />
                </div>
              );
            }
          }}
        />
      </Card>

      {/* 运行历史 Modal */}
      <Modal title={`DAG 运行历史 - ${historyModal.dagId}`} open={historyModal.visible}
        onCancel={() => setHistoryModal({ visible: false, dagId: '', data: [], filter: '' })}
        footer={null} width={960}>
        <div style={{ marginBottom: 12 }}>
          <Radio.Group value={historyModal.filter} onChange={e => showHistory(historyModal.dagId, e.target.value)} size="small">
            <Radio.Button value="">全部</Radio.Button>
            <Radio.Button value="today">今日执行</Radio.Button>
            <Radio.Button value="single">指定日期</Radio.Button>
            <Radio.Button value="backfill">回溯</Radio.Button>
          </Radio.Group>
        </div>
        <Table dataSource={historyModal.data} columns={historyColumns}
          rowKey="run_id" size="small" pagination={{ pageSize: 10 }}
          expandable={{
            rowExpandable: (record: any) => record.run_type === 'backfill' && !!record.backfill_id,
            onExpand: (expanded: boolean, record: any) => {
              if (expanded && record.backfill_id) loadBackfillDetail(record.backfill_id);
            },
            expandedRowRender: (record: any) => {
              const runs = backfillDetail[record.backfill_id];
              if (!runs) return <span style={{ color: '#94a3b8', fontSize: 12 }}>加载中...</span>;
              return (
                <div style={{ padding: '4px 0' }}>
                  <span style={{ color: '#94a3b8', fontSize: 12, marginBottom: 4, display: 'block' }}>
                    回溯详情 ({runs.length} 天)
                  </span>
                  <Table dataSource={runs} columns={backfillSubColumns}
                    rowKey="run_id" size="small" pagination={false}
                    style={{ background: 'rgba(0,0,0,0.15)' }} />
                </div>
              );
            },
          }}
        />
      </Modal>

      {/* 新建 DAG Modal */}
      <Modal title="新建 DAG" open={createModal} onOk={handleCreate}
        onCancel={() => { setCreateModal(false); form.resetFields(); }} okText="创建" width={600}>
        <Form form={form} layout="vertical" size="small">
          <Form.Item name="dag_id" label="DAG ID" rules={[{ required: true, message: '请输入 DAG ID' }]}>
            <Input placeholder="如 daily_update" />
          </Form.Item>
          <Form.Item name="description" label="描述"><Input placeholder="DAG 描述" /></Form.Item>
          <Form.Item name="schedule" label="调度 (Cron)"><Input placeholder="如 0 2 * * * (留空为手动)" /></Form.Item>
          <Form.Item name="tasks_json" label="任务列表 (JSON)" initialValue="[]">
            <Input.TextArea rows={6} placeholder='[{"task_id": "sync", "action": "sync_all", "depends_on": []}]'
              style={{ fontFamily: 'Fira Code, Courier New, monospace', fontSize: 12 }} />
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑 DAG Modal */}
      <Modal title={`编辑 DAG: ${editModal.dag?.dag_id || ''}`} open={editModal.visible}
        onOk={handleEdit} onCancel={() => setEditModal({ visible: false, dag: null })} okText="保存" width={600}>
        <Form form={editForm} layout="vertical" size="small">
          <Form.Item name="description" label="描述"><Input /></Form.Item>
          <Form.Item name="schedule" label="调度 (Cron)"><Input placeholder="留空为手动" /></Form.Item>
          <Form.Item name="tasks_json" label="任务列表 (JSON)">
            <Input.TextArea rows={8}
              style={{ fontFamily: 'Fira Code, Courier New, monospace', fontSize: 12 }} />
          </Form.Item>
        </Form>
      </Modal>

      {/* DAG 执行模态框 */}
      <Modal title={`执行 DAG: ${runModal.dagId}`} open={runModal.visible}
        onOk={handleRunDag} onCancel={() => setRunModal({ visible: false, dagId: '' })}
        okText="开始执行" width={480}>
        <div style={{ marginBottom: 16 }}>
          <Radio.Group value={runMode} onChange={e => setRunMode(e.target.value)}>
            <Radio.Button value="today">今天</Radio.Button>
            <Radio.Button value="date">指定日期</Radio.Button>
            <Radio.Button value="range">日期范围回溯</Radio.Button>
          </Radio.Group>
        </div>
        {runMode === 'date' && (
          <DatePicker style={{ width: '100%' }} value={runDate}
            onChange={v => setRunDate(v)} placeholder="选择执行日期" />
        )}
        {runMode === 'range' && (
          <DatePicker.RangePicker style={{ width: '100%' }}
            value={runRange as any}
            onChange={v => setRunRange(v ? [v[0], v[1]] : [null, null])}
            placeholder={['开始日期', '结束日期']} />
        )}
        {runMode === 'range' && runRange[0] && runRange[1] && (
          <div style={{ marginTop: 8, color: '#94a3b8', fontSize: 12 }}>
            将按交易日逐日执行，共约 {Math.ceil(runRange[1].diff(runRange[0], 'day') * 5 / 7)} 个交易日
          </div>
        )}
      </Modal>

      {/* 回溯结果模态框 */}
      <Modal title="回溯执行结果" open={!!backfillProgress?.visible}
        onCancel={() => setBackfillProgress(null)} footer={null} width={560}>
        {backfillProgress?.data && (() => {
          const d = backfillProgress.data;
          const total = d.total_days || 0;
          const success = d.success_days || 0;
          const failed = d.failed_days || 0;
          const pct = total > 0 ? Math.round((success / total) * 100) : 0;
          return (
            <div>
              <div style={{ marginBottom: 12 }}>
                <Tag color="blue">{d.start_date}</Tag> → <Tag color="blue">{d.end_date}</Tag>
                <span style={{ marginLeft: 8, color: '#94a3b8' }}>共 {total} 天</span>
              </div>
              <Progress percent={pct} status={failed > 0 ? 'exception' : 'success'}
                format={() => `${success}/${total} 成功`} />
              <div style={{ marginTop: 12, display: 'flex', gap: 16 }}>
                <Tag color="green">成功: {success}</Tag>
                <Tag color="red">失败: {failed}</Tag>
              </div>
              {d.details && d.details.length > 0 && (
                <div style={{ marginTop: 12, maxHeight: 300, overflow: 'auto' }}>
                  <Table size="small" dataSource={d.details} rowKey="date" pagination={false}
                    columns={[
                      { title: '日期', dataIndex: 'date', key: 'date', width: 120 },
                      { title: '状态', dataIndex: 'status', key: 'status', width: 80,
                        render: (v: string) => <Tag color={v === 'success' ? 'green' : 'red'}>{v}</Tag> },
                      { title: '详情', dataIndex: 'summary', key: 'summary', ellipsis: true },
                    ]} />
                </div>
              )}
            </div>
          );
        })()}
      </Modal>
    </div>
  );
};

const DataCenter: React.FC = () => {
  // 原有状态
  const [stocks, setStocks] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>('');
  const [dailyData, setDailyData] = useState<any[]>([]);
  const [syncLogs, setSyncLogs] = useState<SyncLog[]>([]);
  const [loading, setLoading] = useState(false);

  // 新增状态
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM daily_basic LIMIT 10');
  const [queryResult, setQueryResult] = useState<any[]>([]);
  const [queryColumns, setQueryColumns] = useState<string[]>([]);
  const [queryLoading, setQueryLoading] = useState(false);
  const [syncingTasks, setSyncingTasks] = useState<Set<string>>(new Set());

  // 同步日志筛选状态
  const [logFilters, setLogFilters] = useState({
    source: undefined as string | undefined,
    dataType: undefined as string | undefined,
    startDate: undefined as string | undefined,
    endDate: undefined as string | undefined,
  });

  // 同步参数模态框
  const [syncModalVisible, setSyncModalVisible] = useState(false);
  const [syncModalTask, setSyncModalTask] = useState<SyncTask | null>(null);
  const [syncTargetDate, setSyncTargetDate] = useState<string>('');
  const [syncStartDate, setSyncStartDate] = useState<string>('');
  const [syncEndDate, setSyncEndDate] = useState<string>('');

  // 任务配置编辑模态框
  const [configModalVisible, setConfigModalVisible] = useState(false);
  const [configModalTask, setConfigModalTask] = useState<any>(null);
  const [configJson, setConfigJson] = useState<string>('');
  const [isNewTask, setIsNewTask] = useState(false);

  // 调度管理状态
  const [scheduleModalVisible, setScheduleModalVisible] = useState(false);
  const [scheduleModalTask, setScheduleModalTask] = useState<SyncTask | null>(null);
  const [scheduleType, setScheduleType] = useState<string>('daily');
  const [cronExpression, setCronExpression] = useState<string>('');
  const [scheduleInfo, setScheduleInfo] = useState<Record<string, any>>({});

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      const [stocksRes, tasksRes, tablesRes] = await Promise.all([
        dataApi.listStocks(),
        dataApi.listSyncTasks(),
        dataApi.listTables(),
      ]);

      setStocks(stocksRes.data.stocks || []);
      setSyncTasks(tasksRes.data.tasks || []);
      setTables(tablesRes.data.tables || []);

      // 加载每个任务的状态
      const tasks = tasksRes.data.tasks || [];
      for (const task of tasks) {
        loadTaskStatus(task.task_id);
        loadTaskScheduleInfo(task.task_id);
      }

      // 加载同步日志
      loadSyncLogs();
    } catch (error) {
      message.error('加载数据失败');
    }
  };

  const loadSyncLogs = async () => {
    try {
      const res = await dataApi.getSyncStatus(
        logFilters.source,
        logFilters.dataType,
        logFilters.startDate,
        logFilters.endDate
      );
      setSyncLogs(res.data.logs || []);
    } catch (error) {
      console.error('Failed to load sync logs');
    }
  };

  const loadTaskStatus = async (taskId: string) => {
    try {
      const res = await dataApi.getTaskStatus(taskId);
      setTaskStatuses((prev) => ({ ...prev, [taskId]: res.data }));
    } catch (error) {
      console.error(`Failed to load status for ${taskId}`);
    }
  };

  const loadTaskScheduleInfo = async (taskId: string) => {
    try {
      const res = await dataApi.getTaskScheduleInfo(taskId);
      setScheduleInfo((prev) => ({ ...prev, [taskId]: res.data }));
    } catch (error) {
      console.error(`Failed to load schedule info for ${taskId}`);
    }
  };

  const loadDaily = async (code: string) => {
    setLoading(true);
    try {
      const r = await dataApi.getDaily(code, '20230101', undefined, 500);
      setDailyData(r.data.data || []);
    } finally {
      setLoading(false);
    }
  };

  const handleSyncTask = async (taskId: string) => {
    const task = syncTasks.find(t => t.task_id === taskId);
    if (!task) return;

    // 显示同步参数模态框
    setSyncModalTask(task);
    setSyncTargetDate('');
    setSyncStartDate('');
    setSyncEndDate('');
    setSyncModalVisible(true);
  };

  const executeSyncTask = async () => {
    if (!syncModalTask) return;

    const taskId = syncModalTask.task_id;
    setSyncingTasks((prev) => new Set(prev).add(taskId));
    setSyncModalVisible(false);

    try {
      // 如果指定了 start_date 和 end_date，使用它们；否则使用 target_date
      await dataApi.syncTask(
        taskId,
        syncTargetDate || undefined,
        syncStartDate || undefined,
        syncEndDate || undefined
      );
      message.success(`任务 ${taskId} 同步已启动`);
      setTimeout(() => {
        loadTaskStatus(taskId);
        loadSyncLogs();
      }, 2000);
    } catch (error) {
      message.error(`任务 ${taskId} 同步失败`);
    } finally {
      setSyncingTasks((prev) => {
        const newSet = new Set(prev);
        newSet.delete(taskId);
        return newSet;
      });
    }
  };

  const handleSyncAll = async () => {
    try {
      // Sync all tasks with latest data only (no target_date parameter)
      await dataApi.syncAllTasks();
      message.success('所有任务已在后台开始同步');
      setTimeout(() => {
        syncTasks.forEach((task) => loadTaskStatus(task.task_id));
        loadSyncLogs();
      }, 3000);
    } catch (error) {
      message.error('启动同步失败');
    }
  };

  const handleEditTask = async (taskId: string) => {
    try {
      const res = await dataApi.getTaskConfig(taskId);
      setConfigModalTask(res.data.config);
      setConfigJson(JSON.stringify(res.data.config, null, 2));
      setIsNewTask(false);
      setConfigModalVisible(true);
    } catch (error) {
      message.error('加载任务配置失败');
    }
  };

  const handleNewTask = () => {
    const template = {
      task_id: "new_task",
      api_name: "api_name",
      description: "任务描述",
      sync_type: "incremental",
      schedule: "daily",
      params: {
        trade_date: "{date}",
        fields: "ts_code,trade_date"
      },
      date_field: "trade_date",
      primary_keys: ["ts_code", "trade_date"],
      table_name: "new_table",
      schema: {
        ts_code: { type: "VARCHAR", nullable: false, comment: "股票代码" },
        trade_date: { type: "VARCHAR", nullable: false, comment: "交易日期" }
      },
      enabled: true,
      batch_size: 5000
    };
    setConfigModalTask(template);
    setConfigJson(JSON.stringify(template, null, 2));
    setIsNewTask(true);
    setConfigModalVisible(true);
  };

  const handleSaveConfig = async () => {
    try {
      const config = JSON.parse(configJson);

      if (isNewTask) {
        await dataApi.createTask(config);
        message.success(`任务 ${config.task_id} 创建成功`);
      } else {
        await dataApi.updateTaskConfig(config.task_id, config);
        message.success(`任务 ${config.task_id} 更新成功`);
      }

      setConfigModalVisible(false);
      loadInitialData();
    } catch (error: any) {
      if (error instanceof SyntaxError) {
        message.error('JSON 格式无效');
      } else {
        message.error(error.response?.data?.detail || '保存配置失败');
      }
    }
  };

  const handleExecuteQuery = async () => {
    if (!sqlQuery.trim()) {
      message.warning('请输入 SQL 查询语句');
      return;
    }

    setQueryLoading(true);
    try {
      const res = await dataApi.executeQuery(sqlQuery);
      setQueryResult(res.data.data || []);
      setQueryColumns(res.data.columns || []);
      message.success(`查询返回 ${res.data.count} 行数据`);
    } catch (error: any) {
      message.error(error.response?.data?.detail || '查询失败');
    } finally {
      setQueryLoading(false);
    }
  };

  const handleTruncateTable = async (tableName: string) => {
    Modal.confirm({
      title: '清空表数据',
      content: `确定要清空表 "${tableName}" 的所有数据吗？此操作不可撤销。`,
      okText: '确定清空',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await dataApi.truncateTable(tableName);
          message.success(`表 ${tableName} 已清空`);
          loadInitialData();
        } catch (error: any) {
          message.error(error.response?.data?.detail || '清空表失败');
        }
      },
    });
  };

  const handleDeleteTask = async (taskId: string) => {
    Modal.confirm({
      title: '删除同步任务',
      content: `确定要删除任务 "${taskId}" 吗？这将删除任务配置，但不会删除数据表。`,
      okText: '确定删除',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await dataApi.deleteTask(taskId);
          message.success(`任务 ${taskId} 已删除`);
          loadInitialData();
        } catch (error: any) {
          message.error(error.response?.data?.detail || '删除任务失败');
        }
      },
    });
  };

  const handleScheduleTask = (task: SyncTask) => {
    setScheduleModalTask(task);
    setScheduleType(task.schedule || 'daily');
    setCronExpression('');
    setScheduleModalVisible(true);
  };

  const handleEnableSchedule = async () => {
    if (!scheduleModalTask) return;

    try {
      await dataApi.enableTaskSchedule(
        scheduleModalTask.task_id,
        scheduleType,
        scheduleType === 'custom' ? cronExpression : undefined
      );
      message.success(`任务 ${scheduleModalTask.task_id} 调度已启用`);
      setScheduleModalVisible(false);
      loadTaskScheduleInfo(scheduleModalTask.task_id);
    } catch (error: any) {
      message.error(error.response?.data?.detail || '启用调度失败');
    }
  };

  const handleDisableSchedule = async (taskId: string) => {
    try {
      await dataApi.disableTaskSchedule(taskId);
      message.success(`任务 ${taskId} 调度已禁用`);
      loadTaskScheduleInfo(taskId);
    } catch (error: any) {
      message.error(error.response?.data?.detail || '禁用调度失败');
    }
  };

  const dailyColumns = [
    { title: '日期', dataIndex: 'trade_date', key: 'trade_date', width: 100 },
    {
      title: '开盘',
      dataIndex: 'open',
      key: 'open',
      render: (v: number) => <span style={{ color: '#94a3b8' }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '最高',
      dataIndex: 'high',
      key: 'high',
      render: (v: number) => <span style={{ color: '#10b981' }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '最低',
      dataIndex: 'low',
      key: 'low',
      render: (v: number) => <span style={{ color: '#ef4444' }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '收盘',
      dataIndex: 'close',
      key: 'close',
      render: (v: number) => <span style={{ color: '#00d4ff', fontWeight: 600 }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '成交量',
      dataIndex: 'vol',
      key: 'vol',
      render: (v: number) => <span style={{ color: '#94a3b8' }}>{v?.toLocaleString()}</span>,
      width: 100
    },
    {
      title: '涨跌幅',
      dataIndex: 'pct_chg',
      key: 'pct_chg',
      render: (v: number) => (
        <Tag
          color={v >= 0 ? 'red' : 'green'}
          style={{
            background: v >= 0 ? 'rgba(239, 68, 68, 0.2)' : 'rgba(16, 185, 129, 0.2)',
            border: `1px solid ${v >= 0 ? '#ef4444' : '#10b981'}`,
            color: v >= 0 ? '#ef4444' : '#10b981',
            fontWeight: 600
          }}
        >
          {v >= 0 ? '+' : ''}{v?.toFixed(2)}%
        </Tag>
      ),
      width: 80,
    },
  ];

  const tableColumns = [
    {
      title: '表名',
      dataIndex: 'table_name',
      key: 'table_name',
      width: 150,
      render: (text: string) => <code style={{ color: '#1890ff', fontSize: '12px' }}>{text}</code>,
    },
    {
      title: '行数',
      dataIndex: 'row_count',
      key: 'row_count',
      width: 100,
      render: (count: number) => count.toLocaleString(),
    },
    { title: '列数', dataIndex: 'column_count', key: 'column_count', width: 80 },
    {
      title: '列名',
      dataIndex: 'columns',
      key: 'columns',
      render: (columns: string[]) => (
        <Tooltip title={columns.join(', ')}>
          <span style={{ fontSize: '12px' }}>{columns.slice(0, 3).join(', ')}{columns.length > 3 ? '...' : ''}</span>
        </Tooltip>
      ),
    },
    {
      title: '操作',
      key: 'action',
      width: 140,
      render: (_: any, record: TableInfo) => (
        <Space size="small">
          <Button
            type="link"
            size="small"
            onClick={() => setSqlQuery(`SELECT * FROM ${record.table_name} LIMIT 100`)}
          >
            查询
          </Button>
          <Button
            type="link"
            size="small"
            danger
            onClick={() => handleTruncateTable(record.table_name)}
          >
            清空
          </Button>
        </Space>
      ),
    },
  ];

  const queryResultColumns = queryColumns.map((col) => ({
    title: col,
    dataIndex: col,
    key: col,
    ellipsis: true,
    render: (value: any) => {
      if (value === null || value === undefined) return <span style={{ color: '#999' }}>NULL</span>;
      if (typeof value === 'number') return value.toLocaleString();
      return String(value);
    },
  }));

  return (
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{
        marginBottom: '16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <div>
          <h1 style={{
            color: '#00d4ff',
            fontSize: '24px',
            fontWeight: 700,
            margin: 0,
            textShadow: '0 0 20px rgba(0, 212, 255, 0.5)',
            letterSpacing: '1px'
          }}>
            <DatabaseOutlined style={{ marginRight: '8px' }} />
            数据中心
          </h1>
          <p style={{
            color: '#94a3b8',
            margin: '4px 0 0 0',
            fontSize: '12px'
          }}>
            实时行情数据管理与分析
          </p>
        </div>
        <div style={{
          background: 'linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(124, 58, 237, 0.1))',
          padding: '8px 16px',
          borderRadius: '6px',
          border: '1px solid rgba(0, 212, 255, 0.3)'
        }}>
          <div style={{ color: '#94a3b8', fontSize: '11px' }}>数据表总数</div>
          <div style={{
            color: '#00d4ff',
            fontSize: '20px',
            fontWeight: 700,
            textShadow: '0 0 10px rgba(0, 212, 255, 0.5)'
          }}>
            {tables.length}
          </div>
        </div>
      </div>

      <Tabs defaultActiveKey="1" className="tech-tabs">
        {/* 原有的数据查看功能 */}
        <TabPane tab="📊 行情数据" key="1">
          <div className="tech-card" style={{ padding: '12px', marginBottom: '12px' }}>
            <Space style={{ marginBottom: 12 }} wrap>
              <Select
                showSearch
                style={{ width: 250 }}
                placeholder="🔍 选择股票代码"
                className="tech-input"
                options={stocks.map((s) => ({ label: s, value: s }))}
                onChange={(v) => {
                  setSelected(v);
                  loadDaily(v);
                }}
              />
            </Space>

            {dailyData.length > 0 && (
              <div style={{
                marginBottom: 12,
                padding: '12px',
                background: 'rgba(0, 0, 0, 0.3)',
                borderRadius: '8px',
                border: '1px solid rgba(0, 212, 255, 0.2)'
              }}>
                <TradingViewChart data={dailyData.slice().reverse()} />
              </div>
            )}

            <Table
              dataSource={dailyData}
              columns={dailyColumns}
              rowKey="trade_date"
              loading={loading}
              size="small"
              pagination={{ pageSize: 20 }}
              scroll={{ x: 800 }}
              className="tech-table"
            />
          </div>
        </TabPane>

        {/* 新增：同步任务管理 */}
        <TabPane tab={<span><SyncOutlined /> 同步任务</span>} key="2">
          <Card
            className="tech-card"
            style={{ marginBottom: '12px', border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>⚡ 同步任务管理</span>}
            extra={
              <Space size="small">
                <Button
                  icon={<ReloadOutlined />}
                  onClick={() => {
                    syncTasks.forEach((task) => loadTaskStatus(task.task_id));
                    message.success('任务状态已刷新');
                  }}
                  type="default"
                  size="small"
                >
                  刷新
                </Button>
                <Button
                  onClick={handleNewTask}
                  type="default"
                  size="small"
                >
                  新建任务
                </Button>
                <Button
                  type="primary"
                  icon={<SyncOutlined />}
                  onClick={handleSyncAll}
                  className="glow-button"
                  size="small"
                >
                  全部同步
                </Button>
              </Space>
            }
          >
            <Collapse
              accordion
              style={{ background: 'transparent', border: 'none' }}
            >
              {syncTasks.map((task) => {
                const status = taskStatuses[task.task_id];
                const taskScheduleInfo = scheduleInfo[task.task_id];
                const hasSchedule = taskScheduleInfo && taskScheduleInfo.next_run_time;
                const isSyncing = syncingTasks.has(task.task_id);

                return (
                  <Panel
                    key={task.task_id}
                    header={
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
                        <Space size="middle" style={{ flex: 1 }}>
                          <code style={{
                            color: '#00d4ff',
                            background: 'rgba(0, 212, 255, 0.1)',
                            padding: '4px 8px',
                            borderRadius: '4px',
                            fontWeight: 600,
                            fontSize: '13px',
                            minWidth: '120px',
                            display: 'inline-block'
                          }}>
                            {task.task_id}
                          </code>
                          <span style={{ color: '#94a3b8', fontSize: '13px', flex: 1 }}>{task.description}</span>
                        </Space>
                        <Space size="middle" onClick={(e) => e.stopPropagation()}>
                          <Tag
                            color={task.enabled ? 'green' : 'red'}
                            style={{
                              background: task.enabled ? 'rgba(16, 185, 129, 0.2)' : 'rgba(239, 68, 68, 0.2)',
                              border: `1px solid ${task.enabled ? '#10b981' : '#ef4444'}`,
                              color: task.enabled ? '#10b981' : '#ef4444',
                              fontWeight: 500,
                              fontSize: '11px'
                            }}
                          >
                            {task.enabled ? '启用' : '禁用'}
                          </Tag>
                          {status?.last_sync_time && (
                            <Tooltip title="上次同步时间（系统拉取数据的实际时间）">
                              <Space size={4} style={{ color: '#94a3b8', fontSize: '12px' }}>
                                <ClockCircleOutlined />
                                <span>{status.last_sync_time}</span>
                              </Space>
                            </Tooltip>
                          )}
                          {status?.table_latest_date && (
                            <Tooltip title="最新数据日期">
                              <Space size={4} style={{ color: '#10b981', fontSize: '12px' }}>
                                <CalendarOutlined />
                                <span>{status.table_latest_date}</span>
                              </Space>
                            </Tooltip>
                          )}
                        </Space>
                      </div>
                    }
                    style={{
                      marginBottom: '8px',
                      background: 'rgba(0, 0, 0, 0.2)',
                      border: '1px solid rgba(0, 212, 255, 0.2)',
                      borderRadius: '6px'
                    }}
                  >
                    <Space direction="vertical" style={{ width: '100%' }} size="middle">
                      <Row gutter={16}>
                        <Col span={4}>
                          <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '4px' }}>类型</div>
                          <Tag
                            color={task.sync_type === 'incremental' ? 'blue' : 'green'}
                            style={{
                              background: task.sync_type === 'incremental' ? 'rgba(0, 212, 255, 0.2)' : 'rgba(16, 185, 129, 0.2)',
                              border: `1px solid ${task.sync_type === 'incremental' ? '#00d4ff' : '#10b981'}`,
                              color: task.sync_type === 'incremental' ? '#00d4ff' : '#10b981',
                              fontWeight: 500
                            }}
                          >
                            {task.sync_type === 'incremental' ? '增量' : '全量'}
                          </Tag>
                        </Col>
                        <Col span={4}>
                          <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '4px' }}>调度</div>
                          {taskScheduleInfo?.schedule_type ? (
                            <Tag style={{
                              background: 'rgba(124, 58, 237, 0.2)',
                              border: '1px solid #7c3aed',
                              color: '#7c3aed',
                              fontWeight: 500
                            }}>
                              {taskScheduleInfo.schedule_type}
                            </Tag>
                          ) : (
                            <span style={{ color: '#666', fontSize: '12px' }}>-</span>
                          )}
                        </Col>
                        <Col span={4}>
                          <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '4px' }}>调度状态</div>
                          {hasSchedule && taskScheduleInfo.next_run_time ? (
                            <Tag color="green" style={{ fontWeight: 500 }}>
                              运行中
                            </Tag>
                          ) : (
                            <Tag color="default" style={{ fontWeight: 500 }}>
                              未启用
                            </Tag>
                          )}
                        </Col>
                        <Col span={5}>
                          <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '4px' }}>下次运行</div>
                          <span style={{ color: '#10b981', fontSize: '12px' }}>
                            {hasSchedule && taskScheduleInfo.next_run_time
                              ? new Date(taskScheduleInfo.next_run_time).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
                              : '-'}
                          </span>
                        </Col>
                        <Col span={5}>
                          <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '4px' }}>上次运行</div>
                          <span style={{ color: '#94a3b8', fontSize: '12px' }}>
                            {taskScheduleInfo?.last_run_time
                              ? new Date(taskScheduleInfo.last_run_time).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
                              : '-'}
                          </span>
                        </Col>
                        <Col span={2}>
                          <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '4px' }}>成功次数</div>
                          <span style={{ color: '#00d4ff', fontSize: '13px', fontWeight: 600 }}>
                            {taskScheduleInfo?.success_count || 0}
                          </span>
                        </Col>
                      </Row>

                      <div style={{ borderTop: '1px solid rgba(0, 212, 255, 0.1)', paddingTop: '12px', marginTop: '4px' }}>
                        <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '8px' }}>数据表</div>
                        <code style={{
                          color: '#10b981',
                          background: 'rgba(16, 185, 129, 0.1)',
                          padding: '4px 8px',
                          borderRadius: '4px',
                          fontSize: '12px'
                        }}>
                          {task.table_name}
                        </code>
                      </div>

                      <div style={{ borderTop: '1px solid rgba(0, 212, 255, 0.1)', paddingTop: '12px' }}>
                        <Space size="small" wrap>
                          <Button
                            type="primary"
                            size="small"
                            icon={<EditOutlined />}
                            onClick={() => handleEditTask(task.task_id)}
                          >
                            编辑
                          </Button>

                          {!hasSchedule ? (
                            <Button
                              size="small"
                              onClick={() => handleScheduleTask(task)}
                            >
                              启用调度
                            </Button>
                          ) : (
                            <Button
                              size="small"
                              danger
                              onClick={() => handleDisableSchedule(task.task_id)}
                            >
                              禁用调度
                            </Button>
                          )}

                          <Button
                            type="default"
                            size="small"
                            icon={<SyncOutlined spin={isSyncing} />}
                            onClick={() => handleSyncTask(task.task_id)}
                            disabled={!task.enabled || isSyncing}
                            loading={isSyncing}
                          >
                            同步
                          </Button>

                          <Button
                            danger
                            size="small"
                            icon={<DeleteOutlined />}
                            onClick={() => handleDeleteTask(task.task_id)}
                          >
                            删除
                          </Button>
                        </Space>
                      </div>
                    </Space>
                  </Panel>
                );
              })}
            </Collapse>
          </Card>

          <Card
            className="tech-card"
            style={{ marginTop: 12, border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>📋 同步日志</span>}
            extra={
              <Button
                icon={<ReloadOutlined />}
                onClick={loadSyncLogs}
                size="small"
                type="link"
              >
                刷新
              </Button>
            }
          >
            <Space style={{ marginBottom: 12 }} wrap size="small">
              <Select
                placeholder="按来源筛选"
                style={{ width: 150 }}
                allowClear
                size="small"
                onChange={(value) => setLogFilters({ ...logFilters, source: value })}
              >
                <Select.Option value="tushare_config">tushare_config</Select.Option>
              </Select>
              <Select
                placeholder="按类型筛选"
                style={{ width: 150 }}
                allowClear
                size="small"
                onChange={(value) => setLogFilters({ ...logFilters, dataType: value })}
              >
                {syncTasks.map(task => (
                  <Select.Option key={task.task_id} value={task.task_id}>{task.task_id}</Select.Option>
                ))}
              </Select>
              <Input
                placeholder="开始日期 (YYYYMMDD)"
                style={{ width: 150 }}
                size="small"
                onChange={(e) => setLogFilters({ ...logFilters, startDate: e.target.value })}
              />
              <Input
                placeholder="结束日期 (YYYYMMDD)"
                style={{ width: 150 }}
                size="small"
                onChange={(e) => setLogFilters({ ...logFilters, endDate: e.target.value })}
              />
              <Button type="primary" onClick={loadSyncLogs} size="small">筛选</Button>
            </Space>
            <Table
              dataSource={syncLogs}
              columns={[
                { title: 'ID', dataIndex: 'id', key: 'id', width: 80 },
                { title: '来源', dataIndex: 'source', key: 'source', width: 150 },
                { title: '类型', dataIndex: 'data_type', key: 'data_type', width: 150 },
                { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 120 },
                {
                  title: '同步行数',
                  dataIndex: 'rows_synced',
                  key: 'rows_synced',
                  width: 120,
                  render: (count: number) => count.toLocaleString()
                },
                {
                  title: '状态',
                  dataIndex: 'status',
                  key: 'status',
                  width: 100,
                  render: (status: string) => (
                    <Tag color={status === 'success' ? 'green' : 'red'}>{status}</Tag>
                  )
                },
                {
                  title: '创建时间',
                  dataIndex: 'created_at',
                  key: 'created_at',
                  width: 180,
                  render: (text: string) => new Date(text).toLocaleString()
                },
              ]}
              rowKey="id"
              size="small"
              pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (total) => `共 ${total} 条日志` }}
              className="tech-table"
            />
          </Card>
        </TabPane>

        {/* 新增：数据库表管理 */}
        <TabPane tab={<span><DatabaseOutlined /> 数据库表</span>} key="3">
          <Card
            className="tech-card"
            style={{ border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>🗄️ 数据库表</span>}
            extra={
              <Button
                icon={<ReloadOutlined />}
                onClick={loadInitialData}
                className="glow-button"
                size="small"
              >
                刷新
              </Button>
            }
          >
            <Table
              dataSource={tables}
              columns={tableColumns}
              rowKey="table_name"
              size="small"
              pagination={{ pageSize: 20 }}
              className="tech-table"
            />
          </Card>
        </TabPane>

        {/* 新增：SQL 查询 */}
        <TabPane tab={<span><CodeOutlined /> SQL 查询</span>} key="4">
          <Card
            className="tech-card"
            style={{ border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>💻 SQL 查询</span>}
          >
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
              <div>
                <div style={{
                  marginBottom: 8,
                  color: '#94a3b8',
                  fontSize: '13px',
                  fontWeight: 500
                }}>
                  SQL 查询编辑器（仅支持 SELECT）
                </div>
                <TextArea
                  value={sqlQuery}
                  onChange={(e) => setSqlQuery(e.target.value)}
                  placeholder="在此输入 SQL 查询语句..."
                  rows={6}
                  style={{
                    fontFamily: 'Fira Code, Courier New, monospace',
                    fontSize: '13px',
                    background: 'rgba(0, 0, 0, 0.4)',
                    border: '1px solid rgba(0, 212, 255, 0.3)',
                    color: '#10b981',
                    borderRadius: '6px'
                  }}
                />
                <div style={{
                  marginTop: 8,
                  padding: '8px',
                  background: 'rgba(255, 193, 7, 0.1)',
                  border: '1px solid rgba(255, 193, 7, 0.3)',
                  borderRadius: '6px',
                  color: '#ffc107',
                  fontSize: 12
                }}>
                  ⚠️ 安全提示：仅允许 SELECT 查询，每次最多返回 10,000 行数据。
                </div>
              </div>

              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleExecuteQuery}
                loading={queryLoading}
                className="glow-button"
              >
                执行查询
              </Button>

              {queryResult.length > 0 && (
                <div>
                  <div style={{
                    marginBottom: 8,
                    color: '#00d4ff',
                    fontSize: '14px',
                    fontWeight: 600
                  }}>
                    📊 查询结果 ({queryResult.length} 行)
                  </div>
                  <Table
                    dataSource={queryResult}
                    columns={queryResultColumns}
                    rowKey={(_, index) => (index ?? 0).toString()}
                    size="small"
                    pagination={{ pageSize: 50 }}
                    scroll={{ x: 'max-content', y: 500 }}
                    className="tech-table"
                  />
                </div>
              )}
            </Space>
          </Card>
        </TabPane>

        <TabPane tab={<span><ApartmentOutlined /> DAG 管理</span>} key="5">
          <DAGManageSection />
        </TabPane>
      </Tabs>
      <Modal
        title={`同步任务: ${syncModalTask?.task_id || ''}`}
        open={syncModalVisible}
        onOk={executeSyncTask}
        onCancel={() => setSyncModalVisible(false)}
        okText="开始同步"
        cancelText="取消"
      >
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Descriptions bordered size="small">
            <Descriptions.Item label="任务ID" span={3}>{syncModalTask?.task_id}</Descriptions.Item>
            <Descriptions.Item label="描述" span={3}>{syncModalTask?.description}</Descriptions.Item>
            <Descriptions.Item label="类型" span={3}>
              <Tag color={syncModalTask?.sync_type === 'incremental' ? 'blue' : 'green'}>
                {syncModalTask?.sync_type}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="数据表" span={3}>{syncModalTask?.table_name}</Descriptions.Item>
          </Descriptions>

          {syncModalTask?.sync_type === 'incremental' && (
            <>
              <div>
                <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>开始日期（可选）</div>
                <Input
                  placeholder="YYYYMMDD（如 20240101）"
                  value={syncStartDate}
                  onChange={(e) => setSyncStartDate(e.target.value)}
                  maxLength={8}
                  size="small"
                />
              </div>

              <div>
                <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>结束日期（可选）</div>
                <Input
                  placeholder="YYYYMMDD（如 20240131）"
                  value={syncEndDate}
                  onChange={(e) => setSyncEndDate(e.target.value)}
                  maxLength={8}
                  size="small"
                />
                <div style={{ marginTop: 6, color: '#999', fontSize: 11 }}>
                  💡 指定日期范围进行同步，留空则只同步最新一天的数据。
                </div>
              </div>
            </>
          )}

          {syncModalTask?.sync_type === 'full' && (
            <div style={{ padding: '12px', background: 'rgba(0, 212, 255, 0.1)', borderRadius: '6px', border: '1px solid rgba(0, 212, 255, 0.3)' }}>
              <div style={{ color: '#00d4ff', fontSize: '13px', fontWeight: 500 }}>
                💡 全量同步模式
              </div>
              <div style={{ marginTop: 6, color: '#94a3b8', fontSize: 12 }}>
                此任务将执行全量数据同步，无需指定日期参数。
              </div>
            </div>
          )}
        </Space>
      </Modal>

      {/* Config Editor Modal */}
      <Modal
        title={isNewTask ? '新建任务' : `编辑任务: ${configModalTask?.task_id || ''}`}
        open={configModalVisible}
        onOk={handleSaveConfig}
        onCancel={() => setConfigModalVisible(false)}
        okText="保存"
        cancelText="取消"
        width={800}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>任务配置 (JSON)</div>
            <TextArea
              value={configJson}
              onChange={(e) => setConfigJson(e.target.value)}
              rows={18}
              style={{
                fontFamily: 'Fira Code, Courier New, monospace',
                fontSize: '12px',
                background: 'rgba(0, 0, 0, 0.4)',
                border: '1px solid rgba(0, 212, 255, 0.3)',
                color: '#10b981',
                borderRadius: '6px'
              }}
            />
            <div style={{ marginTop: 6, color: '#999', fontSize: 11 }}>
              💡 编辑上方 JSON 配置。必填字段：task_id, api_name, sync_type, table_name, primary_keys
            </div>
          </div>
        </Space>
      </Modal>

      {/* Schedule Management Modal */}
      <Modal
        title={`调度设置: ${scheduleModalTask?.task_id || ''}`}
        open={scheduleModalVisible}
        onOk={handleEnableSchedule}
        onCancel={() => setScheduleModalVisible(false)}
        okText="启用调度"
        cancelText="取消"
        width={500}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <div>
            <div style={{ marginBottom: 8, fontWeight: 500, fontSize: '13px' }}>调度类型</div>
            <Select
              value={scheduleType}
              onChange={setScheduleType}
              style={{ width: '100%' }}
              options={[
                { label: '每日 (凌晨 2:00)', value: 'daily' },
                { label: '每周 (周一凌晨 3:00)', value: 'weekly' },
                { label: '每月 (1号凌晨 4:00)', value: 'monthly' },
                { label: '自定义 (Cron 表达式)', value: 'custom' },
              ]}
            />
          </div>
          {scheduleType === 'custom' && (
            <div>
              <div style={{ marginBottom: 8, fontWeight: 500, fontSize: '13px' }}>Cron 表达式</div>
              <Input
                value={cronExpression}
                onChange={(e) => setCronExpression(e.target.value)}
                placeholder="例如: 0 2 * * * (每天凌晨2点)"
                style={{
                  fontFamily: 'Fira Code, Courier New, monospace',
                  fontSize: '12px',
                }}
              />
              <div style={{ marginTop: 6, color: '#999', fontSize: 11 }}>
                💡 Cron 格式: 分 时 日 月 周 (例如: 0 2 * * * 表示每天凌晨2点)
              </div>
            </div>
          )}
          <div style={{ padding: '12px', background: 'rgba(0, 212, 255, 0.1)', borderRadius: '6px', fontSize: '12px' }}>
            <div style={{ color: '#00d4ff', fontWeight: 500, marginBottom: 4 }}>说明</div>
            <div style={{ color: '#94a3b8' }}>
              启用调度后，系统将按照设定的时间自动执行数据同步任务。
              <br />
              • 每日调度：适用于需要每天更新的数据（如日线行情）
              <br />
              • 每周调度：适用于更新频率较低的数据（如股票列表）
              <br />
              • 每月调度：适用于月度更新的数据
              <br />
              • 自定义调度：使用 Cron 表达式设置任意时间
            </div>
          </div>
        </Space>
      </Modal>
    </div>
  );
};

export default DataCenter;
