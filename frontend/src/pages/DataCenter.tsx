import React, { useEffect, useState, useCallback } from 'react';
import {
  Table,
  Button,
  Input,
  Select,
  Tag,
  Toast,
  Card,
  Tabs,
  TabPane,
  Modal,
  Tooltip,
  Collapse,
  Popconfirm,
  DatePicker,
  RadioGroup,
  Radio,
  Progress,
  TextArea,
} from '@douyinfe/semi-ui';
import {
  IconSync,
  IconServer,
  IconCode,
  IconPlay,
  IconRefresh,
  IconEdit,
  IconDelete,
  IconClock,
  IconCalendar,
  IconFlowChartStroked,
  IconHistory,
  IconPlus,
} from '@douyinfe/semi-icons';
import ReactECharts from 'echarts-for-react';
import dayjs from 'dayjs';
import { dataApi, productionApi } from '../api';
import TradingViewChart from '../components/Charts/TradingViewChart';

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
  const [runDate, setRunDate] = useState<Date | null>(null);
  const [runRange, setRunRange] = useState<[Date | null, Date | null]>([null, null]);
  const [backfillProgress, setBackfillProgress] = useState<{ visible: boolean; data: any } | null>(null);

  // DAG form state (managed via useState)
  const [createForm, setCreateForm] = useState({ dag_id: '', description: '', schedule: '', tasks_json: '[]' });
  const [editFormData, setEditFormData] = useState({ description: '', schedule: '', tasks_json: '' });

  const loadDags = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.listDags();
      setDags(res.data?.dags || res.data?.data || []);
    } catch { Toast.error('加载 DAG 列表失败'); }
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
      if (!runDate) { Toast.warning('请选择执行日期'); return; }
      params.target_date = dayjs(runDate).format('YYYY-MM-DD');
      params.run_type = 'single';
    } else if (runMode === 'range') {
      if (!runRange[0] || !runRange[1]) { Toast.warning('请选择日期范围'); return; }
      params.start_date = dayjs(runRange[0]).format('YYYY-MM-DD');
      params.end_date = dayjs(runRange[1]).format('YYYY-MM-DD');
      params.run_type = 'backfill';
    } else {
      params.run_type = 'today';
    }

    setRunModal({ visible: false, dagId: '' });
    setRunLoading(dagId);
    try {
      const res = await productionApi.runDag(dagId, Object.keys(params).length ? params : undefined);
      const data = res.data?.data || res.data;

      if (data?.mode === 'backfill') {
        setBackfillProgress({ visible: true, data });
        loadDags();
        setRunLoading(null);
        return;
      }

      const dagStatus = data?.status || 'unknown';
      const summary = data?.summary || '';
      const failedTasks: any[] = data?.failed_tasks || [];

      if (dagStatus === 'success') {
        Toast.success(`DAG ${dagId} 执行成功${summary ? ` (${summary})` : ''}`);
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
                    <div key={t.task_id} style={{ marginBottom: 4, padding: '4px 8px', background: 'var(--bg-surface)', borderRadius: 4, fontSize: 12 }}>
                      <Tag color="red">{t.task_id}</Tag>
                      <span style={{ color: 'var(--color-loss)' }}>{t.error_message || '无详细信息'}</span>
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
      Toast.error(e.response?.data?.detail || 'DAG 执行失败');
    }
    setRunLoading(null);
  };

  const showHistory = async (dagId: string, runType?: string) => {
    try {
      const res = await productionApi.getDagHistory(dagId, 20, runType || undefined);
      setHistoryModal({ visible: true, dagId, data: res.data?.runs || res.data?.data || [], filter: runType || '' });
      setBackfillDetail({});
    } catch { Toast.error('加载历史失败'); }
  };

  const loadBackfillDetail = async (backfillId: string) => {
    if (backfillDetail[backfillId]) return;
    try {
      const res = await productionApi.getBackfillDetail(backfillId);
      const runs = res.data?.data?.runs || res.data?.runs || [];
      setBackfillDetail(prev => ({ ...prev, [backfillId]: runs }));
    } catch { Toast.error('加载回溯详情失败'); }
  };

  const handleCreate = async () => {
    try {
      if (!createForm.dag_id) { Toast.warning('请输入 DAG ID'); return; }
      const tasks = createForm.tasks_json ? JSON.parse(createForm.tasks_json) : [];
      await productionApi.createDag({ dag_id: createForm.dag_id, description: createForm.description, schedule: createForm.schedule, tasks });
      Toast.success(`DAG ${createForm.dag_id} 创建成功`);
      setCreateModal(false);
      setCreateForm({ dag_id: '', description: '', schedule: '', tasks_json: '[]' });
      loadDags();
    } catch (e: any) {
      if (e.response) Toast.error(e.response?.data?.detail || '创建失败');
      else if (e instanceof SyntaxError) Toast.error('任务 JSON 格式错误');
    }
  };

  const handleEdit = async () => {
    try {
      const tasks = editFormData.tasks_json ? JSON.parse(editFormData.tasks_json) : undefined;
      await productionApi.updateDag(editModal.dag.dag_id, { description: editFormData.description, schedule: editFormData.schedule, tasks });
      Toast.success('DAG 更新成功');
      setEditModal({ visible: false, dag: null });
      loadDags();
    } catch (e: any) {
      if (e.response) Toast.error(e.response?.data?.detail || '更新失败');
      else if (e instanceof SyntaxError) Toast.error('任务 JSON 格式错误');
    }
  };

  const handleDelete = async (dagId: string) => {
    try {
      await productionApi.deleteDag(dagId);
      Toast.success(`DAG ${dagId} 已删除`);
      loadDags();
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '删除失败');
    }
  };

  const openEdit = (record: any) => {
    setEditFormData({
      description: record.description || '',
      schedule: record.schedule || '',
      tasks_json: JSON.stringify(record.tasks || [], null, 2),
    });
    setEditModal({ visible: true, dag: record });
  };

  const dagColumns = [
    { title: 'DAG ID', dataIndex: 'dag_id', key: 'dag_id', render: (text: string) => <Tag color="purple">{text}</Tag> },
    { title: '描述', dataIndex: 'description', key: 'desc' },
    { title: '调度', dataIndex: 'schedule', key: 'schedule', render: (text: string) => text ? <Tag color="cyan">{text}</Tag> : <Tag>手动</Tag> },
    { title: '任务数', dataIndex: 'tasks', key: 'tasks', render: (text: any[]) => text?.length || 0 },
    { title: '最近成功', dataIndex: 'last_success', key: 'last_success', render: (text: string) => text ? <span style={{ color: 'var(--color-gain)', fontSize: 12 }}>{text.slice(0, 19)}</span> : <span style={{ color: 'var(--text-muted)' }}>-</span> },
    {
      title: '操作', key: 'action', width: 280, render: (text: any, record: any) => (
        <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
          <Button size="small" theme="solid" type="primary" icon={<IconPlay />}
            loading={runLoading === record.dag_id}
            onClick={() => openRunModal(record.dag_id)}>执行</Button>
          <Button size="small" icon={<IconHistory />}
            onClick={() => showHistory(record.dag_id)}>历史</Button>
          <Button size="small" icon={<IconEdit />} onClick={() => openEdit(record)} />
          <Popconfirm title="确认删除此 DAG?" onConfirm={() => handleDelete(record.dag_id)}>
            <Button size="small" type="danger" icon={<IconDelete />} />
          </Popconfirm>
        </div>
      )
    },
  ];

  const runTypeLabel: Record<string, { text: string; color: string }> = {
    today: { text: '今日', color: 'blue' },
    single: { text: '指定日期', color: 'cyan' },
    backfill: { text: '回溯', color: 'purple' },
  };

  const historyColumns = [
    { title: '运行ID', dataIndex: 'run_id', key: 'run_id', width: 100, render: (text: string) => <Tooltip content={text}><span>{text?.slice(0, 8)}...</span></Tooltip> },
    { title: '类型', dataIndex: 'run_type', key: 'run_type', width: 80, render: (text: string) => {
      const info = runTypeLabel[text] || { text: text || '-', color: 'grey' };
      return <Tag color={info.color as any}>{info.text}</Tag>;
    }},
    { title: '目标日期', dataIndex: 'target_date', key: 'target_date', width: 110, render: (text: string) => text || '-' },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80, render: (text: string) => <Tag color={text === 'success' ? 'green' : text === 'running' ? 'blue' : 'red'}>{text}</Tag> },
    { title: '开始时间', dataIndex: 'started_at', key: 'start', width: 160, render: (text: string) => text?.slice(0, 19) },
    { title: '结束时间', dataIndex: 'finished_at', key: 'end', width: 160, render: (text: string) => text?.slice(0, 19) || '-' },
    { title: '任务详情', dataIndex: 'task_results', key: 'tasks', render: (text: any) => {
      if (!text) return '-';
      const tasks = typeof text === 'string' ? JSON.parse(text) : text;
      if (Array.isArray(tasks)) {
        return <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>{tasks.map((t: any) => (
          <Tooltip key={t.task_id} content={t.error_message || (t.status === 'success' ? '成功' : t.status)}>
            <Tag color={t.status === 'success' ? 'green' : t.status === 'skipped' ? 'orange' : 'red'}>{t.task_id}</Tag>
          </Tooltip>
        ))}</div>;
      }
      return <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>{Object.entries(tasks).map(([k, s]: any) => <Tag key={k} color={s === 'success' ? 'green' : 'red'}>{k}</Tag>)}</div>;
    }},
  ];

  const backfillSubColumns = [
    { title: '目标日期', dataIndex: 'target_date', key: 'date', width: 120 },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80, render: (text: string) => <Tag color={text === 'success' ? 'green' : text === 'running' ? 'blue' : 'red'}>{text}</Tag> },
    { title: '开始', dataIndex: 'started_at', key: 'start', width: 160, render: (text: string) => text?.slice(0, 19) },
    { title: '结束', dataIndex: 'finished_at', key: 'end', width: 160, render: (text: string) => text?.slice(0, 19) || '-' },
    { title: '任务', dataIndex: 'task_results', key: 'tasks', render: (text: any) => {
      if (!text) return '-';
      const tasks = typeof text === 'string' ? JSON.parse(text) : text;
      if (Array.isArray(tasks)) {
        return <div style={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>{tasks.map((t: any) => (
          <Tooltip key={t.task_id} content={t.error_message || t.status}>
            <Tag color={t.status === 'success' ? 'green' : t.status === 'skipped' ? 'orange' : 'red'} style={{ fontSize: 11 }}>{t.task_id}</Tag>
          </Tooltip>
        ))}</div>;
      }
      return '-';
    }},
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }} className="content-card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>DAG 列表</span>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <Button size="small" icon={<IconPlus />} onClick={() => setCreateModal(true)}>新建 DAG</Button>
            <Button icon={<IconRefresh />} onClick={loadDags} size="small">刷新</Button>
          </div>
        </div>
        <Table dataSource={dags} columns={dagColumns} rowKey="dag_id"
          loading={loading} size="small" pagination={false}
          expandedRowRender={(record) => {
            const tasks: any[] = record?.tasks || [];
            if (!tasks.length) return <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>暂无任务</span>;

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
                label: { show: true, fontSize: 11, color: '#94A3B8', formatter: (p: any) => p.name.length > 10 ? p.name.slice(0, 10) + '..' : p.name },
                itemStyle: {
                  color: (t.depends_on?.length ? '#0077FA' : '#14C9C9'),
                  borderColor: 'var(--border-color)', borderWidth: 2,
                },
                tooltip: { formatter: `${t.task_id}<br/>Action: ${t.action || '-'}<br/>依赖: ${t.depends_on?.join(', ') || '无'}` },
              };
            });

            const edges = tasks.flatMap(t =>
              (t.depends_on || []).map((dep: string) => ({
                source: dep, target: t.task_id,
                lineStyle: { color: '#64748B', width: 2, curveness: 0.1 },
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
                <span style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4, display: 'block' }}>
                  任务依赖图 ({tasks.length} 个任务, {maxLayer + 1} 层)
                </span>
                <ReactECharts option={option} style={{ height: chartH, width: Math.max(300, (maxLayer + 1) * xGap + 80) }} />
              </div>
            );
          }}
        />
      </Card>

      {/* 运行历史 Modal */}
      <Modal title={`DAG 运行历史 - ${historyModal.dagId}`} visible={historyModal.visible}
        onCancel={() => setHistoryModal({ visible: false, dagId: '', data: [], filter: '' })}
        footer={null} width={960}>
        <div style={{ marginBottom: 12 }}>
          <RadioGroup type="button" value={historyModal.filter} onChange={e => showHistory(historyModal.dagId, (e.target as any).value)} style={{ fontSize: 13 }}>
            <Radio value="">全部</Radio>
            <Radio value="today">今日执行</Radio>
            <Radio value="single">指定日期</Radio>
            <Radio value="backfill">回溯</Radio>
          </RadioGroup>
        </div>
        <Table dataSource={historyModal.data} columns={historyColumns}
          rowKey="run_id" size="small" pagination={{ pageSize: 10 }}
          expandedRowRender={(record: any) => {
            if (!(record?.run_type === 'backfill' && record?.backfill_id)) return null;
            const runs = backfillDetail[record.backfill_id];
            if (!runs) return <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>加载中...</span>;
            return (
              <div style={{ padding: '4px 0' }}>
                <span style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4, display: 'block' }}>
                  回溯详情 ({runs.length} 天)
                </span>
                <Table dataSource={runs} columns={backfillSubColumns}
                  rowKey="run_id" size="small" pagination={false} />
              </div>
            );
          }}
          rowExpandable={(record: any) => record?.run_type === 'backfill' && !!record?.backfill_id}
          onExpand={(expanded, record) => {
            if (expanded && record?.backfill_id) loadBackfillDetail(record.backfill_id);
          }}
        />
      </Modal>

      {/* 新建 DAG Modal */}
      <Modal title="新建 DAG" visible={createModal} onOk={handleCreate}
        onCancel={() => { setCreateModal(false); setCreateForm({ dag_id: '', description: '', schedule: '', tasks_json: '[]' }); }} okText="创建" width={600}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>DAG ID <span style={{ color: 'var(--color-loss)' }}>*</span></div>
            <Input placeholder="如 daily_update" value={createForm.dag_id}
              onChange={(v) => setCreateForm(prev => ({ ...prev, dag_id: v }))} />
          </div>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>描述</div>
            <Input placeholder="DAG 描述" value={createForm.description}
              onChange={(v) => setCreateForm(prev => ({ ...prev, description: v }))} />
          </div>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>调度 (Cron)</div>
            <Input placeholder="如 0 2 * * * (留空为手动)" value={createForm.schedule}
              onChange={(v) => setCreateForm(prev => ({ ...prev, schedule: v }))} />
          </div>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>任务列表 (JSON)</div>
            <TextArea rows={6} placeholder='[{"task_id": "sync", "action": "sync_all", "depends_on": []}]'
              value={createForm.tasks_json}
              onChange={(v) => setCreateForm(prev => ({ ...prev, tasks_json: v }))}
              style={{ fontFamily: 'Fira Code, Courier New, monospace', fontSize: 12 }} />
          </div>
        </div>
      </Modal>

      {/* 编辑 DAG Modal */}
      <Modal title={`编辑 DAG: ${editModal.dag?.dag_id || ''}`} visible={editModal.visible}
        onOk={handleEdit} onCancel={() => setEditModal({ visible: false, dag: null })} okText="保存" width={600}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>描述</div>
            <Input value={editFormData.description}
              onChange={(v) => setEditFormData(prev => ({ ...prev, description: v }))} />
          </div>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>调度 (Cron)</div>
            <Input placeholder="留空为手动" value={editFormData.schedule}
              onChange={(v) => setEditFormData(prev => ({ ...prev, schedule: v }))} />
          </div>
          <div>
            <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 13 }}>任务列表 (JSON)</div>
            <TextArea rows={8} value={editFormData.tasks_json}
              onChange={(v) => setEditFormData(prev => ({ ...prev, tasks_json: v }))}
              style={{ fontFamily: 'Fira Code, Courier New, monospace', fontSize: 12 }} />
          </div>
        </div>
      </Modal>

      {/* DAG 执行模态框 */}
      <Modal title={`执行 DAG: ${runModal.dagId}`} visible={runModal.visible}
        onOk={handleRunDag} onCancel={() => setRunModal({ visible: false, dagId: '' })}
        okText="开始执行" width={480}>
        <div style={{ marginBottom: 16 }}>
          <RadioGroup type="button" value={runMode} onChange={e => setRunMode((e.target as any).value)}>
            <Radio value="today">今天</Radio>
            <Radio value="date">指定日期</Radio>
            <Radio value="range">日期范围回溯</Radio>
          </RadioGroup>
        </div>
        {runMode === 'date' && (
          <DatePicker style={{ width: '100%' }} value={runDate as any}
            onChange={(v) => setRunDate(v as Date)} placeholder="选择执行日期" />
        )}
        {runMode === 'range' && (
          <DatePicker type="dateRange" style={{ width: '100%' }}
            value={runRange as any}
            onChange={(v) => {
              if (Array.isArray(v) && v.length === 2) setRunRange([v[0] as Date, v[1] as Date]);
              else setRunRange([null, null]);
            }}
            placeholder={['开始日期', '结束日期'] as any} />
        )}
        {runMode === 'range' && runRange[0] && runRange[1] && (
          <div style={{ marginTop: 8, color: 'var(--text-secondary)', fontSize: 12 }}>
            将按交易日逐日执行，共约 {Math.ceil(dayjs(runRange[1]).diff(dayjs(runRange[0]), 'day') * 5 / 7)} 个交易日
          </div>
        )}
      </Modal>

      {/* 回溯结果模态框 */}
      <Modal title="回溯执行结果" visible={!!backfillProgress?.visible}
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
                <span style={{ marginLeft: 8, color: 'var(--text-secondary)' }}>共 {total} 天</span>
              </div>
              <Progress percent={pct} stroke={failed > 0 ? 'var(--color-loss)' : 'var(--color-gain)'}
                format={(pct) => `${success}/${total} 成功`} />
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
                        render: (text: string) => <Tag color={text === 'success' ? 'green' : 'red'}>{text}</Tag> },
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
  const [stocks, setStocks] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>('');
  const [dailyData, setDailyData] = useState<any[]>([]);
  const [syncLogs, setSyncLogs] = useState<SyncLog[]>([]);
  const [loading, setLoading] = useState(false);

  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM daily_basic LIMIT 10');
  const [queryResult, setQueryResult] = useState<any[]>([]);
  const [queryColumns, setQueryColumns] = useState<string[]>([]);
  const [queryLoading, setQueryLoading] = useState(false);
  const [syncingTasks, setSyncingTasks] = useState<Set<string>>(new Set());

  const [logFilters, setLogFilters] = useState({
    source: undefined as string | undefined,
    dataType: undefined as string | undefined,
    startDate: undefined as string | undefined,
    endDate: undefined as string | undefined,
  });

  const [syncModalVisible, setSyncModalVisible] = useState(false);
  const [syncModalTask, setSyncModalTask] = useState<SyncTask | null>(null);
  const [syncTargetDate, setSyncTargetDate] = useState<string>('');
  const [syncStartDate, setSyncStartDate] = useState<string>('');
  const [syncEndDate, setSyncEndDate] = useState<string>('');

  const [configModalVisible, setConfigModalVisible] = useState(false);
  const [configModalTask, setConfigModalTask] = useState<any>(null);
  const [configJson, setConfigJson] = useState<string>('');
  const [isNewTask, setIsNewTask] = useState(false);

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

      const tasks = tasksRes.data.tasks || [];
      for (const task of tasks) {
        loadTaskStatus(task.task_id);
        loadTaskScheduleInfo(task.task_id);
      }

      loadSyncLogs();
    } catch (error) {
      Toast.error('加载数据失败');
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
      await dataApi.syncTask(
        taskId,
        syncTargetDate || undefined,
        syncStartDate || undefined,
        syncEndDate || undefined
      );
      Toast.success(`任务 ${taskId} 同步已启动`);
      setTimeout(() => {
        loadTaskStatus(taskId);
        loadSyncLogs();
      }, 2000);
    } catch (error) {
      Toast.error(`任务 ${taskId} 同步失败`);
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
      await dataApi.syncAllTasks();
      Toast.success('所有任务已在后台开始同步');
      setTimeout(() => {
        syncTasks.forEach((task) => loadTaskStatus(task.task_id));
        loadSyncLogs();
      }, 3000);
    } catch (error) {
      Toast.error('启动同步失败');
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
      Toast.error('加载任务配置失败');
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
        Toast.success(`任务 ${config.task_id} 创建成功`);
      } else {
        await dataApi.updateTaskConfig(config.task_id, config);
        Toast.success(`任务 ${config.task_id} 更新成功`);
      }
      setConfigModalVisible(false);
      loadInitialData();
    } catch (error: any) {
      if (error instanceof SyntaxError) {
        Toast.error('JSON 格式无效');
      } else {
        Toast.error(error.response?.data?.detail || '保存配置失败');
      }
    }
  };

  const handleExecuteQuery = async () => {
    if (!sqlQuery.trim()) {
      Toast.warning('请输入 SQL 查询语句');
      return;
    }
    setQueryLoading(true);
    try {
      const res = await dataApi.executeQuery(sqlQuery);
      setQueryResult(res.data.data || []);
      setQueryColumns(res.data.columns || []);
      Toast.success(`查询返回 ${res.data.count} 行数据`);
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '查询失败');
    } finally {
      setQueryLoading(false);
    }
  };

  const handleTruncateTable = async (tableName: string) => {
    Modal.confirm({
      title: '清空表数据',
      content: `确定要清空表 "${tableName}" 的所有数据吗？此操作不可撤销。`,
      okText: '确定清空',
      cancelText: '取消',
      onOk: async () => {
        try {
          await dataApi.truncateTable(tableName);
          Toast.success(`表 ${tableName} 已清空`);
          loadInitialData();
        } catch (error: any) {
          Toast.error(error.response?.data?.detail || '清空表失败');
        }
      },
    });
  };

  const handleDeleteTask = async (taskId: string) => {
    Modal.confirm({
      title: '删除同步任务',
      content: `确定要删除任务 "${taskId}" 吗？这将删除任务配置，但不会删除数据表。`,
      okText: '确定删除',
      cancelText: '取消',
      onOk: async () => {
        try {
          await dataApi.deleteTask(taskId);
          Toast.success(`任务 ${taskId} 已删除`);
          loadInitialData();
        } catch (error: any) {
          Toast.error(error.response?.data?.detail || '删除任务失败');
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
      Toast.success(`任务 ${scheduleModalTask.task_id} 调度已启用`);
      setScheduleModalVisible(false);
      loadTaskScheduleInfo(scheduleModalTask.task_id);
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '启用调度失败');
    }
  };

  const handleDisableSchedule = async (taskId: string) => {
    try {
      await dataApi.disableTaskSchedule(taskId);
      Toast.success(`任务 ${taskId} 调度已禁用`);
      loadTaskScheduleInfo(taskId);
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '禁用调度失败');
    }
  };

  const dailyColumns = [
    { title: '日期', dataIndex: 'trade_date', key: 'trade_date', width: 100 },
    {
      title: '开盘', dataIndex: 'open', key: 'open',
      render: (text: number) => <span style={{ color: 'var(--text-secondary)' }}>{text?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '最高', dataIndex: 'high', key: 'high',
      render: (text: number) => <span style={{ color: 'var(--color-gain)' }}>{text?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '最低', dataIndex: 'low', key: 'low',
      render: (text: number) => <span style={{ color: 'var(--color-loss)' }}>{text?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '收盘', dataIndex: 'close', key: 'close',
      render: (text: number) => <span style={{ color: 'var(--color-primary)', fontWeight: 600 }}>{text?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: '成交量', dataIndex: 'vol', key: 'vol',
      render: (text: number) => <span style={{ color: 'var(--text-secondary)' }}>{text?.toLocaleString()}</span>,
      width: 100
    },
    {
      title: '涨跌幅', dataIndex: 'pct_chg', key: 'pct_chg',
      render: (text: number) => (
        <Tag color={text >= 0 ? 'red' : 'green'} style={{ fontWeight: 600 }}>
          {text >= 0 ? '+' : ''}{text?.toFixed(2)}%
        </Tag>
      ),
      width: 80,
    },
  ];

  const tableColumns = [
    {
      title: '表名', dataIndex: 'table_name', key: 'table_name', width: 150,
      render: (text: string) => <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{text}</code>,
    },
    {
      title: '行数', dataIndex: 'row_count', key: 'row_count', width: 100,
      render: (text: number) => text.toLocaleString(),
    },
    { title: '列数', dataIndex: 'column_count', key: 'column_count', width: 80 },
    {
      title: '列名', dataIndex: 'columns', key: 'columns',
      render: (text: string[]) => (
        <Tooltip content={text.join(', ')}>
          <span style={{ fontSize: '12px' }}>{text.slice(0, 3).join(', ')}{text.length > 3 ? '...' : ''}</span>
        </Tooltip>
      ),
    },
    {
      title: '操作', key: 'action', width: 140,
      render: (text: any, record: TableInfo) => (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <Button type="tertiary" size="small"
            onClick={() => setSqlQuery(`SELECT * FROM ${record.table_name} LIMIT 100`)}>
            查询
          </Button>
          <Button type="danger" size="small" theme="borderless"
            onClick={() => handleTruncateTable(record.table_name)}>
            清空
          </Button>
        </div>
      ),
    },
  ];

  const queryResultColumns = queryColumns.map((col) => ({
    title: col,
    dataIndex: col,
    key: col,
    ellipsis: true,
    render: (text: any) => {
      if (text === null || text === undefined) return <span style={{ color: '#999' }}>NULL</span>;
      if (typeof text === 'number') return text.toLocaleString();
      return String(text);
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
            color: 'var(--text-primary)',
            fontSize: '24px',
            fontWeight: 700,
            margin: 0,
            letterSpacing: '1px'
          }}>
            <IconServer style={{ marginRight: '8px' }} />
            数据中心
          </h1>
          <p style={{
            color: 'var(--text-secondary)',
            margin: '4px 0 0 0',
            fontSize: '12px'
          }}>
            实时行情数据管理与分析
          </p>
        </div>
        <div style={{
          background: 'var(--bg-card)',
          padding: '8px 16px',
          borderRadius: '6px',
          border: '1px solid var(--border-color)'
        }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: '11px' }}>数据表总数</div>
          <div style={{
            color: 'var(--color-primary)',
            fontSize: '20px',
            fontWeight: 700,
          }}>
            {tables.length}
          </div>
        </div>
      </div>

      <Tabs defaultActiveKey="1">
        <TabPane tab="行情数据" itemKey="1">
          <div className="content-card" style={{ padding: '12px', marginBottom: '12px' }}>
            <div style={{ marginBottom: 12 }}>
              <Select
                filter
                style={{ width: 250 }}
                placeholder="选择股票代码"
                optionList={stocks.map((s) => ({ label: s, value: s }))}
                onChange={(v) => {
                  setSelected(v as string);
                  loadDaily(v as string);
                }}
              />
            </div>

            {dailyData.length > 0 && (
              <div style={{
                marginBottom: 12,
                padding: '12px',
                background: 'var(--bg-surface)',
                borderRadius: '8px',
                border: '1px solid var(--border-color)'
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
            />
          </div>
        </TabPane>

        <TabPane tab={<span><IconSync /> 同步任务</span>} itemKey="2">
          <Card
            className="content-card"
            style={{ marginBottom: '12px' }}
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>同步任务管理</span>}
            headerExtraContent={
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <Button
                  icon={<IconRefresh />}
                  onClick={() => {
                    syncTasks.forEach((task) => loadTaskStatus(task.task_id));
                    Toast.success('任务状态已刷新');
                  }}
                  size="small"
                >
                  刷新
                </Button>
                <Button onClick={handleNewTask} size="small">
                  新建任务
                </Button>
                <Button
                  theme="solid"
                  type="primary"
                  icon={<IconSync />}
                  onClick={handleSyncAll}
                  size="small"
                >
                  全部同步
                </Button>
              </div>
            }
          >

            <Collapse accordion>
              {syncTasks.map((task) => {
                const status = taskStatuses[task.task_id];
                const taskScheduleInfo = scheduleInfo[task.task_id];
                const hasSchedule = taskScheduleInfo && taskScheduleInfo.next_run_time;
                const isSyncing = syncingTasks.has(task.task_id);

                return (
                  <Collapse.Panel
                    key={task.task_id}
                    itemKey={task.task_id}
                    header={
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
                        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flex: 1 }}>
                          <code style={{
                            color: 'var(--color-primary)',
                            background: 'var(--bg-surface)',
                            padding: '4px 8px',
                            borderRadius: '4px',
                            fontWeight: 600,
                            fontSize: '13px',
                            minWidth: '120px',
                            display: 'inline-block'
                          }}>
                            {task.task_id}
                          </code>
                          <span style={{ color: 'var(--text-secondary)', fontSize: '13px', flex: 1 }}>{task.description}</span>
                        </div>
                        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }} onClick={(e) => e.stopPropagation()}>
                          <Tag color={task.enabled ? 'green' : 'red'} style={{ fontWeight: 500, fontSize: '11px' }}>
                            {task.enabled ? '启用' : '禁用'}
                          </Tag>
                          {status?.last_sync_time && (
                            <Tooltip content="上次同步时间（系统拉取数据的实际时间）">
                              <div style={{ display: 'flex', gap: 4, alignItems: 'center', color: 'var(--text-secondary)', fontSize: '12px' }}>
                                <IconClock />
                                <span>{status.last_sync_time}</span>
                              </div>
                            </Tooltip>
                          )}
                          {status?.table_latest_date && (
                            <Tooltip content="最新数据日期">
                              <div style={{ display: 'flex', gap: 4, alignItems: 'center', color: 'var(--color-gain)', fontSize: '12px' }}>
                                <IconCalendar />
                                <span>{status.table_latest_date}</span>
                              </div>
                            </Tooltip>
                          )}
                        </div>
                      </div>
                    }
                  >

                    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
                        <div style={{ flex: '0 0 auto' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '4px' }}>类型</div>
                          <Tag color={task.sync_type === 'incremental' ? 'blue' : 'green'}>
                            {task.sync_type === 'incremental' ? '增量' : '全量'}
                          </Tag>
                        </div>
                        <div style={{ flex: '0 0 auto' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '4px' }}>调度</div>
                          {taskScheduleInfo?.schedule_type ? (
                            <Tag color="purple">{taskScheduleInfo.schedule_type}</Tag>
                          ) : (
                            <span style={{ color: 'var(--text-muted)', fontSize: '12px' }}>-</span>
                          )}
                        </div>
                        <div style={{ flex: '0 0 auto' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '4px' }}>调度状态</div>
                          {hasSchedule && taskScheduleInfo.next_run_time ? (
                            <Tag color="green" style={{ fontWeight: 500 }}>运行中</Tag>
                          ) : (
                            <Tag color="grey" style={{ fontWeight: 500 }}>未启用</Tag>
                          )}
                        </div>
                        <div style={{ flex: '0 0 auto' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '4px' }}>下次运行</div>
                          <span style={{ color: 'var(--color-gain)', fontSize: '12px' }}>
                            {hasSchedule && taskScheduleInfo.next_run_time
                              ? new Date(taskScheduleInfo.next_run_time).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
                              : '-'}
                          </span>
                        </div>
                        <div style={{ flex: '0 0 auto' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '4px' }}>上次运行</div>
                          <span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>
                            {taskScheduleInfo?.last_run_time
                              ? new Date(taskScheduleInfo.last_run_time).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })
                              : '-'}
                          </span>
                        </div>
                        <div style={{ flex: '0 0 auto' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '4px' }}>成功次数</div>
                          <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 600 }}>
                            {taskScheduleInfo?.success_count || 0}
                          </span>
                        </div>
                      </div>

                      <div style={{ borderTop: '1px solid var(--border-color)', paddingTop: '12px', marginTop: '4px' }}>
                        <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '8px' }}>数据表</div>
                        <code style={{
                          color: 'var(--color-gain)',
                          background: 'var(--bg-surface)',
                          padding: '4px 8px',
                          borderRadius: '4px',
                          fontSize: '12px'
                        }}>
                          {task.table_name}
                        </code>
                      </div>

                      <div style={{ borderTop: '1px solid var(--border-color)', paddingTop: '12px' }}>
                        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                          <Button
                            theme="solid"
                            type="primary"
                            size="small"
                            icon={<IconEdit />}
                            onClick={() => handleEditTask(task.task_id)}
                          >
                            编辑
                          </Button>

                          {!hasSchedule ? (
                            <Button size="small" onClick={() => handleScheduleTask(task)}>
                              启用调度
                            </Button>
                          ) : (
                            <Button size="small" type="danger" onClick={() => handleDisableSchedule(task.task_id)}>
                              禁用调度
                            </Button>
                          )}

                          <Button
                            size="small"
                            icon={<IconSync style={isSyncing ? { animation: 'spin 1s linear infinite' } : undefined} />}
                            onClick={() => handleSyncTask(task.task_id)}
                            disabled={!task.enabled || isSyncing}
                            loading={isSyncing}
                          >
                            同步
                          </Button>

                          <Button
                            type="danger"
                            size="small"
                            icon={<IconDelete />}
                            onClick={() => handleDeleteTask(task.task_id)}
                          >
                            删除
                          </Button>
                        </div>
                      </div>
                    </div>
                  </Collapse.Panel>
                );
              })}
            </Collapse>
          </Card>

          <Card
            className="content-card"
            style={{ marginTop: 12 }}
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>同步日志</span>}
            headerExtraContent={
              <Button icon={<IconRefresh />} onClick={loadSyncLogs} size="small" theme="borderless">
                刷新
              </Button>
            }
          >
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12, alignItems: 'center' }}>
              <Select
                placeholder="按来源筛选"
                style={{ width: 150 }}
                showClear
                size="small"
                optionList={[{ label: 'tushare_config', value: 'tushare_config' }]}
                onChange={(value) => setLogFilters({ ...logFilters, source: value as string | undefined })}
              />
              <Select
                placeholder="按类型筛选"
                style={{ width: 150 }}
                showClear
                size="small"
                optionList={syncTasks.map(task => ({ label: task.task_id, value: task.task_id }))}
                onChange={(value) => setLogFilters({ ...logFilters, dataType: value as string | undefined })}
              />
              <Input
                placeholder="开始日期 (YYYYMMDD)"
                style={{ width: 150 }}
                size="small"
                onChange={(v) => setLogFilters({ ...logFilters, startDate: v })}
              />
              <Input
                placeholder="结束日期 (YYYYMMDD)"
                style={{ width: 150 }}
                size="small"
                onChange={(v) => setLogFilters({ ...logFilters, endDate: v })}
              />
              <Button theme="solid" type="primary" onClick={loadSyncLogs} size="small">筛选</Button>
            </div>
            <Table
              dataSource={syncLogs}
              columns={[
                { title: 'ID', dataIndex: 'id', key: 'id', width: 80 },
                { title: '来源', dataIndex: 'source', key: 'source', width: 150 },
                { title: '类型', dataIndex: 'data_type', key: 'data_type', width: 150 },
                { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 120 },
                {
                  title: '同步行数', dataIndex: 'rows_synced', key: 'rows_synced', width: 120,
                  render: (text: number) => text.toLocaleString()
                },
                {
                  title: '状态', dataIndex: 'status', key: 'status', width: 100,
                  render: (text: string) => <Tag color={text === 'success' ? 'green' : 'red'}>{text}</Tag>
                },
                {
                  title: '创建时间', dataIndex: 'created_at', key: 'created_at', width: 180,
                  render: (text: string) => new Date(text).toLocaleString()
                },
              ]}
              rowKey="id"
              size="small"
              pagination={{ pageSize: 20, showSizeChanger: true }}
            />
          </Card>
        </TabPane>

        <TabPane tab={<span><IconServer /> 数据表</span>} itemKey="3">
          <Card
            className="content-card"
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>数据库表</span>}
            headerExtraContent={
              <Button icon={<IconRefresh />} onClick={loadInitialData} size="small">
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
            />
          </Card>
        </TabPane>

        <TabPane tab={<span><IconCode /> SQL 查询</span>} itemKey="4">
          <Card
            className="content-card"
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>SQL 查询</span>}
          >
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div>
                <div style={{
                  marginBottom: 8,
                  color: 'var(--text-secondary)',
                  fontSize: '13px',
                  fontWeight: 500
                }}>
                  SQL 查询编辑器（仅支持 SELECT）
                </div>
                <TextArea
                  value={sqlQuery}
                  onChange={(v) => setSqlQuery(v)}
                  placeholder="在此输入 SQL 查询语句..."
                  rows={6}
                  style={{
                    fontFamily: 'Fira Code, Courier New, monospace',
                    fontSize: '13px',
                  }}
                />
                <div style={{
                  marginTop: 8,
                  padding: '8px',
                  background: 'var(--bg-surface)',
                  border: '1px solid var(--border-color)',
                  borderRadius: '6px',
                  color: 'var(--text-secondary)',
                  fontSize: 12
                }}>
                  安全提示：仅允许 SELECT 查询，每次最多返回 10,000 行数据。
                </div>
              </div>

              <Button
                theme="solid"
                type="primary"
                icon={<IconPlay />}
                onClick={handleExecuteQuery}
                loading={queryLoading}
              >
                执行查询
              </Button>

              {queryResult.length > 0 && (
                <div>
                  <div style={{
                    marginBottom: 8,
                    color: 'var(--color-primary)',
                    fontSize: '14px',
                    fontWeight: 600
                  }}>
                    查询结果 ({queryResult.length} 行)
                  </div>
                  <Table
                    dataSource={queryResult}
                    columns={queryResultColumns}
                    rowKey={(record: any) => JSON.stringify(record).slice(0, 100)}
                    size="small"
                    pagination={{ pageSize: 50 }}
                    scroll={{ x: 'max-content', y: 500 }}
                  />
                </div>
              )}
            </div>
          </Card>
        </TabPane>

        <TabPane tab={<span><IconFlowChartStroked /> DAG 管理</span>} itemKey="5">
          <DAGManageSection />
        </TabPane>
      </Tabs>

      {/* Sync Task Modal */}
      <Modal
        title={`同步任务: ${syncModalTask?.task_id || ''}`}
        visible={syncModalVisible}
        onOk={executeSyncTask}
        onCancel={() => setSyncModalVisible(false)}
        okText="开始同步"
        cancelText="取消"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '8px 0' }}>
            <div style={{ display: 'flex' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>任务ID</span>
              <span style={{ fontSize: 13 }}>{syncModalTask?.task_id}</span>
            </div>
            <div style={{ display: 'flex' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>描述</span>
              <span style={{ fontSize: 13 }}>{syncModalTask?.description}</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>类型</span>
              <Tag color={syncModalTask?.sync_type === 'incremental' ? 'blue' : 'green'}>
                {syncModalTask?.sync_type}
              </Tag>
            </div>
            <div style={{ display: 'flex' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>数据表</span>
              <span style={{ fontSize: 13 }}>{syncModalTask?.table_name}</span>
            </div>
          </div>

          {syncModalTask?.sync_type === 'incremental' && (
            <>
              <div>
                <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>开始日期（可选）</div>
                <Input
                  placeholder="YYYYMMDD（如 20240101）"
                  value={syncStartDate}
                  onChange={(v) => setSyncStartDate(v)}
                  maxLength={8}
                  size="small"
                />
              </div>
              <div>
                <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>结束日期（可选）</div>
                <Input
                  placeholder="YYYYMMDD（如 20240131）"
                  value={syncEndDate}
                  onChange={(v) => setSyncEndDate(v)}
                  maxLength={8}
                  size="small"
                />
                <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
                  指定日期范围进行同步，留空则只同步最新一天的数据。
                </div>
              </div>
            </>
          )}

          {syncModalTask?.sync_type === 'full' && (
            <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
              <div style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                全量同步模式
              </div>
              <div style={{ marginTop: 6, color: 'var(--text-secondary)', fontSize: 12 }}>
                此任务将执行全量数据同步，无需指定日期参数。
              </div>
            </div>
          )}
        </div>
      </Modal>

      {/* Config Editor Modal */}
      <Modal
        title={isNewTask ? '新建任务' : `编辑任务: ${configModalTask?.task_id || ''}`}
        visible={configModalVisible}
        onOk={handleSaveConfig}
        onCancel={() => setConfigModalVisible(false)}
        okText="保存"
        cancelText="取消"
        width={800}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>任务配置 (JSON)</div>
            <TextArea
              value={configJson}
              onChange={(v) => setConfigJson(v)}
              rows={18}
              style={{
                fontFamily: 'Fira Code, Courier New, monospace',
                fontSize: '12px',
              }}
            />
            <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
              编辑上方 JSON 配置。必填字段：task_id, api_name, sync_type, table_name, primary_keys
            </div>
          </div>
        </div>
      </Modal>

      {/* Schedule Management Modal */}
      <Modal
        title={`调度设置: ${scheduleModalTask?.task_id || ''}`}
        visible={scheduleModalVisible}
        onOk={handleEnableSchedule}
        onCancel={() => setScheduleModalVisible(false)}
        okText="启用调度"
        cancelText="取消"
        width={500}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div>
            <div style={{ marginBottom: 8, fontWeight: 500, fontSize: '13px' }}>调度类型</div>
            <Select
              value={scheduleType}
              onChange={(v) => setScheduleType(v as string)}
              style={{ width: '100%' }}
              optionList={[
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
                onChange={(v) => setCronExpression(v)}
                placeholder="例如: 0 2 * * * (每天凌晨2点)"
                style={{
                  fontFamily: 'Fira Code, Courier New, monospace',
                  fontSize: '12px',
                }}
              />
              <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
                Cron 格式: 分 时 日 月 周 (例如: 0 2 * * * 表示每天凌晨2点)
              </div>
            </div>
          )}
          <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', fontSize: '12px' }}>
            <div style={{ color: 'var(--color-primary)', fontWeight: 500, marginBottom: 4 }}>说明</div>
            <div style={{ color: 'var(--text-secondary)' }}>
              启用调度后，系统将按照设定的时间自动执行数据同步任务。
              <br />
              - 每日调度：适用于需要每天更新的数据（如日线行情）
              <br />
              - 每周调度：适用于更新频率较低的数据（如股票列表）
              <br />
              - 每月调度：适用于月度更新的数据
              <br />
              - 自定义调度：使用 Cron 表达式设置任意时间
            </div>
          </div>
        </div>
      </Modal>
    </div>
  );
};

export default DataCenter;
