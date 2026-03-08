import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Tabs, TabPane, Table, Button, Card, Tag, Select, InputNumber, Spin, Empty,
  Modal, Input, Popconfirm, Checkbox, Tooltip, SideSheet, DatePicker, Banner,
  Toast, Collapse,
} from '@douyinfe/semi-ui';
import { TextArea } from '@douyinfe/semi-ui';
import {
  IconTestScoreStroked, IconPlay, IconRefresh, IconBarChartHStroked, IconPlus,
  IconDelete, IconEdit, IconBolt, IconCode, IconServer, IconInfoCircle,
  IconSearch, IconSave, IconSetting, IconAlertTriangle, IconHistory,
} from '@douyinfe/semi-icons';
import dayjs from 'dayjs';
import ReactECharts from 'echarts-for-react';
import Editor from '@monaco-editor/react';
import { productionApi, dataApi, DEFAULT_PREPROCESS } from '../api';
import { useThemeStore } from '../store';
import { formatCode } from '../utils/codeFormatter';
import type {
  PreprocessOptions,
  DataFieldMapping,
  FactorDefinition,
  FactorRunRecord,
  FactorValue,
  FactorAnalysisResult,
} from '../types';

const formatRunParams = (record: FactorRunRecord): string => {
  const parts: string[] = [];
  const start = record.start_date || '';
  const end = record.end_date || '';
  if (start || end) parts.push(`${start}~${end}`);
  if (record.preprocess) {
    try {
      const pp = typeof record.preprocess === 'string' ? JSON.parse(record.preprocess) : record.preprocess;
      const adjMap: Record<string, string> = { forward: '前复权', backward: '后复权', none: '不复权' };
      if (pp.adjust_price) parts.push(adjMap[pp.adjust_price] || pp.adjust_price);
      if (pp.filter_st === false) parts.push('含ST');
      if (pp.filter_new_stock === false) parts.push('含次新');
      if (pp.handle_suspension === false) parts.push('含停牌');
      if (pp.mark_limit === false) parts.push('不标涨跌停');
    } catch { /* ignore */ }
  }
  return parts.join(' | ') || '-';
};

// ==================== 因子详情/编辑 统一 SideSheet ====================
interface FactorCodeInfo {
  filename: string;
  code: string;
}

interface DataConfigLabel {
  source_label: string;
  values?: Record<string, string>;
}

interface FactorDrawerProps {
  factor: FactorDefinition | null;
  open: boolean;
  initialTab?: string;
  onClose: () => void;
  onSaved: () => void;
}

const FactorDrawer: React.FC<FactorDrawerProps> = ({ factor, open, initialTab, onClose, onSaved }) => {
  const { mode } = useThemeStore();
  const factorId = factor?.factor_id;
  const [activeTab, setActiveTab] = useState<string>('edit');
  // 编辑 - use individual useState instead of Form.useForm()
  const [editDesc, setEditDesc] = useState<string>('');
  const [editCategory, setEditCategory] = useState<string>('');
  const [editComputeMode, setEditComputeMode] = useState<string>('');
  const [editDependsOn, setEditDependsOn] = useState<string[]>([]);
  const [editWindow, setEditWindow] = useState<number | undefined>(undefined);
  const [editLookbackDays, setEditLookbackDays] = useState<number>(60);
  const [editSaving, setEditSaving] = useState<boolean>(false);
  // 预处理
  const [ppEdit, setPpEdit] = useState<PreprocessOptions>({ ...DEFAULT_PREPROCESS });
  // 代码
  const [code, setCode] = useState<FactorCodeInfo | null>(null);
  const [editedCode, setEditedCode] = useState<string>('');
  const [codeChanged, setCodeChanged] = useState<boolean>(false);
  const [codeLoading, setCodeLoading] = useState<boolean>(false);
  const [codeSaving, setCodeSaving] = useState<boolean>(false);
  // 统计 & 数据
  const [stats, setStats] = useState<FactorAnalysisResult | null>(null);
  const [statsLoading, setStatsLoading] = useState<boolean>(false);
  const [factorData, setFactorData] = useState<FactorValue[]>([]);
  const [dataLoading, setDataLoading] = useState<boolean>(false);
  const [dataFilter, setDataFilter] = useState<{ ts_code?: string; start_date?: string; end_date?: string }>({});
  // 计算日志
  const [history, setHistory] = useState<FactorRunRecord[]>([]);
  const [historyLoading, setHistoryLoading] = useState<boolean>(false);
  // 数据源注解
  const [dataConfigLabels, setDataConfigLabels] = useState<Record<string, DataConfigLabel>>({});
  // 编辑器引用
  const codeEditorRef = useRef<unknown>(null);

  // 格式化代码
  const handleFormatCode = async () => {
    try {
      const formatted = await formatCode(editedCode, 'python');
      setEditedCode(formatted);
      setCodeChanged(true);
      Toast.success('代码格式化成功');
    } catch (error: any) {
      Toast.error(error.message || '格式化失败');
    }
  };

  // 打开时初始化
  useEffect(() => {
    if (!factor || !open) return;
    setActiveTab(initialTab || 'edit');
    setCodeChanged(false);
    setCode(null);
    setStats(null);
    setFactorData([]);
    setDataFilter({});
    setHistory([]);
    // 编辑表单
    setEditDesc(factor.description || '');
    setEditCategory(factor.category || '');
    setEditComputeMode(factor.compute_mode || '');
    const rawDeps = factor.depends_on;
    setEditDependsOn(Array.isArray(rawDeps) ? rawDeps : (rawDeps ? (() => { try { return JSON.parse(rawDeps); } catch { return []; } })() : []));
    const params = factor.params as any;
    setEditWindow(params?.window ?? undefined);
    setEditLookbackDays(params?.lookback_days ?? 60);
    // 预处理
    const pp = params?.preprocess || {};
    console.log('[FactorDrawer] 加载因子配置:', { factor_id: factor.factor_id, params: factor.params, preprocess: pp });
    setPpEdit({ ...DEFAULT_PREPROCESS, ...pp });
    // 统计
    setStatsLoading(true);
    if (factorId) {
      productionApi.getFactorStats(factorId).then(r => setStats(r.data?.data)).catch(() => {}).finally(() => setStatsLoading(false));
    }
    // 代码 - 直接加载
    setCodeLoading(true);
    if (factorId) {
      productionApi.getFactorCode(factorId).then(res => {
        const d = res.data?.data;
        setCode(d);
        setEditedCode(d?.code || '');
      }).catch(() => setCode(null)).finally(() => setCodeLoading(false));
    }
    // 数据源注解
    productionApi.getResolvedDataConfig().then(r => setDataConfigLabels(r.data?.data || {})).catch(() => {});
  }, [factor, open, initialTab, factorId]);

  const loadData = useCallback(async () => {
    if (!factorId) return;
    setDataLoading(true);
    try {
      const res = await productionApi.getFactorData(factorId, { ...dataFilter, limit: 200 });
      setFactorData(res.data?.data || []);
    } catch { setFactorData([]); }
    setDataLoading(false);
  }, [factorId, dataFilter]);

  const loadHistory = useCallback(async () => {
    if (!factorId) return;
    setHistoryLoading(true);
    try {
      const res = await productionApi.getProductionHistory(factorId, 50);
      setHistory(res.data?.data || []);
    } catch { setHistory([]); }
    setHistoryLoading(false);
  }, [factorId]);

  useEffect(() => { if (activeTab === 'data' && factorId) loadData(); }, [activeTab, factorId, loadData]);
  useEffect(() => { if (activeTab === 'logs' && factorId) loadHistory(); }, [activeTab, factorId, loadHistory]);

  // 保存编辑（基本信息 + 预处理）
  const handleSave = async () => {
    if (!factor) return;
    setEditSaving(true);
    try {
      if (!factorId) {
        Toast.error('因子ID不能为空');
        return;
      }
      const newParams = {
        ...(factor.params || {}),
        preprocess: ppEdit,
        lookback_days: editLookbackDays,
        ...(editWindow !== undefined ? { window: editWindow } : {}),
      };
      const values = { description: editDesc, category: editCategory, compute_mode: editComputeMode, depends_on: editDependsOn, params: newParams };
      console.log('[FactorDrawer] 保存预处理配置:', ppEdit);
      await productionApi.updateFactor(factorId, values);
      Toast.success('保存成功');
      onSaved();
    } catch (e: any) {
      if (e.response) Toast.error(e.response?.data?.detail || '保存失败');
    }
    setEditSaving(false);
  };

  // 保存代码
  const handleSaveCode = async () => {
    if (!factorId || !code) return;
    setCodeSaving(true);
    try {
      await productionApi.updateFactorCode(factorId, code.filename, editedCode);
      Toast.success('代码已保存');
      setCodeChanged(false);
    } catch (e: any) { Toast.error(e.response?.data?.detail || '保存失败'); }
    setCodeSaving(false);
  };

  const dataColumns = [
    { title: '股票代码', dataIndex: 'ts_code', key: 'ts_code', width: 120 },
    { title: '交易日期', dataIndex: 'trade_date', key: 'trade_date', width: 120 },
    { title: '因子值', dataIndex: 'factor_value', key: 'factor_value', render: (v: number) => v?.toFixed(6) },
  ];

  const logColumns = [
    { title: '模式', dataIndex: 'mode', key: 'mode', width: 80,
      render: (v: string) => <Tag color={v === 'incremental' ? 'cyan' : 'orange'}>{v === 'incremental' ? '增量' : '全量'}</Tag>
    },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80,
      render: (v: string) => <Tag color={v === 'success' ? 'green' : v === 'running' ? 'blue' : 'red'}>{v}</Tag>
    },
    { title: '计算参数', key: 'range',
      render: (_: any, record: any) => {
        const text = formatRunParams(record);
        return <Tooltip content={text}><span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>{text}</span></Tooltip>;
      }
    },
    { title: '行数', dataIndex: 'rows_affected', key: 'rows', width: 100,
      render: (v: number) => v?.toLocaleString() || '-'
    },
    { title: '耗时', dataIndex: 'duration_seconds', key: 'dur', width: 80,
      render: (v: number) => v ? `${v.toFixed(1)}s` : '-'
    },
    { title: '时间', dataIndex: 'created_at', key: 'time',
      render: (v: string) => v ? <Tooltip content={v}><span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>{v.slice(0, 19)}</span></Tooltip> : '-'
    },
  ];

  return (
    <SideSheet
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ color: 'var(--color-primary)' }}>{factorId}</span>
        </div>
      }
      visible={open} onCancel={onClose} width={780}
    >
      <Tabs activeKey={activeTab} onChange={setActiveTab} size="small">
        {/* ---- 编辑（合并基本信息 + 预处理 + 代码） ---- */}
        <TabPane itemKey="edit" tab={<span><IconEdit size="small" /> 编辑</span>}>
          <div>
            {/* 基本信息 */}
            <Collapse defaultActiveKey={['info', 'preprocess', 'code']}>
              <Collapse.Panel itemKey="info" header={<span style={{ fontSize: 13, fontWeight: 500 }}>基本信息</span>}>
                <div style={{ display: 'flex', gap: 16 }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>描述</div>
                    <Input size="small" value={editDesc} onChange={setEditDesc} />
                  </div>
                  <div style={{ flex: '0 0 140px' }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分类</div>
                    <Select size="small" style={{ width: '100%' }} value={editCategory} onChange={v => setEditCategory(v as string)}
                      optionList={['momentum','value','technical','quality','custom'].map(v => ({ label: v, value: v }))} />
                  </div>
                  <div style={{ flex: '0 0 140px' }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>计算模式</div>
                    <Select size="small" style={{ width: '100%' }} value={editComputeMode} onChange={v => setEditComputeMode(v as string)}
                      optionList={[{ label: '增量', value: 'incremental' }, { label: '全量', value: 'full' }]} />
                  </div>
                </div>
                <div style={{ marginTop: 8 }}>
                  <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>数据依赖</div>
                  <Select
                    size="small" multiple style={{ width: '100%' }}
                    value={editDependsOn}
                    onChange={v => setEditDependsOn(v as string[])}
                    optionList={[
                      { label: 'sync_daily_data（日线行情 OHLCV）', value: 'sync_daily_data' },
                      { label: 'sync_daily_basic（每日指标 PE/PB/市值）', value: 'sync_daily_basic' },
                      { label: 'sync_adj_factor（复权因子）', value: 'sync_adj_factor' },
                      { label: 'sync_moneyflow（资金流向）', value: 'sync_moneyflow' },
                      { label: 'sync_stk_limit（涨跌停价格）', value: 'sync_stk_limit' },
                      { label: 'sync_suspend_d（停牌信息）', value: 'sync_suspend_d' },
                      { label: 'sync_stock_st（ST 状态）', value: 'sync_stock_st' },
                      { label: 'sync_stock_basic（股票基本信息）', value: 'sync_stock_basic' },
                    ]}
                  />
                </div>
                <div style={{ display: 'flex', gap: 16, marginTop: 8 }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>
                      计算窗口 window
                      <span style={{ color: 'var(--text-muted)', marginLeft: 4 }}>（滚动窗口大小，如 MA/RSI 的 N 天）</span>
                    </div>
                    <InputNumber size="small" min={1} max={500} style={{ width: '100%' }}
                      placeholder="不填则由代码内部决定"
                      value={editWindow}
                      onChange={v => setEditWindow(v as number | undefined)} />
                  </div>
                  <div style={{ flex: 1 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>
                      向前回溯天数 lookback_days
                      <span style={{ color: 'var(--text-muted)', marginLeft: 4 }}>（增量计算时额外加载的历史数据天数，默认 60）</span>
                    </div>
                    <InputNumber size="small" min={1} max={1000} style={{ width: '100%' }}
                      value={editLookbackDays}
                      onChange={v => setEditLookbackDays((v as number) || 60)} />
                  </div>
                </div>
                {/* 统计概览 */}
                <Spin spinning={statsLoading}>
                  {stats ? (
                    <div style={{ marginTop: 8 }}>
                      <div style={{ display: 'flex', gap: 12 }}>
                        <Card style={{ flex: 1, background: 'var(--bg-card)' }} bodyStyle={{ padding: 12 }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>总行数</div>
                          <div style={{ color: 'var(--color-primary)', fontSize: 16, fontWeight: 600 }}>{stats.total_rows}</div>
                        </Card>
                        <Card style={{ flex: 1, background: 'var(--bg-card)' }} bodyStyle={{ padding: 12 }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>股票数</div>
                          <div style={{ color: 'var(--color-gain)', fontSize: 16, fontWeight: 600 }}>{stats.stock_count}</div>
                        </Card>
                        <Card style={{ flex: 1, background: 'var(--bg-card)' }} bodyStyle={{ padding: 12 }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>起始日期</div>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{stats.min_date || '-'}</div>
                        </Card>
                        <Card style={{ flex: 1, background: 'var(--bg-card)' }} bodyStyle={{ padding: 12 }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>截止日期</div>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{stats.max_date || '-'}</div>
                        </Card>
                      </div>
                      <Card style={{ marginTop: 8, background: 'var(--bg-card)' }} bodyStyle={{ padding: 12 }}
                        title={<span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>分布统计</span>}>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
                          <div><span style={{ color: 'var(--text-muted)', marginRight: 6 }}>均值</span><span style={{ color: 'var(--text-primary)' }}>{stats.mean_val?.toFixed(6) ?? '-'}</span></div>
                          <div><span style={{ color: 'var(--text-muted)', marginRight: 6 }}>标准差</span><span style={{ color: 'var(--text-primary)' }}>{stats.std_val?.toFixed(6) ?? '-'}</span></div>
                          <div><span style={{ color: 'var(--text-muted)', marginRight: 6 }}>最小值</span><span style={{ color: 'var(--text-primary)' }}>{stats.min_val?.toFixed(6) ?? '-'}</span></div>
                          <div><span style={{ color: 'var(--text-muted)', marginRight: 6 }}>最大值</span><span style={{ color: 'var(--text-primary)' }}>{stats.max_val?.toFixed(6) ?? '-'}</span></div>
                        </div>
                      </Card>
                    </div>
                  ) : null}
                </Spin>
              </Collapse.Panel>

              <Collapse.Panel itemKey="preprocess" header={<span style={{ fontSize: 13, fontWeight: 500 }}>预处理配置</span>}>
                <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                  <div style={{ flex: '1 1 200px' }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>复权方式</div>
                    <Select size="small" style={{ width: '100%' }} value={ppEdit.adjust_price}
                      onChange={(v) => setPpEdit(p => ({ ...p, adjust_price: v as PreprocessOptions['adjust_price'] }))}
                      optionList={[
                        { label: '前复权', value: 'forward' },
                        { label: '后复权', value: 'backward' },
                        { label: '不复权', value: 'none' },
                      ]} />
                    {dataConfigLabels.adj_factor && <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>数据源: {dataConfigLabels.adj_factor.source_label}</div>}
                  </div>
                  <div style={{ flex: '1 1 200px' }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>新股排除天数</div>
                    <InputNumber size="small" min={1} max={250} value={ppEdit.new_stock_days}
                      disabled={!ppEdit.filter_new_stock} style={{ width: '100%' }}
                      onChange={(v) => setPpEdit(p => ({ ...p, new_stock_days: (v as number) || 60 }))} />
                  </div>
                </div>
                <div style={{ display: 'flex', gap: 16, marginTop: 12, flexWrap: 'wrap' }}>
                  <span>
                    <Checkbox checked={ppEdit.filter_st} onChange={(e) => setPpEdit(p => ({ ...p, filter_st: !!e.target.checked }))}>过滤 ST</Checkbox>
                    {dataConfigLabels.is_st && (
                      <Tooltip content={dataConfigLabels.is_st.values ? Object.entries(dataConfigLabels.is_st.values).map(([k, v]) => `${k}: ${v}`).join('\n') : undefined} position="bottom">
                        <span style={{ fontSize: 10, color: 'var(--text-muted)', cursor: dataConfigLabels.is_st.values ? 'help' : undefined }}> ({dataConfigLabels.is_st.source_label})</span>
                      </Tooltip>
                    )}
                  </span>
                  <span>
                    <Checkbox checked={ppEdit.filter_new_stock} onChange={(e) => setPpEdit(p => ({ ...p, filter_new_stock: !!e.target.checked }))}>过滤新股</Checkbox>
                    {dataConfigLabels.list_date && <span style={{ fontSize: 10, color: 'var(--text-muted)' }}> ({dataConfigLabels.list_date.source_label})</span>}
                  </span>
                  <span>
                    <Checkbox checked={ppEdit.handle_suspension} onChange={(e) => setPpEdit(p => ({ ...p, handle_suspension: !!e.target.checked }))}>停牌处理</Checkbox>
                    {dataConfigLabels.is_suspend && (
                      <Tooltip content={dataConfigLabels.is_suspend.values ? Object.entries(dataConfigLabels.is_suspend.values).map(([k, v]) => `${k}: ${v}`).join('\n') : undefined} position="bottom">
                        <span style={{ fontSize: 10, color: 'var(--text-muted)', cursor: dataConfigLabels.is_suspend.values ? 'help' : undefined }}> ({dataConfigLabels.is_suspend.source_label})</span>
                      </Tooltip>
                    )}
                  </span>
                  <span>
                    <Checkbox checked={ppEdit.mark_limit} onChange={(e) => setPpEdit(p => ({ ...p, mark_limit: !!e.target.checked }))}>涨跌停标记</Checkbox>
                    {dataConfigLabels.is_limit && (
                      <Tooltip content={dataConfigLabels.is_limit.values ? Object.entries(dataConfigLabels.is_limit.values).map(([k, v]) => `${k}: ${v}`).join('\n') : undefined} position="bottom">
                        <span style={{ fontSize: 10, color: 'var(--text-muted)', cursor: dataConfigLabels.is_limit.values ? 'help' : undefined }}> ({dataConfigLabels.is_limit.source_label})</span>
                      </Tooltip>
                    )}
                  </span>
                </div>
              </Collapse.Panel>

              <Collapse.Panel itemKey="code" header={<span style={{ fontSize: 13, fontWeight: 500 }}>因子代码</span>}>
                <Spin spinning={codeLoading}>
                  {code ? (
                    <div>
                      <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', marginBottom: 8 }}>
                        <div style={{ display: 'flex', gap: 8 }}>
                          <Button size="small" icon={<IconCode />} onClick={handleFormatCode}>格式化</Button>
                          <Button size="small" theme="solid" icon={<IconSave />} disabled={!codeChanged}
                            loading={codeSaving} onClick={handleSaveCode}>保存代码</Button>
                        </div>
                      </div>
                      <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
                        <Editor height="320px" language="python" theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                          value={editedCode} onChange={(v) => { setEditedCode(v || ''); setCodeChanged(true); }}
                          onMount={(editor, monaco) => {
                            codeEditorRef.current = editor;
                            editor.addAction({
                              id: 'format-code',
                              label: 'Format Code',
                              keybindings: [monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF],
                              run: () => handleFormatCode(),
                            });
                          }}
                          options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4, wordWrap: 'on' }} />
                      </div>
                      <CodeTestPanel code={editedCode} preprocess={ppEdit} />
                    </div>
                  ) : <Empty description="未找到源代码文件" />}
                </Spin>
              </Collapse.Panel>
            </Collapse>
            <div style={{ marginTop: 12, textAlign: 'right' }}>
              <Button theme="solid" icon={<IconSave />} loading={editSaving} onClick={handleSave}>保存</Button>
            </div>
          </div>
        </TabPane>
        {/* ---- 数据探查 ---- */}
        <TabPane itemKey="data" tab={<span><IconServer size="small" /> 数据</span>}>
          <div>
            <div style={{ display: 'flex', gap: 8, marginBottom: 12, flexWrap: 'wrap' }}>
              <Input size="small" placeholder="股票代码" style={{ width: 120 }} showClear
                onChange={v => setDataFilter(f => ({ ...f, ts_code: v || undefined }))} />
              <Input size="small" placeholder="起始日期 (yyyymmdd)" style={{ width: 160 }} showClear
                onChange={v => setDataFilter(f => ({ ...f, start_date: v || undefined }))} />
              <Input size="small" placeholder="截止日期 (yyyymmdd)" style={{ width: 160 }} showClear
                onChange={v => setDataFilter(f => ({ ...f, end_date: v || undefined }))} />
              <Button size="small" theme="solid" icon={<IconSearch />} onClick={loadData}>查询</Button>
            </div>
            <Table dataSource={factorData} columns={dataColumns} rowKey={(r: any) => `${r.ts_code}-${r.trade_date}`}
              loading={dataLoading} size="small" pagination={{ pageSize: 15 }}
              scroll={{ y: 400 }} />
          </div>
        </TabPane>
        {/* ---- 计算日志 ---- */}
        <TabPane itemKey="logs" tab={<span><IconHistory size="small" /> 日志</span>}>
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
              <span style={{ color: 'var(--text-secondary)', fontSize: 13 }}>最近 50 条计算记录</span>
              <Button size="small" icon={<IconRefresh />} onClick={loadHistory}>刷新</Button>
            </div>
            <Table dataSource={history} columns={logColumns}
              rowKey={(r: any) => `${r.factor_id}-${r.created_at}`}
              loading={historyLoading} size="small" pagination={{ pageSize: 15 }}
              empty={<Empty description="暂无计算记录" />} />
          </div>
        </TabPane>
      </Tabs>
    </SideSheet>
  );
};

// ==================== 代码测试面板 ====================
interface TestLog {
  level: string;
  phase: string;
  message: string;
}

interface TestStats {
  total_rows?: number;
  stock_count?: number;
  factor_mean?: number;
  factor_std?: number;
  null_count?: number;
}

interface TestResultData {
  ts_code: string;
  trade_date: string;
  factor_value: number | null;
  [key: string]: unknown;
}

interface TestResult {
  stats?: TestStats;
  preview?: TestResultData[];
  truncated?: boolean;
  stocks?: string[];
  dates?: string[];
}

const CODE_TEMPLATE = `"""自定义因子"""
import polars as pl
from engine.production.registry import factor


@factor(
    "factor_custom_01",
    description="自定义因子",
    depends_on=["sync_daily_data"],
    category="custom",
    params={"window": 20, "lookback_days": 40},
)
def compute_custom(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    w = params.get("window", 20)
    return (
        df.sort(["ts_code", "trade_date"])
        .with_columns(
            pl.col("close").rolling_mean(window_size=w).over("ts_code").alias("factor_value")
        )
        .select(["ts_code", "trade_date", "factor_value"])
        .drop_nulls()
    )
`;

const CodeTestPanel: React.FC<{ code: string; dependsOn?: string[]; preprocess?: PreprocessOptions }> = ({ code, dependsOn, preprocess }) => {
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [testing, setTesting] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [testError, setTestError] = useState<string | null>(null);
  const [testLogs, setTestLogs] = useState<TestLog[]>([]);
  const [testStdout, setTestStdout] = useState<string>('');
  const [filterStock, setFilterStock] = useState<string | undefined>(undefined);
  const [filterDate, setFilterDate] = useState<string | undefined>(undefined);
  const [showLogs, setShowLogs] = useState<boolean>(true);

  const handleTest = async (): Promise<void> => {
    if (!code.trim()) { Toast.warning('请先编写因子代码'); return; }
    if (!dateRange[0] || !dateRange[1]) { Toast.warning('请选择测试日期范围'); return; }
    setTesting(true);
    setTestResult(null);
    setTestError(null);
    setTestLogs([]);
    setTestStdout('');
    setFilterStock(undefined);
    setFilterDate(undefined);
    try {
      const res = await productionApi.testFactorCode({
        code,
        start_date: dateRange[0],
        end_date: dateRange[1],
        depends_on: dependsOn || ['sync_daily_data'],
        params: {},
        preprocess: preprocess || undefined,
      });
      const d = res.data;
      if (d.status === 'error') {
        setTestError(`[${d.phase}] ${d.error}`);
        setTestLogs(d.logs || []);
        setTestStdout(d.stdout || '');
      } else {
        setTestResult(d.data);
        setTestLogs(d.data?.logs || []);
        setTestStdout(d.data?.stdout || '');
      }
    } catch (e) {
      const error = e as { response?: { data?: { detail?: string } }; message?: string };
      setTestError(error.response?.data?.detail || error.message || '测试请求失败');
    }
    setTesting(false);
  };

  const filteredPreview: TestResultData[] = testResult?.preview?.filter((row) => {
    if (filterStock && row.ts_code !== filterStock) return false;
    if (filterDate && row.trade_date !== filterDate) return false;
    return true;
  }) || [];

  const resultColumns = [
    { title: '股票代码', dataIndex: 'ts_code', key: 'ts_code', width: 120 },
    { title: '交易日期', dataIndex: 'trade_date', key: 'trade_date', width: 110 },
    { title: '因子值', dataIndex: 'factor_value', key: 'factor_value', width: 140,
      render: (v: number) => v != null ? v.toFixed(6) : <span style={{ color: 'var(--text-muted)' }}>null</span> },
  ];

  const logColorMap: Record<string, string> = {
    info: '#94a3b8', error: '#ef4444', warn: '#f59e0b',
  };
  const phaseColorMap: Record<string, string> = {
    compile: '#818cf8', exec: '#38bdf8', resolve: '#a78bfa',
    data: '#34d399', compute: '#fbbf24', validate: '#fb923c', result: '#22d3ee',
  };

  return (
    <div style={{ marginTop: 8, borderTop: '1px solid var(--border-color)', paddingTop: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{ color: 'var(--text-secondary)', fontSize: 12, whiteSpace: 'nowrap' }}>测试区间:</span>
        <DatePicker type="dateRange" size="small" style={{ flex: 1 }}
          defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
          disabledDate={(current) => current ? dayjs(current).isAfter(dayjs().endOf('day')) : false}
          onChange={(date, dateStr) => {
            const strs = dateStr as unknown as string[];
            if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
              setDateRange([strs[0].replace(/-/g, ''), strs[1].replace(/-/g, '')]);
            } else {
              setDateRange(['', '']);
            }
          }}
          placeholder={['开始日期', '结束日期']} />
        <Button size="small" theme="solid" icon={<IconAlertTriangle />}
          loading={testing} onClick={handleTest}>编译测试</Button>
      </div>

      {/* 日志面板 */}
      {(testLogs.length > 0 || testStdout || testError) && (
        <div style={{ marginBottom: 8 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <span style={{ color: 'var(--text-secondary)', fontSize: 11, cursor: 'pointer' }}
              onClick={() => setShowLogs(!showLogs)}>
              {showLogs ? '▼' : '▶'} 执行日志 ({testLogs.length})
            </span>
          </div>
          {showLogs && (
            <div style={{
              background: 'var(--bg-primary)', border: '1px solid var(--border-color)', borderRadius: 4,
              padding: '6px 8px', maxHeight: 200, overflowY: 'auto',
              fontFamily: 'monospace', fontSize: 11, lineHeight: '18px',
            }}>
              {testLogs.map((log, i: number) => (
                <div key={i} style={{ color: logColorMap[log.level] || '#94a3b8' }}>
                  <span style={{ color: phaseColorMap[log.phase] || 'var(--text-muted)', marginRight: 6 }}>
                    [{log.phase}]
                  </span>
                  <span style={{ whiteSpace: 'pre-wrap' }}>{log.message}</span>
                </div>
              ))}
              {testStdout && (
                <div style={{ borderTop: '1px solid var(--border-color)', marginTop: 4, paddingTop: 4 }}>
                  <span style={{ color: 'var(--text-muted)' }}>[stdout]</span>
                  <pre style={{ color: 'var(--text-primary)', margin: '2px 0 0 0', whiteSpace: 'pre-wrap', fontSize: 11 }}>{testStdout}</pre>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {testError && <Banner type="danger" description={testError} closeIcon={null} style={{ marginBottom: 8, fontSize: 12 }} />}
      {testResult && (
        <div>
          <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>总行数</div>
              <div style={{ color: 'var(--color-primary)', fontSize: 14, fontWeight: 600 }}>{testResult.stats?.total_rows}</div>
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>股票数</div>
              <div style={{ color: 'var(--color-gain)', fontSize: 14, fontWeight: 600 }}>{testResult.stats?.stock_count}</div>
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>均值</div>
              <div style={{ color: 'var(--text-primary)', fontSize: 14 }}>{testResult.stats?.factor_mean != null ? Number(testResult.stats.factor_mean).toFixed(4) : '-'}</div>
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>标准差</div>
              <div style={{ color: 'var(--text-primary)', fontSize: 14 }}>{testResult.stats?.factor_std != null ? Number(testResult.stats.factor_std).toFixed(4) : '-'}</div>
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>空值</div>
              <div style={{ fontSize: 14, color: (testResult.stats?.null_count ?? 0) > 0 ? 'var(--color-loss)' : 'var(--color-gain)' }}>{testResult.stats?.null_count ?? 0}</div>
            </div>
          </div>
          {testResult.truncated && <Banner type="warning" description="结果已截断，仅显示前 2000 行" closeIcon={null} style={{ marginBottom: 8, fontSize: 12 }} />}
          <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
            <Select size="small" showClear placeholder="筛选股票" style={{ width: 160 }}
              value={filterStock} onChange={v => setFilterStock(v as string | undefined)} filter
              optionList={testResult.stocks?.map((s: string) => ({ label: s, value: s })) || []} />
            <Select size="small" showClear placeholder="筛选日期" style={{ width: 140 }}
              value={filterDate} onChange={v => setFilterDate(v as string | undefined)} filter
              optionList={testResult.dates?.map((d: string) => ({ label: d, value: d })) || []} />
            <span style={{ color: 'var(--text-muted)', fontSize: 12, lineHeight: '24px' }}>
              显示 {filteredPreview.length} 条
            </span>
          </div>
          <Table dataSource={filteredPreview} columns={resultColumns}
            rowKey={(r: any) => `${r?.ts_code}-${r?.trade_date}`}
            size="small" pagination={{ pageSize: 10 }}
            scroll={{ y: 240 }} />
        </div>
      )}
    </div>
  );
};

// ==================== 因子管理 Tab ====================
const FactorManageTab: React.FC = () => {
  const { mode } = useThemeStore();
  const [factors, setFactors] = useState<FactorDefinition[]>([]);
  const [history, setHistory] = useState<FactorRunRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [runLoading, setRunLoading] = useState<string | null>(null);
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [batchLoading, setBatchLoading] = useState<boolean>(false);
  const [createModal, setCreateModal] = useState<boolean>(false);
  const [createCode, setCreateCode] = useState<string>(CODE_TEMPLATE);
  const [createPreprocess, setCreatePreprocess] = useState<PreprocessOptions>({ ...DEFAULT_PREPROCESS });
  const [drawerState, setDrawerState] = useState<{ open: boolean; factor: FactorDefinition | null; tab?: string }>({ open: false, factor: null });
  const [fullRunModal, setFullRunModal] = useState<{ visible: boolean; factorId: string | null; computeMode: string }>({ visible: false, factorId: null, computeMode: 'incremental' });
  const [fullRunDates, setFullRunDates] = useState<[dayjs.Dayjs | null, dayjs.Dayjs | null]>([null, null]);
  // Create form state (replaces Form.useForm)
  const [createFactorId, setCreateFactorId] = useState<string>('');
  const [createDesc, setCreateDesc] = useState<string>('');
  const [createCategory, setCreateCategory] = useState<string>('custom');
  const [createComputeMode, setCreateComputeMode] = useState<string>('incremental');
  const [createDependsOn, setCreateDependsOn] = useState<string[]>(['sync_daily_data']);
  const [createWindow, setCreateWindow] = useState<number | undefined>(undefined);
  const [createLookbackDays, setCreateLookbackDays] = useState<number>(60);
  const createEditorRef = useRef<unknown>(null);

  // 格式化创建代码
  const handleFormatCreateCode = async (): Promise<void> => {
    try {
      const formatted = await formatCode(createCode, 'python');
      setCreateCode(formatted);
      Toast.success('代码格式化成功');
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : '格式化失败';
      Toast.error(errorMessage);
    }
  };
  // 批量计算模态框
  const [batchCalcModalVisible, setBatchCalcModalVisible] = useState<boolean>(false);
  const [batchCalcDates, setBatchCalcDates] = useState<[dayjs.Dayjs | null, dayjs.Dayjs | null]>([null, null]);

  const loadFactors = useCallback(async (): Promise<void> => {
    setLoading(true);
    try {
      const res = await productionApi.listFactors();
      setFactors(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load factors:', error);
      Toast.error('加载因子列表失败');
    }
    setLoading(false);
  }, []);

  const loadHistory = useCallback(async (factorId?: string): Promise<void> => {
    try {
      const res = await productionApi.getProductionHistory(factorId, 30);
      setHistory(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load history:', error);
    }
  }, []);

  useEffect(() => { loadFactors(); loadHistory(); }, [loadFactors, loadHistory]);

  const handleRun = async (factorId: string, runMode: string, startDate?: string, endDate?: string) => {
    setRunLoading(factorId);
    try {
      const factor = factors.find(f => f.factor_id === factorId);
      const params = factor?.params as any;
      const pp = params?.preprocess || undefined;
      await productionApi.runProduction(factorId, runMode, undefined, startDate, endDate, pp);
      Toast.success(`因子 ${factorId} ${runMode === 'incremental' ? '增量' : '全量'}计算完成`);
      loadFactors();
      loadHistory(selectedFactor || undefined);
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '执行失败');
    }
    setRunLoading(null);
  };

  const handleBatchRun = async (runMode: string, startDate?: string, endDate?: string) => {
    if (selectedRowKeys.length === 0) { Toast.warning('请先勾选因子'); return; }
    setBatchLoading(true);
    try {
      const res = await productionApi.batchRunFactors(selectedRowKeys, runMode, startDate, endDate);
      const results = res.data?.data || [];
      const ok = results.filter((r: any) => r.success).length;
      const fail = results.length - ok;
      Toast.success(`批量计算完成: ${ok} 成功, ${fail} 失败`);
      setSelectedRowKeys([]);
      loadFactors();
      loadHistory(selectedFactor || undefined);
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '批量执行失败');
    }
    setBatchLoading(false);
  };

  const handleCreate = async () => {
    if (!createFactorId.trim()) { Toast.warning('请输入因子ID'); return; }
    try {
      const values = {
        factor_id: createFactorId,
        description: createDesc,
        category: createCategory,
        compute_mode: createComputeMode,
        depends_on: createDependsOn,
      };
      const params = {
        preprocess: createPreprocess,
        lookback_days: createLookbackDays,
        ...(createWindow !== undefined ? { window: createWindow } : {}),
      };
      await productionApi.createFactor({ ...values, params, code: createCode || undefined });
      Toast.success(`因子 ${values.factor_id} 创建成功`);
      setCreateModal(false);
      setCreateFactorId(''); setCreateDesc(''); setCreateCategory('custom'); setCreateComputeMode('incremental');
      setCreateCode(CODE_TEMPLATE);
      setCreatePreprocess({ ...DEFAULT_PREPROCESS }); setCreateDependsOn(['sync_daily_data']); setCreateWindow(undefined); setCreateLookbackDays(60);
      loadFactors();
    } catch (e: any) {
      if (e.response) Toast.error(e.response?.data?.detail || '创建失败');
    }
  };

  const handleDelete = async (factorId: string) => {
    try {
      await productionApi.deleteFactor(factorId, false);
      Toast.success(`因子 ${factorId} 已删除`);
      loadFactors();
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '删除失败');
    }
  };

  const openDrawer = (record: any, tab?: string) => {
    setDrawerState({ open: true, factor: record, tab });
  };

  const factorColumns = [
    { title: '因子ID', dataIndex: 'factor_id', key: 'factor_id', width: 180,
      render: (v: string, r: any) => (
        <Tooltip content={v}>
          <span style={{ cursor: 'pointer', display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} onClick={() => openDrawer(r)}>
            <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
          </span>
        </Tooltip>
      )
    },
    { title: '描述', dataIndex: 'description', key: 'desc', width: 180,
      render: (v: string) => (
        <Tooltip content={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{v}</div>
        </Tooltip>
      )
    },
    { title: '分类', dataIndex: 'category', key: 'cat', width: 80,
      render: (v: string) => (
        <Tooltip content={v || '-'}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <Tag size="small">{v || '-'}</Tag>
          </div>
        </Tooltip>
      )
    },
    { title: '模式', dataIndex: 'compute_mode', key: 'mode', width: 60,
      render: (v: string) => <Tag size="small" color={v === 'incremental' ? 'blue' : 'green'}>{v === 'incremental' ? '增量' : '全量'}</Tag>
    },
    { title: '最新数据', dataIndex: 'latest_data_date', key: 'latest', width: 90,
      render: (v: string) => {
        if (!v) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
        return (
          <Tooltip content={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--color-gain)' }}>{v}</div>
          </Tooltip>
        );
      }
    },
    { title: '上次计算', dataIndex: 'last_computed_at', key: 'computed', width: 130,
      render: (v: string) => {
        if (!v) return '-';
        return (
          <Tooltip content={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)', fontSize: '12px' }}>
              {v.slice(0, 16)}
            </div>
          </Tooltip>
        );
      }
    },
    {
      title: '操作', key: 'action', width: 160, render: (_: any, record: any) => (
        <div style={{ display: 'flex', gap: 4 }}>
          <Button size="small" icon={<IconHistory />}
            loading={runLoading === record.factor_id}
            onClick={() => {
              setFullRunModal({ visible: true, factorId: record.factor_id, computeMode: record.compute_mode || 'incremental' });
              setFullRunDates([null, null]);
            }}>计算</Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.factor_id)}>
            <Button size="small" type="danger" icon={<IconDelete />} />
          </Popconfirm>
        </div>
      )
    },
  ];

  const historyColumns = [
    { title: '因子', dataIndex: 'factor_id', key: 'fid', width: 150,
      render: (v: string) => (
        <Tooltip content={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
          </div>
        </Tooltip>
      )
    },
    { title: '模式', dataIndex: 'mode', key: 'mode', width: 60,
      render: (v: string) => <Tag size="small" color={v === 'incremental' ? 'blue' : 'green'}>{v === 'incremental' ? '增量' : '全量'}</Tag>
    },
    { title: '状态', dataIndex: 'status', key: 'status', width: 80,
      render: (v: string) => <Tag size="small" color={v === 'success' ? 'green' : v === 'running' ? 'blue' : 'red'}>{v}</Tag>
    },
    { title: '行数', dataIndex: 'rows_affected', key: 'rows', width: 90,
      render: (v: number) => {
        const formatted = v?.toLocaleString() || '-';
        return (
          <Tooltip content={formatted}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{formatted}</div>
          </Tooltip>
        );
      }
    },
    { title: '耗时', dataIndex: 'duration_seconds', key: 'dur', width: 80,
      render: (v: number) => {
        const formatted = v ? `${v.toFixed(1)}s` : '-';
        return (
          <Tooltip content={formatted}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{formatted}</div>
          </Tooltip>
        );
      }
    },
    { title: '计算参数', key: 'range', width: 200,
      render: (_: any, record: any) => {
        const text = formatRunParams(record);
        return (
          <Tooltip content={text}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: '12px', color: 'var(--text-secondary)' }}>{text}</div>
          </Tooltip>
        );
      }
    },
    { title: '时间', dataIndex: 'created_at', key: 'time', width: 150,
      render: (v: string) => {
        const formatted = v?.slice(0, 19) || '-';
        return (
          <Tooltip content={formatted}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{formatted}</div>
          </Tooltip>
        );
      }
    },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>已注册因子</span>
          <div style={{ display: 'flex', gap: 8 }}>
            {selectedRowKeys.length > 0 && (
              <Button size="small" theme="solid" icon={<IconBolt />} loading={batchLoading}
                onClick={() => { setBatchCalcDates([null, null]); setBatchCalcModalVisible(true); }}>批量计算 ({selectedRowKeys.length})</Button>
            )}
            <Button size="small" icon={<IconPlus />} onClick={() => setCreateModal(true)}>新建因子</Button>
            <Button icon={<IconRefresh />} onClick={loadFactors} size="small">刷新</Button>
          </div>
        </div>
        <Table dataSource={factors} columns={factorColumns} rowKey="factor_id"
          loading={loading} size="small" pagination={false}
          rowSelection={{ selectedRowKeys, onChange: (keys) => setSelectedRowKeys(keys?.map(String) || []) }} />
      </Card>

      <Card style={{ background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: 'var(--text-secondary)', fontWeight: 600, fontSize: 15 }}>计算历史</span>
          <Select showClear placeholder="筛选因子" style={{ width: 160 }} size="small"
            value={selectedFactor || undefined} onChange={(v) => { setSelectedFactor((v as string) || null); loadHistory((v as string) || undefined); }}
            optionList={factors.map(f => ({ label: f.factor_id, value: f.factor_id }))} />
        </div>
        <Table dataSource={history} columns={historyColumns} rowKey={(r: any) => `${r.factor_id}-${r.created_at}`}
          size="small" pagination={{ pageSize: 10 }} />
      </Card>

      {/* 新建因子 SideSheet */}
      <SideSheet
        title={<span style={{ color: 'var(--color-primary)' }}>新建因子</span>}
        visible={createModal}
        onCancel={() => {
          setCreateModal(false);
          setCreateFactorId(''); setCreateDesc(''); setCreateCategory('custom'); setCreateComputeMode('incremental');
          setCreateCode(CODE_TEMPLATE); setCreatePreprocess({ ...DEFAULT_PREPROCESS });
        }}
        width={720}
        footer={
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => {
              setCreateModal(false);
              setCreateFactorId(''); setCreateDesc(''); setCreateCategory('custom'); setCreateComputeMode('incremental');
              setCreateCode(CODE_TEMPLATE); setCreatePreprocess({ ...DEFAULT_PREPROCESS });
            }}>取消</Button>
            <Button theme="solid" type="primary" onClick={handleCreate}>创建</Button>
          </div>
        }
      >
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>因子ID <span style={{ color: 'var(--color-loss)' }}>*</span></div>
            <Input size="small" prefix="factor_" placeholder="如 custom_01" value={(createFactorId || '').replace(/^factor_/, '')} onChange={(v) => setCreateFactorId(`factor_${v}`)} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>描述</div>
            <Input size="small" placeholder="因子描述" value={createDesc} onChange={setCreateDesc} />
          </div>
        </div>
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分类</div>
            <Select size="small" style={{ width: '100%' }} value={createCategory} onChange={v => setCreateCategory(v as string)}
              optionList={['momentum','value','technical','quality','custom'].map(v => ({ label: v, value: v }))} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>计算模式</div>
            <Select size="small" style={{ width: '100%' }} value={createComputeMode} onChange={v => setCreateComputeMode(v as string)}
              optionList={[{ label: '增量', value: 'incremental' }, { label: '全量', value: 'full' }]} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>复权方式</div>
            <Select size="small" style={{ width: '100%' }} value={createPreprocess.adjust_price}
              onChange={(v) => setCreatePreprocess(p => ({ ...p, adjust_price: v as PreprocessOptions['adjust_price'] }))}
              optionList={[
                { label: '前复权', value: 'forward' },
                { label: '后复权', value: 'backward' },
                { label: '不复权', value: 'none' },
              ]} />
          </div>
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>数据依赖 <span style={{ color: 'var(--color-loss)' }}>*</span></div>
          <Select
            size="small" multiple style={{ width: '100%' }}
            value={createDependsOn}
            onChange={v => setCreateDependsOn(v as string[])}
            optionList={[
              { label: 'sync_daily_data（日线行情 OHLCV）', value: 'sync_daily_data' },
              { label: 'sync_daily_basic（每日指标 PE/PB/市值）', value: 'sync_daily_basic' },
              { label: 'sync_adj_factor（复权因子）', value: 'sync_adj_factor' },
              { label: 'sync_moneyflow（资金流向）', value: 'sync_moneyflow' },
            ]}
          />
        </div>
        <div style={{ display: 'flex', gap: 16, marginBottom: 8 }}>
          <Checkbox checked={createPreprocess.filter_st} onChange={(e) => setCreatePreprocess(p => ({ ...p, filter_st: !!e.target.checked }))}>过滤 ST</Checkbox>
          <Checkbox checked={createPreprocess.filter_new_stock} onChange={(e) => setCreatePreprocess(p => ({ ...p, filter_new_stock: !!e.target.checked }))}>过滤新股</Checkbox>
          <Checkbox checked={createPreprocess.handle_suspension} onChange={(e) => setCreatePreprocess(p => ({ ...p, handle_suspension: !!e.target.checked }))}>停牌处理</Checkbox>
          <Checkbox checked={createPreprocess.mark_limit} onChange={(e) => setCreatePreprocess(p => ({ ...p, mark_limit: !!e.target.checked }))}>涨跌停标记</Checkbox>
        </div>
        {createPreprocess.filter_new_stock && (
          <div style={{ marginBottom: 8 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>新股排除天数</div>
            <InputNumber size="small" min={1} max={250} value={createPreprocess.new_stock_days}
              onChange={(v) => setCreatePreprocess(p => ({ ...p, new_stock_days: (v as number) || 60 }))} />
          </div>
        )}
        <div style={{ display: 'flex', gap: 16, marginBottom: 8, marginTop: 8 }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>
              计算窗口 window
              <span style={{ color: 'var(--text-muted)', marginLeft: 4 }}>（滚动窗口大小，如 MA/RSI 的 N 天）</span>
            </div>
            <InputNumber size="small" min={1} max={500} style={{ width: '100%' }}
              placeholder="不填则由代码内部决定"
              value={createWindow}
              onChange={v => setCreateWindow(v as number | undefined)} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>
              向前回溯天数 lookback_days
              <span style={{ color: 'var(--text-muted)', marginLeft: 4 }}>（增量计算时额外加载的历史数据天数，默认 60）</span>
            </div>
            <InputNumber size="small" min={1} max={1000} style={{ width: '100%' }}
              value={createLookbackDays}
              onChange={v => setCreateLookbackDays((v as number) || 60)} />
          </div>
        </div>
        <div style={{ marginTop: 8 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>因子计算代码</div>
            <Button size="small" icon={<IconCode />} onClick={handleFormatCreateCode}>格式化</Button>
          </div>
          <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
            <Editor height="300px" language="python" theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
              value={createCode} onChange={(v) => setCreateCode(v || '')}
              onMount={(editor, monaco) => {
                createEditorRef.current = editor;
                editor.addAction({
                  id: 'format-create-code',
                  label: 'Format Code',
                  keybindings: [monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF],
                  run: () => handleFormatCreateCode(),
                });
              }}
              options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4, wordWrap: 'on' }} />
          </div>
          <CodeTestPanel code={createCode} preprocess={createPreprocess} />
        </div>
      </SideSheet>

      <Modal title={`计算因子: ${fullRunModal.factorId}`} visible={fullRunModal.visible}
        onCancel={() => setFullRunModal({ visible: false, factorId: null, computeMode: 'incremental' })}
        onOk={() => {
          if (!fullRunModal.factorId) return;
          if (fullRunModal.computeMode === 'full') {
            setFullRunModal({ visible: false, factorId: null, computeMode: 'incremental' });
            handleRun(fullRunModal.factorId, 'full');
          } else {
            const sd = fullRunDates[0]?.format('YYYYMMDD');
            const ed = fullRunDates[1]?.format('YYYYMMDD');
            setFullRunModal({ visible: false, factorId: null, computeMode: 'incremental' });
            handleRun(fullRunModal.factorId, sd && ed ? 'full' : 'incremental', sd, ed);
          }
        }}
        okText="开始计算" cancelText="取消">
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {fullRunModal.computeMode === 'full' ? (
            <div style={{ color: 'var(--text-secondary)', fontSize: 13 }}>
              该因子为全量计算模式，将执行一次完整计算。
            </div>
          ) : (
            <>
              <div style={{ color: 'var(--text-secondary)', fontSize: 13 }}>
                选择计算日期范围。留空则执行增量计算（仅计算最新数据）。
              </div>
              <div>
                <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>计算日期范围</div>
                <DatePicker type="dateRange" style={{ width: '100%' }} size="small"
                  placeholder={['开始日期', '结束日期']}
                  defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
                  value={fullRunDates[0] && fullRunDates[1] ? [fullRunDates[0].toDate(), fullRunDates[1].toDate()] : undefined}
                  onChange={(dates) => {
                    if (dates && Array.isArray(dates) && dates.length === 2 && dates[0] && dates[1]) {
                      setFullRunDates([dayjs(dates[0]), dayjs(dates[1])]);
                    } else {
                      setFullRunDates([null, null]);
                    }
                  }} />
                {fullRunDates[0] && fullRunDates[1] && (
                  <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                    <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                      共 {fullRunDates[1].diff(fullRunDates[0], 'day') + 1} 天
                    </span>
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </Modal>

      {/* 批量计算模态框 */}
      <Modal
        title={`批量计算 (已选 ${selectedRowKeys.length} 个因子)`}
        visible={batchCalcModalVisible}
        onOk={() => {
          const sd = batchCalcDates[0]?.format('YYYYMMDD');
          const ed = batchCalcDates[1]?.format('YYYYMMDD');
          setBatchCalcModalVisible(false);
          handleBatchRun(sd && ed ? 'full' : 'incremental', sd, ed);
        }}
        onCancel={() => setBatchCalcModalVisible(false)}
        okText="开始计算" cancelText="取消"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
            {(() => {
              const fullIds = selectedRowKeys.filter(id => factors.find(f => f.factor_id === id)?.compute_mode === 'full');
              const incIds = selectedRowKeys.filter(id => !fullIds.includes(id));
              return (<>
                {incIds.length > 0 && (
                  <div style={{ marginBottom: fullIds.length > 0 ? 8 : 0 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>增量因子（按日期范围计算）：</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {incIds.map(id => <Tag key={id} color="cyan" style={{ fontSize: '12px' }}>{id}</Tag>)}
                    </div>
                  </div>
                )}
                {fullIds.length > 0 && (
                  <div>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>全量因子（仅执行一次，忽略日期）：</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {fullIds.map(id => <Tag key={id} color="orange" style={{ fontSize: '12px' }}>{id}</Tag>)}
                    </div>
                  </div>
                )}
              </>);
            })()}
          </div>
          {selectedRowKeys.some(id => factors.find(f => f.factor_id === id)?.compute_mode !== 'full') && (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 8 }}>
              选择计算日期范围。留空则执行增量计算（仅计算最新数据）。
            </div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>计算日期范围</div>
            <DatePicker type="dateRange" style={{ width: '100%' }} size="small"
              placeholder={['开始日期', '结束日期']}
              defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
              value={batchCalcDates[0] && batchCalcDates[1] ? [batchCalcDates[0].toDate(), batchCalcDates[1].toDate()] : undefined}
              onChange={(dates) => {
                if (dates && Array.isArray(dates) && dates.length === 2 && dates[0] && dates[1]) {
                  setBatchCalcDates([dayjs(dates[0]), dayjs(dates[1])]);
                } else {
                  setBatchCalcDates([null, null]);
                }
              }} />
            {batchCalcDates[0] && batchCalcDates[1] && (
              <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {batchCalcDates[1].diff(batchCalcDates[0], 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
          )}
        </div>
      </Modal>

      <FactorDrawer factor={drawerState.factor} open={drawerState.open} initialTab={drawerState.tab}
        onClose={() => setDrawerState({ open: false, factor: null })}
        onSaved={async () => {
          const res = await productionApi.listFactors();
          const list = res.data?.data || [];
          setFactors(list);
          if (drawerState.factor) {
            const updated = list.find((f: any) => f.factor_id === drawerState.factor?.factor_id);
            if (updated) setDrawerState(prev => ({ ...prev, factor: updated }));
          }
        }} />
    </div>
  );
};

// ==================== 因子分析 Tab ====================
const AnalysisTab: React.FC = () => {
  const [factors, setFactors] = useState<any[]>([]);
  const [indexPools, setIndexPools] = useState<any[]>([]);
  const [selectedFactor, setSelectedFactor] = useState<string>('');
  const [periods, setPeriods] = useState<number[]>([1, 5, 10]);
  const [quantiles, setQuantiles] = useState(5);
  const [startDate, setStartDate] = useState<string>('');
  const [endDate, setEndDate] = useState<string>('');
  const [indexPool, setIndexPool] = useState<string>('');
  const [groupbyField, setGroupbyField] = useState<string>('');
  const [useAlphalens, setUseAlphalens] = useState(true);
  const [analysisResult, setAnalysisResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);
  const [analysisHistory, setAnalysisHistory] = useState<any[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  useEffect(() => {
    productionApi.listFactors().then(res => setFactors(res.data?.data || [])).catch(() => {});
    productionApi.listIndexPools().then(res => setIndexPools(res.data?.data || [])).catch(() => {});
  }, []);

  const handleRunAnalysis = async () => {
    if (!selectedFactor) { Toast.warning('请选择因子'); return; }
    setRunLoading(true);
    try {
      let res;
      if (useAlphalens) {
        res = await productionApi.runAlphalensAnalysis({
          factor_id: selectedFactor,
          start_date: startDate || undefined as any,
          end_date: endDate || undefined as any,
          periods,
          quantiles,
          index_pool: indexPool || undefined,
          groupby_field: groupbyField || undefined,
        });
        Toast.success('Alphalens 分析完成');
        setAnalysisResult(res.data?.data);
      } else {
        res = await productionApi.runAnalysis(selectedFactor, startDate || undefined, endDate || undefined, periods, quantiles);
        Toast.success('分析完成');
        setAnalysisResult(res.data?.data);
      }
      // 刷新历史
      if (selectedFactor) loadHistory(selectedFactor);
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '分析失败');
    }
    setRunLoading(false);
  };

  const loadAnalysis = async (factorId: string) => {
    setLoading(true);
    try {
      if (useAlphalens) {
        const res = await productionApi.getLatestAlphalensAnalysis(factorId);
        setAnalysisResult(res.data?.data);
      } else {
        const res = await productionApi.getAnalysis(factorId);
        setAnalysisResult(res.data?.data);
      }
    } catch { setAnalysisResult(null); }
    setLoading(false);
  };

  const loadHistory = async (factorId: string) => {
    setHistoryLoading(true);
    try {
      const res = await productionApi.getAlphalensAnalysisHistory(factorId, 10);
      setAnalysisHistory(res.data?.data?.records || []);
    } catch { setAnalysisHistory([]); }
    setHistoryLoading(false);
  };

  const getICChartOption = () => {
    if (useAlphalens) {
      // Alphalens 格式: ic_summary.ic_mean, ic_by_period
      const icByPeriod = analysisResult?.ic_by_period;
      if (!icByPeriod || !icByPeriod.length) return {};
      return {
        backgroundColor: 'transparent',
        tooltip: { trigger: 'axis' },
        legend: { textStyle: { color: '#94A3B8' }, top: 0 },
        grid: { top: 40, bottom: 30, left: 60, right: 20 },
        xAxis: { type: 'category', data: icByPeriod.map((d: any) => d.period), axisLabel: { color: '#94A3B8' } },
        yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
        series: [
          { name: 'IC均值', type: 'bar', data: icByPeriod.map((d: any) => d.ic_mean?.toFixed(4)), itemStyle: { color: '#0077FA' } },
          { name: 'ICIR', type: 'bar', data: icByPeriod.map((d: any) => d.ic_ir?.toFixed(4)), itemStyle: { color: '#14C9C9' } },
        ]
      };
    }
    if (!analysisResult?.ic_summary) return {};
    const data = analysisResult.ic_summary;
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: { type: 'category', data: data.map((d: any) => `${d.period}D`), axisLabel: { color: '#94A3B8' } },
      yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
      series: [
        { name: 'IC均值', type: 'bar', data: data.map((d: any) => d.ic_mean?.toFixed(4)), itemStyle: { color: '#0077FA' } },
        { name: 'ICIR', type: 'bar', data: data.map((d: any) => d.icir?.toFixed(4)), itemStyle: { color: '#14C9C9' } },
      ]
    };
  };

  const getLayerReturnOption = () => {
    const returns = useAlphalens ? analysisResult?.quantile_returns : analysisResult?.layer_returns;
    if (!returns || !returns.length) return {};
    const data = returns;
    const periodGroups = [...new Set(data.map((d: any) => d.period))];
    const quantileGroups = [...new Set(data.map((d: any) => d.quantile))];
    const colors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0077FA'];
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: quantileGroups as string[], textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: { type: 'category', data: periodGroups.map((p: any) => `${p}`), axisLabel: { color: '#94A3B8' } },
      yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
      series: quantileGroups.map((q: any, i: number) => ({
        name: `Q${q}`, type: 'bar',
        data: periodGroups.map((p: any) => {
          const item = data.find((d: any) => d.period === p && d.quantile === q);
          return item?.mean_return || 0;
        }),
        itemStyle: { color: colors[i % colors.length] },
      }))
    };
  };

  const getICTimeSeriesOption = () => {
    const icTs = analysisResult?.ic_ts;
    if (!icTs || !icTs.length) return {};
    const dates = icTs.map((d: any) => d.date);
    const periods = Object.keys(icTs[0]).filter(k => k !== 'date');
    const colors = ['#0077FA', '#14C9C9', '#f97316', '#22c55e'];
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: periods, textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: { type: 'category', data: dates, axisLabel: { color: '#94A3B8', rotate: 30 } },
      yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
      series: periods.map((p, i) => ({
        name: p, type: 'line', smooth: true,
        data: icTs.map((d: any) => d[p]),
        lineStyle: { color: colors[i % colors.length] },
        itemStyle: { color: colors[i % colors.length] },
        showSymbol: false,
      }))
    };
  };

  const icColumns = [
    { title: '周期', dataIndex: 'period', key: 'period', render: (v: any) => `${v}` },
    { title: 'IC均值', dataIndex: 'ic_mean', key: 'ic_mean', render: (v: number) => <span style={{ color: v > 0 ? 'var(--color-gain)' : 'var(--color-loss)' }}>{v?.toFixed(4)}</span> },
    { title: 'IC标准差', dataIndex: 'ic_std', key: 'ic_std', render: (v: number) => v?.toFixed(4) },
    { title: 'ICIR', dataIndex: 'ic_ir', key: 'ic_ir', render: (v: number) => <span style={{ color: Math.abs(v) > 0.5 ? 'var(--color-primary)' : 'var(--text-secondary)', fontWeight: Math.abs(v) > 0.5 ? 700 : 400 }}>{v?.toFixed(4)}</span> },
    { title: 'IC胜率', dataIndex: 'ic_win_rate', key: 'win_rate', render: (v: number) => `${(v * 100).toFixed(1)}%` },
  ];

  const historyColumns = [
    { title: '分析日期', dataIndex: 'analysis_date', key: 'analysis_date', render: (v: any) => v ? new Date(v).toLocaleString('zh-CN') : '-' },
    { title: '日期范围', key: 'range', render: (_: any, r: any) => `${r.start_date || '-'} ~ ${r.end_date || '-'}` },
    { title: '股票池', dataIndex: 'index_pool', key: 'index_pool', render: (v: string) => v || '全市场' },
    { title: '分组', dataIndex: 'groupby_field', key: 'groupby_field', render: (v: string) => v || '-' },
    { title: '状态', dataIndex: 'task_status', key: 'task_status', render: (v: string) => <Tag color={v === 'completed' ? 'green' : 'orange'}>{v}</Tag> },
    {
      title: '操作', key: 'action', render: (_: any, r: any) => (
        <Button size="small" onClick={async () => {
          setLoading(true);
          try {
            const res = await productionApi.getLatestAlphalensAnalysis(selectedFactor);
            setAnalysisResult(res.data?.data);
          } catch { Toast.error('加载失败'); }
          setLoading(false);
        }}>查看</Button>
      )
    },
  ];

  const icSummary = useAlphalens ? analysisResult?.ic_summary : null;

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
          <Select placeholder="选择因子" style={{ width: 180 }} value={selectedFactor || undefined}
            onChange={(v) => { setSelectedFactor(v as string); loadAnalysis(v as string); loadHistory(v as string); }}
            optionList={factors.map(f => ({ label: `${f.factor_id}`, value: f.factor_id }))} />
          <DatePicker type="dateRange" placeholder={['开始日期', '结束日期']} style={{ width: 240 }}
            onChange={(v: any) => {
              if (v && v.length === 2) {
                setStartDate(v[0] ? dayjs(v[0]).format('YYYYMMDD') : '');
                setEndDate(v[1] ? dayjs(v[1]).format('YYYYMMDD') : '');
              }
            }} />
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>周期:</span>
          <Select multiple style={{ width: 200 }} value={periods} onChange={v => setPeriods(v as number[])}
            optionList={[1,2,3,5,10,20].map(v => ({ label: `${v}D`, value: v }))} />
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>分层:</span>
          <InputNumber min={3} max={10} value={quantiles} onChange={v => v && setQuantiles(v as number)} size="small" style={{ width: 60 }} />
        </div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center', marginTop: 12 }}>
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>股票池:</span>
          <Select placeholder="全市场" style={{ width: 180 }} value={indexPool || undefined} showClear
            onChange={(v) => setIndexPool(v as string || '')}
            optionList={indexPools.map(p => ({ label: `${p.index_code} ${p.index_name}`, value: p.index_code }))} />
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>分组字段:</span>
          <Select placeholder="不分组" style={{ width: 150 }} value={groupbyField || undefined} showClear
            onChange={(v) => setGroupbyField(v as string || '')}
            optionList={[
              { label: '行业', value: 'industry' },
              { label: '市值', value: 'market_cap' },
            ]} />
          <Checkbox checked={useAlphalens} onChange={(v) => setUseAlphalens(!!v)}>
            使用 Alphalens
          </Checkbox>
          <Button theme="solid" icon={<IconBarChartHStroked />} loading={runLoading} onClick={handleRunAnalysis}>运行分析</Button>
        </div>
      </Card>

      {loading ? <Spin style={{ display: 'block', margin: '60px auto' }} /> : analysisResult ? (
        <>
          {/* IC 汇总统计 */}
          {icSummary && (
            <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--color-primary)' }}>IC 汇总</span>}>
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                {[
                  { label: 'IC 均值', value: icSummary.ic_mean?.toFixed(4), color: icSummary.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)' },
                  { label: 'IC 标准差', value: icSummary.ic_std?.toFixed(4), color: 'var(--text-primary)' },
                  { label: 'IC IR', value: icSummary.ic_ir?.toFixed(4), color: Math.abs(icSummary.ic_ir) > 0.5 ? 'var(--color-primary)' : 'var(--text-primary)' },
                  { label: 'IC 胜率', value: `${(icSummary.ic_win_rate * 100).toFixed(1)}%`, color: icSummary.ic_win_rate > 0.5 ? 'var(--color-gain)' : 'var(--text-primary)' },
                ].map(item => (
                  <div key={item.label} className="stat-card" style={{ minWidth: 120 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{item.label}</div>
                    <div style={{ color: item.color, fontSize: 20, fontWeight: 700 }}>{item.value}</div>
                  </div>
                ))}
              </div>
            </Card>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
            <Card style={{ background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--color-primary)' }}>IC 分析</span>}>
              <ReactECharts option={getICChartOption()} style={{ height: 240 }} />
            </Card>
            <Card style={{ background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--color-accent, #14C9C9)' }}>分层收益</span>}>
              <ReactECharts option={getLayerReturnOption()} style={{ height: 240 }} />
            </Card>
          </div>

          {/* IC 时间序列 */}
          {analysisResult?.ic_ts && analysisResult.ic_ts.length > 0 && (
            <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--text-secondary)' }}>IC 时间序列</span>}>
              <ReactECharts option={getICTimeSeriesOption()} style={{ height: 240 }} />
            </Card>
          )}

          {/* 分周期 IC 表格 */}
          {analysisResult?.ic_by_period && (
            <Card style={{ background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--text-secondary)' }}>分周期 IC 指标</span>}>
              <Table dataSource={analysisResult.ic_by_period || []} columns={icColumns}
                rowKey="period" size="small" pagination={false} />
            </Card>
          )}
        </>
      ) : <Empty description="选择因子并运行分析" style={{ marginTop: 60 }} />}

      {/* 分析历史 */}
      {selectedFactor && (
        <Card style={{ marginTop: 16, background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--text-secondary)' }}>分析历史</span>}>
          <Spin spinning={historyLoading}>
            <Table dataSource={analysisHistory} columns={historyColumns}
              rowKey="id" size="small" pagination={{ pageSize: 5 }} />
          </Spin>
        </Card>
      )}
    </div>
  );
};

// ==================== 数据配置 Tab ====================
const DataConfigTab: React.FC = () => {
  const [mappings, setMappings] = useState<DataFieldMapping[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<Record<string, string[]>>({});
  const [changed, setChanged] = useState(false);

  const loadConfig = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.getDataConfig();
      setMappings(res.data?.data || []);
      setChanged(false);
    } catch { Toast.error('加载数据配置失败'); }
    setLoading(false);
  }, []);

  const loadTables = useCallback(async () => {
    try {
      const res = await dataApi.listTables();
      const list: string[] = (res.data?.tables || []).map((t: any) => t.table_name || t.name || t);
      setTables(list);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadConfig(); loadTables(); }, [loadConfig, loadTables]);

  const loadColumnsForTable = async (tableName: string) => {
    if (!tableName || tableColumns[tableName]) return;
    try {
      const res = await dataApi.getTableInfo(tableName);
      const cols: string[] = (res.data?.columns || []).map((c: any) => c.name || c);
      setTableColumns(prev => ({ ...prev, [tableName]: cols }));
    } catch { /* ignore */ }
  };

  const updateMapping = (idx: number, field: Partial<DataFieldMapping>) => {
    setMappings(prev => prev.map((m, i) => i === idx ? { ...m, ...field } as DataFieldMapping : m));
    setChanged(true);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await productionApi.updateDataConfig(mappings as any);
      Toast.success('配置已保存');
      setChanged(false);
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '保存失败');
    }
    setSaving(false);
  };

  return (
    <Card style={{ background: 'var(--bg-card)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>数据字段映射</span>
          <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>配置因子计算引擎使用的数据表和字段映射。主键: trade_date + ts_code。留空表示使用引擎内置逻辑。</span>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Button size="small" icon={<IconRefresh />} onClick={loadConfig}>刷新</Button>
          <Button size="small" theme="solid" icon={<IconSave />} disabled={!changed} loading={saving}
            onClick={handleSave}>保存配置</Button>
        </div>
      </div>
      <Spin spinning={loading}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {mappings.map((m, idx) => {
            const extra = (() => { try { return JSON.parse(m.extra_config || '{}'); } catch { return {}; } })();
            const hasTable = !!m.table_name;
            const enumValues: Record<string, string> | undefined = extra.values;
            return (
              <div key={m.field_key} style={{
                display: 'flex', flexDirection: 'column', gap: 8, padding: '10px 14px',
                background: 'var(--bg-surface)', borderRadius: 6, border: '1px solid var(--border-color)',
              }}>
                {/* 第一行: 字段信息 + 枚举标签 + 状态 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <code style={{ color: 'var(--color-primary)', fontSize: 12, fontWeight: 600 }}>{m.field_key}</code>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>{m.description?.split('。')[0]}</span>
                  {enumValues && (
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {Object.entries(enumValues).map(([k, v]) => (
                        <Tag key={k} size="small" style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}
                          color={k === '0' ? 'green' : k === '1' ? 'red' : k === '-1' ? 'blue' : 'grey'}
                        >{k}={v}</Tag>
                      ))}
                    </div>
                  )}
                  <div style={{ marginLeft: 'auto' }}>
                    {hasTable ? (
                      <Tag color="green" style={{ fontSize: 11 }}>{m.table_name}{m.column_name ? `.${m.column_name}` : ''}</Tag>
                    ) : extra.mode ? (
                      <Tag color="blue" style={{ fontSize: 11 }}>{extra.mode === 'infer_from_gaps' ? '从交易日缺失推断' : extra.mode === 'compute_from_ohlcv' ? '从OHLCV计算' : extra.mode}</Tag>
                    ) : (
                      <Tag color="grey" style={{ fontSize: 11 }}>未配置</Tag>
                    )}
                  </div>
                </div>
                {/* 第二行: 数据表 + 列选择器 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11, flexShrink: 0 }}>数据表</span>
                  <Select size="small" style={{ width: 240 }} placeholder="选择数据表" showClear filter
                    value={m.table_name || undefined}
                    onChange={(v) => {
                      updateMapping(idx, { table_name: (v as string) || '', column_name: '' });
                      if (v) loadColumnsForTable(v as string);
                    }}
                    optionList={tables.map(t => ({ label: t, value: t }))}
                  />
                  <span style={{ color: 'var(--text-muted)', fontSize: 11, flexShrink: 0 }}>列</span>
                  <Select size="small" style={{ width: 200 }} placeholder="选择列" showClear filter
                    value={m.column_name || undefined}
                    disabled={!m.table_name}
                    onFocus={() => { if (m.table_name) loadColumnsForTable(m.table_name); }}
                    onChange={(v) => updateMapping(idx, { column_name: (v as string) || '' })}
                    optionList={(tableColumns[m.table_name] || []).map(c => ({ label: c, value: c }))}
                  />
                </div>
              </div>
            );
          })}
        </div>
        {mappings.length === 0 && !loading && <Empty description="暂无配置数据" />}
      </Spin>
    </Card>
  );
};

// ==================== 主组件 ====================
const FactorCenter: React.FC = () => (
  <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
    <div style={{ marginBottom: '16px' }}>
      <h1 style={{
        color: 'var(--color-primary)', fontSize: '24px', fontWeight: 700, margin: 0,
        letterSpacing: '1px'
      }}>
        <IconTestScoreStroked style={{ marginRight: '8px' }} />
        因子
      </h1>
      <p style={{ color: 'var(--text-secondary)', margin: '4px 0 0 0', fontSize: '12px' }}>
        因子注册管理与 IC 分析
      </p>
    </div>

    <Tabs defaultActiveKey="factors">
      <TabPane itemKey="factors" tab={<span><IconTestScoreStroked /> 因子管理</span>}>
        <FactorManageTab />
      </TabPane>
      <TabPane itemKey="analysis" tab={<span><IconBarChartHStroked /> 因子分析</span>}>
        <AnalysisTab />
      </TabPane>
      <TabPane itemKey="dataconfig" tab={<span><IconSetting /> 数据配置</span>}>
        <DataConfigTab />
      </TabPane>
    </Tabs>
  </div>
);

export default FactorCenter;
