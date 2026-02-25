import React, { useState, useEffect, useCallback } from 'react';
import {
  Tabs, TabPane, Table, Button, Card, Tag, Select, InputNumber, Spin, Empty,
  Modal, Input, Popconfirm, Checkbox, Tooltip, SideSheet, DatePicker, Banner,
  Toast, Collapse,
} from '@douyinfe/semi-ui';
import { TextArea } from '@douyinfe/semi-ui';
import {
  IconTestScoreStroked, IconPlay, IconRefresh, IconBarChartHStroked, IconPlus,
  IconDelete, IconEdit, IconBolt, IconCode, IconServer, IconInfoCircle,
  IconSearch, IconSave, IconSetting, IconAlertTriangle,
} from '@douyinfe/semi-icons';
import dayjs from 'dayjs';
import ReactECharts from 'echarts-for-react';
import Editor from '@monaco-editor/react';
import { productionApi, DEFAULT_PREPROCESS } from '../api';
import type { PreprocessOptions } from '../api';
import { useThemeStore } from '../store';

// ==================== 因子详情/编辑 统一 SideSheet ====================
interface FactorDrawerProps {
  factor: any;
  open: boolean;
  initialTab?: string;
  onClose: () => void;
  onSaved: () => void;
}
const FactorDrawer: React.FC<FactorDrawerProps> = ({ factor, open, initialTab, onClose, onSaved }) => {
  const { mode } = useThemeStore();
  const factorId = factor?.factor_id;
  const [activeTab, setActiveTab] = useState('edit');
  // 编辑 - use individual useState instead of Form.useForm()
  const [editDesc, setEditDesc] = useState('');
  const [editCategory, setEditCategory] = useState('');
  const [editComputeMode, setEditComputeMode] = useState('');
  const [editSaving, setEditSaving] = useState(false);
  // 预处理
  const [ppEdit, setPpEdit] = useState<PreprocessOptions>({ ...DEFAULT_PREPROCESS });
  const [ppSaving, setPpSaving] = useState(false);
  // 代码
  const [code, setCode] = useState<{ filename: string; code: string } | null>(null);
  const [editedCode, setEditedCode] = useState('');
  const [codeChanged, setCodeChanged] = useState(false);
  const [codeLoading, setCodeLoading] = useState(false);
  const [codeSaving, setCodeSaving] = useState(false);
  // 统计 & 数据
  const [stats, setStats] = useState<any>(null);
  const [statsLoading, setStatsLoading] = useState(false);
  const [factorData, setFactorData] = useState<any[]>([]);
  const [dataLoading, setDataLoading] = useState(false);
  const [dataFilter, setDataFilter] = useState<{ ts_code?: string; start_date?: string; end_date?: string }>({});

  // 打开时初始化
  useEffect(() => {
    if (!factor || !open) return;
    setActiveTab(initialTab || 'edit');
    setCodeChanged(false);
    setCode(null);
    setStats(null);
    setFactorData([]);
    setDataFilter({});
    // 编辑表单
    setEditDesc(factor.description || '');
    setEditCategory(factor.category || '');
    setEditComputeMode(factor.compute_mode || '');
    // 预处理
    const pp = factor.params?.preprocess || {};
    setPpEdit({ ...DEFAULT_PREPROCESS, ...pp });
    // 统计
    setStatsLoading(true);
    productionApi.getFactorStats(factorId).then(r => setStats(r.data?.data)).catch(() => {}).finally(() => setStatsLoading(false));
  }, [factor, open, initialTab, factorId]);

  // 切到代码 tab 时加载
  const loadCode = useCallback(async () => {
    if (!factorId) return;
    setCodeLoading(true);
    try {
      const res = await productionApi.getFactorCode(factorId);
      const d = res.data?.data;
      setCode(d);
      setEditedCode(d?.code || '');
      setCodeChanged(false);
    } catch { setCode(null); }
    setCodeLoading(false);
  }, [factorId]);

  const loadData = useCallback(async () => {
    if (!factorId) return;
    setDataLoading(true);
    try {
      const res = await productionApi.getFactorData(factorId, { ...dataFilter, limit: 200 });
      setFactorData(res.data?.data || []);
    } catch { setFactorData([]); }
    setDataLoading(false);
  }, [factorId, dataFilter]);

  useEffect(() => { if (activeTab === 'code' && factorId) loadCode(); }, [activeTab, factorId, loadCode]);
  useEffect(() => { if (activeTab === 'data' && factorId) loadData(); }, [activeTab, factorId, loadData]);

  // 保存编辑
  const handleSaveEdit = async () => {
    if (!factor) return;
    setEditSaving(true);
    try {
      const values = { description: editDesc, category: editCategory, compute_mode: editComputeMode };
      await productionApi.updateFactor(factorId, values);
      Toast.success('基本信息已保存');
      onSaved();
    } catch (e: any) {
      if (e.response) Toast.error(e.response?.data?.detail || '保存失败');
    }
    setEditSaving(false);
  };

  // 保存预处理
  const handleSavePp = async () => {
    if (!factor) return;
    setPpSaving(true);
    try {
      const newParams = { ...(factor.params || {}), preprocess: ppEdit };
      await productionApi.updateFactor(factorId, { params: newParams });
      Toast.success('预处理配置已保存');
      onSaved();
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '保存失败');
    }
    setPpSaving(false);
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

  return (
    <SideSheet
      title={<span style={{ color: 'var(--color-primary)' }}>{factorId}</span>}
      visible={open} onCancel={onClose} width={780}
    >
      <Tabs activeKey={activeTab} onChange={setActiveTab} size="small">
        {/* ---- 编辑信息 ---- */}
        <TabPane itemKey="edit" tab={<span><IconEdit size="small" /> 编辑</span>}>
          <div>
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
            <div style={{ marginTop: 12, textAlign: 'right' }}>
              <Button theme="solid" icon={<IconSave />} loading={editSaving} onClick={handleSaveEdit}>保存信息</Button>
            </div>
          </div>
        </TabPane>
        {/* ---- 预处理配置 ---- */}
        <TabPane itemKey="preprocess" tab={<span><IconSetting size="small" /> 预处理</span>}>
          <div>
            <div style={{ display: 'flex', gap: 16, marginTop: 4, flexWrap: 'wrap' }}>
              <div style={{ flex: '1 1 200px' }}>
                <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>复权方式</div>
                <Select size="small" style={{ width: '100%' }} value={ppEdit.adjust_price}
                  onChange={(v) => setPpEdit(p => ({ ...p, adjust_price: v as PreprocessOptions['adjust_price'] }))}
                  optionList={[
                    { label: '前复权', value: 'forward' },
                    { label: '后复权', value: 'backward' },
                    { label: '不复权', value: 'none' },
                  ]} />
              </div>
              <div style={{ flex: '1 1 200px' }}>
                <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>新股排除天数</div>
                <InputNumber size="small" min={1} max={250} value={ppEdit.new_stock_days}
                  disabled={!ppEdit.filter_new_stock} style={{ width: '100%' }}
                  onChange={(v) => setPpEdit(p => ({ ...p, new_stock_days: (v as number) || 60 }))} />
              </div>
            </div>
            <div style={{ display: 'flex', gap: 16, marginTop: 12, flexWrap: 'wrap' }}>
              <Checkbox checked={ppEdit.filter_st} onChange={(e) => setPpEdit(p => ({ ...p, filter_st: !!e.target.checked }))}>过滤 ST</Checkbox>
              <Checkbox checked={ppEdit.filter_new_stock} onChange={(e) => setPpEdit(p => ({ ...p, filter_new_stock: !!e.target.checked }))}>过滤新股</Checkbox>
              <Checkbox checked={ppEdit.handle_suspension} onChange={(e) => setPpEdit(p => ({ ...p, handle_suspension: !!e.target.checked }))}>停牌处理</Checkbox>
              <Checkbox checked={ppEdit.mark_limit} onChange={(e) => setPpEdit(p => ({ ...p, mark_limit: !!e.target.checked }))}>涨跌停标记</Checkbox>
            </div>
            <div style={{ marginTop: 16, textAlign: 'right' }}>
              <Button theme="solid" icon={<IconSave />} loading={ppSaving} onClick={handleSavePp}>保存预处理</Button>
            </div>
          </div>
        </TabPane>
        {/* ---- 源代码 + 测试 ---- */}
        <TabPane itemKey="code" tab={<span><IconCode size="small" /> 代码</span>}>
          <Spin spinning={codeLoading}>
            {code ? (
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                  <Tag color="blue">{code.filename}</Tag>
                  <Button size="small" theme="solid" icon={<IconSave />} disabled={!codeChanged}
                    loading={codeSaving} onClick={handleSaveCode}>保存代码</Button>
                </div>
                <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
                  <Editor height="380px" language="python" theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                    value={editedCode} onChange={(v) => { setEditedCode(v || ''); setCodeChanged(true); }}
                    options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4 }} />
                </div>
                <CodeTestPanel code={editedCode} />
              </div>
            ) : <Empty description="未找到源代码文件" />}
          </Spin>
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
      </Tabs>
    </SideSheet>
  );
};

// ==================== 代码测试面板 ====================
const CODE_TEMPLATE = `"""自定义因子"""
import polars as pl
from engine.production.registry import factor


@factor(
    "factor_custom_01",
    description="自定义因子",
    depends_on=["daily_data"],
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

const CodeTestPanel: React.FC<{ code: string; dependsOn?: string[] }> = ({ code, dependsOn }) => {
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<any>(null);
  const [testError, setTestError] = useState<string | null>(null);
  const [testLogs, setTestLogs] = useState<any[]>([]);
  const [testStdout, setTestStdout] = useState('');
  const [filterStock, setFilterStock] = useState<string | undefined>(undefined);
  const [filterDate, setFilterDate] = useState<string | undefined>(undefined);
  const [showLogs, setShowLogs] = useState(true);

  const handleTest = async () => {
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
        depends_on: dependsOn || ['daily_data'],
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
    } catch (e: any) {
      setTestError(e.response?.data?.detail || e.message || '测试请求失败');
    }
    setTesting(false);
  };

  const filteredPreview: any[] = testResult?.preview?.filter((row: any) => {
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
              {testLogs.map((log: any, i: number) => (
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
              <div style={{ fontSize: 14, color: testResult.stats?.null_count > 0 ? 'var(--color-loss)' : 'var(--color-gain)' }}>{testResult.stats?.null_count}</div>
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
            rowKey={(r: any) => `${r.ts_code}-${r.trade_date}`}
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
  const [factors, setFactors] = useState<any[]>([]);
  const [history, setHistory] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState<string | null>(null);
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [batchLoading, setBatchLoading] = useState(false);
  const [createModal, setCreateModal] = useState(false);
  const [createCode, setCreateCode] = useState(CODE_TEMPLATE);
  const [createPreprocess, setCreatePreprocess] = useState<PreprocessOptions>({ ...DEFAULT_PREPROCESS });
  const [drawerState, setDrawerState] = useState<{ open: boolean; factor: any; tab?: string }>({ open: false, factor: null });
  const [fullRunModal, setFullRunModal] = useState<{ visible: boolean; factorId: string | null }>({ visible: false, factorId: null });
  const [fullRunDates, setFullRunDates] = useState<[dayjs.Dayjs | null, dayjs.Dayjs | null]>([null, null]);
  // Create form state (replaces Form.useForm)
  const [createFactorId, setCreateFactorId] = useState('');
  const [createDesc, setCreateDesc] = useState('');
  const [createCategory, setCreateCategory] = useState('custom');
  const [createComputeMode, setCreateComputeMode] = useState('incremental');

  const loadFactors = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.listFactors();
      setFactors(res.data?.data || []);
    } catch { Toast.error('加载因子列表失败'); }
    setLoading(false);
  }, []);

  const loadHistory = useCallback(async (factorId?: string) => {
    try {
      const res = await productionApi.getProductionHistory(factorId, 30);
      setHistory(res.data?.data || []);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadFactors(); loadHistory(); }, [loadFactors, loadHistory]);

  const handleRun = async (factorId: string, runMode: string, startDate?: string, endDate?: string) => {
    setRunLoading(factorId);
    try {
      const factor = factors.find(f => f.factor_id === factorId);
      const pp = factor?.params?.preprocess || undefined;
      await productionApi.runProduction(factorId, runMode, undefined, startDate, endDate, pp);
      Toast.success(`因子 ${factorId} ${runMode === 'incremental' ? '增量' : '全量'}计算完成`);
      loadFactors();
      loadHistory(selectedFactor || undefined);
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '执行失败');
    }
    setRunLoading(null);
  };

  const handleBatchRun = async (runMode: string) => {
    if (selectedRowKeys.length === 0) { Toast.warning('请先勾选因子'); return; }
    setBatchLoading(true);
    try {
      const res = await productionApi.batchRunFactors(selectedRowKeys, runMode);
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
      };
      const params = { preprocess: createPreprocess };
      await productionApi.createFactor({ ...values, params, code: createCode || undefined });
      Toast.success(`因子 ${values.factor_id} 创建成功`);
      setCreateModal(false);
      setCreateFactorId(''); setCreateDesc(''); setCreateCategory('custom'); setCreateComputeMode('incremental');
      setCreateCode(CODE_TEMPLATE);
      setCreatePreprocess({ ...DEFAULT_PREPROCESS });
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
    { title: '因子ID', dataIndex: 'factor_id', key: 'factor_id', width: 160,
      render: (v: string, r: any) => (
        <span style={{ cursor: 'pointer' }} onClick={() => openDrawer(r)}>
          <Tag color="blue">{v}</Tag>
        </span>
      )
    },
    { title: '描述', dataIndex: 'description', key: 'desc', ellipsis: true },
    { title: '分类', dataIndex: 'category', key: 'cat', width: 90, render: (v: string) => <Tag>{v || '-'}</Tag> },
    { title: '模式', dataIndex: 'compute_mode', key: 'mode', width: 80,
      render: (v: string) => <Tag color={v === 'incremental' ? 'cyan' : 'orange'}>{v === 'incremental' ? '增量' : '全量'}</Tag>
    },
    { title: '最新数据', dataIndex: 'latest_data_date', key: 'latest', width: 110,
      render: (v: string) => v ? <span style={{ color: 'var(--color-gain)' }}>{v}</span> : <span style={{ color: 'var(--text-muted)' }}>-</span>
    },
    { title: '上次计算', dataIndex: 'last_computed_at', key: 'computed', width: 140,
      render: (v: string) => v ? <Tooltip content={v}><span style={{ color: 'var(--text-secondary)' }}>{v.slice(0, 16)}</span></Tooltip> : '-'
    },
    {
      title: '操作', key: 'action', width: 320, render: (_: any, record: any) => (
        <div style={{ display: 'flex', gap: 4 }}>
          <Button size="small" icon={<IconInfoCircle />}
            onClick={() => openDrawer(record)}>详情</Button>
          <Button size="small" theme="solid" icon={<IconPlay />}
            loading={runLoading === record.factor_id}
            onClick={() => handleRun(record.factor_id, 'incremental')}>增量</Button>
          <Button size="small" icon={<IconRefresh />}
            loading={runLoading === record.factor_id}
            onClick={() => { setFullRunModal({ visible: true, factorId: record.factor_id }); setFullRunDates([null, null]); }}>回溯</Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.factor_id)}>
            <Button size="small" type="danger" icon={<IconDelete />} />
          </Popconfirm>
        </div>
      )
    },
  ];

  const historyColumns = [
    { title: '因子', dataIndex: 'factor_id', key: 'fid', render: (v: string) => <Tag color="blue">{v}</Tag> },
    { title: '模式', dataIndex: 'mode', key: 'mode', render: (v: string) => <Tag color={v === 'incremental' ? 'cyan' : 'orange'}>{v === 'incremental' ? '增量' : '全量'}</Tag> },
    { title: '状态', dataIndex: 'status', key: 'status', render: (v: string) => <Tag color={v === 'success' ? 'green' : v === 'running' ? 'blue' : 'red'}>{v}</Tag> },
    { title: '行数', dataIndex: 'rows_affected', key: 'rows', render: (v: number) => v?.toLocaleString() || '-' },
    { title: '耗时', dataIndex: 'duration_seconds', key: 'dur', render: (v: number) => v ? `${v.toFixed(1)}s` : '-' },
    { title: '时间', dataIndex: 'created_at', key: 'time', render: (v: string) => v?.slice(0, 19) },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>已注册因子</span>
          <div style={{ display: 'flex', gap: 8 }}>
            {selectedRowKeys.length > 0 && (
              <Button size="small" theme="solid" icon={<IconBolt />} loading={batchLoading}
                onClick={() => handleBatchRun('incremental')}>批量增量 ({selectedRowKeys.length})</Button>
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

      {/* 新建因子 Modal */}
      <Modal title="新建因子" visible={createModal} onOk={handleCreate} width={820}
        onCancel={() => {
          setCreateModal(false);
          setCreateFactorId(''); setCreateDesc(''); setCreateCategory('custom'); setCreateComputeMode('incremental');
          setCreateCode(CODE_TEMPLATE); setCreatePreprocess({ ...DEFAULT_PREPROCESS });
        }} okText="创建">
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>因子ID <span style={{ color: 'var(--color-loss)' }}>*</span></div>
            <Input size="small" placeholder="如 factor_custom_01" value={createFactorId} onChange={setCreateFactorId} />
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
        <div style={{ marginTop: 8 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>因子计算代码</div>
          <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
            <Editor height="300px" language="python" theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
              value={createCode} onChange={(v) => setCreateCode(v || '')}
              options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4 }} />
          </div>
          <CodeTestPanel code={createCode} />
        </div>
      </Modal>

      <Modal title="全量计算" visible={fullRunModal.visible}
        onCancel={() => setFullRunModal({ visible: false, factorId: null })}
        onOk={() => {
          if (!fullRunModal.factorId) return;
          const sd = fullRunDates[0]?.format('YYYYMMDD');
          const ed = fullRunDates[1]?.format('YYYYMMDD');
          setFullRunModal({ visible: false, factorId: null });
          handleRun(fullRunModal.factorId, 'full', sd, ed);
        }}
        okText="开始计算" cancelText="取消">
        <p style={{ marginBottom: 12 }}>因子: <Tag color="blue">{fullRunModal.factorId}</Tag></p>
        <p style={{ marginBottom: 8 }}>选择日期范围（留空则使用默认252个交易日）:</p>
        <DatePicker type="dateRange" style={{ width: '100%' }}
          value={fullRunDates as any}
          onChange={(dates) => {
            if (dates && Array.isArray(dates) && dates.length === 2) {
              setFullRunDates([dates[0] ? dayjs(dates[0]) : null, dates[1] ? dayjs(dates[1]) : null]);
            } else {
              setFullRunDates([null, null]);
            }
          }} />
      </Modal>

      <FactorDrawer factor={drawerState.factor} open={drawerState.open} initialTab={drawerState.tab}
        onClose={() => setDrawerState({ open: false, factor: null })}
        onSaved={async () => {
          const res = await productionApi.listFactors();
          const list = res.data?.data || [];
          setFactors(list);
          if (drawerState.factor) {
            const updated = list.find((f: any) => f.factor_id === drawerState.factor.factor_id);
            if (updated) setDrawerState(prev => ({ ...prev, factor: updated }));
          }
        }} />
    </div>
  );
};

// ==================== 因子分析 Tab ====================
const AnalysisTab: React.FC = () => {
  const [factors, setFactors] = useState<any[]>([]);
  const [selectedFactor, setSelectedFactor] = useState<string>('');
  const [periods, setPeriods] = useState<number[]>([1, 5, 10]);
  const [quantiles, setQuantiles] = useState(5);
  const [analysisResult, setAnalysisResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);

  useEffect(() => {
    productionApi.listFactors().then(res => setFactors(res.data?.data || [])).catch(() => {});
  }, []);

  const handleRunAnalysis = async () => {
    if (!selectedFactor) { Toast.warning('请选择因子'); return; }
    setRunLoading(true);
    try {
      const res = await productionApi.runAnalysis(selectedFactor, undefined, undefined, periods, quantiles);
      Toast.success('分析完成');
      setAnalysisResult(res.data?.data);
    } catch (e: any) {
      Toast.error(e.response?.data?.detail || '分析失败');
    }
    setRunLoading(false);
  };

  const loadAnalysis = async (factorId: string) => {
    setLoading(true);
    try {
      const res = await productionApi.getAnalysis(factorId);
      setAnalysisResult(res.data?.data);
    } catch { setAnalysisResult(null); }
    setLoading(false);
  };

  const getICChartOption = () => {
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
    if (!analysisResult?.layer_returns) return {};
    const data = analysisResult.layer_returns;
    const periodGroups = [...new Set(data.map((d: any) => d.period))];
    const quantileGroups = [...new Set(data.map((d: any) => d.quantile))];
    const colors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0077FA'];
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: quantileGroups as string[], textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: { type: 'category', data: periodGroups.map((p: any) => `${p}D`), axisLabel: { color: '#94A3B8' } },
      yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
      series: quantileGroups.map((q: any, i: number) => ({
        name: q, type: 'bar',
        data: periodGroups.map((p: any) => { const item = data.find((d: any) => d.period === p && d.quantile === q); return item?.mean_return || 0; }),
        itemStyle: { color: colors[i % colors.length] },
      }))
    };
  };

  const icColumns = [
    { title: '周期', dataIndex: 'period', key: 'period', render: (v: number) => `${v}D` },
    { title: 'IC均值', dataIndex: 'ic_mean', key: 'ic_mean', render: (v: number) => <span style={{ color: v > 0 ? 'var(--color-gain)' : 'var(--color-loss)' }}>{v?.toFixed(4)}</span> },
    { title: 'IC标准差', dataIndex: 'ic_std', key: 'ic_std', render: (v: number) => v?.toFixed(4) },
    { title: 'ICIR', dataIndex: 'icir', key: 'icir', render: (v: number) => <span style={{ color: Math.abs(v) > 0.5 ? 'var(--color-primary)' : 'var(--text-secondary)', fontWeight: Math.abs(v) > 0.5 ? 700 : 400 }}>{v?.toFixed(4)}</span> },
    { title: 'IC>0占比', dataIndex: 'ic_positive_ratio', key: 'ratio', render: (v: number) => `${(v * 100).toFixed(1)}%` },
    { title: 'p值', dataIndex: 'p_value', key: 'p', render: (v: number) => <span style={{ color: v < 0.05 ? 'var(--color-gain)' : 'var(--color-loss)' }}>{v?.toFixed(4)}</span> },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
          <Select placeholder="选择因子" style={{ width: 180 }} value={selectedFactor || undefined}
            onChange={(v) => { setSelectedFactor(v as string); loadAnalysis(v as string); }}
            optionList={factors.map(f => ({ label: `${f.factor_id}`, value: f.factor_id }))} />
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>周期:</span>
          <Select multiple style={{ width: 200 }} value={periods} onChange={v => setPeriods(v as number[])}
            optionList={[1,2,3,5,10,20].map(v => ({ label: `${v}D`, value: v }))} />
          <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>分层:</span>
          <InputNumber min={3} max={10} value={quantiles} onChange={v => v && setQuantiles(v as number)} size="small" style={{ width: 60 }} />
          <Button theme="solid" icon={<IconBarChartHStroked />} loading={runLoading} onClick={handleRunAnalysis}>运行分析</Button>
        </div>
      </Card>

      {loading ? <Spin style={{ display: 'block', margin: '60px auto' }} /> : analysisResult ? (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
            <Card style={{ background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--color-primary)' }}>IC 分析</span>}>
              <ReactECharts option={getICChartOption()} style={{ height: 240 }} />
            </Card>
            <Card style={{ background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--color-accent, #14C9C9)' }}>分层收益</span>}>
              <ReactECharts option={getLayerReturnOption()} style={{ height: 240 }} />
            </Card>
          </div>
          <Card style={{ background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--text-secondary)' }}>IC 详细指标</span>}>
            <Table dataSource={analysisResult.ic_summary || []} columns={icColumns}
              rowKey="period" size="small" pagination={false} />
          </Card>
        </>
      ) : <Empty description="选择因子并运行分析" style={{ marginTop: 60 }} />}
    </div>
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
    </Tabs>
  </div>
);

export default FactorCenter;
