import React, { useState, useEffect, useCallback } from 'react';
import { Tabs, Table, Button, Card, Tag, Space, Select, message, InputNumber, Spin, Empty, Modal, Input, Form, Popconfirm, Checkbox, Tooltip, Drawer, Descriptions, Statistic, Row, Col, DatePicker, Alert } from 'antd';
import { ExperimentOutlined, PlayCircleOutlined, ReloadOutlined, BarChartOutlined, PlusOutlined, DeleteOutlined, EditOutlined, ThunderboltOutlined, CodeOutlined, DatabaseOutlined, InfoCircleOutlined, SearchOutlined, SaveOutlined, SettingOutlined, BugOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import ReactECharts from 'echarts-for-react';
import Editor from '@monaco-editor/react';
import { productionApi, DEFAULT_PREPROCESS } from '../api';
import type { PreprocessOptions } from '../api';

// ==================== 因子详情/编辑 统一 Drawer ====================
interface FactorDrawerProps {
  factor: any;
  open: boolean;
  initialTab?: string;
  onClose: () => void;
  onSaved: () => void;
}
const FactorDrawer: React.FC<FactorDrawerProps> = ({ factor, open, initialTab, onClose, onSaved }) => {
  const factorId = factor?.factor_id;
  const [activeTab, setActiveTab] = useState('edit');
  // 编辑
  const [editForm] = Form.useForm();
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
    editForm.setFieldsValue({
      description: factor.description,
      category: factor.category,
      compute_mode: factor.compute_mode,
    });
    // 预处理
    const pp = factor.params?.preprocess || {};
    setPpEdit({ ...DEFAULT_PREPROCESS, ...pp });
    // 统计
    setStatsLoading(true);
    productionApi.getFactorStats(factorId).then(r => setStats(r.data?.data)).catch(() => {}).finally(() => setStatsLoading(false));
  }, [factor, open, initialTab, factorId, editForm]);

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
      const values = await editForm.validateFields();
      // 只更新基本信息字段，不要覆盖 params
      await productionApi.updateFactor(factorId, values);
      message.success('基本信息已保存');
      onSaved();
    } catch (e: any) {
      if (e.response) message.error(e.response?.data?.detail || '保存失败');
    }
    setEditSaving(false);
  };

  // 保存预处理
  const handleSavePp = async () => {
    if (!factor) return;
    setPpSaving(true);
    try {
      // 只更新 params.preprocess，保留其他 params 字段
      const newParams = { ...(factor.params || {}), preprocess: ppEdit };
      await productionApi.updateFactor(factorId, { params: newParams });
      message.success('预处理配置已保存');
      onSaved();
    } catch (e: any) {
      message.error(e.response?.data?.detail || '保存失败');
    }
    setPpSaving(false);
  };

  // 保存代码
  const handleSaveCode = async () => {
    if (!factorId || !code) return;
    setCodeSaving(true);
    try {
      await productionApi.updateFactorCode(factorId, code.filename, editedCode);
      message.success('代码已保存');
      setCodeChanged(false);
    } catch (e: any) { message.error(e.response?.data?.detail || '保存失败'); }
    setCodeSaving(false);
  };

  const dataColumns = [
    { title: '股票代码', dataIndex: 'ts_code', key: 'ts_code', width: 120 },
    { title: '交易日期', dataIndex: 'trade_date', key: 'trade_date', width: 120 },
    { title: '因子值', dataIndex: 'factor_value', key: 'factor_value', render: (v: number) => v?.toFixed(6) },
  ];

  return (
    <Drawer title={<span style={{ color: '#00d4ff' }}>📊 {factorId}</span>} open={open} onClose={onClose}
      width={780} styles={{ body: { background: '#0f172a', padding: '12px 16px' }, header: { background: '#1e293b', borderBottom: '1px solid #334155' } }}>
      <Tabs activeKey={activeTab} onChange={setActiveTab} size="small" className="tech-tabs" items={[
        /* ---- 编辑信息 ---- */
        { key: 'edit', label: <span><EditOutlined /> 编辑</span>, children: (
          <div>
            <Form form={editForm} layout="vertical" size="small">
              <Row gutter={16}>
                <Col span={12}><Form.Item name="description" label="描述"><Input /></Form.Item></Col>
                <Col span={6}>
                  <Form.Item name="category" label="分类">
                    <Select options={['momentum','value','technical','quality','custom'].map(v => ({ label: v, value: v }))} />
                  </Form.Item>
                </Col>
                <Col span={6}>
                  <Form.Item name="compute_mode" label="计算模式">
                    <Select options={[{ label: '增量', value: 'incremental' }, { label: '全量', value: 'full' }]} />
                  </Form.Item>
                </Col>
              </Row>
            </Form>
            {/* 统计概览 */}
            <Spin spinning={statsLoading}>
              {stats ? (
                <div style={{ marginTop: 4 }}>
                  <Row gutter={[12, 12]}>
                    <Col span={6}><Card size="small" className="tech-card"><Statistic title="总行数" value={stats.total_rows} valueStyle={{ color: '#00d4ff', fontSize: 16 }} /></Card></Col>
                    <Col span={6}><Card size="small" className="tech-card"><Statistic title="股票数" value={stats.stock_count} valueStyle={{ color: '#10b981', fontSize: 16 }} /></Card></Col>
                    <Col span={6}><Card size="small" className="tech-card"><Statistic title="起始日期" value={stats.min_date || '-'} valueStyle={{ fontSize: 13, color: '#94a3b8' }} /></Card></Col>
                    <Col span={6}><Card size="small" className="tech-card"><Statistic title="截止日期" value={stats.max_date || '-'} valueStyle={{ fontSize: 13, color: '#94a3b8' }} /></Card></Col>
                  </Row>
                  <Card size="small" className="tech-card" style={{ marginTop: 8 }} title={<span style={{ color: '#94a3b8', fontSize: 12 }}>分布统计</span>}>
                    <Descriptions size="small" column={2} labelStyle={{ color: '#64748b' }} contentStyle={{ color: '#e2e8f0' }}>
                      <Descriptions.Item label="均值">{stats.mean_val?.toFixed(6) ?? '-'}</Descriptions.Item>
                      <Descriptions.Item label="标准差">{stats.std_val?.toFixed(6) ?? '-'}</Descriptions.Item>
                      <Descriptions.Item label="最小值">{stats.min_val?.toFixed(6) ?? '-'}</Descriptions.Item>
                      <Descriptions.Item label="最大值">{stats.max_val?.toFixed(6) ?? '-'}</Descriptions.Item>
                    </Descriptions>
                  </Card>
                </div>
              ) : null}
            </Spin>
            <div style={{ marginTop: 12, textAlign: 'right' }}>
              <Button type="primary" icon={<SaveOutlined />} loading={editSaving} onClick={handleSaveEdit}>保存信息</Button>
            </div>
          </div>
        )},
        /* ---- 预处理配置 ---- */
        { key: 'preprocess', label: <span><SettingOutlined /> 预处理</span>, children: (
          <div>
            <Row gutter={[16, 16]} style={{ marginTop: 4 }}>
              <Col span={12}>
                <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 4 }}>复权方式</div>
                <Select size="small" style={{ width: '100%' }} value={ppEdit.adjust_price}
                  onChange={(v) => setPpEdit(p => ({ ...p, adjust_price: v }))}>
                  <Select.Option value="forward">前复权</Select.Option>
                  <Select.Option value="backward">后复权</Select.Option>
                  <Select.Option value="none">不复权</Select.Option>
                </Select>
              </Col>
              <Col span={12}>
                <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 4 }}>新股排除天数</div>
                <InputNumber size="small" min={1} max={250} value={ppEdit.new_stock_days}
                  disabled={!ppEdit.filter_new_stock} style={{ width: '100%' }}
                  onChange={(v) => setPpEdit(p => ({ ...p, new_stock_days: v || 60 }))} />
              </Col>
              <Col span={6}><Checkbox checked={ppEdit.filter_st} onChange={(e) => setPpEdit(p => ({ ...p, filter_st: e.target.checked }))}>过滤 ST</Checkbox></Col>
              <Col span={6}><Checkbox checked={ppEdit.filter_new_stock} onChange={(e) => setPpEdit(p => ({ ...p, filter_new_stock: e.target.checked }))}>过滤新股</Checkbox></Col>
              <Col span={6}><Checkbox checked={ppEdit.handle_suspension} onChange={(e) => setPpEdit(p => ({ ...p, handle_suspension: e.target.checked }))}>停牌处理</Checkbox></Col>
              <Col span={6}><Checkbox checked={ppEdit.mark_limit} onChange={(e) => setPpEdit(p => ({ ...p, mark_limit: e.target.checked }))}>涨跌停标记</Checkbox></Col>
            </Row>
            <div style={{ marginTop: 16, textAlign: 'right' }}>
              <Button type="primary" icon={<SaveOutlined />} loading={ppSaving} onClick={handleSavePp}>保存预处理</Button>
            </div>
          </div>
        )},
        /* ---- 源代码 + 测试 ---- */
        { key: 'code', label: <span><CodeOutlined /> 代码</span>, children: (
          <Spin spinning={codeLoading}>
            {code ? (
              <div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                  <Tag color="blue">{code.filename}</Tag>
                  <Button size="small" type="primary" icon={<SaveOutlined />} disabled={!codeChanged}
                    loading={codeSaving} onClick={handleSaveCode}>保存代码</Button>
                </div>
                <div style={{ border: '1px solid #334155', borderRadius: 4, overflow: 'hidden' }}>
                  <Editor height="380px" language="python" theme="vs-dark"
                    value={editedCode} onChange={(v) => { setEditedCode(v || ''); setCodeChanged(true); }}
                    options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4 }} />
                </div>
                <CodeTestPanel code={editedCode} />
              </div>
            ) : <Empty description="未找到源代码文件" />}
          </Spin>
        )},
        /* ---- 数据探查 ---- */
        { key: 'data', label: <span><DatabaseOutlined /> 数据</span>, children: (
          <div>
            <Space style={{ marginBottom: 12 }} wrap>
              <Input size="small" placeholder="股票代码" style={{ width: 120 }} allowClear
                onChange={(e) => setDataFilter(f => ({ ...f, ts_code: e.target.value || undefined }))} />
              <Input size="small" placeholder="起始日期 (yyyymmdd)" style={{ width: 160 }} allowClear
                onChange={(e) => setDataFilter(f => ({ ...f, start_date: e.target.value || undefined }))} />
              <Input size="small" placeholder="截止日期 (yyyymmdd)" style={{ width: 160 }} allowClear
                onChange={(e) => setDataFilter(f => ({ ...f, end_date: e.target.value || undefined }))} />
              <Button size="small" type="primary" icon={<SearchOutlined />} onClick={loadData}>查询</Button>
            </Space>
            <Table dataSource={factorData} columns={dataColumns} rowKey={(r) => `${r.ts_code}-${r.trade_date}`}
              loading={dataLoading} size="small" pagination={{ pageSize: 15 }} className="tech-table"
              scroll={{ y: 400 }} />
          </div>
        )},
      ]} />
    </Drawer>
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
    if (!code.trim()) { message.warning('请先编写因子代码'); return; }
    if (!dateRange[0] || !dateRange[1]) { message.warning('请选择测试日期范围'); return; }
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
      render: (v: number) => v != null ? v.toFixed(6) : <span style={{ color: '#64748b' }}>null</span> },
  ];

  const logColorMap: Record<string, string> = {
    info: '#94a3b8', error: '#ef4444', warn: '#f59e0b',
  };
  const phaseColorMap: Record<string, string> = {
    compile: '#818cf8', exec: '#38bdf8', resolve: '#a78bfa',
    data: '#34d399', compute: '#fbbf24', validate: '#fb923c', result: '#22d3ee',
  };

  return (
    <div style={{ marginTop: 8, borderTop: '1px solid #334155', paddingTop: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{ color: '#94a3b8', fontSize: 12, whiteSpace: 'nowrap' }}>测试区间:</span>
        <DatePicker.RangePicker size="small" style={{ flex: 1 }}
          onChange={(_, ds) => setDateRange([ds[0].replace(/-/g, ''), ds[1].replace(/-/g, '')])}
          placeholder={['开始日期', '结束日期']} />
        <Button size="small" type="primary" icon={<BugOutlined />}
          loading={testing} onClick={handleTest}>编译测试</Button>
      </div>

      {/* 日志面板 */}
      {(testLogs.length > 0 || testStdout || testError) && (
        <div style={{ marginBottom: 8 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <span style={{ color: '#94a3b8', fontSize: 11, cursor: 'pointer' }}
              onClick={() => setShowLogs(!showLogs)}>
              {showLogs ? '▼' : '▶'} 执行日志 ({testLogs.length})
            </span>
          </div>
          {showLogs && (
            <div style={{
              background: '#0c1222', border: '1px solid #1e293b', borderRadius: 4,
              padding: '6px 8px', maxHeight: 200, overflowY: 'auto',
              fontFamily: 'monospace', fontSize: 11, lineHeight: '18px',
            }}>
              {testLogs.map((log: any, i: number) => (
                <div key={i} style={{ color: logColorMap[log.level] || '#94a3b8' }}>
                  <span style={{ color: phaseColorMap[log.phase] || '#64748b', marginRight: 6 }}>
                    [{log.phase}]
                  </span>
                  <span style={{ whiteSpace: 'pre-wrap' }}>{log.message}</span>
                </div>
              ))}
              {testStdout && (
                <div style={{ borderTop: '1px solid #1e293b', marginTop: 4, paddingTop: 4 }}>
                  <span style={{ color: '#64748b' }}>[stdout]</span>
                  <pre style={{ color: '#e2e8f0', margin: '2px 0 0 0', whiteSpace: 'pre-wrap', fontSize: 11 }}>{testStdout}</pre>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {testError && <Alert type="error" message={testError} showIcon closable style={{ marginBottom: 8, fontSize: 12 }}
        onClose={() => setTestError(null)} />}
      {testResult && (
        <div>
          <Row gutter={8} style={{ marginBottom: 8 }}>
            <Col span={5}><Statistic title="总行数" value={testResult.stats?.total_rows} valueStyle={{ color: '#00d4ff', fontSize: 14 }} /></Col>
            <Col span={5}><Statistic title="股票数" value={testResult.stats?.stock_count} valueStyle={{ color: '#10b981', fontSize: 14 }} /></Col>
            <Col span={5}><Statistic title="均值" value={testResult.stats?.factor_mean ?? '-'} precision={4} valueStyle={{ fontSize: 14, color: '#e2e8f0' }} /></Col>
            <Col span={5}><Statistic title="标准差" value={testResult.stats?.factor_std ?? '-'} precision={4} valueStyle={{ fontSize: 14, color: '#e2e8f0' }} /></Col>
            <Col span={4}><Statistic title="空值" value={testResult.stats?.null_count} valueStyle={{ fontSize: 14, color: testResult.stats?.null_count > 0 ? '#f59e0b' : '#10b981' }} /></Col>
          </Row>
          {testResult.truncated && <Alert type="warning" message="结果已截断，仅显示前 2000 行" showIcon style={{ marginBottom: 8, fontSize: 12 }} />}
          <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
            <Select size="small" allowClear placeholder="筛选股票" style={{ width: 160 }}
              value={filterStock} onChange={setFilterStock} showSearch
              options={testResult.stocks?.map((s: string) => ({ label: s, value: s }))} />
            <Select size="small" allowClear placeholder="筛选日期" style={{ width: 140 }}
              value={filterDate} onChange={setFilterDate} showSearch
              options={testResult.dates?.map((d: string) => ({ label: d, value: d }))} />
            <span style={{ color: '#64748b', fontSize: 12, lineHeight: '24px' }}>
              显示 {filteredPreview.length} 条
            </span>
          </div>
          <Table dataSource={filteredPreview} columns={resultColumns}
            rowKey={(r) => `${r.ts_code}-${r.trade_date}`}
            size="small" pagination={{ pageSize: 10, size: 'small' }}
            scroll={{ y: 240 }} className="tech-table" />
        </div>
      )}
    </div>
  );
};

// ==================== 因子管理 Tab ====================
const FactorManageTab: React.FC = () => {
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
  const [form] = Form.useForm();

  const loadFactors = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.listFactors();
      setFactors(res.data?.data || []);
    } catch { message.error('加载因子列表失败'); }
    setLoading(false);
  }, []);

  const loadHistory = useCallback(async (factorId?: string) => {
    try {
      const res = await productionApi.getProductionHistory(factorId, 30);
      setHistory(res.data?.data || []);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadFactors(); loadHistory(); }, [loadFactors, loadHistory]);

  const handleRun = async (factorId: string, mode: string, startDate?: string, endDate?: string) => {
    setRunLoading(factorId);
    try {
      const factor = factors.find(f => f.factor_id === factorId);
      const pp = factor?.params?.preprocess || undefined;
      await productionApi.runProduction(factorId, mode, undefined, startDate, endDate, pp);
      message.success(`因子 ${factorId} ${mode === 'incremental' ? '增量' : '全量'}计算完成`);
      loadFactors();
      loadHistory(selectedFactor || undefined);
    } catch (e: any) {
      message.error(e.response?.data?.detail || '执行失败');
    }
    setRunLoading(null);
  };

  const handleBatchRun = async (mode: string) => {
    if (selectedRowKeys.length === 0) { message.warning('请先勾选因子'); return; }
    setBatchLoading(true);
    try {
      // 批量运行不传 preprocess，由后端从各因子 params 中读取
      const res = await productionApi.batchRunFactors(selectedRowKeys, mode);
      const results = res.data?.data || [];
      const ok = results.filter((r: any) => r.success).length;
      const fail = results.length - ok;
      message.success(`批量计算完成: ${ok} 成功, ${fail} 失败`);
      setSelectedRowKeys([]);
      loadFactors();
      loadHistory(selectedFactor || undefined);
    } catch (e: any) {
      message.error(e.response?.data?.detail || '批量执行失败');
    }
    setBatchLoading(false);
  };

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const params = { ...(values.params || {}), preprocess: createPreprocess };
      await productionApi.createFactor({ ...values, params, code: createCode || undefined });
      message.success(`因子 ${values.factor_id} 创建成功`);
      setCreateModal(false);
      form.resetFields();
      setCreateCode(CODE_TEMPLATE);
      setCreatePreprocess({ ...DEFAULT_PREPROCESS });
      loadFactors();
    } catch (e: any) {
      if (e.response) message.error(e.response?.data?.detail || '创建失败');
    }
  };

  const handleDelete = async (factorId: string) => {
    try {
      await productionApi.deleteFactor(factorId, false);
      message.success(`因子 ${factorId} 已删除`);
      loadFactors();
    } catch (e: any) {
      message.error(e.response?.data?.detail || '删除失败');
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
      render: (v: string) => v ? <span style={{ color: '#10b981' }}>{v}</span> : <span style={{ color: '#64748b' }}>-</span>
    },
    { title: '上次计算', dataIndex: 'last_computed_at', key: 'computed', width: 140,
      render: (v: string) => v ? <Tooltip title={v}><span style={{ color: '#94a3b8' }}>{v.slice(0, 16)}</span></Tooltip> : '-'
    },
    {
      title: '操作', key: 'action', width: 320, render: (_: any, record: any) => (
        <Space size={4}>
          <Button size="small" icon={<InfoCircleOutlined />}
            onClick={() => openDrawer(record)}>详情</Button>
          <Button size="small" type="primary" icon={<PlayCircleOutlined />}
            loading={runLoading === record.factor_id}
            onClick={() => handleRun(record.factor_id, 'incremental')}>增量</Button>
          <Button size="small" icon={<ReloadOutlined />}
            loading={runLoading === record.factor_id}
            onClick={() => { setFullRunModal({ visible: true, factorId: record.factor_id }); setFullRunDates([null, null]); }}>回溯</Button>
          <Popconfirm title="确认删除?" onConfirm={() => handleDelete(record.factor_id)}>
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
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
      <Card className="tech-card" style={{ marginBottom: 16 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: '#00d4ff', fontWeight: 600, fontSize: 15 }}>📋 已注册因子</span>
          <Space>
            {selectedRowKeys.length > 0 && (
              <>
                <Button size="small" type="primary" icon={<ThunderboltOutlined />} loading={batchLoading}
                  onClick={() => handleBatchRun('incremental')}>批量增量 ({selectedRowKeys.length})</Button>
              </>
            )}
            <Button size="small" icon={<PlusOutlined />} onClick={() => setCreateModal(true)}>新建因子</Button>
            <Button icon={<ReloadOutlined />} onClick={loadFactors} size="small">刷新</Button>
          </Space>
        </div>
        <Table dataSource={factors} columns={factorColumns} rowKey="factor_id"
          loading={loading} size="small" pagination={false} className="tech-table"
          rowSelection={{ selectedRowKeys, onChange: (keys) => setSelectedRowKeys(keys as string[]) }} />
      </Card>

      <Card className="tech-card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: '#94a3b8', fontWeight: 600, fontSize: 15 }}>📜 计算历史</span>
          <Select allowClear placeholder="筛选因子" style={{ width: 160 }} size="small"
            value={selectedFactor} onChange={(v) => { setSelectedFactor(v); loadHistory(v || undefined); }}
            options={factors.map(f => ({ label: f.factor_id, value: f.factor_id }))} />
        </div>
        <Table dataSource={history} columns={historyColumns} rowKey={(r) => `${r.factor_id}-${r.created_at}`}
          size="small" pagination={{ pageSize: 10 }} className="tech-table" />
      </Card>

      {/* 新建因子 Modal */}
      <Modal title="新建因子" open={createModal} onOk={handleCreate} width={820}
        onCancel={() => { setCreateModal(false); form.resetFields(); setCreateCode(CODE_TEMPLATE); setCreatePreprocess({ ...DEFAULT_PREPROCESS }); }} okText="创建">
        <Form form={form} layout="vertical" size="small">
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="factor_id" label="因子ID" rules={[{ required: true, message: '请输入因子ID' }]}>
                <Input placeholder="如 factor_custom_01" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="description" label="描述"><Input placeholder="因子描述" /></Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item name="category" label="分类" initialValue="custom">
                <Select options={['momentum','value','technical','quality','custom'].map(v => ({ label: v, value: v }))} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="compute_mode" label="计算模式" initialValue="incremental">
                <Select options={[{ label: '增量', value: 'incremental' }, { label: '全量', value: 'full' }]} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="复权方式">
                <Select size="small" value={createPreprocess.adjust_price}
                  onChange={(v) => setCreatePreprocess(p => ({ ...p, adjust_price: v }))}>
                  <Select.Option value="forward">前复权</Select.Option>
                  <Select.Option value="backward">后复权</Select.Option>
                  <Select.Option value="none">不复权</Select.Option>
                </Select>
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={6}><Checkbox checked={createPreprocess.filter_st} onChange={(e) => setCreatePreprocess(p => ({ ...p, filter_st: e.target.checked }))}>过滤 ST</Checkbox></Col>
            <Col span={6}><Checkbox checked={createPreprocess.filter_new_stock} onChange={(e) => setCreatePreprocess(p => ({ ...p, filter_new_stock: e.target.checked }))}>过滤新股</Checkbox></Col>
            <Col span={6}><Checkbox checked={createPreprocess.handle_suspension} onChange={(e) => setCreatePreprocess(p => ({ ...p, handle_suspension: e.target.checked }))}>停牌处理</Checkbox></Col>
            <Col span={6}><Checkbox checked={createPreprocess.mark_limit} onChange={(e) => setCreatePreprocess(p => ({ ...p, mark_limit: e.target.checked }))}>涨跌停标记</Checkbox></Col>
          </Row>
          {createPreprocess.filter_new_stock && (
            <Form.Item label="新股排除天数" style={{ marginTop: 8, marginBottom: 8 }}>
              <InputNumber size="small" min={1} max={250} value={createPreprocess.new_stock_days}
                onChange={(v) => setCreatePreprocess(p => ({ ...p, new_stock_days: v || 60 }))} />
            </Form.Item>
          )}
          <Form.Item label="因子计算代码" style={{ marginTop: 8 }}>
            <div style={{ border: '1px solid #334155', borderRadius: 4, overflow: 'hidden' }}>
              <Editor height="300px" language="python" theme="vs-dark"
                value={createCode} onChange={(v) => setCreateCode(v || '')}
                options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4 }} />
            </div>
            <CodeTestPanel code={createCode} />
          </Form.Item>
        </Form>
      </Modal>

      <Modal title="全量计算" open={fullRunModal.visible}
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
        <DatePicker.RangePicker style={{ width: '100%' }}
          value={fullRunDates}
          onChange={(dates) => setFullRunDates(dates ? [dates[0], dates[1]] : [null, null])} />
      </Modal>

      <FactorDrawer factor={drawerState.factor} open={drawerState.open} initialTab={drawerState.tab}
        onClose={() => setDrawerState({ open: false, factor: null })}
        onSaved={async () => {
          const res = await productionApi.listFactors();
          const list = res.data?.data || [];
          setFactors(list);
          // 同步更新 drawer 中的 factor 对象，避免后续保存用旧值
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
    if (!selectedFactor) { message.warning('请选择因子'); return; }
    setRunLoading(true);
    try {
      const res = await productionApi.runAnalysis(selectedFactor, undefined, undefined, periods, quantiles);
      message.success('分析完成');
      setAnalysisResult(res.data?.data);
    } catch (e: any) {
      message.error(e.response?.data?.detail || '分析失败');
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
      legend: { textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: { type: 'category', data: data.map((d: any) => `${d.period}D`), axisLabel: { color: '#94a3b8' } },
      yAxis: { type: 'value', axisLabel: { color: '#94a3b8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.1)' } } },
      series: [
        { name: 'IC均值', type: 'bar', data: data.map((d: any) => d.ic_mean?.toFixed(4)), itemStyle: { color: '#00d4ff' } },
        { name: 'ICIR', type: 'bar', data: data.map((d: any) => d.icir?.toFixed(4)), itemStyle: { color: '#7c3aed' } },
      ]
    };
  };

  const getLayerReturnOption = () => {
    if (!analysisResult?.layer_returns) return {};
    const data = analysisResult.layer_returns;
    const periodGroups = [...new Set(data.map((d: any) => d.period))];
    const quantileGroups = [...new Set(data.map((d: any) => d.quantile))];
    const colors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#00d4ff'];
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: quantileGroups as string[], textStyle: { color: '#94a3b8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: { type: 'category', data: periodGroups.map((p: any) => `${p}D`), axisLabel: { color: '#94a3b8' } },
      yAxis: { type: 'value', axisLabel: { color: '#94a3b8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.1)' } } },
      series: quantileGroups.map((q: any, i: number) => ({
        name: q, type: 'bar',
        data: periodGroups.map((p: any) => { const item = data.find((d: any) => d.period === p && d.quantile === q); return item?.mean_return || 0; }),
        itemStyle: { color: colors[i % colors.length] },
      }))
    };
  };

  const icColumns = [
    { title: '周期', dataIndex: 'period', key: 'period', render: (v: number) => `${v}D` },
    { title: 'IC均值', dataIndex: 'ic_mean', key: 'ic_mean', render: (v: number) => <span style={{ color: v > 0 ? '#10b981' : '#ef4444' }}>{v?.toFixed(4)}</span> },
    { title: 'IC标准差', dataIndex: 'ic_std', key: 'ic_std', render: (v: number) => v?.toFixed(4) },
    { title: 'ICIR', dataIndex: 'icir', key: 'icir', render: (v: number) => <span style={{ color: Math.abs(v) > 0.5 ? '#00d4ff' : '#94a3b8', fontWeight: Math.abs(v) > 0.5 ? 700 : 400 }}>{v?.toFixed(4)}</span> },
    { title: 'IC>0占比', dataIndex: 'ic_positive_ratio', key: 'ratio', render: (v: number) => `${(v * 100).toFixed(1)}%` },
    { title: 'p值', dataIndex: 'p_value', key: 'p', render: (v: number) => <span style={{ color: v < 0.05 ? '#10b981' : '#ef4444' }}>{v?.toFixed(4)}</span> },
  ];

  return (
    <div>
      <Card className="tech-card" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Select placeholder="选择因子" style={{ width: 180 }} value={selectedFactor || undefined}
            onChange={(v) => { setSelectedFactor(v); loadAnalysis(v); }}
            options={factors.map(f => ({ label: `${f.factor_id}`, value: f.factor_id }))} />
          <span style={{ color: '#94a3b8', fontSize: 12 }}>周期:</span>
          <Select mode="multiple" style={{ width: 200 }} value={periods} onChange={setPeriods}
            options={[1,2,3,5,10,20].map(v => ({ label: `${v}D`, value: v }))} />
          <span style={{ color: '#94a3b8', fontSize: 12 }}>分层:</span>
          <InputNumber min={3} max={10} value={quantiles} onChange={v => v && setQuantiles(v)} size="small" style={{ width: 60 }} />
          <Button type="primary" icon={<BarChartOutlined />} loading={runLoading} onClick={handleRunAnalysis}>运行分析</Button>
        </Space>
      </Card>

      {loading ? <Spin style={{ display: 'block', margin: '60px auto' }} /> : analysisResult ? (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
            <Card className="tech-card" title={<span style={{ color: '#00d4ff' }}>IC 分析</span>} size="small">
              <ReactECharts option={getICChartOption()} style={{ height: 240 }} />
            </Card>
            <Card className="tech-card" title={<span style={{ color: '#7c3aed' }}>分层收益</span>} size="small">
              <ReactECharts option={getLayerReturnOption()} style={{ height: 240 }} />
            </Card>
          </div>
          <Card className="tech-card" title={<span style={{ color: '#94a3b8' }}>IC 详细指标</span>} size="small">
            <Table dataSource={analysisResult.ic_summary || []} columns={icColumns}
              rowKey="period" size="small" pagination={false} className="tech-table" />
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
        color: '#00d4ff', fontSize: '24px', fontWeight: 700, margin: 0,
        textShadow: '0 0 20px rgba(0, 212, 255, 0.5)', letterSpacing: '1px'
      }}>
        <ExperimentOutlined style={{ marginRight: '8px' }} />
        因子中心
      </h1>
      <p style={{ color: '#94a3b8', margin: '4px 0 0 0', fontSize: '12px' }}>
        因子注册管理与 IC 分析
      </p>
    </div>

    <Tabs
      defaultActiveKey="factors"
      className="tech-tabs"
      items={[
        { key: 'factors', label: <span><ExperimentOutlined /> 因子管理</span>, children: <FactorManageTab /> },
        { key: 'analysis', label: <span><BarChartOutlined /> 因子分析</span>, children: <AnalysisTab /> },
      ]}
    />
  </div>
);

export default FactorCenter;