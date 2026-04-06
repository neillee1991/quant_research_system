/**
 * 因子编辑抽屉组件
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Drawer, Tabs, Button, Input, Select, InputNumber,
  Collapse, Card, Spin, Empty, Table, Tooltip, Checkbox,
} from 'antd';
import {
  EditOutlined, SaveOutlined, CodeOutlined, DatabaseOutlined, SearchOutlined, BarChartOutlined,
} from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import Editor from '@monaco-editor/react';
import { productionApi, DEFAULT_PREPROCESS } from '../../api';
import { useThemeStore } from '../../store';
import { formatCode } from '../../utils/codeFormatter';
import type { PreprocessOptions, FactorValue, FactorAnalysisResult } from '../../types';
import type { FactorDrawerProps, FactorCodeInfo, DataConfigLabel } from './types';
import TestPanel from './TestPanel';
import { DataInspection } from '../../components/DataInspection';
import QuantDatePicker from '../../components/QuantDatePicker';
import TaskLogTable from '../../components/TaskLogTable';

const FactorDrawer: React.FC<FactorDrawerProps> = ({ factor, open, initialTab, onClose, onSaved }) => {
  const message = useMessage();
  const { mode } = useThemeStore();
  const factorId = factor?.factor_id;
  const [activeTab, setActiveTab] = useState<string>('edit');

  // 编辑状态
  const [editDesc, setEditDesc] = useState<string>('');
  const [editCategory, setEditCategory] = useState<string>('');
  const [editComputeMode, setEditComputeMode] = useState<string>('');
  const [editDependsOn, setEditDependsOn] = useState<string[]>([]);
  const [editLookbackDays, setEditLookbackDays] = useState<number>(60);
  const [editAlignCalendar, setEditAlignCalendar] = useState<boolean>(false);
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
  const [history, setHistory] = useState<any[]>([]);
  const [historyLoading, setHistoryLoading] = useState<boolean>(false);
  const [filterStartDate, setFilterStartDate] = useState<string>('');
  const [filterEndDate, setFilterEndDate] = useState<string>('');

  // 数据源注解
  const [dataConfigLabels, setDataConfigLabels] = useState<Record<string, DataConfigLabel>>({});

  // 可用表列表
  const [availableTables, setAvailableTables] = useState<Array<{value: string; label: string; description: string; type: string}>>([]);

  // 编辑器引用
  const codeEditorRef = useRef<unknown>(null);

  // 格式化代码
  const handleFormatCode = async (): Promise<void> => {
    try {
      const formatted = await formatCode(editedCode, 'python');
      setEditedCode(formatted);
      setCodeChanged(true);
      message.success('代码格式化成功');
    } catch (error: any) {
      message.error(error.message || '格式化失败');
    }
  };

  // 打开时初始化
  useEffect(() => {
    console.log('[FactorDrawer] useEffect triggered, factor=', factor?.factor_id, 'open=', open, 'factorId=', factorId);
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
    setEditLookbackDays(params?.lookback_days ?? 60);
    setEditAlignCalendar(factor.align_calendar ?? false);

    // 预处理
    const pp = params?.preprocess || {};
    console.log('[FactorDrawer] 加载因子配置:', { factor_id: factor.factor_id, params: factor.params, preprocess: pp });
    setPpEdit({ ...DEFAULT_PREPROCESS, ...pp });

    // 统计
    setStatsLoading(true);
    if (factorId) {
      productionApi.getFactorStats(factorId).then(r => setStats(r.data?.data)).catch((error) => {
        console.error('Failed to load factor stats:', error);
      }).finally(() => setStatsLoading(false));
    }

    // 代码 - 直接加载
    if (factorId) {
      setCodeLoading(true);
      console.log('[FactorDrawer] 开始加载代码, factorId=', factorId);
      productionApi.getFactorCode(factorId).then(res => {
        console.log('[FactorDrawer] 代码加载成功, data=', res.data?.data);
        const d = res.data?.data;
        setCode(d);
        setEditedCode(d?.code || '');
      }).catch((error) => {
        console.error('[FactorDrawer] 代码加载失败:', error);
        setCode(null);
      }).finally(() => {
        console.log('[FactorDrawer] finally: setCodeLoading(false)');
        setCodeLoading(false);
      });
    } else {
      console.warn('[FactorDrawer] factorId 为空，跳过代码加载');
    }

    // 数据源注解
    productionApi.getResolvedDataConfig().then(r => setDataConfigLabels(r.data?.data || {})).catch((error) => {
      console.error('Failed to load data config labels:', error);
    });

    // 加载可用表列表
    productionApi.getAvailableTables().then(r => setAvailableTables(r.data?.data || [])).catch((error) => {
      console.error('Failed to load available tables:', error);
    });
  }, [factor, open, initialTab, factorId]);

  const loadData = useCallback(async () => {
    if (!factorId) return;
    setDataLoading(true);
    try {
      const res = await productionApi.getFactorData(factorId, { ...dataFilter, limit: 200 });
      setFactorData(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load factor data:', error);
      setFactorData([]);
    }
    setDataLoading(false);
  }, [factorId, dataFilter]);

  const loadHistory = useCallback(async () => {
    if (!factorId) return;
    setHistoryLoading(true);
    try {
      const res = await productionApi.getProductionHistory(
        factorId,
        50,
        filterStartDate || undefined,
        filterEndDate || undefined
      );
      setHistory(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load history:', error);
      setHistory([]);
    }
    setHistoryLoading(false);
  }, [factorId, filterStartDate, filterEndDate]);

  useEffect(() => { if (activeTab === 'data' && factorId) loadData(); }, [activeTab, factorId, loadData]);
  useEffect(() => { if (activeTab === 'logs' && factorId) loadHistory(); }, [activeTab, factorId, loadHistory]);

  // 保存编辑（基本信息 + 预处理）
  const handleSave = async (): Promise<void> => {
    if (!factor) return;
    setEditSaving(true);
    try {
      if (!factorId) {
        message.error('因子ID不能为空');
        return;
      }
      const newParams = {
        ...(factor.params || {}),
        preprocess: ppEdit,
        lookback_days: editLookbackDays,
      };
      const values = { description: editDesc, category: editCategory, compute_mode: editComputeMode, depends_on: editDependsOn, params: newParams, align_calendar: editAlignCalendar };
      console.log('[FactorDrawer] 保存预处理配置:', ppEdit);
      await productionApi.updateFactor(factorId, values);
      message.success('保存成功');
      onSaved();
    } catch (e: any) {
      const errorMessage = e.response?.data?.detail || '保存失败';
      console.error('Failed to save factor:', e);
      message.error(errorMessage);
    }
    setEditSaving(false);
  };

  // 保存代码
  const handleSaveCode = async (): Promise<void> => {
    if (!factorId || !code) return;
    setCodeSaving(true);
    try {
      await productionApi.updateFactorCode(factorId, code.filename, editedCode);
      message.success('代码已保存');
      setCodeChanged(false);
    } catch (e: any) {
      const errorMessage = e.response?.data?.detail || '保存失败';
      console.error('Failed to save code:', e);
      message.error(errorMessage);
    }
    setCodeSaving(false);
  };

  const dataColumns = [
    { title: '股票代码', dataIndex: 'ts_code', key: 'ts_code', width: 120 },
    { title: '交易日期', dataIndex: 'trade_date', key: 'trade_date', width: 120,
      render: (v: string) => {
        if (!v) return '-';
        const str = String(v);
        // YYYYMMDD -> YYYY-MM-DD
        if (str.length === 8) {
          return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
        }
        return str;
      }
    },
    { title: '因子值', dataIndex: 'factor_value', key: 'factor_value', render: (v: number) => v?.toFixed(6) },
  ];

  return (
    <Drawer
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ color: 'var(--color-primary)' }}>{factorId}</span>
        </div>
      }
      open={open} onClose={onClose} width={780}
    >
      <Tabs activeKey={activeTab} onChange={setActiveTab} size="middle" items={[
        {
          key: 'edit',
          label: <span><EditOutlined /> 编辑</span>,
          children: (
            <div>
              <Collapse defaultActiveKey={['info', 'preprocess', 'code']} items={[
                {
                  key: 'info',
                  label: <span style={{ fontSize: 13, fontWeight: 500 }}>基本信息</span>,
                  children: (
                    <>
                      <div style={{ display: 'flex', gap: 16 }}>
                        <div style={{ flex: 1 }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>描述</div>
                          <Input size="middle" value={editDesc} onChange={e => setEditDesc(e.target.value)} />
                        </div>
                        <div style={{ flex: '0 0 140px' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分类</div>
                          <Select size="middle" style={{ width: '100%' }} value={editCategory} onChange={v => setEditCategory(v as string)}
                            options={['momentum','value','technical','quality','custom'].map(v => ({ label: v, value: v }))} />
                        </div>
                        <div style={{ flex: '0 0 140px' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>计算模式</div>
                          <Select size="middle" style={{ width: '100%' }} value={editComputeMode} onChange={v => setEditComputeMode(v as string)}
                            options={[{ label: '增量', value: 'incremental' }, { label: '全量', value: 'full' }]} />
                        </div>
                      </div>
                      <div style={{ marginTop: 8 }}>
                        <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>数据依赖</div>
                        <Select
                          size="middle" mode="multiple" style={{ width: '100%' }} value={editDependsOn}
                          onChange={(v) => setEditDependsOn(v as string[])}
                          options={availableTables.map(t => ({ label: t.label, value: t.value }))}
                          showSearch
                          placeholder="选择数据表"
                        />
                      </div>
                      <div style={{ marginTop: 8, display: 'flex', alignItems: 'center', gap: 12 }}>
                        <div style={{ flex: 1 }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>回溯天数</div>
                          <InputNumber size="middle" min={1} max={500} value={editLookbackDays} style={{ width: '100%' }}
                            onChange={(v) => setEditLookbackDays((v as number) || 60)} />
                        </div>
                        <div style={{ paddingTop: 18 }}>
                          <Checkbox checked={editAlignCalendar} onChange={e => setEditAlignCalendar(!!e.target.checked)}>
                            对齐交易日历
                          </Checkbox>
                        </div>
                      </div>
                    </>
                  ),
                },
                {
                  key: 'preprocess',
                  label: <span style={{ fontSize: 13, fontWeight: 500 }}>预处理选项</span>,
                  children: (
                    <>
                      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                        <div style={{ flex: '1 1 200px' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>复权方式</div>
                          <Select size="middle" style={{ width: '100%' }} value={ppEdit.adjust_price}
                            onChange={(v) => setPpEdit(p => ({ ...p, adjust_price: v as PreprocessOptions['adjust_price'] }))}
                            options={[
                              { label: '前复权', value: 'forward' },
                              { label: '后复权', value: 'backward' },
                              { label: '不复权', value: 'none' },
                            ]} />
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
                            数据源: {editDependsOn.includes('sync_adj_factor') ? 'sync_adj_factor.adj_factor' : '需要添加 sync_adj_factor 到数据依赖'}
                          </div>
                        </div>
                        <div style={{ flex: '1 1 200px' }}>
                          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>新股排除天数</div>
                          <InputNumber size="middle" min={1} max={250} value={ppEdit.new_stock_days}
                            disabled={!ppEdit.filter_new_stock} style={{ width: '100%' }}
                            onChange={(v) => setPpEdit(p => ({ ...p, new_stock_days: (v as number) || 60 }))} />
                        </div>
                      </div>
                      <div style={{ display: 'flex', gap: 16, marginTop: 12, flexWrap: 'wrap' }}>
                        <span>
                          <Checkbox checked={ppEdit.filter_st} onChange={(e) => setPpEdit(p => ({ ...p, filter_st: !!e.target.checked }))}>过滤 ST</Checkbox>
                          {dataConfigLabels.is_st ? (
                            <Tooltip title={dataConfigLabels.is_st.values ? Object.entries(dataConfigLabels.is_st.values).map(([k, v]) => `${k}: ${v}`).join('\n') : undefined} placement="bottom">
                              <span style={{ fontSize: 10, color: 'var(--text-muted)', cursor: dataConfigLabels.is_st.values ? 'help' : undefined }}> ({dataConfigLabels.is_st.source_label})</span>
                            </Tooltip>
                          ) : (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}> (需配置数据源)</span>
                          )}
                        </span>
                        <span>
                          <Checkbox checked={ppEdit.filter_new_stock} onChange={(e) => setPpEdit(p => ({ ...p, filter_new_stock: !!e.target.checked }))}>过滤新股</Checkbox>
                          {dataConfigLabels.list_date ? (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}> ({dataConfigLabels.list_date.source_label})</span>
                          ) : (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}> (需配置数据源)</span>
                          )}
                        </span>
                        <span>
                          <Checkbox checked={ppEdit.mark_limit} onChange={(e) => setPpEdit(p => ({ ...p, mark_limit: !!e.target.checked }))}>涨跌停标记</Checkbox>
                          {dataConfigLabels.is_limit ? (
                            <Tooltip title={dataConfigLabels.is_limit.values ? Object.entries(dataConfigLabels.is_limit.values).map(([k, v]) => `${k}: ${v}`).join('\n') : undefined} placement="bottom">
                              <span style={{ fontSize: 10, color: 'var(--text-muted)', cursor: dataConfigLabels.is_limit.values ? 'help' : undefined }}> ({dataConfigLabels.is_limit.source_label})</span>
                            </Tooltip>
                          ) : (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}> (需配置数据源)</span>
                          )}
                        </span>
                      </div>
                    </>
                  ),
                },
                {
                  key: 'code',
                  label: <span style={{ fontSize: 13, fontWeight: 500 }}>因子代码</span>,
                  children: (
                    <Spin spinning={codeLoading}>
                      {code ? (
                        <div>
                          <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', marginBottom: 8 }}>
                            <div style={{ display: 'flex', gap: 8 }}>
                              <Button size="middle" icon={<CodeOutlined />} onClick={handleFormatCode}>格式化</Button>
                              <Button size="middle" type="primary" icon={<SaveOutlined />} disabled={!codeChanged}
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
                          <TestPanel code={editedCode} dependsOn={editDependsOn} preprocess={ppEdit} lookbackDays={editLookbackDays} />
                        </div>
                      ) : <Empty description="未找到源代码文件" />}
                    </Spin>
                  ),
                },
              ]} />
              <div style={{ marginTop: 12, textAlign: 'right' }}>
                <Button type="primary" icon={<SaveOutlined />} loading={editSaving} onClick={handleSave}>保存</Button>
              </div>
            </div>
          ),
        },
        {
          key: 'data',
          label: <span><DatabaseOutlined /> 数据</span>,
          children: (
            <div>
              {factorId && <DataInspection taskType="factor" taskId={factorId} />}
              <div style={{ display: 'flex', gap: 8, marginBottom: 12, flexWrap: 'wrap' }}>
                <Input size="middle" placeholder="股票代码" style={{ width: 120 }} allowClear
                  onChange={e => setDataFilter(f => ({ ...f, ts_code: e.target.value || undefined }))} />
                <Input size="middle" placeholder="起始日期 (yyyymmdd)" style={{ width: 160 }} allowClear
                  onChange={e => setDataFilter(f => ({ ...f, start_date: e.target.value || undefined }))} />
                <Input size="middle" placeholder="截止日期 (yyyymmdd)" style={{ width: 160 }} allowClear
                  onChange={e => setDataFilter(f => ({ ...f, end_date: e.target.value || undefined }))} />
                <Button size="middle" icon={<SearchOutlined />} onClick={loadData}>查询</Button>
              </div>
              <Table dataSource={factorData} columns={dataColumns}
                rowKey={(r: any) => `${r.ts_code}-${r.trade_date}`}
                loading={dataLoading} size="middle" pagination={{ pageSize: 20 }}
                locale={{ emptyText: <Empty description="暂无数据" /> }} />
            </div>
          ),
        },
        {
          key: 'logs',
          label: <span><BarChartOutlined /> 日志</span>,
          children: (
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12, gap: 8 }}>
                <span style={{ color: 'var(--text-secondary)', fontSize: 13 }}>最近 50 条计算记录</span>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <QuantDatePicker
                    value={[filterStartDate, filterEndDate]}
                    style={{ width: 280 }}
                    onChange={(s, e) => { setFilterStartDate(s); setFilterEndDate(e); }}
                  />
                  <Button size="middle" icon={<SearchOutlined />} onClick={loadHistory}>筛选</Button>
                  <span style={{ fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>
                    按任务完成日期筛选
                  </span>
                </div>
              </div>
              <TaskLogTable logs={history} loading={historyLoading} />
            </div>
          ),
        },
      ]} />
    </Drawer>
  );
};

export default FactorDrawer;