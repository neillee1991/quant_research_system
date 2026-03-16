/**
 * 因子管理标签页
 */

import React, { useState, useEffect, useRef } from 'react';
import {
  Card, Table, Button, Tag, Select, Modal, Popconfirm, Checkbox,
  Toast, Banner, SideSheet, Input, InputNumber, Tooltip,
} from '@douyinfe/semi-ui';
import {
  IconPlus, IconDelete, IconEdit, IconBolt, IconPlay, IconRefresh, IconCode,
} from '@douyinfe/semi-icons';
import dayjs from 'dayjs';
import Editor from '@monaco-editor/react';
import QuantDatePicker from '../../components/QuantDatePicker';
import { productionApi, DEFAULT_PREPROCESS } from '../../api';
import { useThemeStore } from '../../store';
import { formatCode } from '../../utils/codeFormatter';
import type { PreprocessOptions, FactorDefinition } from '../../types';
import { CODE_TEMPLATE, formatRunParams } from './types';
import { useFactorList } from './hooks/useFactorList';
import FactorDrawer from './FactorDrawer';

const FactorManageTab: React.FC = () => {
  const { mode } = useThemeStore();
  const {
    factors,
    history,
    loading,
    runLoading,
    selectedFactor,
    setSelectedFactor,
    loadFactors,
    loadHistory,
    runFactor,
    deleteFactor,
  } = useFactorList();

  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [batchLoading, setBatchLoading] = useState<boolean>(false);
  const [createModal, setCreateModal] = useState<boolean>(false);
  const [createCode, setCreateCode] = useState<string>(CODE_TEMPLATE);
  const [createPreprocess, setCreatePreprocess] = useState<PreprocessOptions>({ ...DEFAULT_PREPROCESS });
  const [drawerState, setDrawerState] = useState<{ open: boolean; factor: FactorDefinition | null; tab?: string }>({ open: false, factor: null });
  const [fullRunModal, setFullRunModal] = useState<{ visible: boolean; factorId: string | null; computeMode: string }>({ visible: false, factorId: null, computeMode: 'incremental' });
  const [fullRunDates, setFullRunDates] = useState<[string, string]>(['', '']);

  // Create form state
  const [createFactorId, setCreateFactorId] = useState<string>('');
  const [createDesc, setCreateDesc] = useState<string>('');
  const [createCategory, setCreateCategory] = useState<string>('custom');
  const [createComputeMode, setCreateComputeMode] = useState<string>('incremental');
  const [createDependsOn, setCreateDependsOn] = useState<string[]>(['sync_daily_data']);
  const [createLookbackDays, setCreateLookbackDays] = useState<number>(60);
  const [createAlignCalendar, setCreateAlignCalendar] = useState<boolean>(false);
  const createEditorRef = useRef<unknown>(null);

  // 可用表列表
  const [availableTables, setAvailableTables] = useState<Array<{value: string; label: string; description: string; type: string}>>([]);

  // 批量计算模态框
  const [batchCalcModalVisible, setBatchCalcModalVisible] = useState<boolean>(false);
  const [batchCalcDates, setBatchCalcDates] = useState<[string, string]>(['', '']);

  // 加载可用表列表
  useEffect(() => {
    productionApi.getAvailableTables().then(r => {
      const tables = r.data?.data || [];
      console.log('[FactorManageTab] Loaded available tables:', tables.length, tables);
      setAvailableTables(tables);
    }).catch((error) => {
      console.error('Failed to load available tables:', error);
    });
  }, []);

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

  const handleRun = async (factorId: string, runMode: string, startDate?: string, endDate?: string): Promise<void> => {
    try {
      await runFactor(factorId, runMode, startDate, endDate);
    } catch (error) {
      console.error('Failed to run factor:', error);
    }
  };

  const handleBatchRun = async (runMode: string, startDate?: string, endDate?: string): Promise<void> => {
    if (selectedRowKeys.length === 0) {
      Toast.warning('请先勾选因子');
      return;
    }
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
      const errorMessage = e.response?.data?.detail || '批量执行失败';
      console.error('Batch run failed:', e);
      Toast.error(errorMessage);
    }
    setBatchLoading(false);
  };

  const handleCreate = async (): Promise<void> => {
    if (!createFactorId.trim()) {
      Toast.warning('请输入因子ID');
      return;
    }
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
      };
      await productionApi.createFactor({ ...values, params, code: createCode || undefined, align_calendar: createAlignCalendar });
      Toast.success(`因子 ${values.factor_id} 创建成功`);
      setCreateModal(false);
      setCreateFactorId('');
      setCreateDesc('');
      setCreateCategory('custom');
      setCreateComputeMode('incremental');
      setCreateCode(CODE_TEMPLATE);
      setCreatePreprocess({ ...DEFAULT_PREPROCESS });
      setCreateDependsOn(['sync_daily_data']);
      setCreateLookbackDays(60);
      setCreateAlignCalendar(false);
      loadFactors();
    } catch (e: any) {
      const errorMessage = e.response?.data?.detail || '创建失败';
      console.error('Failed to create factor:', e);
      Toast.error(errorMessage);
    }
  };

  const handleDelete = async (factorId: string): Promise<void> => {
    try {
      await deleteFactor(factorId);
    } catch (error) {
      console.error('Failed to delete factor:', error);
    }
  };

  const openDrawer = (record: any, tab?: string): void => {
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
    { title: '最新数据', dataIndex: 'latest_date', key: 'latest', width: 100,
      render: (v: string) => {
        if (!v) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
        return (
          <Tooltip content={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--color-gain)', fontSize: '12px' }}>{v}</div>
          </Tooltip>
        );
      }
    },
    { title: '上次计算', key: 'last_run', width: 150,
      render: (_: any, record: any) => {
        const lastRun = history.find(h => h.factor_id === record.factor_id);
        if (!lastRun) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
        const dateTimeStr = lastRun.created_at?.slice(0, 19).replace('T', ' ') || '-';  // YYYY-MM-DD HH:mm:ss
        return (
          <Tooltip content={lastRun.created_at}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)', fontSize: '12px' }}>
              {dateTimeStr}
            </div>
          </Tooltip>
        );
      }
    },
    {
      title: '操作', key: 'action', width: 160, render: (_: any, record: any) => (
        <div style={{ display: 'flex', gap: 4 }}>
          <Button size="small" icon={<IconPlay />} theme="borderless"
            loading={runLoading === record.factor_id}
            onClick={() => handleRun(record.factor_id, record.compute_mode || 'incremental')}>运行</Button>
          <Button size="small" icon={<IconEdit />} theme="borderless" onClick={() => openDrawer(record)}>编辑</Button>
          <Popconfirm title="确认删除此因子？" onConfirm={() => handleDelete(record.factor_id)}>
            <Button size="small" icon={<IconDelete />} theme="borderless" type="danger">删除</Button>
          </Popconfirm>
        </div>
      )
    },
  ];

  const historyColumns = [
    { title: '因子ID', dataIndex: 'factor_id', key: 'factor_id', width: 150,
      render: (v: string) => {
        const formatted = v || '-';
        return (
          <Tooltip content={formatted}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              <code style={{ fontSize: '12px' }}>{formatted}</code>
            </div>
          </Tooltip>
        );
      }
    },
    { title: '状态', dataIndex: 'status', key: 'status', width: 70,
      render: (v: string) => <Tag size="small" color={v === 'success' ? 'green' : v === 'running' ? 'blue' : 'red'}>{v}</Tag>
    },
    { title: '行数', dataIndex: 'rows', key: 'rows', width: 100,
      render: (v: number) => {
        const formatted = v?.toLocaleString() || '-';
        return (
          <Tooltip content={formatted}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{formatted}</div>
          </Tooltip>
        );
      }
    },
    { title: '耗时', dataIndex: 'elapsed_seconds', key: 'dur', width: 80,
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
    { title: '时间', dataIndex: 'created_at', key: 'time', width: 140,
      render: (v: string) => {
        const formatted = v?.slice(0, 16) || '-';  // 只显示到分钟
        return (
          <Tooltip content={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: '12px', color: 'var(--text-secondary)' }}>{formatted}</div>
          </Tooltip>
        );
      }
    },
    { title: '失败原因', dataIndex: 'error_message', key: 'error', width: 200,
      render: (v: string) => v ? (
        <Tooltip content={v} position="topLeft">
          <span style={{ color: 'var(--color-loss)', fontSize: '12px', cursor: 'help' }}>
            {v.length > 40 ? v.slice(0, 40) + '…' : v}
          </span>
        </Tooltip>
      ) : null
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
                onClick={() => { setBatchCalcDates(['', '']); setBatchCalcModalVisible(true); }}>批量计算 ({selectedRowKeys.length})</Button>
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
            size="small" multiple style={{ width: '100%' }} value={createDependsOn}
            onChange={(v) => setCreateDependsOn(v as string[])}
            optionList={(() => {
              const options = availableTables.map(t => ({
                label: t.label,
                value: t.value,
                ...(t.description ? { otherKey: t.description } : {})
              }));
              console.log('[FactorManageTab] Rendering Select with options:', options.length, options.slice(0, 3));
              return options;
            })()}
            filter
            placeholder="选择数据表"
          />
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>回溯天数</div>
          <InputNumber size="small" min={1} max={500} value={createLookbackDays} style={{ width: '100%' }}
            onChange={(v) => setCreateLookbackDays((v as number) || 60)} />
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
            用于滚动计算时向前加载的历史数据天数
          </div>
        </div>
        <div style={{ marginBottom: 12 }}>
          <Checkbox checked={createAlignCalendar} onChange={e => setCreateAlignCalendar(!!e.target.checked)}>
            对齐交易日历
          </Checkbox>
          <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 4 }}>
            （开启后，窗口内有停牌缺口时该日因子值置 null）
          </span>
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>因子代码 <span style={{ color: 'var(--color-loss)' }}>*</span></div>
            <Button size="small" icon={<IconCode />} onClick={handleFormatCreateCode}>格式化</Button>
          </div>
          <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
            <Editor height="400px" language="python" theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
              value={createCode} onChange={(v) => setCreateCode(v || '')}
              onMount={(editor) => { createEditorRef.current = editor; }}
              options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4, wordWrap: 'on' }} />
          </div>
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>预处理选项</div>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            <Checkbox checked={createPreprocess.filter_st} onChange={(e) => setCreatePreprocess(p => ({ ...p, filter_st: !!e.target.checked }))}>过滤 ST</Checkbox>
            <Checkbox checked={createPreprocess.filter_new_stock} onChange={(e) => setCreatePreprocess(p => ({ ...p, filter_new_stock: !!e.target.checked }))}>过滤新股</Checkbox>
            <Checkbox checked={createPreprocess.mark_limit} onChange={(e) => setCreatePreprocess(p => ({ ...p, mark_limit: !!e.target.checked }))}>涨跌停标记</Checkbox>
          </div>
        </div>
      </SideSheet>

      {/* 批量计算模态框 */}
      <Modal
        title="批量计算因子"
        visible={batchCalcModalVisible}
        onCancel={() => setBatchCalcModalVisible(false)}
        onOk={() => {
          const startDate = batchCalcDates[0] || undefined;
          const endDate = batchCalcDates[1] || undefined;
          handleBatchRun('full', startDate, endDate);
          setBatchCalcModalVisible(false);
        }}
        okText="开始计算"
        cancelText="取消"
      >
        <div>
          <Banner type="info" description="选择计算日期范围。留空则执行增量计算（仅计算最新数据）。" style={{ marginBottom: 12 }} />
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>计算日期范围</div>
            <QuantDatePicker
              value={batchCalcDates}
              onChange={(s, e) => setBatchCalcDates([s, e])}
              style={{ width: '100%' }}
            />
            {batchCalcDates[0] && batchCalcDates[1] && (
              <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {dayjs(batchCalcDates[1], 'YYYYMMDD').diff(dayjs(batchCalcDates[0], 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
        </div>
      </Modal>

      <FactorDrawer factor={drawerState.factor} open={drawerState.open} initialTab={drawerState.tab}
        onClose={() => setDrawerState({ open: false, factor: null })}
        onSaved={async () => {
          const res = await productionApi.listFactors();
          const list = res.data?.data || [];
          if (drawerState.factor) {
            const updated = list.find((f: any) => f.factor_id === drawerState.factor!.factor_id);
            if (updated) setDrawerState(prev => ({ ...prev, factor: updated }));
          }
          loadFactors();
        }} />
    </div>
  );
};

export default FactorManageTab;
