import { notify } from '../../utils/notify';
/**
 * 因子管理标签页
 */

import React, { useState, useEffect, useRef } from 'react';
import {
  Card, Table, Button, Tag, Select, Modal, Popconfirm, Checkbox,
  Alert, Drawer, Input, InputNumber, Tooltip,
} from 'antd';
import {
  PlusOutlined, DeleteOutlined, EditOutlined, ThunderboltOutlined, PlayCircleOutlined, ReloadOutlined, CodeOutlined,
} from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import dayjs from 'dayjs';
import Editor from '@monaco-editor/react';
import QuantDatePicker from '../../components/QuantDatePicker';
import { productionApi, DEFAULT_PREPROCESS } from '../../api';
import { useThemeStore } from '../../store';
import { formatCode } from '../../utils/codeFormatter';
import type { PreprocessOptions, FactorDefinition } from '../../types';
import { CODE_TEMPLATE } from './types';
import { useFactorList } from './hooks/useFactorList';
import FactorDrawer from './FactorDrawer';
import TaskLogTable from '../../components/TaskLogTable';

const FactorManageTab: React.FC = () => {
  const message = useMessage();
  const { mode } = useThemeStore();
  const {
    factors,
    history,
    loading,
    runLoading,
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
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : '格式化失败';
      notify.error(errorMessage);
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
      notify.warning('请先勾选因子');
      return;
    }
    setBatchLoading(true);
    try {
      const res = await productionApi.batchRunFactors(selectedRowKeys, runMode, startDate, endDate);
      const tasks = res.data?.data?.tasks || [];
      notify.success(`已提交 ${tasks.length} 个因子计算任务，后台执行中`);
      setSelectedRowKeys([]);
      loadFactors();
      loadHistory();
    } catch (e: any) {
      const errorMessage = e.response?.data?.detail || '批量执行失败';
      notify.error(errorMessage);
    }
    setBatchLoading(false);
  };

  const handleCreate = async (): Promise<void> => {
    if (!createFactorId.trim()) {
      notify.warning('请输入因子ID');
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
      notify.success(`因子 ${values.factor_id} 创建成功`);
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
      notify.error(errorMessage);
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
        <Tooltip title={v}>
          <span style={{ cursor: 'pointer', display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} onClick={() => openDrawer(r)}>
            <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
          </span>
        </Tooltip>
      )
    },
    { title: '描述', dataIndex: 'description', key: 'desc', width: 180,
      render: (v: string) => (
        <Tooltip title={v}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{v}</div>
        </Tooltip>
      )
    },
    { title: '分类', dataIndex: 'category', key: 'cat', width: 80,
      render: (v: string) => (
        <Tooltip title={v || '-'}>
          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            <Tag>{v || '-'}</Tag>
          </div>
        </Tooltip>
      )
    },
    { title: '模式', dataIndex: 'compute_mode', key: 'mode', width: 60,
      render: (v: string) => <Tag color={v === 'incremental' ? 'blue' : 'green'}>{v === 'incremental' ? '增量' : '全量'}</Tag>
    },
    { title: '最新数据', dataIndex: 'latest_date', key: 'latest', width: 100,
      render: (v: string) => {
        if (!v) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
        return (
          <Tooltip title={v}>
            <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--color-gain)', fontSize: '12px' }}>{v}</div>
          </Tooltip>
        );
      }
    },
    { title: '上次计算', key: 'last_run', width: 150,
      render: (_: any, record: any) => {
        const lastRun = history.find(h => h.task_id === record.factor_id);
        if (!lastRun) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
        const dateTimeStr = lastRun.started_at?.slice(0, 19).replace('T', ' ') || '-';  // YYYY-MM-DD HH:mm:ss
        return (
          <Tooltip title={lastRun.started_at}>
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
          <Button icon={<PlayCircleOutlined />} type="text"
            loading={runLoading === record.factor_id}
            onClick={() => {
              setFullRunModal({ visible: true, factorId: record.factor_id, computeMode: 'incremental' });
              setFullRunDates(['', '']);
            }}>计算</Button>
          <Button icon={<EditOutlined />} type="text" onClick={() => openDrawer(record)}>编辑</Button>
          <Popconfirm title="确认删除此因子？" onConfirm={() => handleDelete(record.factor_id)}>
            <Button icon={<DeleteOutlined />} type="text" danger>删除</Button>
          </Popconfirm>
        </div>
      )
    },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>已注册因子</span>
          <div style={{ display: 'flex', gap: 8 }}>
            {selectedRowKeys.length > 0 && (
              <Button type="primary" icon={<ThunderboltOutlined />} loading={batchLoading}
                onClick={() => { setBatchCalcDates(['', '']); setBatchCalcModalVisible(true); }}>批量计算 ({selectedRowKeys.length})</Button>
            )}
            <Button icon={<PlusOutlined />} onClick={() => setCreateModal(true)}>新建因子</Button>
            <Button icon={<ReloadOutlined />} onClick={loadFactors}>刷新</Button>
          </div>
        </div>
        <Table dataSource={factors} columns={factorColumns} rowKey="factor_id"
          loading={loading} pagination={false}
          rowSelection={{ selectedRowKeys, onChange: (keys) => setSelectedRowKeys(keys?.map(String) || []) }} />
      </Card>

      <Card style={{ background: 'var(--bg-card)' }}>
        <div style={{ marginBottom: 12 }}>
          <span style={{ color: 'var(--text-secondary)', fontWeight: 600, fontSize: 15 }}>计算历史</span>
        </div>
        <TaskLogTable logs={history} taskIdLabel="因子ID" onFilter={loadHistory} />
      </Card>

      {/* 新建因子 Drawer */}
      <Drawer
        title={<span style={{ color: 'var(--color-primary)' }}>新建因子</span>}
        open={createModal}
        onClose={() => {
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
            <Button type="primary" onClick={handleCreate}>创建</Button>
          </div>
        }
      >
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>因子ID <span style={{ color: 'var(--color-loss)' }}>*</span></div>
            <Input prefix="factor_" placeholder="如 custom_01" value={(createFactorId || '').replace(/^factor_/, '')} onChange={(e) => setCreateFactorId(`factor_${e.target.value}`)} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>描述</div>
            <Input placeholder="因子描述" value={createDesc} onChange={(e) => setCreateDesc(e.target.value)} />
          </div>
        </div>
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分类</div>
            <Select style={{ width: '100%' }} value={createCategory} onChange={v => setCreateCategory(v as string)}
              options={['momentum','value','technical','quality','custom'].map(v => ({ label: v, value: v }))} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>计算模式</div>
            <Select style={{ width: '100%' }} value={createComputeMode} onChange={v => setCreateComputeMode(v as string)}
              options={[{ label: '增量', value: 'incremental' }, { label: '全量', value: 'full' }]} />
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>复权方式</div>
            <Select style={{ width: '100%' }} value={createPreprocess.adjust_price}
              onChange={(v) => setCreatePreprocess(p => ({ ...p, adjust_price: v as PreprocessOptions['adjust_price'] }))}
              options={[
                { label: '前复权', value: 'forward' },
                { label: '后复权', value: 'backward' },
                { label: '不复权', value: 'none' },
              ]} />
          </div>
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>数据依赖 <span style={{ color: 'var(--color-loss)' }}>*</span></div>
          <Select
            mode="multiple" style={{ width: '100%' }} value={createDependsOn}
            onChange={(v) => setCreateDependsOn(v as string[])}
            options={(() => {
              const options = availableTables.map(t => ({
                label: t.label,
                value: t.value,
              }));
              console.log('[FactorManageTab] Rendering Select with options:', options.length, options.slice(0, 3));
              return options;
            })()}
            showSearch
            placeholder="选择数据表"
          />
        </div>
        <div style={{ marginBottom: 12 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>回溯天数</div>
          <InputNumber min={1} max={500} value={createLookbackDays} style={{ width: '100%' }}
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
            <Button icon={<CodeOutlined />} onClick={handleFormatCreateCode}>格式化</Button>
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
      </Drawer>

      {/* 单因子计算模态框 */}
      <Modal
        title={`计算因子：${fullRunModal.factorId}`}
        open={fullRunModal.visible}
        onCancel={() => setFullRunModal(s => ({ ...s, visible: false }))}
        onOk={() => {
          const startDate = fullRunDates[0] || undefined;
          const endDate = fullRunDates[1] || undefined;
          const runMode = (startDate && endDate) ? 'full' : 'incremental';
          handleRun(fullRunModal.factorId!, runMode, startDate, endDate);
          setFullRunModal(s => ({ ...s, visible: false }));
        }}
        okText="开始计算"
        cancelText="取消"
      >
        <div>
          <Alert
            type="info"
            message="选择计算日期范围。留空则执行增量计算（仅计算缺失数据）。"
            style={{ marginBottom: 12 }}
            closable={false}
          />
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: 13 }}>计算日期范围</div>
            <QuantDatePicker
              value={fullRunDates}
              onChange={(s, e) => setFullRunDates([s, e])}
              style={{ width: '100%' }}
            />
            {fullRunDates[0] && fullRunDates[1] && (
              <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: 6 }}>
                <span style={{ color: 'var(--color-primary)', fontSize: 13, fontWeight: 500 }}>
                  共 {dayjs(fullRunDates[1], 'YYYYMMDD').diff(dayjs(fullRunDates[0], 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
        </div>
      </Modal>

      {/* 批量计算模态框 */}
      <Modal
        title="批量计算因子"
        open={batchCalcModalVisible}
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
          <Alert type="info" message="选择计算日期范围。留空则执行增量计算（仅计算最新数据）。" style={{ marginBottom: 12 }} closable={false} />
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
