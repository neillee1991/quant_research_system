/**
 * 指数订阅抽屉组件
 * 三阶段流程：基础配置 → 指数列表 → 任务配置
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Drawer,
  Button,
  Input,
  Select,
  Table,
  Tag,
  Space,
  Spin,
  Empty,
  Alert,
  Steps,
  Divider,
  Modal,
  Switch,
} from 'antd';
import {
  SearchOutlined,
  PlusOutlined,
  CheckOutlined,
  ArrowLeftOutlined,
  SaveOutlined,
  DatabaseOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import { indexApi, dataApi } from '../../api';
import type { IndexInfo, FilterOptions, UserPreference, FilterFieldConfig } from '../../types/indexSubscribe';

interface IndexSubscribeDrawerProps {
  visible: boolean;
  onClose: () => void;
  onSubscribeSuccess?: () => void;
  onUnsubscribeSuccess?: () => void;
  onSubscribe?: (index: IndexInfo, config: any) => void;
}

const marketMap: Record<string, string> = {
  'SSE': '上交所',
  'SZSE': '深交所',
  'CICC': '中金所',
};

// 指数权重表的默认 schema
const DEFAULT_INDEX_WEIGHT_SCHEMA = {
  index_code: { type: 'SYMBOL' },
  con_code: { type: 'SYMBOL' },
  trade_date: { type: 'DATE' },
  weight: { type: 'DOUBLE' },
};

export const IndexSubscribeDrawer: React.FC<IndexSubscribeDrawerProps> = ({
  visible,
  onClose,
  onSubscribeSuccess,
  onUnsubscribeSuccess,
  onSubscribe,
}) => {
  const message = useMessage();

  // 阶段状态
  const [currentStep, setCurrentStep] = useState(0); // 0: 配置, 1: 指数列表, 2: 任务配置

  // 加载状态
  const [loading, setLoading] = useState(false);
  const [savingPreference, setSavingPreference] = useState(false);

  // 阶段1: 基础配置
  const [userPreference, setUserPreference] = useState<UserPreference | null>(null);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<string[]>([]);
  const [filterConfig, setFilterConfig] = useState<FilterFieldConfig[]>([]);
  const [loadingColumns, setLoadingColumns] = useState(false);

  // 阶段2: 指数列表
  const [indices, setIndices] = useState<IndexInfo[]>([]);
  const [filterOptions, setFilterOptions] = useState<FilterOptions>({
    markets: [],
    publishers: [],
  });
  const [searchText, setSearchText] = useState('');
  const [selectedMarket, setSelectedMarket] = useState<string>();
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);

  // 从表名提取后缀
  const getTableSuffix = (tableName: string): string => {
    // 如果表名是 sync_index_basic 格式，提取 basic
    const match = tableName.match(/^sync_index_(.+)$/);
    if (match) return match[1];
    // 否则使用表名的哈希或固定后缀
    return tableName.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
  };

  // 生成默认任务配置
  const generateDefaultConfig = (index: IndexInfo, tableName: string): any => {
    const tableSuffix = getTableSuffix(tableName);
    const indexCodeClean = index.ts_code.replace(/\./g, '_').toUpperCase();
    const taskId = `sync_index_weight_${indexCodeClean}`;

    return {
      task_id: taskId,
      api_name: 'index_weight',
      description: `${index.name}成分股同步`,
      table_name: taskId, // 表名与任务名相同
      sync_type: 'incremental',
      source: 'tushare',
      date_field: 'trade_date',
      api_limit: 5000,
      params_json: JSON.stringify({
        index_code: index.ts_code,
        trade_date: '{date}',
        fields: 'index_code,con_code,trade_date,weight',
      }),
      primary_keys_json: JSON.stringify(['index_code', 'con_code', 'trade_date']),
      schema_json: JSON.stringify(DEFAULT_INDEX_WEIGHT_SCHEMA),
      enabled: true,
      schedule: '',
      cron_expression: '',
    };
  };

  // 加载可用表列表
  const loadAvailableTables = useCallback(async () => {
    try {
      const res = await dataApi.listTables();
      const tables = res.data.tables || [];
      // 筛选指数相关的表 - 后端返回的是对象数组，需要提取 table_name
      const indexTables = tables
        .filter((t: any) => t.table_name?.toLowerCase().includes('index'))
        .map((t: any) => t.table_name);
      setAvailableTables(indexTables);
    } catch (error) {
      console.error('Failed to load tables:', error);
    }
  }, []);

  // 加载表字段列表
  const loadTableColumns = useCallback(async (tableName: string) => {
    if (!tableName) return;
    setLoadingColumns(true);
    try {
      const res = await dataApi.getTableInfo(tableName);
      const cols: string[] = res.data.columns || [];
      setTableColumns(cols);
      setFilterConfig(prev => {
        if (prev.length > 0) return prev;
        return cols.map(col => ({
          field: col,
          label: col,
          enabled: false,
          default_value: null,
        }));
      });
    } catch (e) {
      console.error('Failed to load table columns:', e);
    } finally {
      setLoadingColumns(false);
    }
  }, []);

  // 加载用户偏好
  const loadUserPreference = useCallback(async () => {
    try {
      const res = await indexApi.getUserPreference();
      const pref = res.data;
      setUserPreference(pref);
      if (pref?.index_basic_table) {
        setSelectedTable(pref.index_basic_table);
        // 如果已有配置，直接进入指数列表阶段
        setCurrentStep(1);
      }
      if (pref?.filter_config && pref.filter_config.length > 0) {
        setFilterConfig(pref.filter_config);
      }
    } catch (error) {
      console.error('Failed to load user preference:', error);
    }
  }, []);

  // 保存用户偏好
  const handleSavePreference = async () => {
    if (!selectedTable) {
      message.warning('请选择指数基础信息表');
      return;
    }

    setSavingPreference(true);
    try {
      await indexApi.saveUserPreference({
        index_basic_table: selectedTable,
        filter_config: filterConfig.length > 0 ? filterConfig : undefined,
      });
      message.success('配置已保存');
      setCurrentStep(1);
    } catch (error: any) {
      message.error(
        `保存配置失败: ${error.response?.data?.detail || error.message}`
      );
    } finally {
      setSavingPreference(false);
    }
  };

  // 加载筛选选项
  const loadFilterOptions = useCallback(async () => {
    try {
      const res = await indexApi.getFilterOptions();
      setFilterOptions(res.data || { markets: [], publishers: [] });
    } catch (error) {
      console.error('Failed to load filter options:', error);
    }
  }, []);

  // 加载指数列表
  const loadIndices = useCallback(async (
    currentPage = 1,
    currentPageSize = 20,
    search = '',
    market = undefined as string | undefined
  ) => {
    setLoading(true);
    try {
      const res = await indexApi.listAvailableIndices({
        page: currentPage,
        limit: currentPageSize,
        search: search || undefined,
        market: market || undefined,
      });
      setIndices(res.data.indices || []);
      setTotal(res.data.total || 0);
    } catch (error) {
      console.error('Failed to load indices:', error);
      message.error('加载指数列表失败');
    } finally {
      setLoading(false);
    }
  }, [message]);

  // 处理取消订阅
  const handleUnsubscribeClick = (index: IndexInfo) => {
    Modal.confirm({
      title: '取消订阅',
      content: `确定取消订阅指数 ${index.name}（${index.ts_code}）？相关同步任务及数据表将被删除，此操作不可撤销。`,
      okText: '确认取消订阅',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await indexApi.unsubscribeIndex(index.ts_code);
          message.success(`已取消订阅 ${index.name}`);
          onUnsubscribeSuccess?.();
          loadIndices(page, pageSize, searchText, selectedMarket);
        } catch (error: any) {
          message.error(`取消订阅失败: ${error.response?.data?.detail || error.message}`);
        }
      },
    });
  };

  // 处理订阅
  const handleSubscribeClick = async (index: IndexInfo) => {
    const defaultConfig = generateDefaultConfig(index, selectedTable);

    if (onSubscribe) {
      // 使用回调通知父组件
      onSubscribe(index, defaultConfig);
    } else {
      // 默认行为：直接创建任务
      try {
        await dataApi.createSyncTask(defaultConfig);
        message.success(`成功订阅指数 ${index.name}`);
        onSubscribeSuccess?.();
        onClose();
      } catch (error: any) {
        message.error(
          `订阅失败: ${error.response?.data?.detail || error.message}`
        );
      }
    }
  };

  // 返回配置阶段
  const handleBackToConfig = () => {
    setCurrentStep(0);
  };

  // 重置状态
  const resetState = () => {
    setCurrentStep(0);
    setSearchText('');
    setSelectedMarket(undefined);
    setPage(1);
  };

  // 初始化数据
  useEffect(() => {
    if (visible) {
      loadAvailableTables();
      loadUserPreference();
      // 不重置currentStep，让loadUserPreference决定
      setSearchText('');
      setSelectedMarket(undefined);
      setPage(1);
    }
  }, [visible, loadAvailableTables, loadUserPreference]);

  // 进入指数列表阶段时加载数据
  useEffect(() => {
    if (visible && currentStep === 1) {
      loadFilterOptions();
      loadIndices(page, pageSize, searchText, selectedMarket);
    }
  }, [visible, currentStep, loadFilterOptions, loadIndices, page, pageSize, searchText, selectedMarket]);

  // selectedTable 变化时加载字段列表
  useEffect(() => {
    if (selectedTable) {
      loadTableColumns(selectedTable);
    }
  }, [selectedTable, loadTableColumns]);

  const handleSearch = () => {
    setPage(1);
    loadIndices(1, pageSize, searchText, selectedMarket);
  };

  const handleReset = () => {
    setSearchText('');
    setSelectedMarket(undefined);
    setPage(1);
    loadIndices(1, pageSize, '', undefined);
  };

  const columns = [
    {
      title: '指数代码',
      dataIndex: 'ts_code',
      key: 'ts_code',
      width: 120,
      render: (code: string) => (
        <code style={{ fontSize: '12px' }}>{code}</code>
      ),
    },
    {
      title: '指数名称',
      dataIndex: 'name',
      key: 'name',
      width: 180,
      render: (name: string, record: IndexInfo) => (
        <Space>
          <span>{name}</span>
          {record.is_subscribed && (
            <Tag color="green" icon={<CheckOutlined />}>
              已订阅
            </Tag>
          )}
        </Space>
      ),
    },
    {
      title: '市场',
      dataIndex: 'market',
      key: 'market',
      width: 100,
      render: (market?: string) => {
        if (!market) return '-';
        const colorMap: Record<string, string> = {
          'SSE': 'blue',
          'SZSE': 'green',
          'CSI': 'orange',
          'CFFEX': 'purple',
        };
        return <Tag color={colorMap[market] || 'default'}>{marketMap[market] || market}</Tag>;
      },
    },
    {
      title: '发布机构',
      dataIndex: 'publisher',
      key: 'publisher',
      width: 120,
      render: (publisher?: string) => publisher || '-',
    },
    {
      title: '发布日期',
      dataIndex: 'list_date',
      key: 'list_date',
      width: 100,
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      render: (_: any, record: IndexInfo) =>
        record.is_subscribed ? (
          <Button
            type="default"
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleUnsubscribeClick(record)}
          >
            取消订阅
          </Button>
        ) : (
          <Button
            type="primary"
            size="small"
            icon={<PlusOutlined />}
            onClick={() => handleSubscribeClick(record)}
          >
            订阅
          </Button>
        ),
    },
  ];

  // 渲染阶段1: 基础配置
  const renderConfigStage = () => (
    <div style={{ padding: '24px' }}>
      <Alert
        type="info"
        message="请先选择指数基础信息表，该表将用于确定指数权重表的命名规则。"
        showIcon
        style={{ marginBottom: '24px' }}
      />

      <div style={{ maxWidth: '500px' }}>
        <div style={{ marginBottom: '8px', fontSize: '14px', fontWeight: 500 }}>
          指数基础信息表
        </div>
        <Select
          placeholder="请选择指数基础信息表"
          value={selectedTable}
          onChange={setSelectedTable}
          style={{ width: '100%', marginBottom: '8px' }}
          options={availableTables.map((t) => ({
            label: t,
            value: t,
          }))}
          showSearch
          filterOption={(input, option) =>
            (option?.label ?? '').toLowerCase().includes(input.toLowerCase())
          }
        />
        <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: '24px' }}>
          提示：通常选择 sync_index_basic 或类似的表
        </div>

        {tableColumns.length > 0 && (
          <div style={{ marginBottom: '24px' }}>
            <div style={{ marginBottom: '8px', fontSize: '14px', fontWeight: 500 }}>
              筛选字段配置
            </div>
            <Table
              size="small"
              dataSource={filterConfig}
              rowKey="field"
              pagination={false}
              loading={loadingColumns}
              columns={[
                {
                  title: '字段名',
                  dataIndex: 'field',
                  width: 150,
                },
                {
                  title: '显示标签',
                  dataIndex: 'label',
                  width: 150,
                  render: (_: string, record: FilterFieldConfig, index: number) => (
                    <Input
                      size="small"
                      value={record.label}
                      onChange={e => {
                        setFilterConfig(prev => prev.map((f, i) =>
                          i === index ? { ...f, label: e.target.value } : f
                        ));
                      }}
                    />
                  ),
                },
                {
                  title: '作为筛选项',
                  dataIndex: 'enabled',
                  width: 100,
                  render: (_: boolean, record: FilterFieldConfig, index: number) => (
                    <Switch
                      size="small"
                      checked={record.enabled}
                      onChange={val => {
                        setFilterConfig(prev => prev.map((f, i) =>
                          i === index ? { ...f, enabled: val } : f
                        ));
                      }}
                    />
                  ),
                },
                {
                  title: '默认值',
                  dataIndex: 'default_value',
                  render: (_: string | null, record: FilterFieldConfig, index: number) => (
                    <Input
                      size="small"
                      placeholder="留空表示无默认值"
                      value={record.default_value || ''}
                      disabled={!record.enabled}
                      onChange={e => {
                        setFilterConfig(prev => prev.map((f, i) =>
                          i === index ? { ...f, default_value: e.target.value || null } : f
                        ));
                      }}
                    />
                  ),
                },
              ]}
            />
          </div>
        )}

        <Button
          type="primary"
          size="large"
          icon={<SaveOutlined />}
          onClick={handleSavePreference}
          loading={savingPreference}
          disabled={!selectedTable}
        >
          保存配置
        </Button>
      </div>
    </div>
  );

  // 渲染阶段2: 指数列表
  const renderIndexListStage = () => (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* 筛选区域 */}
      <div
        style={{
          padding: '16px 24px',
          borderBottom: '1px solid var(--border-color)',
          background: 'var(--bg-tertiary)',
        }}
      >
        <Space wrap size="middle">
          <Button
            icon={<ArrowLeftOutlined />}
            onClick={handleBackToConfig}
          >
            重新配置
          </Button>
          <Divider type="vertical" />
          <Input
            placeholder="搜索指数代码或名称"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            prefix={<SearchOutlined />}
            style={{ width: 240 }}
            allowClear
            onPressEnter={handleSearch}
          />
          <Select
            placeholder="选择市场"
            value={selectedMarket}
            onChange={setSelectedMarket}
            style={{ width: 150 }}
            allowClear
            options={filterOptions.markets.map((m) => ({
              label: marketMap[m] || m,
              value: m,
            }))}
          />
          <Button type="primary" onClick={handleSearch}>
            搜索
          </Button>
          <Button onClick={handleReset}>重置</Button>
        </Space>
      </div>

      {/* 表格区域 */}
      <div style={{ flex: 1, overflow: 'auto', padding: '16px 24px' }}>
        <Spin spinning={loading}>
          {!loading && indices.length === 0 ? (
            <Empty
              description={
                searchText || selectedMarket
                  ? '没有找到匹配的指数'
                  : '暂无可订阅的指数'
              }
              image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
          ) : (
            <Table
              dataSource={indices}
              columns={columns}
              rowKey="ts_code"
              size="middle"
              pagination={{
                current: page,
                pageSize,
                total,
                showSizeChanger: true,
                showQuickJumper: true,
                showTotal: (total) => `共 ${total} 条`,
                onChange: (p, ps) => {
                  setPage(p);
                  setPageSize(ps || 20);
                },
              }}
              locale={{
                emptyText: (
                  <Empty
                    description="暂无指数数据"
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                  />
                ),
              }}
            />
          )}
        </Spin>
      </div>

      {/* 底部提示 */}
      <div
        style={{
          padding: '12px 24px',
          borderTop: '1px solid var(--border-color)',
          background: 'var(--bg-tertiary)',
        }}
      >
        <Alert
          type="info"
          message={`当前配置：${selectedTable}`}
          description="订阅指数后将打开任务配置界面，用于创建指数成分股权重数据同步任务。"
          showIcon
          style={{ border: 'none', padding: 0 }}
        />
      </div>
    </div>
  );

  return (
    <Drawer
      title="订阅指数"
      open={visible}
      onClose={onClose}
      width={currentStep === 0 ? 700 : 900}
      styles={{ body: { padding: 0, display: 'flex', flexDirection: 'column' } }}
    >
      {/* 步骤指示器 */}
      <div style={{
        padding: '16px 24px',
        borderBottom: '1px solid var(--border-color)',
        background: 'var(--bg-secondary)',
      }}>
        <Steps
          current={currentStep}
          size="small"
          items={[
            { title: '基础配置', icon: <DatabaseOutlined /> },
            { title: '选择指数' },
          ]}
        />
      </div>

      {/* 阶段内容 */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {currentStep === 0 && renderConfigStage()}
        {currentStep === 1 && renderIndexListStage()}
      </div>
    </Drawer>
  );
};