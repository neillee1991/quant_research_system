/**
 * 指数订阅管理 Tab
 * 从 DataCenter/IndexSubscribeDrawer.tsx 改造，去掉 Drawer 外壳
 */
import { notify } from '../../utils/notify';
import React, { useState, useEffect, useCallback } from 'react';
import {
  Button, Input, Select, Table, Tag, Space, Spin, Empty,
  Alert, Divider, Modal, Switch,
} from 'antd';
import {
  SearchOutlined, PlusOutlined, CheckOutlined,
  SaveOutlined, DeleteOutlined,
} from '@ant-design/icons';
import { indexApi, productionApi } from '../../api';
import type { IndexInfo, UserPreference, FilterFieldConfig } from '../../types/indexSubscribe';

const marketMap: Record<string, string> = {
  'SSE': '上交所',
  'SZSE': '深交所',
  'CICC': '中金所',
};

const IndexSubscribeTab: React.FC = () => {
  // 配置阶段状态
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<string[]>([]);
  const [filterConfig, setFilterConfig] = useState<FilterFieldConfig[]>([]);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [savingPreference, setSavingPreference] = useState(false);
  const [configDone, setConfigDone] = useState(false);

  // 指数列表状态
  const [indices, setIndices] = useState<IndexInfo[]>([]);
  const [activeFilters, setActiveFilters] = useState<Record<string, string>>({});
  const [searchText, setSearchText] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);

  // 加载可用表列表
  const loadAvailableTables = useCallback(async () => {
    try {
      const res = await productionApi.getAvailableTables();
      const tables = res.data?.data || [];
      const indexTables = tables
        .filter((t: any) => (t.value || '').toLowerCase().includes('index'))
        .map((t: any) => t.value);
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
      const res = await productionApi.getTableColumns(tableName);
      const cols: string[] = res.data?.columns || [];
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
      if (pref?.index_basic_table) {
        setSelectedTable(pref.index_basic_table);
        setConfigDone(true);
      }
      if (pref?.filter_config && pref.filter_config.length > 0) {
        setFilterConfig(pref.filter_config);
      }
      const initialFilters: Record<string, string> = {};
      (pref?.filter_config || [])
        .filter((f: any) => f.enabled && f.default_value)
        .forEach((f: any) => { initialFilters[f.field] = f.default_value; });
      setActiveFilters(initialFilters);
    } catch (error) {
      console.error('Failed to load user preference:', error);
    }
  }, []);

  // 保存用户偏好
  const handleSavePreference = async () => {
    if (!selectedTable) {
      notify.warning('请选择指数基础信息表');
      return;
    }
    setSavingPreference(true);
    try {
      await indexApi.saveUserPreference({
        index_basic_table: selectedTable,
        filter_config: filterConfig.length > 0 ? filterConfig : undefined,
      });
      notify.success('配置已保存');
      setConfigDone(true);
    } catch (error: any) {
      notify.error(`保存配置失败: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSavingPreference(false);
    }
  };

  // 加载指数列表
  const loadIndices = useCallback(async (
    currentPage = 1,
    currentPageSize = 20,
    search = '',
    filters: Record<string, string> = {}
  ) => {
    setLoading(true);
    try {
      const res = await indexApi.listAvailableIndices({
        page: currentPage,
        limit: currentPageSize,
        search: search || undefined,
        filters: Object.keys(filters).length > 0 ? filters : undefined,
      });
      setIndices(res.data.indices || []);
      setTotal(res.data.total || 0);
    } catch (error) {
      console.error('Failed to load indices:', error);
    } finally {
      setLoading(false);
    }
  }, []);

  // 处理订阅
  const handleSubscribeClick = async (index: IndexInfo) => {
    try {
      await indexApi.subscribeIndex({ index_code: index.ts_code });
      notify.success(`成功订阅指数 ${index.name}`);
      loadIndices(page, pageSize, searchText, activeFilters);
    } catch (error: any) {
      notify.error(`订阅失败: ${error.response?.data?.detail || error.message}`);
    }
  };

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
          notify.success(`已取消订阅 ${index.name}`);
          loadIndices(page, pageSize, searchText, activeFilters);
        } catch (error: any) {
          notify.error(`取消订阅失败: ${error.response?.data?.detail || error.message}`);
        }
      },
    });
  };

  const handleSearch = () => {
    setPage(1);
    loadIndices(1, pageSize, searchText, activeFilters);
  };

  const handleReset = () => {
    setSearchText('');
    const defaultFilters: Record<string, string> = {};
    filterConfig.filter(f => f.enabled && f.default_value)
      .forEach(f => { defaultFilters[f.field] = f.default_value!; });
    setActiveFilters(defaultFilters);
    setPage(1);
    loadIndices(1, pageSize, '', defaultFilters);
  };

  // 初始化
  useEffect(() => {
    loadAvailableTables();
    loadUserPreference();
  }, [loadAvailableTables, loadUserPreference]);

  useEffect(() => {
    if (configDone) {
      loadIndices(page, pageSize, searchText, activeFilters);
    }
  }, [configDone]);

  useEffect(() => {
    if (selectedTable) {
      loadTableColumns(selectedTable);
    }
  }, [selectedTable, loadTableColumns]);

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
            <Tag color="green" icon={<CheckOutlined />}>已订阅</Tag>
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
          'SSE': 'blue', 'SZSE': 'green', 'CSI': 'orange', 'CFFEX': 'purple',
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
          <Button type="default" size="small" danger icon={<DeleteOutlined />}
            onClick={() => handleUnsubscribeClick(record)}>
            取消订阅
          </Button>
        ) : (
          <Button type="primary" size="small" icon={<PlusOutlined />}
            onClick={() => handleSubscribeClick(record)}>
            订阅
          </Button>
        ),
    },
  ];

  return (
    <div>
      {/* 配置区域 */}
      <div style={{ marginBottom: 16, padding: 16, background: 'var(--bg-surface)', borderRadius: 6, border: '1px solid var(--border-color)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
          <span style={{ fontWeight: 500 }}>指数基础信息表</span>
          <Select
            placeholder="请选择指数基础信息表"
            value={selectedTable}
            onChange={(v) => { setSelectedTable(v); setConfigDone(false); }}
            style={{ width: 300 }}
            options={availableTables.map(t => ({ label: t, value: t }))}
            showSearch
            filterOption={(input, option) =>
              (option?.label ?? '').toLowerCase().includes(input.toLowerCase())
            }
          />
          <Button
            type="primary"
            icon={<SaveOutlined />}
            onClick={handleSavePreference}
            loading={savingPreference}
            disabled={!selectedTable}
          >
            保存配置
          </Button>
          {configDone && (
            <Tag color="green">已配置: {selectedTable}</Tag>
          )}
        </div>
      </div>

      {/* 筛选区域 */}
      <div style={{ marginBottom: 16 }}>
        <Space wrap size="middle">
          <Input
            placeholder="搜索指数代码或名称"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            prefix={<SearchOutlined />}
            style={{ width: 240 }}
            allowClear
            onPressEnter={handleSearch}
          />
          {filterConfig.filter(f => f.enabled).map(f => (
            <Input
              key={f.field}
              placeholder={f.label}
              value={activeFilters[f.field] || ''}
              onChange={e => setActiveFilters(prev => ({
                ...prev,
                [f.field]: e.target.value,
              }))}
              style={{ width: 150 }}
              allowClear
              onClear={() => setActiveFilters(prev => {
                const next = { ...prev };
                delete next[f.field];
                return next;
              })}
            />
          ))}
          <Button type="primary" onClick={handleSearch}>搜索</Button>
          <Button onClick={handleReset}>重置</Button>
        </Space>
      </div>

      {/* 指数列表 */}
      <Spin spinning={loading}>
        {!loading && indices.length === 0 ? (
          <Empty
            description={
              searchText || Object.keys(activeFilters).length > 0
                ? '没有找到匹配的指数'
                : configDone ? '暂无可订阅的指数' : '请先保存基础信息表配置'
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
          />
        )}
      </Spin>
    </div>
  );
};

export default IndexSubscribeTab;
