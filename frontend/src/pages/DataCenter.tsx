import React, { useEffect, useState, useCallback, useMemo } from 'react';
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
  Popconfirm,
  DatePicker,
  RadioGroup,
  Radio,
  Progress,
  TextArea,
  SideSheet,
  Switch,
  Collapse,
} from '@douyinfe/semi-ui';
import {
  IconSync,
  IconServer,
  IconCode,
  IconPlay,
  IconRefresh,
  IconDelete,
  IconClock,
  IconCalendar,
  IconHistory,
  IconExternalOpen,
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
  const [selectedTaskIds, setSelectedTaskIds] = useState<string[]>([]);

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

  const [batchSyncModalVisible, setBatchSyncModalVisible] = useState(false);
  const [batchSyncStartDate, setBatchSyncStartDate] = useState<string>('');
  const [batchSyncEndDate, setBatchSyncEndDate] = useState<string>('');

  const [configModalVisible, setConfigModalVisible] = useState(false);
  const [configModalTask, setConfigModalTask] = useState<any>(null);
  const [configJson, setConfigJson] = useState<string>('');
  const [isNewTask, setIsNewTask] = useState(false);

  const [scheduleInfo, setScheduleInfo] = useState<Record<string, any>>({});

  const [taskDrawerVisible, setTaskDrawerVisible] = useState(false);
  const [taskDrawerTask, setTaskDrawerTask] = useState<SyncTask | null>(null);
  const [taskDrawerJson, setTaskDrawerJson] = useState<string>('');
  const [taskDrawerConfig, setTaskDrawerConfig] = useState<any>(null);
  const [taskDrawerTab, setTaskDrawerTab] = useState<string>('visual');

  // 解析 JSON 并更新配置对象
  const parseTaskDrawerJson = (json: string) => {
    try {
      const config = JSON.parse(json);
      setTaskDrawerConfig(config);
      return true;
    } catch {
      return false;
    }
  };

  // 更新配置字段并同步到 JSON
  const updateTaskDrawerConfig = (key: string, value: any) => {
    const newConfig = { ...taskDrawerConfig, [key]: value };
    setTaskDrawerConfig(newConfig);
    setTaskDrawerJson(JSON.stringify(newConfig, null, 2));
  };

  // 更新 schema 字段
  const updateSchemaField = (fieldName: string, prop: string, value: any) => {
    const newSchema = { ...taskDrawerConfig.schema };
    if (!newSchema[fieldName]) newSchema[fieldName] = {};
    newSchema[fieldName] = { ...newSchema[fieldName], [prop]: value };
    updateTaskDrawerConfig('schema', newSchema);
  };

  // 更新 params 字段
  const updateParamsField = (key: string, value: string) => {
    const newParams = { ...taskDrawerConfig.params, [key]: value };
    updateTaskDrawerConfig('params', newParams);
  };

  const openTaskDrawer = async (task: SyncTask) => {
    setTaskDrawerTask(task);
    setTaskDrawerTab('visual');
    try {
      const res = await dataApi.getTaskConfig(task.task_id);
      const json = JSON.stringify(res.data.config, null, 2);
      setTaskDrawerJson(json);
      parseTaskDrawerJson(json);
    } catch {
      const json = JSON.stringify(task, null, 2);
      setTaskDrawerJson(json);
      parseTaskDrawerJson(json);
    }
    setTaskDrawerVisible(true);
  };

  const handleSaveTaskDrawer = async () => {
    try {
      const config = JSON.parse(taskDrawerJson);
      await dataApi.updateTaskConfig(config.task_id, config);
      Toast.success(`任务 ${config.task_id} 更新成功`);
      setTaskDrawerVisible(false);
      loadInitialData();
    } catch (error: any) {
      if (error instanceof SyntaxError) {
        Toast.error('JSON 格式无效');
      } else {
        Toast.error(error.response?.data?.detail || '保存配置失败');
      }
    }
  };

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

  const handleBatchSync = () => {
    if (selectedTaskIds.length === 0) {
      Toast.warning('请先选择要同步的任务');
      return;
    }
    const enabledTasks = selectedTaskIds.filter(id => {
      const task = syncTasks.find(t => t.task_id === id);
      return task?.enabled;
    });
    if (enabledTasks.length === 0) {
      Toast.warning('所选任务均已禁用');
      return;
    }
    setBatchSyncStartDate('');
    setBatchSyncEndDate('');
    setBatchSyncModalVisible(true);
  };

  const executeBatchSync = async () => {
    const enabledTasks = selectedTaskIds.filter(id => {
      const task = syncTasks.find(t => t.task_id === id);
      return task?.enabled;
    });
    setBatchSyncModalVisible(false);
    for (const taskId of enabledTasks) {
      setSyncingTasks((prev) => new Set(prev).add(taskId));
    }
    Toast.info(`开始同步 ${enabledTasks.length} 个任务`);
    for (const taskId of enabledTasks) {
      try {
        await dataApi.syncTask(
          taskId,
          undefined,
          batchSyncStartDate || undefined,
          batchSyncEndDate || undefined
        );
      } catch (error) {
        Toast.error(`任务 ${taskId} 同步失败`);
      }
    }
    setTimeout(() => {
      enabledTasks.forEach((taskId) => {
        loadTaskStatus(taskId);
        setSyncingTasks((prev) => {
          const newSet = new Set(prev);
          newSet.delete(taskId);
          return newSet;
        });
      });
      loadSyncLogs();
      setSelectedTaskIds([]);
    }, 2000);
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
      <div style={{ marginBottom: '16px' }}>
        <h1 style={{
          color: 'var(--color-primary)',
          fontSize: '24px',
          fontWeight: 700,
          margin: 0,
          letterSpacing: '1px'
        }}>
          <IconServer style={{ marginRight: '8px' }} />
          数据
        </h1>
        <p style={{
          color: 'var(--text-secondary)',
          margin: '4px 0 0 0',
          fontSize: '12px'
        }}>
          数据同步管理与 SQL 查询
        </p>
      </div>

      <Tabs defaultActiveKey="1">
        <TabPane tab={<span><IconSync /> 同步任务</span>} itemKey="1">
          <Card
            className="content-card"
            style={{ marginBottom: '12px' }}
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>同步任务管理</span>}
            headerExtraContent={
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                {selectedTaskIds.length > 0 && (
                  <Button
                    theme="solid"
                    type="primary"
                    icon={<IconSync />}
                    onClick={handleBatchSync}
                    size="small"
                  >
                    批量同步 ({selectedTaskIds.length})
                  </Button>
                )}
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
              </div>
            }
          >

            <Table
              dataSource={syncTasks.map((task) => {
                const status = taskStatuses[task.task_id];
                const taskScheduleInfo = scheduleInfo[task.task_id];
                return { ...task, status, taskScheduleInfo };
              })}
              rowKey="task_id"
              size="small"
              pagination={false}
              rowSelection={{
                selectedRowKeys: selectedTaskIds,
                onChange: (selectedRowKeys) => setSelectedTaskIds(selectedRowKeys as string[]),
                getCheckboxProps: (record: any) => ({
                  disabled: !record.enabled,
                }),
              }}
              scroll={{ x: 900 }}
              columns={[
                { title: '任务ID', dataIndex: 'task_id', key: 'task_id', width: 120, fixed: 'left' as const,
                  render: (v: string, r: any) => (
                    <span style={{ cursor: 'pointer' }} onClick={() => openTaskDrawer(r)}>
                      <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
                    </span>
                  )
                },
                { title: '描述', dataIndex: 'description', key: 'desc', width: 180, ellipsis: true },
                { title: '状态', dataIndex: 'enabled', key: 'enabled', width: 60,
                  render: (v: boolean) => <Tag color={v ? 'green' : 'red'} style={{ fontSize: '11px' }}>{v ? '启用' : '禁用'}</Tag>
                },
                { title: '类型', key: 'sync_type', width: 60,
                  render: (_: any, r: any) => <Tag color={r.sync_type === 'incremental' ? 'blue' : 'green'}>{r.sync_type === 'incremental' ? '增量' : '全量'}</Tag>
                },
                { title: '数据表', dataIndex: 'table_name', key: 'table_name', width: 120,
                  render: (v: string) => <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v}</code>
                },
                { title: '最新数据', key: 'latest', width: 100,
                  render: (_: any, r: any) => r.status?.table_latest_date
                    ? <span style={{ color: 'var(--color-gain)' }}>{r.status.table_latest_date}</span>
                    : <span style={{ color: 'var(--text-muted)' }}>-</span>
                },
                { title: '上次同步', key: 'last_sync', width: 130,
                  render: (_: any, r: any) => r.status?.last_sync_time
                    ? <Tooltip content={r.status.last_sync_time}><span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>{r.status.last_sync_time.slice(0, 16)}</span></Tooltip>
                    : '-'
                },
                { title: '操作', key: 'action', width: 130, fixed: 'right' as const,
                  render: (_: any, r: any) => {
                    const isSyncing = syncingTasks.has(r.task_id);
                    return (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <Button size="small"
                          icon={<IconSync style={isSyncing ? { animation: 'spin 1s linear infinite' } : undefined} />}
                          onClick={() => handleSyncTask(r.task_id)}
                          disabled={!r.enabled || isSyncing} loading={isSyncing}>同步</Button>
                        <Button size="small" type="danger" icon={<IconDelete />}
                          onClick={() => handleDeleteTask(r.task_id)} />
                      </div>
                    );
                  }
                },
              ]}
            />
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

        <TabPane tab={<span><IconServer /> 数据表</span>} itemKey="2">
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

        <TabPane tab={<span><IconCode /> SQL 查询</span>} itemKey="3">
          <Card
            className="content-card"
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>SQL 查询</span>}
            headerExtraContent={
              <Button
                icon={<IconExternalOpen />}
                onClick={() => window.open('http://localhost:8848', '_blank')}
                size="small"
              >
                数据库管理
              </Button>
            }
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

      {/* Batch Sync Modal */}
      <Modal
        title={`批量同步 (${selectedTaskIds.length} 个任务)`}
        visible={batchSyncModalVisible}
        onOk={executeBatchSync}
        onCancel={() => setBatchSyncModalVisible(false)}
        okText="开始同步"
        cancelText="取消"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 8 }}>已选择的任务：</div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {selectedTaskIds.map(id => {
                const task = syncTasks.find(t => t.task_id === id);
                return (
                  <Tag key={id} color={task?.enabled ? 'blue' : 'grey'} style={{ fontSize: '12px' }}>
                    {id}
                  </Tag>
                );
              })}
            </div>
          </div>

          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>开始日期（可选）</div>
            <Input
              placeholder="YYYYMMDD（如 20240101）"
              value={batchSyncStartDate}
              onChange={(v) => setBatchSyncStartDate(v)}
              maxLength={8}
              size="small"
            />
          </div>
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>结束日期（可选）</div>
            <Input
              placeholder="YYYYMMDD（如 20240131）"
              value={batchSyncEndDate}
              onChange={(v) => setBatchSyncEndDate(v)}
              maxLength={8}
              size="small"
            />
            <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
              指定日期范围进行同步，留空则只同步最新一天的数据。全量同步任务会忽略日期参数。
            </div>
          </div>
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

      {/* Task Detail SideSheet */}
      <SideSheet
        title={<span style={{ color: 'var(--color-primary)' }}>{taskDrawerTask?.task_id}</span>}
        visible={taskDrawerVisible}
        onCancel={() => setTaskDrawerVisible(false)}
        width={720}
      >
        {taskDrawerTask && taskDrawerConfig && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* 状态信息栏 */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px' }}>
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>最新数据</div>
                <span style={{ color: 'var(--color-gain)', fontSize: '13px', fontWeight: 500 }}>
                  {taskStatuses[taskDrawerTask.task_id]?.table_latest_date || '-'}
                </span>
              </div>
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>上次同步</div>
                <span style={{ color: 'var(--text-secondary)', fontSize: '13px' }}>
                  {taskStatuses[taskDrawerTask.task_id]?.last_sync_time || '-'}
                </span>
              </div>
            </div>

            <Tabs activeKey={taskDrawerTab} onChange={setTaskDrawerTab} size="small">
              <TabPane tab="可视化编辑" itemKey="visual">
                <div style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingTop: 8 }}>
                  {/* 基本信息 */}
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>任务ID</div>
                      <Input size="small" value={taskDrawerConfig.task_id || ''} disabled style={{ background: 'var(--bg-surface)' }} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>API名称</div>
                      <Input size="small" value={taskDrawerConfig.api_name || ''} onChange={(v) => updateTaskDrawerConfig('api_name', v)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>描述</div>
                      <Input size="small" value={taskDrawerConfig.description || ''} onChange={(v) => updateTaskDrawerConfig('description', v)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>数据表</div>
                      <Input size="small" value={taskDrawerConfig.table_name || ''} onChange={(v) => updateTaskDrawerConfig('table_name', v)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>同步类型</div>
                      <Select size="small" value={taskDrawerConfig.sync_type} onChange={(v) => updateTaskDrawerConfig('sync_type', v)} style={{ width: '100%' }}>
                        <Select.Option value="incremental">增量</Select.Option>
                        <Select.Option value="full">全量</Select.Option>
                      </Select>
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>调度周期</div>
                      <Select size="small" value={taskDrawerConfig.schedule} onChange={(v) => updateTaskDrawerConfig('schedule', v)} style={{ width: '100%' }}>
                        <Select.Option value="daily">每日</Select.Option>
                        <Select.Option value="weekly">每周</Select.Option>
                        <Select.Option value="monthly">每月</Select.Option>
                      </Select>
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>日期字段</div>
                      <Input size="small" value={taskDrawerConfig.date_field || ''} onChange={(v) => updateTaskDrawerConfig('date_field', v)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>API限制</div>
                      <Input size="small" type="number" value={taskDrawerConfig.api_limit || ''} onChange={(v) => updateTaskDrawerConfig('api_limit', parseInt(v) || 0)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>主键</div>
                      <Input size="small" value={taskDrawerConfig.primary_keys?.join(', ') || ''} onChange={(v) => updateTaskDrawerConfig('primary_keys', v.split(',').map((s: string) => s.trim()).filter(Boolean))} />
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>启用</div>
                      <Switch checked={taskDrawerConfig.enabled} onChange={(v) => updateTaskDrawerConfig('enabled', v)} size="small" />
                    </div>
                  </div>

                  {/* API 参数 */}
                  {taskDrawerConfig.params && (
                    <Collapse defaultActiveKey={['params']}>
                      <Collapse.Panel header={<span style={{ fontSize: '13px', fontWeight: 500 }}>API 参数</span>} itemKey="params">
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                          {Object.entries(taskDrawerConfig.params).map(([key, value]) => (
                            <div key={key}>
                              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>{key}</div>
                              <Input size="small" value={String(value)} onChange={(v) => updateParamsField(key, v)} />
                            </div>
                          ))}
                        </div>
                      </Collapse.Panel>
                    </Collapse>
                  )}

                  {/* Schema 字段表格 */}
                  {taskDrawerConfig.schema && (
                    <Collapse defaultActiveKey={['schema']}>
                      <Collapse.Panel header={<span style={{ fontSize: '13px', fontWeight: 500 }}>字段定义 (Schema)</span>} itemKey="schema">
                        <Table
                          dataSource={Object.entries(taskDrawerConfig.schema).map(([name, props]: [string, any]) => ({
                            name,
                            type: props?.type || '',
                            nullable: props?.nullable,
                            comment: props?.comment || '',
                          }))}
                          rowKey="name"
                          size="small"
                          pagination={false}
                          columns={[
                            { title: '字段名', dataIndex: 'name', key: 'name', width: 120,
                              render: (v: string) => <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
                            },
                            { title: '类型', dataIndex: 'type', key: 'type', width: 100,
                              render: (v: string, r: any) => (
                                <Input size="small" value={v} onChange={(val) => updateSchemaField(r.name, 'type', val)} style={{ width: 90 }} />
                              )
                            },
                            { title: '可空', dataIndex: 'nullable', key: 'nullable', width: 60,
                              render: (v: boolean, r: any) => (
                                <Switch size="small" checked={v} onChange={(val) => updateSchemaField(r.name, 'nullable', val)} />
                              )
                            },
                            { title: '备注', dataIndex: 'comment', key: 'comment',
                              render: (v: string, r: any) => (
                                <Input size="small" value={v} onChange={(val) => updateSchemaField(r.name, 'comment', val)} />
                              )
                            },
                          ]}
                        />
                      </Collapse.Panel>
                    </Collapse>
                  )}
                </div>
              </TabPane>

              <TabPane tab="JSON 编辑" itemKey="json">
                <div style={{ paddingTop: 8 }}>
                  <TextArea
                    value={taskDrawerJson}
                    onChange={(v) => {
                      setTaskDrawerJson(v);
                      parseTaskDrawerJson(v);
                    }}
                    rows={20}
                    style={{
                      fontFamily: 'Fira Code, Courier New, monospace',
                      fontSize: '12px',
                    }}
                  />
                  <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
                    直接编辑 JSON 配置，修改会同步到可视化编辑界面
                  </div>
                </div>
              </TabPane>
            </Tabs>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, borderTop: '1px solid var(--border-color)', paddingTop: 12 }}>
              <Button onClick={() => setTaskDrawerVisible(false)}>取消</Button>
              <Button theme="solid" type="primary" onClick={handleSaveTaskDrawer}>保存</Button>
            </div>
          </div>
        )}
      </SideSheet>
    </div>
  );
};

export default DataCenter;
