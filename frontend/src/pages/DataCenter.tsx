import React, { useEffect, useState, useCallback, useMemo } from 'react';
import Editor from '@monaco-editor/react';
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
  IconCopy,
  IconPlus,
} from '@douyinfe/semi-icons';
import ReactECharts from 'echarts-for-react';
import dayjs from 'dayjs';
import { dataApi, productionApi } from '../api';
import { useThemeStore } from '../store';
import TradingViewChart from '../components/Charts/TradingViewChart';

interface SyncTask {
  task_id: string;
  description: string;
  sync_type: string;
  table_name: string;
  source?: string;
  enabled?: boolean;
}

interface TaskStatus {
  task_id: string;
  description: string;
  sync_type: string;
  last_sync_date: string | null;
  last_sync_time: string | null;
  table_name: string;
  table_latest_date?: string | null;
  source?: string;
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
  const { mode } = useThemeStore();
  const [stocks, setStocks] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>('');
  const [dailyData, setDailyData] = useState<any[]>([]);
  const [syncLogs, setSyncLogs] = useState<SyncLog[]>([]);
  const [loading, setLoading] = useState(false);

  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM sync_daily_basic LIMIT 10');
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
  const [isNewTask, setIsNewTask] = useState(false);

  const [scheduleInfo, setScheduleInfo] = useState<Record<string, any>>({});

  const [taskDrawerVisible, setTaskDrawerVisible] = useState(false);
  const [taskDrawerTask, setTaskDrawerTask] = useState<SyncTask | null>(null);
  const [taskDrawerJson, setTaskDrawerJson] = useState<string>('');
  const [taskDrawerConfig, setTaskDrawerConfig] = useState<any>(null);
  const [taskDrawerTab, setTaskDrawerTab] = useState<string>('visual');

  const [copyTaskModalVisible, setCopyTaskModalVisible] = useState(false);
  const [copyTaskSource, setCopyTaskSource] = useState<SyncTask | null>(null);
  const [copyTaskNewId, setCopyTaskNewId] = useState<string>('');
  const [copyTaskNewTableName, setCopyTaskNewTableName] = useState<string>('');
  const [copyTaskStep, setCopyTaskStep] = useState<'input' | 'table_not_exists'>('input');
  const [copyTaskConfig, setCopyTaskConfig] = useState<any>(null);

  const [taskSyncHistory, setTaskSyncHistory] = useState<SyncLog[]>([]);

  // ETL 任务状态
  const [etlTasks, setEtlTasks] = useState<any[]>([]);
  const [etlDrawerVisible, setEtlDrawerVisible] = useState(false);
  const [etlDrawerConfig, setEtlDrawerConfig] = useState<any>({ task_id: '', description: '', script: '', sync_type: 'incremental', date_field: '', primary_keys: [] as string[], table_name: '' });
  const [isNewEtlTask, setIsNewEtlTask] = useState(false);
  const [runningEtlTasks, setRunningEtlTasks] = useState<Set<string>>(new Set());
  const [etlTestResult, setEtlTestResult] = useState<any>(null);
  const [etlTestLoading, setEtlTestLoading] = useState(false);
  const [etlTestDate, setEtlTestDate] = useState<string>('');
  const [etlBackfillModalVisible, setEtlBackfillModalVisible] = useState(false);
  const [etlBackfillTaskId, setEtlBackfillTaskId] = useState<string>('');
  const [etlBackfillStartDate, setEtlBackfillStartDate] = useState<string>('');
  const [etlBackfillEndDate, setEtlBackfillEndDate] = useState<string>('');
  const [selectedEtlTaskIds, setSelectedEtlTaskIds] = useState<string[]>([]);
  const [batchBackfillModalVisible, setBatchBackfillModalVisible] = useState(false);
  const [batchBackfillStartDate, setBatchBackfillStartDate] = useState<string>('');
  const [batchBackfillEndDate, setBatchBackfillEndDate] = useState<string>('');
  const [etlSelectedPrimaryKeys, setEtlSelectedPrimaryKeys] = useState<string[]>([]);
  const [etlLogs, setEtlLogs] = useState<SyncLog[]>([]);
  const [etlLogFilters, setEtlLogFilters] = useState({ taskId: undefined as string | undefined, startDate: undefined as string | undefined, endDate: undefined as string | undefined });
  const [etlTaskStatus, setEtlTaskStatus] = useState<{ last_date: string | null; last_sync_time: string | null } | null>(null);
  const [etlDrawerTab, setEtlDrawerTab] = useState('config');
  const [etlTaskHistory, setEtlTaskHistory] = useState<SyncLog[]>([]);
  const [etlSavedSchema, setEtlSavedSchema] = useState<{ name: string; type: string }[]>([]);
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
    setIsNewTask(false);
    setTaskSyncHistory([]);
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
    // 加载历史调度数据
    try {
      const historyRes = await dataApi.getSyncStatus(undefined, task.task_id, undefined, undefined, 50);
      setTaskSyncHistory(historyRes.data.logs || []);
    } catch {
      setTaskSyncHistory([]);
    }
    setTaskDrawerVisible(true);
  };

  const handleSaveTaskDrawer = async () => {
    try {
      const config = JSON.parse(taskDrawerJson);
      if (isNewTask) {
        await dataApi.createTask(config);
        Toast.success(`任务 ${config.task_id} 创建成功`);
      } else {
        await dataApi.updateTaskConfig(config.task_id, config);
        Toast.success(`任务 ${config.task_id} 更新成功`);
      }
      setTaskDrawerVisible(false);
      setIsNewTask(false);
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
    const [stocksRes, tasksRes, tablesRes, etlRes] = await Promise.all([
      dataApi.listStocks().catch(() => ({ data: { stocks: [] } })),
      dataApi.listSyncTasks().catch(() => ({ data: { tasks: [] } })),
      dataApi.listTables().catch(() => ({ data: { tables: [] } })),
      dataApi.listEtlTasks().catch(() => ({ data: { tasks: [] } })),
    ]);

    setStocks(stocksRes.data.stocks || []);
    setSyncTasks(tasksRes.data.tasks || []);
    setTables(tablesRes.data.tables || []);
    setEtlTasks(etlRes.data.tasks || []);

    const tasks = tasksRes.data.tasks || [];
    for (const task of tasks) {
      loadTaskStatus(task.task_id);
      loadTaskScheduleInfo(task.task_id);
    }

    loadSyncLogs();
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

  // ETL 任务操作
  const loadEtlTasks = async () => {
    try {
      const res = await dataApi.listEtlTasks();
      setEtlTasks(res.data.tasks || []);
    } catch (error) {
      console.error('Failed to load ETL tasks');
    }
  };

  const loadEtlLogs = async () => {
    try {
      const res = await dataApi.getEtlLogs(etlLogFilters.taskId, etlLogFilters.startDate, etlLogFilters.endDate);
      setEtlLogs(res.data.logs || []);
    } catch (error) {
      console.error('Failed to load ETL logs');
    }
  };

  const handleNewEtlTask = () => {
    setEtlDrawerConfig({ task_id: 'etl_', description: '', script: 'select ts_code, close, adj_factor\nfrom lj(\n  select ts_code, close from loadTable("dfs://quant", "sync_daily_data") where trade_date = {date},\n  select ts_code, adj_factor from loadTable("dfs://quant", "sync_adj_factor") where trade_date = {date},\n  `ts_code\n)', sync_type: 'incremental', date_field: '', primary_keys: [], table_name: '' });
    setIsNewEtlTask(true);
    setEtlTestResult(null);
    setEtlSelectedPrimaryKeys([]);
    setEtlDrawerTab('config');
    setEtlTaskHistory([]);
    setEtlTaskStatus(null);
    setEtlSavedSchema([]);
    setEtlDrawerVisible(true);
  };

  const handleEditEtlTask = (task: any) => {
    const pks = task.primary_keys_json ? (typeof task.primary_keys_json === 'string' ? JSON.parse(task.primary_keys_json) : task.primary_keys_json) : [];
    setEtlDrawerConfig({ ...task, sync_type: task.sync_type || 'incremental', primary_keys: pks });
    setIsNewEtlTask(false);
    setEtlTestResult(null);
    setEtlSelectedPrimaryKeys(pks);
    setEtlTaskStatus(null);
    setEtlDrawerTab('config');
    setEtlTaskHistory([]);
    setEtlSavedSchema([]);
    dataApi.getEtlTaskStatus(task.task_id).then((res) => setEtlTaskStatus(res.data)).catch(() => {});
    dataApi.getEtlLogs(task.task_id, undefined, undefined, 100).then((res) => setEtlTaskHistory(res.data.logs || [])).catch(() => {});
    dataApi.getEtlTableSchema(task.task_id).then((res) => setEtlSavedSchema(res.data.fields || [])).catch(() => {});
    setEtlDrawerVisible(true);
  };

  const handleCopyEtlTask = (task: any) => {
    const pks = task.primary_keys_json ? (typeof task.primary_keys_json === 'string' ? JSON.parse(task.primary_keys_json) : task.primary_keys_json) : [];
    setEtlDrawerConfig({ ...task, task_id: `${task.task_id}_copy`, table_name: `${task.task_id}_copy`, sync_type: task.sync_type || 'incremental', primary_keys: pks });
    setIsNewEtlTask(true);
    setEtlTestResult(null);
    setEtlSelectedPrimaryKeys(pks);
    setEtlSavedSchema([]);
    setEtlDrawerVisible(true);
  };

  const handleSaveEtlTask = async () => {
    try {
      if (!etlDrawerConfig.task_id) {
        Toast.warning('请输入任务 ID');
        return;
      }
      const config = {
        ...etlDrawerConfig,
        table_name: etlDrawerConfig.task_id,
        primary_keys: etlSelectedPrimaryKeys,
      };
      if (isNewEtlTask) {
        await dataApi.createEtlTask(config);
        Toast.success(`ETL 任务 ${config.task_id} 创建成功`);
      } else {
        await dataApi.updateEtlTask(config.task_id, config);
        Toast.success(`ETL 任务 ${config.task_id} 更新成功`);
      }
      // 如果有测试结果的字段类型，自动建表
      if (etlTestResult?.field_types?.length > 0) {
        try {
          await dataApi.createEtlTable(config.task_id, config.table_name || config.task_id, etlTestResult.field_types);
          Toast.success(`目标表 ${config.table_name || config.task_id} 已创建`);
        } catch (e: any) {
          if (!e.response?.data?.detail?.includes('已存在')) {
            Toast.warning(e.response?.data?.detail || '建表失败');
          }
        }
      }
      setEtlDrawerVisible(false);
      loadEtlTasks();
      // 询问是否回溯
      setEtlBackfillTaskId(config.task_id);
      setEtlBackfillStartDate('');
      setEtlBackfillEndDate('');
      setEtlBackfillModalVisible(true);
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '保存 ETL 任务失败');
    }
  };

  const handleTestEtlScript = async () => {
    if (!etlDrawerConfig.script?.trim()) {
      Toast.warning('请先输入脚本');
      return;
    }
    setEtlTestLoading(true);
    setEtlTestResult(null);
    try {
      const res = await dataApi.testEtlScript(etlDrawerConfig.script, etlTestDate || undefined);
      setEtlTestResult(res.data);
      // 自动设置主键选择
      if (res.data.field_types) {
        setEtlSelectedPrimaryKeys(etlDrawerConfig.primary_keys || []);
      }
    } catch (error: any) {
      setEtlTestResult({ status: 'error', error: error.response?.data?.detail || '脚本执行失败' });
    } finally {
      setEtlTestLoading(false);
    }
  };

  const handleBackfillEtlTask = async () => {
    if (!etlBackfillStartDate || !etlBackfillEndDate) {
      Toast.warning('请选择回溯日期范围');
      return;
    }
    setEtlBackfillModalVisible(false);
    Toast.info(`开始回溯 ${etlBackfillTaskId}...`);
    try {
      const res = await dataApi.backfillEtlTask(etlBackfillTaskId, etlBackfillStartDate, etlBackfillEndDate);
      Toast.success(res.data.message || '回溯完成');
      loadEtlLogs();
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '回溯执行失败');
    }
  };

  const openEtlBackfillModal = (taskId: string) => {
    setEtlBackfillTaskId(taskId);
    setEtlBackfillStartDate('');
    setEtlBackfillEndDate('');
    setEtlBackfillModalVisible(true);
  };

  const handleBatchBackfill = () => {
    if (selectedEtlTaskIds.length === 0) {
      Toast.warning('请先选择要回溯的 ETL 任务');
      return;
    }
    setBatchBackfillStartDate('');
    setBatchBackfillEndDate('');
    setBatchBackfillModalVisible(true);
  };

  const executeBatchBackfill = async () => {
    const incrementalIds = selectedEtlTaskIds.filter(id => {
      const t = etlTasks.find(t => t.task_id === id);
      return !t || t.sync_type !== 'full';
    });
    if (incrementalIds.length > 0 && (!batchBackfillStartDate || !batchBackfillEndDate)) {
      Toast.warning('存在增量任务，请选择回溯日期范围');
      return;
    }
    setBatchBackfillModalVisible(false);
    const taskIds = [...selectedEtlTaskIds];
    Toast.info(`开始回溯 ${taskIds.length} 个 ETL 任务`);
    for (const taskId of taskIds) {
      setRunningEtlTasks((prev) => new Set(prev).add(taskId));
      const task = etlTasks.find(t => t.task_id === taskId);
      const isFull = task?.sync_type === 'full';
      try {
        if (isFull) {
          await dataApi.runEtlTask(taskId);
        } else {
          await dataApi.backfillEtlTask(taskId, batchBackfillStartDate, batchBackfillEndDate);
        }
        Toast.success(`ETL 任务 ${taskId} ${isFull ? '执行' : '回溯'}完成`);
      } catch (error: any) {
        Toast.error(`ETL 任务 ${taskId} 失败: ${error.response?.data?.detail || error.message}`);
      } finally {
        setRunningEtlTasks((prev) => { const s = new Set(prev); s.delete(taskId); return s; });
      }
    }
    loadEtlLogs();
    setSelectedEtlTaskIds([]);
  };

  const handleDeleteEtlTask = async (taskId: string) => {
    setDeleteConfirmTaskId(taskId);
    setDeleteConfirmType('etl');
    setDeleteConfirmVisible(true);
  };

  const handleRunEtlTask = async (taskId: string) => {
    setRunningEtlTasks((prev) => new Set(prev).add(taskId));
    try {
      const res = await dataApi.runEtlTask(taskId);
      Toast.success(res.data?.message || `ETL 任务 ${taskId} 执行成功`);
      loadEtlLogs();
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || `ETL 任务 ${taskId} 执行失败`);
    } finally {
      setRunningEtlTasks((prev) => { const s = new Set(prev); s.delete(taskId); return s; });
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
    setBatchSyncStartDate('');
    setBatchSyncEndDate('');
    setBatchSyncModalVisible(true);
  };

  const executeBatchSync = async () => {
    const fullIds = selectedTaskIds.filter(id => {
      const t = syncTasks.find(t => t.task_id === id);
      return t?.sync_type === 'full';
    });
    const incrementalIds = selectedTaskIds.filter(id => !fullIds.includes(id));
    if (incrementalIds.length > 0 && (!batchSyncStartDate || !batchSyncEndDate)) {
      Toast.warning('存在增量任务，请选择日期范围');
      return;
    }
    setBatchSyncModalVisible(false);
    for (const taskId of selectedTaskIds) {
      setSyncingTasks((prev) => new Set(prev).add(taskId));
    }
    Toast.info(`开始同步 ${selectedTaskIds.length} 个任务`);
    for (const taskId of selectedTaskIds) {
      const isFull = fullIds.includes(taskId);
      try {
        if (isFull) {
          await dataApi.syncTask(taskId);
        } else {
          await dataApi.syncTask(
            taskId,
            undefined,
            batchSyncStartDate || undefined,
            batchSyncEndDate || undefined
          );
        }
      } catch (error) {
        Toast.error(`任务 ${taskId} 同步失败`);
      }
    }
    setTimeout(() => {
      selectedTaskIds.forEach((taskId) => {
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
      task_id: "sync_",
      api_name: "api_name",
      description: "任务描述",
      sync_type: "incremental",
      source: "tushare",
      params: {
        trade_date: "{date}",
        fields: "ts_code,trade_date"
      },
      date_field: "trade_date",
      primary_keys: ["ts_code", "trade_date"],
      table_name: "sync_",
      schema: {
        ts_code: { type: "VARCHAR", nullable: false, comment: "股票代码" },
        trade_date: { type: "VARCHAR", nullable: false, comment: "交易日期" }
      },
      api_limit: 5000
    };
    setTaskDrawerTask(null);
    setTaskDrawerConfig(template);
    setTaskDrawerJson(JSON.stringify(template, null, 2));
    setTaskDrawerTab('visual');
    setIsNewTask(true);
    setTaskDrawerVisible(true);
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

  const [deleteConfirmVisible, setDeleteConfirmVisible] = useState(false);
  const [deleteConfirmTaskId, setDeleteConfirmTaskId] = useState<string>('');
  const [deleteConfirmType, setDeleteConfirmType] = useState<'sync' | 'etl'>('sync');

  const handleDeleteTask = async (taskId: string) => {
    setDeleteConfirmTaskId(taskId);
    setDeleteConfirmType('sync');
    setDeleteConfirmVisible(true);
  };

  const executeDeleteTask = async () => {
    const taskId = deleteConfirmTaskId;
    const type = deleteConfirmType;
    setDeleteConfirmVisible(false);
    try {
      if (type === 'sync') {
        await dataApi.deleteTask(taskId, true);
        Toast.success(`同步任务 ${taskId} 已删除`);
        loadInitialData();
      } else {
        await dataApi.deleteEtlTask(taskId, true);
        Toast.success(`ETL 任务 ${taskId} 已删除`);
        loadEtlTasks();
      }
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '删除失败');
    }
  };

  const handleCopyTask = (task: SyncTask) => {
    setCopyTaskSource(task);
    setCopyTaskNewId(`${task.task_id}_copy`);
    setCopyTaskNewTableName(`${task.table_name}_copy`);
    setCopyTaskStep('input');
    setCopyTaskConfig(null);
    setCopyTaskModalVisible(true);
  };

  const executeCopyTask = async () => {
    if (copyTaskStep === 'input') {
      if (!copyTaskSource || !copyTaskNewId.trim()) {
        Toast.warning('请输入新任务ID');
        return;
      }
      if (!copyTaskNewTableName.trim()) {
        Toast.warning('请输入数据表名称');
        return;
      }
      // 检查表名是否已被占用
      const tableRes = await dataApi.getTableInfo(copyTaskNewTableName.trim());
      if (tableRes.data.exists) {
        Toast.error(`数据表 ${copyTaskNewTableName.trim()} 已存在，请使用其他表名`);
        return;
      }
      try {
        const configRes = await dataApi.getTaskConfig(copyTaskSource.task_id);
        const newConfig = { ...configRes.data.config, task_id: copyTaskNewId.trim(), table_name: copyTaskNewTableName.trim() };
        setCopyTaskConfig(newConfig);
        setCopyTaskStep('table_not_exists');
      } catch (error: any) {
        Toast.error(error.response?.data?.detail || '获取任务配置失败');
      }
    } else if (copyTaskStep === 'table_not_exists') {
      // 首次同步时建表
      try {
        await dataApi.createTask(copyTaskConfig);
        Toast.success(`任务 ${copyTaskNewId} 创建成功，将在首次同步时自动建表`);
        setCopyTaskModalVisible(false);
        loadInitialData();
      } catch (error: any) {
        Toast.error(error.response?.data?.detail || '复制任务失败');
      }
    }
  };

  const executeCopyTaskCreateTableNow = async () => {
    try {
      await dataApi.createTask(copyTaskConfig);
      await dataApi.createSyncTaskTable(copyTaskNewId.trim());
      Toast.success(`任务 ${copyTaskNewId} 创建成功，表已建立`);
      setCopyTaskModalVisible(false);
      loadInitialData();
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '创建失败');
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
        <span style={{ fontSize: '12px', wordBreak: 'break-word', whiteSpace: 'normal', lineHeight: '1.6' }}>{text.join(', ')}</span>
      ),
    },
    {
      title: '操作', key: 'action', width: 80,
      render: (text: any, record: TableInfo) => (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
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
                { title: '来源', dataIndex: 'source', key: 'source', width: 100,
                  render: (v: string) => <Tag color="cyan" style={{ fontSize: '11px' }}>{v || 'tushare'}</Tag>
                },
                { title: '类型', key: 'sync_type', width: 60,
                  render: (_: any, r: any) => <Tag color={r.sync_type === 'incremental' ? 'blue' : 'green'}>{r.sync_type === 'incremental' ? '增量' : '全量'}</Tag>
                },
                { title: '数据表', dataIndex: 'table_name', key: 'table_name', width: 120,
                  render: (v: string) => <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v}</code>
                },
                { title: '最新数据', key: 'latest', width: 100,
                  render: (_: any, r: any) => {
                    const dateStr = r.status?.table_latest_date;
                    if (!dateStr) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
                    // 格式化日期显示 YYYYMMDD -> YYYY-MM-DD
                    const formatted = typeof dateStr === 'string' && dateStr.length === 8
                      ? `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`
                      : String(dateStr).slice(0, 10);
                    return <span style={{ color: 'var(--color-gain)' }}>{formatted}</span>;
                  }
                },
                { title: '上次同步', key: 'last_sync', width: 140,
                  render: (_: any, r: any) => {
                    const syncTime = r.status?.last_sync_time;
                    if (!syncTime) return '-';
                    return <Tooltip content={syncTime}><span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>{syncTime.slice(0, 16)}</span></Tooltip>;
                  }
                },
                { title: '操作', key: 'action', width: 180, fixed: 'right' as const,
                  render: (_: any, r: any) => {
                    const isSyncing = syncingTasks.has(r.task_id);
                    return (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <Button size="small"
                          icon={<IconSync style={isSyncing ? { animation: 'spin 1s linear infinite' } : undefined} />}
                          onClick={() => handleSyncTask(r.task_id)}
                          disabled={isSyncing} loading={isSyncing}>同步</Button>
                        <Tooltip content="复制任务">
                          <Button size="small" icon={<IconCopy />} onClick={() => handleCopyTask(r)} />
                        </Tooltip>
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
                placeholder="按任务筛选"
                style={{ width: 150 }}
                showClear
                size="small"
                optionList={syncTasks.map(task => ({ label: task.task_id, value: task.task_id }))}
                onChange={(value) => setLogFilters({ ...logFilters, dataType: value as string | undefined })}
              />
              <DatePicker
                type="dateRange"
                placeholder={['开始日期', '结束日期']}
                style={{ width: 280 }}
                size="small"
                value={(logFilters.startDate && logFilters.endDate) ? [dayjs(logFilters.startDate, 'YYYYMMDD').toDate(), dayjs(logFilters.endDate, 'YYYYMMDD').toDate()] : undefined}
                onChange={(date, dateStr) => {
                  const strs = dateStr as unknown as string[];
                  if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                    setLogFilters({ ...logFilters, startDate: strs[0].replace(/-/g, ''), endDate: strs[1].replace(/-/g, '') });
                  } else {
                    setLogFilters({ ...logFilters, startDate: undefined, endDate: undefined });
                  }
                }}
              />
              <Button theme="solid" type="primary" onClick={loadSyncLogs} size="small">筛选</Button>
            </div>
            <Table
              dataSource={syncLogs}
              columns={[
                { title: '任务ID', dataIndex: 'data_type', key: 'data_type', width: 150,
                  render: (v: string) => <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
                },
                { title: '来源', dataIndex: 'source', key: 'source', width: 120 },
                { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 120 },
                {
                  title: '参数', dataIndex: 'params', key: 'params', width: 200,
                  render: (v: string) => v ? <code style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>{v}</code> : '-'
                },
                {
                  title: '同步行数', dataIndex: 'rows_synced', key: 'rows_synced', width: 100,
                  render: (text: number) => text.toLocaleString()
                },
                {
                  title: '状态', dataIndex: 'status', key: 'status', width: 80,
                  render: (text: string, record: any) => {
                    const tag = <Tag color={text === 'success' ? 'green' : 'red'}>{text}</Tag>;
                    return (text !== 'success' && record.error_message)
                      ? <Tooltip content={record.error_message}>{tag}</Tooltip>
                      : tag;
                  }
                },
                {
                  title: '创建时间', dataIndex: 'created_at', key: 'created_at', width: 160,
                  render: (text: string) => new Date(text).toLocaleString()
                },
              ]}
              rowKey={(record: any) => `${record.data_type}-${record.sync_date}-${record.created_at}`}
              size="small"
              pagination={{ pageSize: 20, showSizeChanger: true }}
            />
          </Card>
        </TabPane>

        <TabPane tab={<span><IconPlay /> ETL 任务</span>} itemKey="1.5">
          <Card
            className="content-card"
            style={{ marginBottom: '12px' }}
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>ETL 任务管理</span>}
            headerExtraContent={
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                {selectedEtlTaskIds.length > 0 && (
                  <Button
                    theme="solid"
                    type="primary"
                    icon={<IconHistory />}
                    onClick={handleBatchBackfill}
                    size="small"
                  >
                    批量回溯 ({selectedEtlTaskIds.length})
                  </Button>
                )}
                <Button icon={<IconRefresh />} onClick={loadEtlTasks} size="small">刷新</Button>
                <Button onClick={handleNewEtlTask} size="small">新建任务</Button>
              </div>
            }
          >
            <Table
              dataSource={etlTasks}
              rowKey="task_id"
              size="small"
              pagination={false}
              rowSelection={{
                selectedRowKeys: selectedEtlTaskIds,
                onChange: (selectedRowKeys) => setSelectedEtlTaskIds(selectedRowKeys as string[]),
              }}
              empty={<div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无 ETL 任务</div>}
              columns={[
                { title: '任务ID', dataIndex: 'task_id', key: 'task_id', width: 120, fixed: 'left' as const,
                  render: (v: string, r: any) => (
                    <span style={{ cursor: 'pointer' }} onClick={() => handleEditEtlTask(r)}>
                      <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
                    </span>
                  )
                },
                { title: '描述', dataIndex: 'description', key: 'desc', width: 180, ellipsis: true },
                { title: '类型', dataIndex: 'sync_type', key: 'sync_type', width: 60,
                  render: (v: string) => <Tag color={v === 'incremental' ? 'blue' : 'green'}>{v === 'incremental' ? '增量' : '全量'}</Tag>
                },
                { title: '数据表', dataIndex: 'table_name', key: 'table_name', width: 120,
                  render: (v: string) => <code style={{ color: 'var(--color-gain)', fontSize: '12px' }}>{v || '-'}</code>
                },
                { title: '最新数据', dataIndex: 'last_date', key: 'last_date', width: 100,
                  render: (v: string) => {
                    if (!v) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
                    const formatted = typeof v === 'string' && v.length === 8
                      ? `${v.slice(0, 4)}-${v.slice(4, 6)}-${v.slice(6, 8)}`
                      : String(v).slice(0, 10);
                    return <span style={{ color: 'var(--color-gain)' }}>{formatted}</span>;
                  }
                },
                { title: '上次同步', dataIndex: 'last_sync_time', key: 'last_sync_time', width: 140,
                  render: (v: string) => {
                    if (!v) return <span style={{ color: 'var(--text-muted)' }}>-</span>;
                    return <Tooltip content={v}><span style={{ color: 'var(--text-secondary)', fontSize: '12px' }}>{v.slice(0, 16)}</span></Tooltip>;
                  }
                },
                { title: '操作', key: 'action', width: 180, fixed: 'right' as const,
                  render: (_: any, r: any) => {
                    return (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <Button size="small" icon={<IconHistory />}
                          onClick={() => openEtlBackfillModal(r.task_id)}>回溯</Button>
                        <Tooltip content="复制任务">
                          <Button size="small" icon={<IconCopy />} onClick={() => handleCopyEtlTask(r)} />
                        </Tooltip>
                        <Button size="small" type="danger" icon={<IconDelete />}
                          onClick={() => handleDeleteEtlTask(r.task_id)} />
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
            title={<span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>ETL 任务日志</span>}
            headerExtraContent={
              <Button icon={<IconRefresh />} onClick={loadEtlLogs} size="small" theme="borderless">刷新</Button>
            }
          >
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 12, alignItems: 'center' }}>
              <Select
                placeholder="按任务筛选"
                style={{ width: 150 }}
                showClear
                size="small"
                optionList={etlTasks.map(t => ({ label: t.task_id, value: t.task_id }))}
                onChange={(value) => setEtlLogFilters({ ...etlLogFilters, taskId: value as string | undefined })}
              />
              <DatePicker
                type="dateRange"
                placeholder={['开始日期', '结束日期']}
                style={{ width: 280 }}
                size="small"
                onChange={(date: any, dateStr: any) => {
                  const strs = dateStr as unknown as string[];
                  if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                    setEtlLogFilters({ ...etlLogFilters, startDate: strs[0].replace(/-/g, ''), endDate: strs[1].replace(/-/g, '') });
                  } else {
                    setEtlLogFilters({ ...etlLogFilters, startDate: undefined, endDate: undefined });
                  }
                }}
              />
              <Button theme="solid" type="primary" onClick={loadEtlLogs} size="small">筛选</Button>
            </div>
            <Table
              dataSource={etlLogs}
              columns={[
                { title: '任务ID', dataIndex: 'data_type', key: 'data_type', width: 150,
                  render: (v: string) => <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{v}</code>
                },
                { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 120 },
                { title: '参数', dataIndex: 'params', key: 'params', width: 200,
                  render: (v: string) => v ? <code style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>{v}</code> : '-'
                },
                { title: '同步行数', dataIndex: 'rows_synced', key: 'rows_synced', width: 100,
                  render: (text: number) => text?.toLocaleString() || '0'
                },
                { title: '状态', dataIndex: 'status', key: 'status', width: 80,
                  render: (text: string, record: any) => {
                    const tag = <Tag color={text === 'success' ? 'green' : 'red'}>{text}</Tag>;
                    return (text !== 'success' && record.error_message)
                      ? <Tooltip content={record.error_message}>{tag}</Tooltip>
                      : tag;
                  }
                },
                { title: '执行时间', dataIndex: 'created_at', key: 'created_at', width: 160,
                  render: (text: string) => text ? new Date(text).toLocaleString() : '-'
                },
              ]}
              rowKey={(record: any) => `${record.data_type}-${record.sync_date}-${record.created_at}`}
              size="small"
              pagination={{ pageSize: 20, showSizeChanger: true }}
              empty={<div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无日志</div>}
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
                <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
                  <Editor
                    height="180px"
                    language="sql"
                    theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                    value={sqlQuery}
                    onChange={(v) => setSqlQuery(v || '')}
                    options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4, wordWrap: 'on' }}
                  />
                </div>
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
                <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>日期范围（可选）</div>
                <DatePicker
                  type="dateRange"
                  placeholder={['开始日期', '结束日期']}
                  value={(syncStartDate && syncEndDate) ? [dayjs(syncStartDate, 'YYYYMMDD').toDate(), dayjs(syncEndDate, 'YYYYMMDD').toDate()] : undefined}
                  onChange={(date, dateStr) => {
                    const strs = dateStr as unknown as string[];
                    if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                      setSyncStartDate(strs[0].replace(/-/g, ''));
                      setSyncEndDate(strs[1].replace(/-/g, ''));
                    } else {
                      setSyncStartDate('');
                      setSyncEndDate('');
                    }
                  }}
                  style={{ width: '100%' }}
                  size="small"
                />
                {syncStartDate && syncEndDate && (
                  <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                    <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                      共 {dayjs(syncEndDate, 'YYYYMMDD').diff(dayjs(syncStartDate, 'YYYYMMDD'), 'day') + 1} 天
                    </span>
                  </div>
                )}
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
            {(() => {
              const fullIds = selectedTaskIds.filter(id => syncTasks.find(t => t.task_id === id)?.sync_type === 'full');
              const incIds = selectedTaskIds.filter(id => !fullIds.includes(id));
              return (<>
                {incIds.length > 0 && (
                  <div style={{ marginBottom: fullIds.length > 0 ? 8 : 0 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>增量任务（按日期范围同步）：</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {incIds.map(id => <Tag key={id} color="blue" style={{ fontSize: '12px' }}>{id}</Tag>)}
                    </div>
                  </div>
                )}
                {fullIds.length > 0 && (
                  <div>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>全量任务（仅执行一次，忽略日期）：</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {fullIds.map(id => <Tag key={id} color="green" style={{ fontSize: '12px' }}>{id}</Tag>)}
                    </div>
                  </div>
                )}
              </>);
            })()}
          </div>

          {selectedTaskIds.some(id => syncTasks.find(t => t.task_id === id)?.sync_type !== 'full') && (
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>日期范围</div>
            <DatePicker
              type="dateRange"
              placeholder={['开始日期', '结束日期']}
              value={(batchSyncStartDate && batchSyncEndDate) ? [dayjs(batchSyncStartDate, 'YYYYMMDD').toDate(), dayjs(batchSyncEndDate, 'YYYYMMDD').toDate()] : undefined}
              onChange={(date, dateStr) => {
                const strs = dateStr as unknown as string[];
                if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                  setBatchSyncStartDate(strs[0].replace(/-/g, ''));
                  setBatchSyncEndDate(strs[1].replace(/-/g, ''));
                } else {
                  setBatchSyncStartDate('');
                  setBatchSyncEndDate('');
                }
              }}
              style={{ width: '100%' }}
              size="small"
            />
            {batchSyncStartDate && batchSyncEndDate && (
              <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {dayjs(batchSyncEndDate, 'YYYYMMDD').diff(dayjs(batchSyncStartDate, 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
          )}
        </div>
      </Modal>

      {/* Task Detail SideSheet */}
      <SideSheet
        title={<span style={{ color: 'var(--color-primary)' }}>{isNewTask ? '新建任务' : taskDrawerTask?.task_id}</span>}
        visible={taskDrawerVisible}
        onCancel={() => { setTaskDrawerVisible(false); setIsNewTask(false); }}
        width={720}
      >
        {taskDrawerConfig && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* 状态信息栏 - 仅编辑模式显示 */}
            {!isNewTask && taskDrawerTask && (
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
            )}

            <Tabs activeKey={taskDrawerTab} onChange={setTaskDrawerTab} size="small">
              <TabPane tab="可视化编辑" itemKey="visual">
                <div style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingTop: 8 }}>
                  {/* 基本信息 */}
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>任务ID</div>
                      <Input size="small" prefix="sync_" value={(taskDrawerConfig.task_id || '').replace(/^sync_/, '')} onChange={(v) => updateTaskDrawerConfig('task_id', `sync_${v}`)} disabled={!isNewTask} style={!isNewTask ? { background: 'var(--bg-surface)' } : undefined} />
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
                      <Input size="small" prefix="sync_" value={(taskDrawerConfig.table_name || '').replace(/^sync_/, '')} onChange={(v) => updateTaskDrawerConfig('table_name', `sync_${v}`)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>同步类型</div>
                      <Select size="small" value={taskDrawerConfig.sync_type} onChange={(v) => updateTaskDrawerConfig('sync_type', v)} style={{ width: '100%' }}>
                        <Select.Option value="incremental">增量</Select.Option>
                        <Select.Option value="full">全量</Select.Option>
                      </Select>
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>来源</div>
                      <Input size="small" value={taskDrawerConfig.source || 'tushare'} onChange={(v) => updateTaskDrawerConfig('source', v)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>日期字段</div>
                      <Input size="small" value={taskDrawerConfig.date_field || ''} onChange={(v) => updateTaskDrawerConfig('date_field', v)} />
                    </div>
                    <div>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>API限制</div>
                      <Input size="small" type="number" value={taskDrawerConfig.api_limit || ''} onChange={(v) => updateTaskDrawerConfig('api_limit', parseInt(v) || 0)} />
                    </div>
                    <div style={{ gridColumn: 'span 2' }}>
                      <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>主键（逗号分隔）</div>
                      <Input size="small" value={taskDrawerConfig.primary_keys?.join(', ') || ''} onChange={(v) => updateTaskDrawerConfig('primary_keys', v.split(',').map((s: string) => s.trim()).filter(Boolean))} />
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
                            { title: '字段名', dataIndex: 'name', key: 'name', width: 140,
                              render: (v: string) => (
                                <Input size="small" value={v} onChange={(val) => {
                                  const newSchema = { ...taskDrawerConfig.schema };
                                  const entries = Object.entries(newSchema);
                                  const rebuilt: Record<string, any> = {};
                                  entries.forEach(([key, props]) => {
                                    rebuilt[key === v ? val : key] = props;
                                  });
                                  updateTaskDrawerConfig('schema', rebuilt);
                                }} style={{ fontSize: '12px' }} />
                              )
                            },
                            { title: '类型', dataIndex: 'type', key: 'type', width: 130,
                              render: (v: string, r: any) => (
                                <Select size="small" value={v} onChange={(val) => updateSchemaField(r.name, 'type', val)} style={{ width: 120 }}>
                                  <Select.Option value="BOOL">BOOL</Select.Option>
                                  <Select.Option value="CHAR">CHAR</Select.Option>
                                  <Select.Option value="SHORT">SHORT</Select.Option>
                                  <Select.Option value="INT">INT</Select.Option>
                                  <Select.Option value="LONG">LONG</Select.Option>
                                  <Select.Option value="FLOAT">FLOAT</Select.Option>
                                  <Select.Option value="DOUBLE">DOUBLE</Select.Option>
                                  <Select.Option value="DATE">DATE</Select.Option>
                                  <Select.Option value="MONTH">MONTH</Select.Option>
                                  <Select.Option value="TIME">TIME</Select.Option>
                                  <Select.Option value="MINUTE">MINUTE</Select.Option>
                                  <Select.Option value="SECOND">SECOND</Select.Option>
                                  <Select.Option value="DATETIME">DATETIME</Select.Option>
                                  <Select.Option value="TIMESTAMP">TIMESTAMP</Select.Option>
                                  <Select.Option value="NANOTIME">NANOTIME</Select.Option>
                                  <Select.Option value="NANOTIMESTAMP">NANOTIMESTAMP</Select.Option>
                                  <Select.Option value="SYMBOL">SYMBOL</Select.Option>
                                  <Select.Option value="STRING">STRING</Select.Option>
                                  <Select.Option value="UUID">UUID</Select.Option>
                                  <Select.Option value="BLOB">BLOB</Select.Option>
                                </Select>
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
                            { title: '', dataIndex: 'name', key: 'action', width: 40,
                              render: (_: string, r: any) => (
                                <Button size="small" type="danger" theme="borderless" icon={<IconDelete />} onClick={() => {
                                  const newSchema = { ...taskDrawerConfig.schema };
                                  delete newSchema[r.name];
                                  updateTaskDrawerConfig('schema', newSchema);
                                }} />
                              )
                            },
                          ]}
                        />
                        <Button size="small" theme="light" icon={<IconPlus />} style={{ marginTop: 8, width: '100%' }} onClick={() => {
                          const newSchema = { ...taskDrawerConfig.schema };
                          let newName = 'new_field';
                          let i = 1;
                          while (newSchema[newName]) { newName = `new_field_${i++}`; }
                          newSchema[newName] = { type: 'STRING', nullable: true, comment: '' };
                          updateTaskDrawerConfig('schema', newSchema);
                        }}>新增字段</Button>
                      </Collapse.Panel>
                    </Collapse>
                  )}
                </div>
              </TabPane>

              <TabPane tab="JSON 编辑" itemKey="json">
                <div style={{ paddingTop: 8 }}>
                  <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
                    <Editor
                      height="500px"
                      language="json"
                      theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                      value={taskDrawerJson}
                      onChange={(v) => {
                        setTaskDrawerJson(v || '');
                        parseTaskDrawerJson(v || '');
                      }}
                      options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 2, wordWrap: 'on' }}
                    />
                  </div>
                  <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
                    直接编辑 JSON 配置，修改会同步到可视化编辑界面
                  </div>
                </div>
              </TabPane>

              {!isNewTask && (
                <TabPane tab="历史调度" itemKey="history">
                  <div style={{ paddingTop: 8 }}>
                    <Table
                      dataSource={taskSyncHistory}
                      rowKey={(record: any) => `${record.sync_date}-${record.created_at}`}
                      size="small"
                      pagination={{ pageSize: 10 }}
                      empty={<div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无调度记录</div>}
                      columns={[
                        { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 100 },
                        { title: '同步行数', dataIndex: 'rows_synced', key: 'rows_synced', width: 100,
                          render: (v: number) => v?.toLocaleString() || '-'
                        },
                        { title: '状态', dataIndex: 'status', key: 'status', width: 80,
                          render: (v: string, record: any) => {
                            const tag = <Tag color={v === 'success' ? 'green' : 'red'}>{v}</Tag>;
                            return (v !== 'success' && record.error_message)
                              ? <Tooltip content={record.error_message}>{tag}</Tooltip>
                              : tag;
                          }
                        },
                        { title: '执行时间', dataIndex: 'created_at', key: 'created_at',
                          render: (v: string) => v ? new Date(v).toLocaleString() : '-'
                        },
                      ]}
                    />
                  </div>
                </TabPane>
              )}
            </Tabs>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, borderTop: '1px solid var(--border-color)', paddingTop: 12 }}>
              <Button onClick={() => { setTaskDrawerVisible(false); setIsNewTask(false); }}>取消</Button>
              <Button theme="solid" type="primary" onClick={handleSaveTaskDrawer}>{isNewTask ? '创建' : '保存'}</Button>
            </div>
          </div>
        )}
      </SideSheet>

      {/* Copy Task Modal */}
      <Modal
        title={copyTaskStep === 'input' ? '复制任务' : '选择建表方式'}
        visible={copyTaskModalVisible}
        onCancel={() => { if (copyTaskStep === 'input') setCopyTaskModalVisible(false); else setCopyTaskStep('input'); }}
        footer={
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => { if (copyTaskStep === 'input') setCopyTaskModalVisible(false); else setCopyTaskStep('input'); }}>
              {copyTaskStep === 'input' ? '取消' : '返回'}
            </Button>
            {copyTaskStep === 'input' && (
              <Button theme="solid" type="primary" onClick={executeCopyTask}>下一步</Button>
            )}
            {copyTaskStep === 'table_not_exists' && (
              <>
                <Button onClick={executeCopyTask}>首次同步时建表</Button>
                <Button theme="solid" type="primary" onClick={executeCopyTaskCreateTableNow}>立即建表并创建任务</Button>
              </>
            )}
          </div>
        }
      >
        {copyTaskStep === 'input' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 4 }}>源任务</div>
              <code style={{ color: 'var(--color-primary)', fontSize: '14px' }}>{copyTaskSource?.task_id}</code>
              <span style={{ marginLeft: 12, color: 'var(--text-muted)', fontSize: '12px' }}>表: {copyTaskSource?.table_name}</span>
            </div>
            <div>
              <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>新任务ID</div>
              <Input prefix="sync_" placeholder="输入新任务ID" value={(copyTaskNewId || '').replace(/^sync_/, '')} onChange={(v) => setCopyTaskNewId(`sync_${v}`)} size="small" />
            </div>
            <div>
              <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>数据表名称</div>
              <Input prefix="sync_" placeholder="输入数据表名称" value={(copyTaskNewTableName || '').replace(/^sync_/, '')} onChange={(v) => setCopyTaskNewTableName(`sync_${v}`)} size="small" />
              <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
                新任务将复制源任务的所有配置，使用新的任务ID和数据表名称。表名不允许与已有表重复。
              </div>
            </div>
          </div>
        )}
        {copyTaskStep === 'table_not_exists' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
              <div style={{ fontSize: '13px' }}>
                表 <code style={{ color: 'var(--color-primary)' }}>{copyTaskNewTableName}</code> 不存在，请选择建表时机：
              </div>
            </div>
            <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>
              <div style={{ marginBottom: 4 }}><strong>立即建表</strong>：现在创建空表，之后同步时直接写入数据</div>
              <div><strong>首次同步时建表</strong>：创建任务后，在第一次执行同步时自动建表</div>
            </div>
          </div>
        )}
      </Modal>

      {/* ETL Task SideSheet */}
      <SideSheet
        title={isNewEtlTask ? '新建 ETL 任务' : `编辑 ETL 任务: ${etlDrawerConfig.task_id}`}
        visible={etlDrawerVisible}
        onCancel={() => { setEtlDrawerVisible(false); setEtlTestResult(null); }}
        width={720}
        footer={
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => { setEtlDrawerVisible(false); setEtlTestResult(null); }}>取消</Button>
            <Button theme="solid" type="primary" onClick={handleSaveEtlTask}>保存</Button>
          </div>
        }
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: '8px 0' }}>
          {/* 状态信息栏 - 仅编辑模式显示 */}
          {!isNewEtlTask && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px' }}>
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>最新数据</div>
                <span style={{ color: 'var(--color-gain)', fontSize: '13px', fontWeight: 500 }}>
                  {etlTaskStatus?.last_date || '-'}
                </span>
              </div>
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>上次同步</div>
                <span style={{ color: 'var(--text-secondary)', fontSize: '13px' }}>
                  {etlTaskStatus?.last_sync_time ? new Date(etlTaskStatus.last_sync_time).toLocaleString() : '-'}
                </span>
              </div>
              {etlSelectedPrimaryKeys.length > 0 && (
                <div>
                  <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>主键</div>
                  <span style={{ fontSize: '13px' }}>
                    {etlSelectedPrimaryKeys.map((k) => <Tag key={k} size="small" style={{ marginRight: 4 }}>{k}</Tag>)}
                  </span>
                </div>
              )}
            </div>
          )}

          <Tabs activeKey={etlDrawerTab} onChange={setEtlDrawerTab} size="small">
            <TabPane tab="配置" itemKey="config">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12, paddingTop: 8 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            <div>
              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>任务 ID</div>
              <Input size="small" prefix="etl_" value={(etlDrawerConfig.task_id || '').replace(/^etl_/, '')} disabled={!isNewEtlTask}
                onChange={(v) => setEtlDrawerConfig({ ...etlDrawerConfig, task_id: `etl_${v}` })} />
            </div>
            <div>
              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>目标表名（= 任务ID）</div>
              <Input size="small" value={etlDrawerConfig.task_id} disabled />
            </div>
          </div>
          <div>
            <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>描述</div>
            <Input size="small" value={etlDrawerConfig.description}
              onChange={(v) => setEtlDrawerConfig({ ...etlDrawerConfig, description: v })} />
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            <div>
              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>同步类型</div>
              <RadioGroup value={etlDrawerConfig.sync_type || 'incremental'} onChange={(e: any) => setEtlDrawerConfig({ ...etlDrawerConfig, sync_type: e.target.value })}>
                <Radio value="incremental">增量</Radio>
                <Radio value="full">全量</Radio>
              </RadioGroup>
            </div>
            {etlDrawerConfig.sync_type === 'incremental' && (
              <div>
                <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>日期字段</div>
                <Input size="small" value={etlDrawerConfig.date_field} placeholder="如 trade_date"
                  onChange={(v) => setEtlDrawerConfig({ ...etlDrawerConfig, date_field: v })} />
              </div>
            )}
          </div>
          <div>
            <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
              DolphinDB ETL 脚本
              <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontSize: '11px' }}>
                使用 {'{date}'} 变量表示执行日期，格式为 YYYY.MM.DD
              </span>
            </div>
            <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
              <Editor
                height="300px"
                language="sql"
                theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                value={etlDrawerConfig.script}
                onChange={(v) => { setEtlDrawerConfig({ ...etlDrawerConfig, script: v || '' }); setEtlTestResult(null); }}
                options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4, wordWrap: 'on' }}
              />
            </div>
            <div style={{ marginTop: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
              {etlDrawerConfig.sync_type === 'incremental' && (
                <DatePicker size="small" placeholder="测试日期（可选）" style={{ width: 160 }}
                  onChange={(date: any) => setEtlTestDate(date ? dayjs(date).format('YYYYMMDD') : '')} />
              )}
              <Button size="small" theme="light" onClick={handleTestEtlScript} loading={etlTestLoading}>测试脚本</Button>
              {etlTestResult && etlTestResult.status === 'success' && (
                <span style={{ fontSize: '12px', color: 'var(--color-gain)' }}>测试通过: {etlTestResult.rows} 行</span>
              )}
              {etlTestResult && etlTestResult.status === 'error' && (
                <span style={{ fontSize: '12px', color: 'var(--color-loss)' }}>{etlTestResult.error}</span>
              )}
            </div>
          </div>
          {etlTestResult?.field_types?.length > 0 && (
            <div>
              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>字段定义（勾选主键）</div>
              <Table
                dataSource={etlTestResult.field_types}
                rowKey="name"
                size="small"
                pagination={false}
                rowSelection={{
                  selectedRowKeys: etlSelectedPrimaryKeys,
                  onChange: (keys) => setEtlSelectedPrimaryKeys(keys as string[]),
                }}
                columns={[
                  { title: '字段名', dataIndex: 'name', key: 'name', width: 200,
                    render: (v: string) => <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
                  },
                  { title: '类型', dataIndex: 'type', key: 'type', width: 120 },
                ]}
              />
              {etlSelectedPrimaryKeys.length > 0 && (
                <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>
                  主键: {etlSelectedPrimaryKeys.join(', ')}
                </div>
              )}
            </div>
          )}
          {!etlTestResult?.field_types?.length && etlSavedSchema.length > 0 && (
            <div>
              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>表字段（勾选主键）</div>
              <Table
                dataSource={etlSavedSchema}
                rowKey="name"
                size="small"
                pagination={false}
                rowSelection={{
                  selectedRowKeys: etlSelectedPrimaryKeys,
                  onChange: (keys) => setEtlSelectedPrimaryKeys(keys as string[]),
                }}
                columns={[
                  { title: '字段名', dataIndex: 'name', key: 'name', width: 200,
                    render: (v: string) => <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
                  },
                  { title: '类型', dataIndex: 'type', key: 'type', width: 120 },
                ]}
              />
              {etlSelectedPrimaryKeys.length > 0 && (
                <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>
                  主键: {etlSelectedPrimaryKeys.join(', ')}
                </div>
              )}
            </div>
          )}
          {etlTestResult?.preview?.length > 0 && (
            <div>
              <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>数据预览（前 5 行）</div>
              <Table
                dataSource={etlTestResult.preview}
                rowKey={(r: any) => JSON.stringify(r).slice(0, 80)}
                size="small"
                pagination={false}
                columns={etlTestResult.columns.map((col: string) => ({
                  title: col, dataIndex: col, key: col, ellipsis: true,
                  render: (v: any) => v === null || v === undefined ? <span style={{ color: '#999' }}>NULL</span> : String(v),
                }))}
              />
            </div>
          )}
              </div>
            </TabPane>

            {!isNewEtlTask && (
              <TabPane tab="历史日志" itemKey="history">
                <div style={{ paddingTop: 8 }}>
                  <Table
                    dataSource={etlTaskHistory}
                    rowKey={(record: any) => `${record.sync_date}-${record.created_at}`}
                    size="small"
                    pagination={{ pageSize: 10 }}
                    empty={<div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>暂无执行记录</div>}
                    columns={[
                      { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 100 },
                      { title: '参数', dataIndex: 'params', key: 'params', width: 180,
                        render: (v: string) => v ? <code style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>{v}</code> : '-'
                      },
                      { title: '同步行数', dataIndex: 'rows_synced', key: 'rows_synced', width: 100,
                        render: (v: number) => v?.toLocaleString() || '-'
                      },
                      { title: '状态', dataIndex: 'status', key: 'status', width: 80,
                        render: (v: string, record: any) => {
                          const tag = <Tag color={v === 'success' ? 'green' : 'red'}>{v}</Tag>;
                          return (v !== 'success' && record.error_message)
                            ? <Tooltip content={record.error_message}>{tag}</Tooltip>
                            : tag;
                        }
                      },
                      { title: '执行时间', dataIndex: 'created_at', key: 'created_at',
                        render: (v: string) => v ? new Date(v).toLocaleString() : '-'
                      },
                    ]}
                  />
                </div>
              </TabPane>
            )}
          </Tabs>
        </div>
      </SideSheet>

      {/* ETL Backfill Modal */}
      <Modal
        title={`回溯 ETL 任务: ${etlBackfillTaskId}`}
        visible={etlBackfillModalVisible}
        onOk={handleBackfillEtlTask}
        onCancel={() => setEtlBackfillModalVisible(false)}
        okText="开始回溯"
        cancelText="跳过"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ color: 'var(--text-secondary)', fontSize: 13 }}>
            任务已保存成功。是否需要回溯历史数据？系统将逐天替换脚本中的 {'{date}'} 变量执行。
          </div>
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>回溯日期范围</div>
            <DatePicker
              type="dateRange"
              placeholder={['开始日期', '结束日期']}
              value={(etlBackfillStartDate && etlBackfillEndDate) ? [dayjs(etlBackfillStartDate, 'YYYYMMDD').toDate(), dayjs(etlBackfillEndDate, 'YYYYMMDD').toDate()] : undefined}
              onChange={(date: any, dateStr: any) => {
                const strs = dateStr as unknown as string[];
                if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                  setEtlBackfillStartDate(strs[0].replace(/-/g, ''));
                  setEtlBackfillEndDate(strs[1].replace(/-/g, ''));
                } else {
                  setEtlBackfillStartDate('');
                  setEtlBackfillEndDate('');
                }
              }}
              style={{ width: '100%' }}
              size="small"
            />
            {etlBackfillStartDate && etlBackfillEndDate && (
              <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {dayjs(etlBackfillEndDate, 'YYYYMMDD').diff(dayjs(etlBackfillStartDate, 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
        </div>
      </Modal>

      {/* 批量回溯 Modal */}
      <Modal
        title={`批量回溯 (已选 ${selectedEtlTaskIds.length} 个任务)`}
        visible={batchBackfillModalVisible}
        onOk={executeBatchBackfill}
        onCancel={() => setBatchBackfillModalVisible(false)}
        okText="开始回溯"
        cancelText="取消"
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div style={{ padding: '12px', background: 'var(--bg-surface)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
            {(() => {
              const fullIds = selectedEtlTaskIds.filter(id => etlTasks.find(t => t.task_id === id)?.sync_type === 'full');
              const incIds = selectedEtlTaskIds.filter(id => !fullIds.includes(id));
              return (<>
                {incIds.length > 0 && (
                  <div style={{ marginBottom: fullIds.length > 0 ? 8 : 0 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>增量任务（按日期范围回溯）：</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {incIds.map(id => <Tag key={id} color="blue" style={{ fontSize: '12px' }}>{id}</Tag>)}
                    </div>
                  </div>
                )}
                {fullIds.length > 0 && (
                  <div>
                    <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>全量任务（仅执行一次，忽略日期）：</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {fullIds.map(id => <Tag key={id} color="green" style={{ fontSize: '12px' }}>{id}</Tag>)}
                    </div>
                  </div>
                )}
              </>);
            })()}
          </div>
          {selectedEtlTaskIds.some(id => etlTasks.find(t => t.task_id === id)?.sync_type !== 'full') && (
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>回溯日期范围</div>
            <DatePicker
              type="dateRange"
              placeholder={['开始日期', '结束日期']}
              value={(batchBackfillStartDate && batchBackfillEndDate) ? [dayjs(batchBackfillStartDate, 'YYYYMMDD').toDate(), dayjs(batchBackfillEndDate, 'YYYYMMDD').toDate()] : undefined}
              onChange={(date: any, dateStr: any) => {
                const strs = dateStr as unknown as string[];
                if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                  setBatchBackfillStartDate(strs[0].replace(/-/g, ''));
                  setBatchBackfillEndDate(strs[1].replace(/-/g, ''));
                } else {
                  setBatchBackfillStartDate('');
                  setBatchBackfillEndDate('');
                }
              }}
              style={{ width: '100%' }}
              size="small"
            />
            {batchBackfillStartDate && batchBackfillEndDate && (
              <div style={{ marginTop: 8, padding: '8px 12px', background: 'var(--color-primary-light-default)', borderRadius: '6px' }}>
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {dayjs(batchBackfillEndDate, 'YYYYMMDD').diff(dayjs(batchBackfillStartDate, 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
          )}
        </div>
      </Modal>

      {/* 删除确认 Modal */}
      <Modal
        title={`删除${deleteConfirmType === 'sync' ? '同步' : 'ETL'}任务`}
        visible={deleteConfirmVisible}
        onCancel={() => setDeleteConfirmVisible(false)}
        onOk={executeDeleteTask}
        okText="确认删除"
        okType="danger"
        cancelText="取消"
      >
        <div style={{ color: 'var(--text-secondary)', fontSize: 13 }}>
          确定要删除任务 <code style={{ color: 'var(--color-primary)' }}>{deleteConfirmTaskId}</code> 及其数据表吗？此操作不可恢复。
        </div>
      </Modal>
    </div>
  );
};

export default DataCenter;
