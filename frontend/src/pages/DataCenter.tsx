import React, { useEffect, useState } from 'react';
import {
  Table,
  Button,
  Input,
  Select,
  Space,
  Tag,
  message,
  Card,
  Tabs,
  Modal,
  Descriptions,
  Badge,
  Tooltip,
  Row,
  Col,
} from 'antd';
import {
  SyncOutlined,
  DatabaseOutlined,
  CodeOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { dataApi } from '../api';
import KLineChart from '../components/Charts/KLineChart';

const { Search, TextArea } = Input;
const { TabPane } = Tabs;

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
  table_name: string;
}

interface TableInfo {
  table_name: string;
  row_count: number;
  column_count: number;
  columns: string[];
}

const DataCenter: React.FC = () => {
  // 原有状态
  const [stocks, setStocks] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>('');
  const [dailyData, setDailyData] = useState<any[]>([]);
  const [syncLogs, setSyncLogs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  // 新增状态
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [taskStatuses, setTaskStatuses] = useState<Record<string, TaskStatus>>({});
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM daily_basic LIMIT 10');
  const [queryResult, setQueryResult] = useState<any[]>([]);
  const [queryColumns, setQueryColumns] = useState<string[]>([]);
  const [queryLoading, setQueryLoading] = useState(false);
  const [syncingTasks, setSyncingTasks] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      const [stocksRes, logsRes, tasksRes, tablesRes] = await Promise.all([
        dataApi.listStocks(),
        dataApi.getSyncStatus(),
        dataApi.listSyncTasks(),
        dataApi.listTables(),
      ]);

      setStocks(stocksRes.data.stocks || []);
      setSyncLogs(logsRes.data.logs || []);
      setSyncTasks(tasksRes.data.tasks || []);
      setTables(tablesRes.data.tables || []);

      // 加载每个任务的状态
      const tasks = tasksRes.data.tasks || [];
      for (const task of tasks) {
        loadTaskStatus(task.task_id);
      }
    } catch (error) {
      message.error('Failed to load data');
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
    setSyncingTasks((prev) => new Set(prev).add(taskId));
    try {
      await dataApi.syncTask(taskId);
      message.success(`Task ${taskId} sync started`);
      setTimeout(() => loadTaskStatus(taskId), 2000);
    } catch (error) {
      message.error(`Failed to sync task ${taskId}`);
    } finally {
      setSyncingTasks((prev) => {
        const newSet = new Set(prev);
        newSet.delete(taskId);
        return newSet;
      });
    }
  };

  const handleSyncAll = async () => {
    try {
      await dataApi.syncAllTasks();
      message.success('All tasks sync started in background');
      setTimeout(() => {
        syncTasks.forEach((task) => loadTaskStatus(task.task_id));
      }, 3000);
    } catch (error) {
      message.error('Failed to start sync');
    }
  };

  const handleExecuteQuery = async () => {
    if (!sqlQuery.trim()) {
      message.warning('Please enter a SQL query');
      return;
    }

    setQueryLoading(true);
    try {
      const res = await dataApi.executeQuery(sqlQuery);
      setQueryResult(res.data.data || []);
      setQueryColumns(res.data.columns || []);
      message.success(`Query returned ${res.data.count} rows`);
    } catch (error: any) {
      message.error(error.response?.data?.detail || 'Query failed');
    } finally {
      setQueryLoading(false);
    }
  };

  const dailyColumns = [
    { title: 'Date', dataIndex: 'trade_date', key: 'trade_date', width: 100 },
    {
      title: 'Open',
      dataIndex: 'open',
      key: 'open',
      render: (v: number) => <span style={{ color: '#94a3b8' }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: 'High',
      dataIndex: 'high',
      key: 'high',
      render: (v: number) => <span style={{ color: '#10b981' }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: 'Low',
      dataIndex: 'low',
      key: 'low',
      render: (v: number) => <span style={{ color: '#ef4444' }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: 'Close',
      dataIndex: 'close',
      key: 'close',
      render: (v: number) => <span style={{ color: '#00d4ff', fontWeight: 600 }}>{v?.toFixed(2)}</span>,
      width: 80
    },
    {
      title: 'Volume',
      dataIndex: 'vol',
      key: 'vol',
      render: (v: number) => <span style={{ color: '#94a3b8' }}>{v?.toLocaleString()}</span>,
      width: 100
    },
    {
      title: 'Chg%',
      dataIndex: 'pct_chg',
      key: 'pct_chg',
      render: (v: number) => (
        <Tag
          color={v >= 0 ? 'red' : 'green'}
          style={{
            background: v >= 0 ? 'rgba(239, 68, 68, 0.2)' : 'rgba(16, 185, 129, 0.2)',
            border: `1px solid ${v >= 0 ? '#ef4444' : '#10b981'}`,
            color: v >= 0 ? '#ef4444' : '#10b981',
            fontWeight: 600
          }}
        >
          {v >= 0 ? '+' : ''}{v?.toFixed(2)}%
        </Tag>
      ),
      width: 80,
    },
  ];

  const taskColumns = [
    {
      title: 'Task ID',
      dataIndex: 'task_id',
      key: 'task_id',
      width: 150,
      render: (text: string) => (
        <code style={{
          color: '#00d4ff',
          background: 'rgba(0, 212, 255, 0.1)',
          padding: '4px 8px',
          borderRadius: '4px',
          fontWeight: 600
        }}>
          {text}
        </code>
      ),
    },
    { title: 'Description', dataIndex: 'description', key: 'description', width: 200 },
    {
      title: 'Type',
      dataIndex: 'sync_type',
      key: 'sync_type',
      width: 100,
      render: (type: string) => (
        <Tag
          color={type === 'incremental' ? 'blue' : 'green'}
          style={{
            background: type === 'incremental' ? 'rgba(0, 212, 255, 0.2)' : 'rgba(16, 185, 129, 0.2)',
            border: `1px solid ${type === 'incremental' ? '#00d4ff' : '#10b981'}`,
            color: type === 'incremental' ? '#00d4ff' : '#10b981',
            fontWeight: 500
          }}
        >
          {type}
        </Tag>
      ),
    },
    {
      title: 'Schedule',
      dataIndex: 'schedule',
      key: 'schedule',
      width: 100,
      render: (schedule: string) => (
        <Tag style={{
          background: 'rgba(124, 58, 237, 0.2)',
          border: '1px solid #7c3aed',
          color: '#7c3aed',
          fontWeight: 500
        }}>
          {schedule}
        </Tag>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'enabled',
      key: 'enabled',
      width: 80,
      render: (enabled: boolean) => (
        <Badge status={enabled ? 'success' : 'default'} text={enabled ? 'Enabled' : 'Disabled'} />
      ),
    },
    {
      title: 'Last Sync',
      key: 'last_sync',
      width: 120,
      render: (_: any, record: SyncTask) => {
        const status = taskStatuses[record.task_id];
        return status?.last_sync_date || <span style={{ color: '#999' }}>Never</span>;
      },
    },
    { title: 'Table', dataIndex: 'table_name', key: 'table_name', width: 120 },
    {
      title: 'Action',
      key: 'action',
      width: 100,
      render: (_: any, record: SyncTask) => (
        <Button
          type="link"
          size="small"
          icon={<SyncOutlined spin={syncingTasks.has(record.task_id)} />}
          onClick={() => handleSyncTask(record.task_id)}
          disabled={!record.enabled || syncingTasks.has(record.task_id)}
        >
          Sync
        </Button>
      ),
    },
  ];

  const tableColumns = [
    {
      title: 'Table Name',
      dataIndex: 'table_name',
      key: 'table_name',
      render: (text: string) => <code style={{ color: '#1890ff' }}>{text}</code>,
    },
    {
      title: 'Rows',
      dataIndex: 'row_count',
      key: 'row_count',
      render: (count: number) => count.toLocaleString(),
    },
    { title: 'Columns', dataIndex: 'column_count', key: 'column_count' },
    {
      title: 'Column Names',
      dataIndex: 'columns',
      key: 'columns',
      render: (columns: string[]) => (
        <Tooltip title={columns.join(', ')}>
          <span>{columns.slice(0, 3).join(', ')}{columns.length > 3 ? '...' : ''}</span>
        </Tooltip>
      ),
    },
    {
      title: 'Action',
      key: 'action',
      render: (_: any, record: TableInfo) => (
        <Button
          type="link"
          size="small"
          onClick={() => setSqlQuery(`SELECT * FROM ${record.table_name} LIMIT 100`)}
        >
          Query
        </Button>
      ),
    },
  ];

  const queryResultColumns = queryColumns.map((col) => ({
    title: col,
    dataIndex: col,
    key: col,
    ellipsis: true,
    render: (value: any) => {
      if (value === null || value === undefined) return <span style={{ color: '#999' }}>NULL</span>;
      if (typeof value === 'number') return value.toLocaleString();
      return String(value);
    },
  }));

  return (
    <div style={{ padding: '32px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{
        marginBottom: '32px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <div>
          <h1 style={{
            color: '#00d4ff',
            fontSize: '32px',
            fontWeight: 700,
            margin: 0,
            textShadow: '0 0 20px rgba(0, 212, 255, 0.5)',
            letterSpacing: '1px'
          }}>
            <DatabaseOutlined style={{ marginRight: '12px' }} />
            Data Center
          </h1>
          <p style={{
            color: '#94a3b8',
            margin: '8px 0 0 0',
            fontSize: '14px'
          }}>
            Real-time market data management and analysis
          </p>
        </div>
        <div style={{
          background: 'linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(124, 58, 237, 0.1))',
          padding: '12px 24px',
          borderRadius: '8px',
          border: '1px solid rgba(0, 212, 255, 0.3)'
        }}>
          <div style={{ color: '#94a3b8', fontSize: '12px' }}>Total Tables</div>
          <div style={{
            color: '#00d4ff',
            fontSize: '24px',
            fontWeight: 700,
            textShadow: '0 0 10px rgba(0, 212, 255, 0.5)'
          }}>
            {tables.length}
          </div>
        </div>
      </div>

      <Tabs defaultActiveKey="1" className="tech-tabs">
        {/* 原有的数据查看功能 */}
        <TabPane tab="📊 Stock Data" key="1">
          <div className="tech-card" style={{ padding: '24px', marginBottom: '24px' }}>
            <Space style={{ marginBottom: 16 }} wrap>
              <Select
                showSearch
                style={{ width: 250 }}
                placeholder="🔍 Select stock code"
                className="tech-input"
                options={stocks.map((s) => ({ label: s, value: s }))}
                onChange={(v) => {
                  setSelected(v);
                  loadDaily(v);
                }}
              />
            </Space>

            {dailyData.length > 0 && (
              <div style={{
                marginBottom: 24,
                padding: '16px',
                background: 'rgba(0, 0, 0, 0.3)',
                borderRadius: '12px',
                border: '1px solid rgba(0, 212, 255, 0.2)'
              }}>
                <KLineChart data={dailyData.slice().reverse()} />
              </div>
            )}

            <Table
              dataSource={dailyData}
              columns={dailyColumns}
              rowKey="trade_date"
              loading={loading}
              size="small"
              pagination={{ pageSize: 20 }}
              scroll={{ x: 800 }}
              className="tech-table"
            />
          </div>
        </TabPane>

        {/* 新增：同步任务管理 */}
        <TabPane tab={<span><SyncOutlined /> Sync Tasks</span>} key="2">
          <Card
            className="tech-card"
            style={{ marginBottom: '24px', border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '18px', fontWeight: 600 }}>⚡ Sync Task Management</span>}
            extra={
              <Button
                type="primary"
                icon={<SyncOutlined />}
                onClick={handleSyncAll}
                className="glow-button"
                style={{ height: '40px', padding: '0 24px' }}
              >
                Sync All Tasks
              </Button>
            }
          >
            <Table
              dataSource={syncTasks}
              columns={taskColumns}
              rowKey="task_id"
              size="small"
              pagination={false}
              scroll={{ x: 1200 }}
              className="tech-table"
            />
          </Card>

          <Card
            className="tech-card"
            style={{ marginTop: 16, border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>📋 Sync Logs</span>}
          >
            <Table
              dataSource={syncLogs}
              columns={[
                { title: 'Source', dataIndex: 'source', key: 'source' },
                { title: 'Type', dataIndex: 'data_type', key: 'data_type' },
                { title: 'Last Date', dataIndex: 'last_date', key: 'last_date' },
                { title: 'Updated', dataIndex: 'updated_at', key: 'updated_at' },
              ]}
              rowKey={(record, index) => `${record.data_type}-${index ?? 0}`}
              size="small"
              pagination={{ pageSize: 10 }}
              className="tech-table"
            />
          </Card>
        </TabPane>

        {/* 新增：数据库表管理 */}
        <TabPane tab={<span><DatabaseOutlined /> Database Tables</span>} key="3">
          <Card
            className="tech-card"
            style={{ border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '18px', fontWeight: 600 }}>🗄️ Database Tables</span>}
            extra={
              <Button
                icon={<ReloadOutlined />}
                onClick={loadInitialData}
                className="glow-button"
                type="primary"
              >
                Refresh
              </Button>
            }
          >
            <Table
              dataSource={tables}
              columns={tableColumns}
              rowKey="table_name"
              size="small"
              pagination={{ pageSize: 20 }}
              className="tech-table"
            />
          </Card>
        </TabPane>

        {/* 新增：SQL 查询 */}
        <TabPane tab={<span><CodeOutlined /> SQL Query</span>} key="4">
          <Card
            className="tech-card"
            style={{ border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '18px', fontWeight: 600 }}>💻 SQL Query Interface</span>}
          >
            <Space direction="vertical" style={{ width: '100%' }} size="large">
              <div>
                <div style={{
                  marginBottom: 12,
                  color: '#94a3b8',
                  fontSize: '14px',
                  fontWeight: 500
                }}>
                  SQL Query Editor (SELECT only)
                </div>
                <TextArea
                  value={sqlQuery}
                  onChange={(e) => setSqlQuery(e.target.value)}
                  placeholder="Enter your SQL query here..."
                  rows={8}
                  style={{
                    fontFamily: 'Fira Code, Courier New, monospace',
                    fontSize: '14px',
                    background: 'rgba(0, 0, 0, 0.4)',
                    border: '1px solid rgba(0, 212, 255, 0.3)',
                    color: '#10b981',
                    borderRadius: '8px'
                  }}
                />
                <div style={{
                  marginTop: 12,
                  padding: '12px',
                  background: 'rgba(255, 193, 7, 0.1)',
                  border: '1px solid rgba(255, 193, 7, 0.3)',
                  borderRadius: '8px',
                  color: '#ffc107',
                  fontSize: 13
                }}>
                  ⚠️ Security: Only SELECT queries are allowed. Maximum 10,000 rows per query.
                </div>
              </div>

              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleExecuteQuery}
                loading={queryLoading}
                size="large"
                className="glow-button"
                style={{ height: '48px', fontSize: '16px' }}
              >
                Execute Query
              </Button>

              {queryResult.length > 0 && (
                <div>
                  <div style={{
                    marginBottom: 12,
                    color: '#00d4ff',
                    fontSize: '16px',
                    fontWeight: 600
                  }}>
                    📊 Query Results ({queryResult.length} rows)
                  </div>
                  <Table
                    dataSource={queryResult}
                    columns={queryResultColumns}
                    rowKey={(_, index) => (index ?? 0).toString()}
                    size="small"
                    pagination={{ pageSize: 50 }}
                    scroll={{ x: 'max-content', y: 500 }}
                    className="tech-table"
                  />
                </div>
              )}
            </Space>
          </Card>
        </TabPane>
      </Tabs>
    </div>
  );
};

export default DataCenter;
