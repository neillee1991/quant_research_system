import React, { useEffect, useCallback, useState } from 'react';
import {
  Badge,
  Button,
  List,
  Spin,
  Tag,
  Empty,
  Typography,
  Space,
  Collapse,
} from 'antd';
import { notify } from '../../utils/notify';
import {
  LoadingOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  SyncOutlined,
  HistoryOutlined,
  ClearOutlined,
} from '@ant-design/icons';
import { taskMonitorApi, type RunningTask, type TaskRun } from '../../api';
import { useTaskMonitorStore } from '../../store';

const { Text } = Typography;

const POLL_INTERVAL = 5000;

const getStatusColor = (status: string): string => {
  const s = status.toLowerCase();
  if (s.includes('pending') || s.includes('waiting')) return 'orange';
  if (s.includes('running') || s.includes('processing')) return 'blue';
  if (s.includes('success') || s.includes('completed')) return 'green';
  if (s.includes('failed') || s.includes('error')) return 'red';
  return 'default';
};

const getStatusIcon = (status: string) => {
  const s = status.toLowerCase();
  if (s.includes('pending') || s.includes('waiting')) return <LoadingOutlined spin />;
  if (s.includes('running') || s.includes('processing')) return <SyncOutlined spin />;
  if (s.includes('success') || s.includes('completed')) return <CheckCircleOutlined />;
  if (s.includes('failed') || s.includes('error')) return <CloseCircleOutlined />;
  return null;
};

const getTaskTypeColor = (type: string): string => {
  switch (type) {
    case 'factor': return 'blue';
    case 'sync': return 'green';
    case 'etl': return 'orange';
    case 'analysis': return 'purple';
    case 'backtest': return 'cyan';
    default: return 'default';
  }
};

const getTaskTypeLabel = (type: string): string => {
  switch (type) {
    case 'factor': return '因子计算';
    case 'sync': return '数据同步';
    case 'etl': return 'ETL任务';
    case 'analysis': return '因子分析';
    case 'backtest': return '回测';
    default: return type;
  }
};

const formatParams = (params: string | object | undefined): string => {
  if (!params) return '';
  try {
    let obj;
    if (typeof params === 'string') {
      // 如果是字符串，尝试解析为 JSON 对象
      obj = JSON.parse(params.replace(/'/g, '"'));
    } else if (typeof params === 'object') {
      // 如果是对象，直接使用
      obj = params;
    } else {
      // 如果是其他类型，直接返回字符串
      return String(params);
    }

    return Object.entries(obj)
      .filter(([, v]) => v != null && v !== '')
      .map(([k, v]) => {
        // 对于对象或数组，先转为字符串，避免 React 渲染错误
        const strVal = typeof v === 'object' ? JSON.stringify(v) : String(v);
        return `${k}: ${strVal}`;
      })
      .join('  ');
  } catch {
    return String(params);
  }
};

const useElapsed = (startedAt: string | undefined | null, status: string): string => {
  const [elapsed, setElapsed] = useState('');
  useEffect(() => {
    if (status !== 'running' || !startedAt) { setElapsed(''); return; }
    const update = () => {
      const sec = Math.floor((Date.now() - new Date(startedAt).getTime()) / 1000);
      setElapsed(sec >= 60 ? `${Math.floor(sec / 60)}m${sec % 60}s` : `${sec}s`);
    };
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [startedAt, status]);
  return elapsed;
};

const TaskItem: React.FC<{ task: RunningTask | TaskRun }> = ({ task }) => {
  const liveElapsed = useElapsed(
    'started_at' in task ? task.started_at : undefined,
    task.status
  );
  const elapsedText = task.status === 'running'
    ? liveElapsed
    : ('elapsed_sec' in task && task.elapsed_sec != null ? `${task.elapsed_sec.toFixed(1)}s` : '');

  const paramsText = formatParams(task.params);
  const rowsText = task.rows != null && task.rows > 0 ? `${task.rows.toLocaleString()} 行` : '';

  return (
    <List.Item style={{ padding: '8px 16px' }}>
      <List.Item.Meta
        avatar={getStatusIcon(task.status)}
        title={
          <Space size="small">
            <Text strong style={{ fontSize: '13px' }}>{task.task_id || task.task_name}</Text>
            <Tag color={getStatusColor(task.status)} style={{ fontSize: '11px' }}>
              {task.status}{elapsedText ? ` · ${elapsedText}` : ''}
            </Tag>
            <Tag color={getTaskTypeColor(task.task_type)} style={{ fontSize: '11px' }}>
              {getTaskTypeLabel(task.task_type)}
            </Tag>
          </Space>
        }
        description={
          <Space direction="vertical" size={2} style={{ width: '100%' }}>
            {(paramsText || rowsText) && (
              <Text type="secondary" style={{ fontSize: '11px' }}>
                {[paramsText, rowsText].filter(Boolean).join('  ·  ')}
              </Text>
            )}
            {'error' in task && task.error && (
              <Text type="danger" style={{ fontSize: '11px' }} ellipsis={{ tooltip: task.error }}>
                {task.error}
              </Text>
            )}
          </Space>
        }
      />
    </List.Item>
  );
};

export const TaskMonitor: React.FC = () => {
  const {
    runningTasks,
    isLoading,
    setRunningTasks,
    setLoading,
    setLastFetched,
  } = useTaskMonitorStore();

  const [expanded, setExpanded] = useState(false);
  const [historyTasks, setHistoryTasks] = useState<TaskRun[]>([]);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [cleaning, setCleaning] = useState(false);
  const prevRunningIds = React.useRef<Set<string>>(new Set());
  const runningTasksRef = React.useRef<RunningTask[]>(runningTasks);
  runningTasksRef.current = runningTasks;

  const fetchRunningTasks = useCallback(async () => {
    try {
      setLoading(true);
      const response = await taskMonitorApi.getRunningTasks();
      const newTasks = response.data.tasks;

      // 检查任务列表是否真的发生了变化
      const hasTasksChanged = JSON.stringify(newTasks) !== JSON.stringify(runningTasksRef.current);

      if (hasTasksChanged) {
        // 检测新开始的任务
        newTasks.forEach((t) => {
          if (!prevRunningIds.current.has(t.run_id)) {
            notify.info(`任务 ${t.task_id} 开始执行`);
          }
        });

        // 检测刚完成的任务
        prevRunningIds.current.forEach((id) => {
          const stillRunning = newTasks.find((t) => t.run_id === id);
          if (!stillRunning) {
            const old = runningTasksRef.current.find((t) => t.run_id === id);
            if (old) {
              setHistoryLoaded(false);
            }
          }
        });
        prevRunningIds.current = new Set(newTasks.map((t) => t.run_id));

        // 更新 ref 和 state
        runningTasksRef.current = newTasks;
        setRunningTasks(newTasks);
        setLastFetched(Date.now());
      }
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  }, [setRunningTasks, setLoading, setLastFetched]);

  const fetchHistory = useCallback(async () => {
    if (historyLoaded) return;
    try {
      setHistoryLoading(true);
      const response = await taskMonitorApi.getTaskHistory(50);
      setHistoryTasks(response.data.tasks);
      setHistoryLoaded(true);
    } catch {
      // silent
    } finally {
      setHistoryLoading(false);
    }
  }, [historyLoaded]);

  const handleCleanup = useCallback(async () => {
    try {
      setCleaning(true);
      const res = await taskMonitorApi.cleanupStale(0);
      const cleaned = (res.data as any)?.data?.cleaned ?? 0;
      if (cleaned > 0) {
        notify.success(`已清理 ${cleaned} 个僵尸任务`);
        setHistoryLoaded(false);
        fetchRunningTasks();
      } else {
        notify.info('没有需要清理的任务');
      }
    } catch {
      notify.error('清理失败');
    } finally {
      setCleaning(false);
    }
  }, [fetchRunningTasks]);

  useEffect(() => {
    fetchRunningTasks();
    const interval = setInterval(fetchRunningTasks, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchRunningTasks]);

  useEffect(() => {
    if (expanded) {
      fetchRunningTasks();
      fetchHistory();
    }
  }, [expanded, fetchRunningTasks, fetchHistory]);

  const hasRunning = runningTasks.length > 0;

  // 收起态：圆形徽章
  if (!expanded) {
    return (
      <div
        onClick={() => setExpanded(true)}
        style={{
          position: 'fixed',
          bottom: 24,
          right: 24,
          width: 44,
          height: 44,
          borderRadius: '50%',
          background: hasRunning ? 'var(--color-primary)' : 'var(--bg-elevated)',
          border: '1px solid var(--border-default)',
          boxShadow: 'var(--shadow-md)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer',
          zIndex: 1000,
          animation: hasRunning ? 'pulse-glow 2s infinite' : undefined,
        }}
      >
        <Badge count={runningTasks.length} size="small" offset={[4, -4]}>
          <SyncOutlined
            spin={hasRunning}
            style={{ fontSize: 18, color: hasRunning ? '#fff' : 'var(--text-secondary)' }}
          />
        </Badge>
      </div>
    );
  }

  // 展开态：悬浮卡片
  return (
    <div
      style={{
        position: 'fixed',
        bottom: 24,
        right: 24,
        width: 400,
        maxHeight: 520,
        background: 'var(--bg-elevated)',
        border: '1px solid var(--border-default)',
        borderRadius: 8,
        boxShadow: 'var(--shadow-lg)',
        display: 'flex',
        flexDirection: 'column',
        zIndex: 1000,
        overflow: 'hidden',
      }}
    >
      {/* 头部 */}
      <div style={{
        padding: '10px 16px',
        borderBottom: '1px solid var(--border-default)',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        flexShrink: 0,
      }}>
        <Text strong style={{ fontSize: '13px' }}>任务监控</Text>
        <Space size="small">
          <Button
            type="text"
            size="small"
            icon={<ClearOutlined />}
            loading={cleaning}
            onClick={handleCleanup}
            title="清理僵尸任务"
          />
          <Button
            type="text"
            size="small"
            icon={<SyncOutlined spin={isLoading} />}
            onClick={() => { fetchRunningTasks(); setHistoryLoaded(false); }}
          >
            刷新
          </Button>
          <Button
            type="text"
            size="small"
            onClick={() => setExpanded(false)}
            style={{ color: 'var(--text-secondary)' }}
          >
            ✕
          </Button>
        </Space>
      </div>

      {/* 内容区 */}
      <div style={{ overflow: 'auto', flex: 1 }}>
        {/* 运行中任务 */}
        <div style={{ padding: '8px 0' }}>
          <div style={{ padding: '4px 16px', color: 'var(--text-secondary)', fontSize: '12px' }}>
            运行中 ({runningTasks.length})
          </div>
          {isLoading && runningTasks.length === 0 ? (
            <div style={{ padding: '24px 0', textAlign: 'center' }}>
              <Spin size="small" />
            </div>
          ) : runningTasks.length === 0 ? (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="暂无运行中的任务"
              style={{ padding: '16px 0', margin: 0 }}
            />
          ) : (
            <List
              dataSource={runningTasks}
              renderItem={(task) => <TaskItem key={task.run_id} task={task} />}
            />
          )}
        </div>

        {/* 历史任务（可折叠） */}
        <Collapse
          ghost
          size="small"
          onChange={(keys) => { if (keys.length > 0) fetchHistory(); }}
          items={[{
            key: 'history',
            label: (
              <Space size="small">
                <HistoryOutlined />
                <Text style={{ fontSize: '12px' }}>最近完成</Text>
                {historyTasks.length > 0 && (
                  <Tag style={{ fontSize: '11px', lineHeight: '16px' }}>{historyTasks.length}</Tag>
                )}
              </Space>
            ),
            children: historyLoading ? (
              <div style={{ padding: '16px 0', textAlign: 'center' }}>
                <Spin size="small" />
              </div>
            ) : historyTasks.length === 0 ? (
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description="暂无历史记录"
                style={{ padding: '12px 0', margin: 0 }}
              />
            ) : (
              <List
                dataSource={historyTasks}
                renderItem={(task) => <TaskItem key={task.run_id} task={task} />}
              />
            ),
          }]}
          style={{ borderTop: '1px solid var(--border-default)' }}
        />
      </div>
    </div>
  );
};

export default TaskMonitor;
