/**
 * 通用任务面板容器组件
 * 整合任务列表、操作按钮、日志展示
 */
import React, { useEffect } from 'react';
import { Card, Button } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import type { TaskTypeConfig, GenericTaskStatus } from '../../config/taskTypes';
import { TaskTable } from './TaskTable';
import { TaskLogTable } from '../TaskLogTable';
import type { UseTasksResult } from '../../hooks/useTasks';

interface TaskPanelProps<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>> {
  // 任务类型配置
  config: TaskTypeConfig<TTask, TStatus, TRunParams>;

  // 任务数据和操作（来自 useTasks）
  tasksHook: UseTasksResult<TTask, TStatus, TRunParams>;

  // 额外的操作按钮
  extraActions?: React.ReactNode;

  // 事件处理
  onNewTask?: () => void;
  onEditTask?: (task: TTask) => void;
  onRefresh?: () => void;

  // 显示控制
  showLogs?: boolean;
  logTitle?: string;
}

export function TaskPanel<TTask, TStatus extends GenericTaskStatus, TRunParams extends Record<string, unknown> = Record<string, unknown>>({
  config,
  tasksHook,
  extraActions,
  onNewTask,
  onEditTask,
  onRefresh,
  showLogs = true,
  logTitle,
}: TaskPanelProps<TTask, TStatus, TRunParams>) {
  const {
    tasks,
    taskStatuses,
    runningTasks,
    selectedTaskIds,
    logs,
    loading: logsLoading,
    loadLogs,
    setSelectedTaskIds,
    loadTasks,
    deleteTask,
  } = tasksHook;

  // 自动加载日志
  useEffect(() => {
    loadLogs();
  }, [loadLogs]);

  const handleRefresh = () => {
    if (onRefresh) {
      onRefresh();
    } else {
      loadTasks();
    }
  };

  return (
    <>
      <Card
        className="content-card"
        style={{ marginBottom: '12px' }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            {config.label}管理
          </span>
        }
        extra={
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {/* 批量操作按钮 */}
            {config.batchActions?.map((batchAction) =>
              selectedTaskIds.length > 0 ? (
                <Button
                  key={batchAction.key}
                  type={batchAction.type || 'primary'}
                  icon={batchAction.icon}
                  onClick={() => batchAction.onClick(selectedTaskIds)}
                >
                  {batchAction.label} ({selectedTaskIds.length})
                </Button>
              ) : null
            )}

            {/* 额外操作按钮 */}
            {extraActions}

            {/* 新建按钮 */}
            {onNewTask && (
              <Button onClick={onNewTask}>
                新建任务
              </Button>
            )}

            {/* 刷新按钮 */}
            <Button icon={<ReloadOutlined />} onClick={handleRefresh}>
              刷新
            </Button>
          </div>
        }
      >
        <TaskTable
          config={config}
          tasks={tasks}
          taskStatuses={taskStatuses}
          runningTasks={runningTasks}
          selectedTaskIds={selectedTaskIds}
          onSelectedTaskIdsChange={setSelectedTaskIds}
          onEditTask={onEditTask}
        />
      </Card>

      {/* 日志展示 */}
      {showLogs && (
        <Card
          className="content-card"
          style={{ marginTop: 12 }}
          title={
            <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
              {logTitle || `${config.label}日志`}
            </span>
          }
          extra={
            <Button
              icon={<ReloadOutlined />}
              onClick={loadLogs}
              type="text"
            >
              刷新
            </Button>
          }
        >
          <TaskLogTable
            logs={logs}
            loading={logsLoading}
            taskIdLabel="任务ID"
            onFilter={loadLogs}
          />
        </Card>
      )}
    </>
  );
}
