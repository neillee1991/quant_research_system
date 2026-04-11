import { notify } from '../../../utils/notify';
/**
 * 同步任务配置抽屉 - 组装层
 */
import React, { useState, useEffect } from 'react';
import { Tabs, Modal } from 'antd';
import { useMessage } from '../../../hooks/useMessage';
import { dataApi } from '../../../api';
import { BaseTaskDrawer } from '../../../components/TaskDrawer/BaseTaskDrawer';
import { UniversalJsonEditorTab } from '../../../components/TaskDrawer/tabs/UniversalJsonEditorTab';
import { useTaskLogs } from '../../../hooks/useTaskLogs';
import { TaskLogTable } from '../../../components/TaskLogTable';
import { SyncVisualEditorTab } from './VisualEditorTab';
import { SyncDataInspectTab } from './DataInspectTab';
import type { SyncTask } from '../../../types';

interface SyncTaskDrawerProps {
  visible: boolean;
  task: SyncTask | null;
  isNew: boolean;
  onClose: () => void;
  onSave: () => void;
}

function parseJsonFields(cfg: any): any {
  const result = { ...cfg };
  if (result.params_json && typeof result.params_json === 'string') {
    try { result.params = JSON.parse(result.params_json); } catch { result.params = {}; }
  }
  if (result.schema_json && typeof result.schema_json === 'string') {
    try { result.schema = JSON.parse(result.schema_json); } catch { result.schema = {}; }
  }
  if (result.primary_keys_json && typeof result.primary_keys_json === 'string') {
    try { result.primary_keys = JSON.parse(result.primary_keys_json); } catch { result.primary_keys = []; }
  }
  if (!result.params) result.params = {};
  if (!result.schema) result.schema = {};
  if (!result.primary_keys) result.primary_keys = [];
  return result;
}

function buildInitialConfig(task: SyncTask | null): any {
  if (task) {
    return parseJsonFields({ ...task });
  }
  return {
    task_id: 'sync_',
    description: '',
    api_name: '',
    table_name: 'sync_',
    sync_type: 'incremental',
    source: 'tushare',
    date_field: '',
    api_limit: 0,
    primary_keys: [],
    params: {},
    schema: {},
    enabled: true,
    schedule: '',
    cron_expression: '',
  };
}

export const SyncTaskDrawer: React.FC<SyncTaskDrawerProps> = ({
  visible,
  task,
  isNew,
  onClose,
  onSave,
}) => {
  const message = useMessage();
  const { logs: syncLogs, loading: syncLogsLoading, loadLogs: loadSyncLogs } = useTaskLogs('sync', 50);
  const [activeTab, setActiveTab] = useState('visual');
  const [config, setConfig] = useState<any>(null);
  const [jsonText, setJsonText] = useState('');

  useEffect(() => {
    if (activeTab === 'history' && task?.task_id && !isNew) {
      loadSyncLogs({ taskId: task.task_id });
    }
  }, [activeTab, task?.task_id, isNew]);

  useEffect(() => {
    if (!visible) return;

    if (isNew) {
      const initial = buildInitialConfig(task);
      setConfig(initial);
      setJsonText(JSON.stringify(initial, null, 2));
      return;
    }

    if (task) {
      loadTaskConfig();
    }
  }, [visible, task, isNew]);

  const loadTaskConfig = async () => {
    if (!task) return;
    try {
      const res = await dataApi.getTaskConfig(task.task_id);
      const cfg = parseJsonFields(res.data.config);
      setConfig(cfg);
      setJsonText(JSON.stringify(cfg, null, 2));
    } catch (error) {
      console.error('Failed to load task config:', error);
      const fallback = parseJsonFields({ ...task });
      setConfig(fallback);
      setJsonText(JSON.stringify(fallback, null, 2));
    }
  };

  const handleConfigChange = (newConfig: any) => {
    setConfig(newConfig);
    setJsonText(JSON.stringify(newConfig, null, 2));
  };

  const handleJsonChange = (value: string) => {
    setJsonText(value);
    try {
      setConfig(JSON.parse(value || '{}'));
    } catch {
      // invalid JSON, don't update config
    }
  };

  const handleSave = async (confirmSchemaChange = false) => {
    try {
      const configToSave = {
        ...JSON.parse(jsonText),
        confirm_schema_change: confirmSchemaChange,
      };

      if (isNew) {
        await dataApi.createSyncTask(configToSave);
        notify.success(`任务 ${configToSave.task_id} 创建成功`);
        onSave();
        onClose();
        return;
      }

      const response = await dataApi.updateSyncTask(configToSave.task_id, configToSave);
      const result = response.data;

      if (result.status === 'warning' && result.require_confirmation) {
        showSchemaChangeConfirm(result);
        return;
      }

      notify.success(`任务 ${configToSave.task_id} 更新成功`);
      onSave();
      onClose();
    } catch (error: any) {
      if (error instanceof SyntaxError) {
        notify.error('JSON 格式无效');
      } else {
        notify.error(error.response?.data?.detail || '保存配置失败');
      }
    }
  };

  const showSchemaChangeConfirm = (result: any) => {
    const changes = result.changes || {};
    const oldSchema = changes.old_schema || {};
    const newSchema = changes.new_schema || {};
    const oldFields = Object.keys(oldSchema);
    const newFields = Object.keys(newSchema);

    const addedFields = newFields.filter(f => !oldFields.includes(f));
    const removedFields = oldFields.filter(f => !newFields.includes(f));
    const modifiedFields = oldFields.filter(f => {
      if (!newFields.includes(f)) return false;
      return oldSchema[f]?.type !== newSchema[f]?.type || oldSchema[f]?.nullable !== newSchema[f]?.nullable;
    });

    Modal.confirm({
      title: '表结构或主键已变更',
      content: (
        <div style={{ marginTop: 12 }}>
          <p style={{ marginBottom: 12, fontWeight: 500 }}>检测到以下变更，需要清空历史数据：</p>

          {changes.schema_changed && (
            <div style={{ marginBottom: 16 }}>
              <strong style={{ color: 'var(--color-warning)' }}>表结构变更：</strong>
              {addedFields.length > 0 && (
                <div style={{ marginTop: 8, marginLeft: 12 }}>
                  <div style={{ fontSize: 12, color: 'var(--color-gain)', marginBottom: 4 }}>✓ 新增字段 ({addedFields.length}):</div>
                  {addedFields.map(field => (
                    <div key={field} style={{ fontSize: 11, marginLeft: 12, color: 'var(--text-primary)' }}>
                      • {field}: {newSchema[field]?.type} {newSchema[field]?.nullable ? '(可空)' : '(非空)'}
                      {newSchema[field]?.comment && ` - ${newSchema[field].comment}`}
                    </div>
                  ))}
                </div>
              )}
              {removedFields.length > 0 && (
                <div style={{ marginTop: 8, marginLeft: 12 }}>
                  <div style={{ fontSize: 12, color: 'var(--color-loss)', marginBottom: 4 }}>✗ 删除字段 ({removedFields.length}):</div>
                  {removedFields.map(field => (
                    <div key={field} style={{ fontSize: 11, marginLeft: 12, color: 'var(--text-primary)' }}>
                      • {field}: {oldSchema[field]?.type} {oldSchema[field]?.nullable ? '(可空)' : '(非空)'}
                    </div>
                  ))}
                </div>
              )}
              {modifiedFields.length > 0 && (
                <div style={{ marginTop: 8, marginLeft: 12 }}>
                  <div style={{ fontSize: 12, color: 'var(--color-warning)', marginBottom: 4 }}>⚠ 修改字段 ({modifiedFields.length}):</div>
                  {modifiedFields.map(field => (
                    <div key={field} style={{ fontSize: 11, marginLeft: 12, color: 'var(--text-primary)' }}>
                      • {field}:
                      <div style={{ marginLeft: 12 }}>旧: {oldSchema[field]?.type} {oldSchema[field]?.nullable ? '(可空)' : '(非空)'}</div>
                      <div style={{ marginLeft: 12 }}>新: {newSchema[field]?.type} {newSchema[field]?.nullable ? '(可空)' : '(非空)'}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {changes.primary_keys_changed && (
            <div style={{ marginBottom: 16 }}>
              <strong style={{ color: 'var(--color-warning)' }}>主键变更：</strong>
              <div style={{ marginTop: 8, marginLeft: 12, fontSize: 12 }}>
                <div style={{ color: 'var(--text-secondary)' }}>旧主键: <code>{(changes.old_primary_keys || []).join(', ')}</code></div>
                <div style={{ color: 'var(--text-primary)', marginTop: 4 }}>新主键: <code>{(changes.new_primary_keys || []).join(', ')}</code></div>
              </div>
            </div>
          )}

          <p style={{ color: 'var(--color-loss)', marginTop: 16, padding: 12, background: 'rgba(248, 81, 73, 0.1)', borderRadius: 4, fontSize: 12 }}>
            ⚠️ 确认后将删除表 <code style={{ fontWeight: 600 }}>{result.table_name}</code> 的所有历史数据
          </p>
        </div>
      ),
      okText: '确认清空并保存',
      cancelText: '取消',
      okButtonProps: { danger: true },
      onOk: () => handleSave(true),
    });
  };

  if (!config) return null;

  const drawerTitle = isNew ? '新建任务' : (task?.task_id || '');

  const tabs = [
    {
      key: 'visual',
      label: '可视化编辑',
      children: (
        <SyncVisualEditorTab
          config={config}
          onChange={handleConfigChange}
          isNew={isNew}
        />
      ),
    },
    {
      key: 'json',
      label: 'JSON 编辑',
      children: (
        <UniversalJsonEditorTab
          value={jsonText}
          onChange={handleJsonChange}
        />
      ),
    },
    ...(!isNew && task ? [
      {
        key: 'history',
        label: '历史与数据',
        children: (
          <div>
            <TaskLogTable
              logs={syncLogs}
              loading={syncLogsLoading}
              taskIdLabel="任务ID"
              onFilter={(f) => loadSyncLogs({ ...f, taskId: f.taskId || task.task_id })}
            />
            <div style={{ marginTop: 24 }}>
              <SyncDataInspectTab taskId={task.task_id} />
            </div>
          </div>
        ),
      },
    ] : []),
  ];

  return (
    <BaseTaskDrawer
      visible={visible}
      title={drawerTitle}
      onClose={onClose}
      onSave={() => handleSave(false)}
      saveText={isNew ? '创建' : '保存'}
      width={720}
    >
      <div style={{ padding: '0 24px' }}>
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          size="middle"
          items={tabs}
        />
      </div>
    </BaseTaskDrawer>
  );
};
