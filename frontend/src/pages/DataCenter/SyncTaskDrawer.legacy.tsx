import { notify } from '../../utils/notify';
/**
 * 同步任务配置抽屉组件
 * 匹配原始实现：可视化编辑 + JSON编辑 + 历史调度
 */
import React, { useState, useEffect, useRef } from 'react';
import {
  Drawer,
  Button,
  Tabs,
  Input,
  Select,
  Table,
  Collapse,
  Tag,
  Tooltip,
  Checkbox,
  Modal,
  Descriptions,
  Progress,
  Spin,
  Alert,
} from 'antd';
import { CodeOutlined, PlusOutlined, DeleteOutlined, SearchOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import Editor from '@monaco-editor/react';
import { dataApi, taskMonitorApi } from '../../api';
import { useThemeStore } from '../../store';
import type { SyncTask } from '../../types';

interface SyncTaskDrawerProps {
  visible: boolean;
  task: SyncTask | null;
  isNew: boolean;
  onClose: () => void;
  onSave: () => void;
}

export const SyncTaskDrawer: React.FC<SyncTaskDrawerProps> = ({
  visible,
  task,
  isNew,
  onClose,
  onSave,
}) => {
  const { mode } = useThemeStore();
  const message = useMessage();
  const [activeTab, setActiveTab] = useState('visual');
  const [config, setConfig] = useState<any>(null);
  const [jsonText, setJsonText] = useState('');
  const [syncHistory, setSyncHistory] = useState<any[]>([]);
  const [taskStatus, setTaskStatus] = useState<any>(null);
  const [inspectionData, setInspectionData] = useState<any>(null);
  const [inspectionLoading, setInspectionLoading] = useState(false);
  const [showInspection, setShowInspection] = useState(false);
  const jsonEditorRef = useRef<any>(null);

  // 加载任务配置
  useEffect(() => {
    if (visible && task && !isNew) {
      loadTaskConfig();
      loadSyncHistory();
      loadTaskStatus();
    } else if (visible && isNew) {
      // 检查是否有预设配置（从指数订阅传递来的）
      let initialConfig;
      if (task) {
        // 使用预设配置
        initialConfig = { ...task };
        // 解析 JSON 字段
        if (initialConfig.params_json && typeof initialConfig.params_json === 'string') {
          try {
            initialConfig.params = JSON.parse(initialConfig.params_json);
          } catch (e) {
            initialConfig.params = {};
          }
        }
        if (initialConfig.schema_json && typeof initialConfig.schema_json === 'string') {
          try {
            initialConfig.schema = JSON.parse(initialConfig.schema_json);
          } catch (e) {
            initialConfig.schema = {};
          }
        }
        if (initialConfig.primary_keys_json && typeof initialConfig.primary_keys_json === 'string') {
          try {
            initialConfig.primary_keys = JSON.parse(initialConfig.primary_keys_json);
          } catch (e) {
            initialConfig.primary_keys = [];
          }
        }
        // 如果没有 params/schema，设置空对象
        if (!initialConfig.params) initialConfig.params = {};
        if (!initialConfig.schema) initialConfig.schema = {};
        if (!initialConfig.primary_keys) initialConfig.primary_keys = [];
      } else {
        // 新建任务的默认配置
        initialConfig = {
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
      setConfig(initialConfig);
      setJsonText(JSON.stringify(initialConfig, null, 2));
      setSyncHistory([]);
      setTaskStatus(null);
    }
  }, [visible, task, isNew]);

  const loadTaskConfig = async () => {
    if (!task) return;
    try {
      const res = await dataApi.getTaskConfig(task.task_id);
      let cfg = res.data.config;

      // 解析 JSON 字段
      if (cfg.params_json && typeof cfg.params_json === 'string') {
        try {
          cfg.params = JSON.parse(cfg.params_json);
        } catch (e) {
          console.error('Failed to parse params_json:', e);
          cfg.params = {};
        }
      }

      if (cfg.schema_json && typeof cfg.schema_json === 'string') {
        try {
          cfg.schema = JSON.parse(cfg.schema_json);
        } catch (e) {
          console.error('Failed to parse schema_json:', e);
          cfg.schema = {};
        }
      }

      if (cfg.primary_keys_json && typeof cfg.primary_keys_json === 'string') {
        try {
          cfg.primary_keys = JSON.parse(cfg.primary_keys_json);
        } catch (e) {
          console.error('Failed to parse primary_keys_json:', e);
          cfg.primary_keys = [];
        }
      }

      setConfig(cfg);
      setJsonText(JSON.stringify(cfg, null, 2));
    } catch (error) {
      console.error('Failed to load task config:', error);
      const fallback = task;
      setConfig(fallback);
      setJsonText(JSON.stringify(fallback, null, 2));
    }
  };

  const loadSyncHistory = async () => {
    if (!task) return;
    try {
      // 使用统一的 taskMonitorApi 获取任务历史
      const res = await taskMonitorApi.getTaskHistory(50, 'sync', task.task_id);
      // 转换格式以兼容现有代码
      const history = (res.data.tasks || []).map(task => ({
        source: 'tushare_config',
        data_type: task.task_id,
        last_date: task.params ? JSON.parse(task.params).start_date || '' : '',
        sync_date: task.params ? JSON.parse(task.params).start_date || '' : '',
        rows_synced: task.rows || 0,
        status: task.status,
        error_message: task.error || '',
        params: task.params || '',
        created_at: task.started_at || ''
      }));
      setSyncHistory(history);
    } catch (error) {
      console.error('Failed to load sync history:', error);
      setSyncHistory([]);
    }
  };

  const loadTaskStatus = async () => {
    // 不再需要单独加载任务状态，统一使用 taskMonitorApi
    return;
  };

  const handleInspectData = async () => {
    if (!task) return;
    setInspectionLoading(true);
    try {
      const response = await fetch(
        `http://localhost:8000/api/v1/tasks/sync/${task.task_id}/inspect`
      );
      const data = await response.json();
      setInspectionData(data);
      setShowInspection(true);
    } catch (error) {
      console.error('Data inspection failed:', error);
      notify.error('数据探查失败');
    } finally {
      setInspectionLoading(false);
    }
  };

  const updateConfig = (key: string, value: any) => {
    const newConfig = { ...(config || {}), [key]: value };
    setConfig(newConfig);
    setJsonText(JSON.stringify(newConfig, null, 2));
  };

  const updateSchemaField = (fieldName: string, prop: string, value: any) => {
    const newSchema = { ...(config.schema || {}) };
    if (!newSchema[fieldName]) newSchema[fieldName] = {};
    newSchema[fieldName] = { ...newSchema[fieldName], [prop]: value };
    updateConfig('schema', newSchema);
  };

  const updateParamsField = (key: string, value: string) => {
    const newParams = { ...(config.params || {}), [key]: value };
    updateConfig('params', newParams);
  };

  const handleJsonChange = (value: string | undefined) => {
    setJsonText(value || '');
    try {
      const parsed = JSON.parse(value || '{}');
      setConfig(parsed);
    } catch (error) {
      // JSON 格式错误，不更新 config
    }
  };

  const handleFormatJson = () => {
    try {
      const parsed = JSON.parse(jsonText);
      const formatted = JSON.stringify(parsed, null, 2);
      setJsonText(formatted);
      if (jsonEditorRef.current) {
        jsonEditorRef.current.setValue(formatted);
      }
    } catch (error) {
      notify.error('JSON 格式无效');
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
      } else {
        const response = await dataApi.updateSyncTask(configToSave.task_id, configToSave);
        const result = response.data;

        // 检查是否需要确认 schema 变更
        if (result.status === 'warning' && result.require_confirmation) {
          const changes = result.changes || {};

          // 分析 schema 详细变化
          const oldSchema = changes.old_schema || {};
          const newSchema = changes.new_schema || {};
          const oldFields = Object.keys(oldSchema);
          const newFields = Object.keys(newSchema);

          const addedFields = newFields.filter(f => !oldFields.includes(f));
          const removedFields = oldFields.filter(f => !newFields.includes(f));
          const modifiedFields = oldFields.filter(f => {
            if (!newFields.includes(f)) return false;
            const oldType = oldSchema[f]?.type;
            const newType = newSchema[f]?.type;
            const oldNullable = oldSchema[f]?.nullable;
            const newNullable = newSchema[f]?.nullable;
            return oldType !== newType || oldNullable !== newNullable;
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
                        <div style={{ fontSize: 12, color: 'var(--color-gain)', marginBottom: 4 }}>
                          ✓ 新增字段 ({addedFields.length}):
                        </div>
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
                        <div style={{ fontSize: 12, color: 'var(--color-loss)', marginBottom: 4 }}>
                          ✗ 删除字段 ({removedFields.length}):
                        </div>
                        {removedFields.map(field => (
                          <div key={field} style={{ fontSize: 11, marginLeft: 12, color: 'var(--text-primary)' }}>
                            • {field}: {oldSchema[field]?.type} {oldSchema[field]?.nullable ? '(可空)' : '(非空)'}
                          </div>
                        ))}
                      </div>
                    )}

                    {modifiedFields.length > 0 && (
                      <div style={{ marginTop: 8, marginLeft: 12 }}>
                        <div style={{ fontSize: 12, color: 'var(--color-warning)', marginBottom: 4 }}>
                          ⚠ 修改字段 ({modifiedFields.length}):
                        </div>
                        {modifiedFields.map(field => (
                          <div key={field} style={{ fontSize: 11, marginLeft: 12, color: 'var(--text-primary)' }}>
                            • {field}:
                            <div style={{ marginLeft: 12 }}>
                              旧: {oldSchema[field]?.type} {oldSchema[field]?.nullable ? '(可空)' : '(非空)'}
                            </div>
                            <div style={{ marginLeft: 12 }}>
                              新: {newSchema[field]?.type} {newSchema[field]?.nullable ? '(可空)' : '(非空)'}
                            </div>
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
                      <div style={{ color: 'var(--text-secondary)' }}>
                        旧主键: <code>{(changes.old_primary_keys || []).join(', ')}</code>
                      </div>
                      <div style={{ color: 'var(--text-primary)', marginTop: 4 }}>
                        新主键: <code>{(changes.new_primary_keys || []).join(', ')}</code>
                      </div>
                    </div>
                  </div>
                )}

                <p style={{
                  color: 'var(--color-loss)',
                  marginTop: 16,
                  padding: 12,
                  background: 'rgba(248, 81, 73, 0.1)',
                  borderRadius: 4,
                  fontSize: 12
                }}>
                  ⚠️ 确认后将删除表 <code style={{ fontWeight: 600 }}>{result.table_name}</code> 的所有历史数据
                </p>
              </div>
            ),
            okText: '确认清空并保存',
            cancelText: '取消',
            okButtonProps: { danger: true },
            onOk: () => handleSave(true),
          });
          return;
        }

        notify.success(`任务 ${configToSave.task_id} 更新成功`);
        onSave();
        onClose();
      }
    } catch (error: any) {
      if (error instanceof SyntaxError) {
        notify.error('JSON 格式无效');
      } else {
        notify.error(error.response?.data?.detail || '保存配置失败');
      }
    }
  };

  const handleAddSchemaField = () => {
    const newSchema = { ...(config.schema || {}) };
    let newName = 'new_field';
    let counter = 1;
    while (newSchema[newName]) {
      newName = `new_field_${counter}`;
      counter++;
    }
    newSchema[newName] = { type: 'STRING', nullable: true, comment: '' };
    updateConfig('schema', newSchema);
  };

  const handleDeleteSchemaField = (fieldName: string) => {
    const newSchema = { ...(config.schema || {}) };
    delete newSchema[fieldName];
    updateConfig('schema', newSchema);
  };

  if (!config) return null;

  return (
    <>
      <Drawer
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span style={{ color: 'var(--color-primary)' }}>
              {isNew ? '新建任务' : task?.task_id}
            </span>
          </div>
        }
        open={visible}
        onClose={onClose}
        width={720}
        styles={{ body: { padding: 0 } }}
      >
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        {/* 状态信息栏 - 仅编辑模式显示 */}
        {!isNew && taskStatus && (
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 16,
              padding: '12px 24px',
              background: 'var(--bg-tertiary)',
              borderBottom: '1px solid var(--border-color)',
            }}
          >
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>
                最新数据
              </div>
              <span style={{ color: 'var(--color-gain)', fontSize: '13px', fontWeight: 500 }}>
                {taskStatus.table_latest_date || '-'}
              </span>
            </div>
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>
                上次同步
              </div>
              <span style={{ color: 'var(--text-secondary)', fontSize: '13px' }}>
                {taskStatus.last_sync_time || '-'}
              </span>
            </div>
          </div>
        )}

        {/* 标签页内容 */}
        <div style={{ flex: 1, overflow: 'auto', padding: '16px 24px' }}>
          <Tabs activeKey={activeTab} onChange={setActiveTab} size="middle" items={[
            {
              key: 'visual',
              label: '可视化编辑',
              children: (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingTop: 8 }}>
                {/* 基本信息 */}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      任务ID
                    </div>
                    <Input
                      size="middle"
                      prefix="sync_"
                      value={(config.task_id || '').replace(/^sync_/, '')}
                      onChange={(e) => updateConfig('task_id', `sync_${e.target.value}`)}
                      disabled={!isNew}
                      style={!isNew ? { background: 'var(--bg-tertiary)' } : undefined}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      API名称
                    </div>
                    <Input
                      size="middle"
                      value={config.api_name || ''}
                      onChange={(e) => updateConfig('api_name', e.target.value)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      描述
                    </div>
                    <Input
                      size="middle"
                      value={config.description || ''}
                      onChange={(e) => updateConfig('description', e.target.value)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      数据表
                    </div>
                    <Input
                      size="middle"
                      prefix="sync_"
                      value={(config.table_name || '').replace(/^sync_/, '')}
                      onChange={(e) => updateConfig('table_name', `sync_${e.target.value}`)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      同步类型
                    </div>
                    <Select
                      size="middle"
                      value={config.sync_type}
                      onChange={(v) => updateConfig('sync_type', v)}
                      style={{ width: '100%' }}
                      options={[
                        { value: 'incremental', label: '增量' },
                        { value: 'full', label: '全量' },
                      ]}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      来源
                    </div>
                    <Input
                      size="middle"
                      value={config.source || 'tushare'}
                      onChange={(e) => updateConfig('source', e.target.value)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      日期字段
                    </div>
                    <Input
                      size="middle"
                      value={config.date_field || ''}
                      onChange={(e) => updateConfig('date_field', e.target.value)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      API限制
                    </div>
                    <Input
                      size="middle"
                      type="number"
                      value={config.api_limit || ''}
                      onChange={(e) => updateConfig('api_limit', parseInt(e.target.value) || 0)}
                    />
                  </div>
                  <div style={{ gridColumn: 'span 2' }}>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      主键（逗号分隔）
                    </div>
                    <Input
                      size="middle"
                      value={config.primary_keys?.join(', ') || ''}
                      onChange={(e) =>
                        updateConfig(
                          'primary_keys',
                          (e.target.value || '').split(',').map((s: string) => s.trim()).filter(Boolean)
                        )
                      }
                    />
                  </div>
                </div>

                {/* API 参数 */}
                <Collapse defaultActiveKey={['params']} items={[{
                  key: 'params',
                  label: <span style={{ fontSize: '13px', fontWeight: 500 }}>API 参数</span>,
                  children: config.params && Object.keys(config.params).length > 0 ? (
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                        {Object.entries(config.params).map(([key, value]) => (
                          <div key={key}>
                            <div
                              style={{
                                marginBottom: 4,
                                fontSize: '12px',
                                color: 'var(--text-secondary)',
                              }}
                            >
                              {key}
                            </div>
                            <Input
                              size="middle"
                              value={String(value)}
                              onChange={(e) => updateParamsField(key, e.target.value)}
                            />
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div style={{ padding: 12, color: 'var(--text-secondary)', fontSize: '12px' }}>
                        暂无 API 参数配置
                      </div>
                    ),
                }]} />

                {/* Schema 字段表格 */}
                {config.schema && (
                  <Collapse defaultActiveKey={['schema']} items={[{
                    key: 'schema',
                    label: <span style={{ fontSize: '13px', fontWeight: 500 }}>字段定义 (Schema)</span>,
                    children: (<>
                      <Table
                        dataSource={Object.entries(config.schema || {}).map(([name, props]: [string, any]) => ({
                          name,
                          type: props?.type || '',
                          nullable: props?.nullable,
                          comment: props?.comment || '',
                        }))}
                        rowKey="name"
                        size="middle"
                        pagination={false}
                        columns={[
                          {
                            title: '字段名',
                            dataIndex: 'name',
                            key: 'name',
                            width: 140,
                            render: (v: string) => (
                              <Input
                                size="middle"
                                value={v}
                                onChange={(e) => {
                                  const val = e.target.value;
                                  const newSchema = { ...(config.schema || {}) };
                                  const entries = Object.entries(newSchema);
                                  const rebuilt: Record<string, any> = {};
                                  entries.forEach(([key, props]) => {
                                    rebuilt[key === v ? val : key] = props;
                                  });
                                  updateConfig('schema', rebuilt);
                                }}
                                style={{ fontSize: '12px' }}
                              />
                            ),
                          },
                          {
                            title: '类型',
                            dataIndex: 'type',
                            key: 'type',
                            width: 130,
                            render: (v: string, r: any) => (
                              <Select
                                size="middle"
                                value={v}
                                onChange={(val) => updateSchemaField(r.name, 'type', val)}
                                style={{ width: 120 }}
                                options={['BOOL','CHAR','SHORT','INT','LONG','FLOAT','DOUBLE','DATE','MONTH','TIME','MINUTE','SECOND','DATETIME','TIMESTAMP','NANOTIME','NANOTIMESTAMP','SYMBOL','STRING','UUID','BLOB'].map(t => ({ value: t, label: t }))}
                              />
                            ),
                          },
                          {
                            title: '可空',
                            dataIndex: 'nullable',
                            key: 'nullable',
                            width: 60,
                            render: (v: boolean, r: any) => (
                              <Checkbox
                                checked={v}
                                onChange={(e: any) =>
                                  updateSchemaField(r.name, 'nullable', e.target.checked)
                                }
                              />
                            ),
                          },
                          {
                            title: '注释',
                            dataIndex: 'comment',
                            key: 'comment',
                            render: (v: string, r: any) => (
                              <Input
                                size="middle"
                                value={v}
                                onChange={(e) => updateSchemaField(r.name, 'comment', e.target.value)}
                                style={{ fontSize: '12px' }}
                              />
                            ),
                          },
                          {
                            title: '操作',
                            key: 'action',
                            width: 60,
                            render: (_: any, r: any) => (
                              <Button
                                danger
                                type="text"
                                icon={<DeleteOutlined />}
                                size="middle"
                                onClick={() => handleDeleteSchemaField(r.name)}
                              />
                            ),
                          },
                        ]}
                      />
                      <Button
                        icon={<PlusOutlined />}
                        size="middle"
                        onClick={handleAddSchemaField}
                        style={{ marginTop: 8 }}
                      >
                        新增字段
                      </Button>
                    </>),
                  }]} />
                )}
              </div>
            ),
            },
            {
              key: 'json',
              label: 'JSON 编辑',
              children: (
              <div style={{ paddingTop: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
                  <Button size="middle" icon={<CodeOutlined />} onClick={handleFormatJson}>
                    格式化
                  </Button>
                </div>
                <div
                  style={{
                    border: '1px solid var(--border-color)',
                    borderRadius: 4,
                    overflow: 'hidden',
                  }}
                >
                  <Editor
                    height="500px"
                    language="json"
                    theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                    value={jsonText}
                    onChange={handleJsonChange}
                    onMount={(editor) => {
                      jsonEditorRef.current = editor;
                    }}
                    options={{
                      minimap: { enabled: false },
                      fontSize: 13,
                      scrollBeyondLastLine: false,
                      automaticLayout: true,
                      tabSize: 2,
                      wordWrap: 'on',
                    }}
                  />
                </div>
                <div style={{ marginTop: 6, color: 'var(--text-secondary)', fontSize: 11 }}>
                  直接编辑 JSON 配置，修改会同步到可视化编辑界面
                </div>
              </div>
            ),
            },
            ...(!isNew ? [{
              key: 'history',
              label: '历史调度',
              children: (
                <div style={{ paddingTop: 8 }}>
                  {/* 数据探查按钮 - 仅增量任务显示 */}
                  {task?.sync_type === 'incremental' && (
                    <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Button
                        icon={<SearchOutlined />}
                        onClick={handleInspectData}
                        loading={inspectionLoading}
                        type="primary"
                      >
                        数据探查
                      </Button>
                    </div>
                  )}

                  {/* 数据探查结果 */}
                  {showInspection && inspectionData && (
                    <div style={{ marginBottom: 16 }}>
                      {!inspectionData.exists ? (
                        <Alert
                          type="warning"
                          message={inspectionData.message}
                          closable={false}
                        />
                      ) : !inspectionData.has_data ? (
                        <Alert
                          type="info"
                          message={inspectionData.message}
                          closable={false}
                        />
                      ) : (
                        <Collapse defaultActiveKey={['1']} items={[{
                          key: '1',
                          label: '数据完整性报告',
                          children: (<>
                            <Descriptions size="middle" items={[
                              { key: 'table', label: '表名', children: inspectionData.table_name },
                              { key: 'field', label: '日期字段', children: inspectionData.date_field },
                              { key: 'min', label: '最早日期', children: inspectionData.min_date },
                              { key: 'max', label: '最晚日期', children: inspectionData.max_date },
                              { key: 'actual', label: '实际天数', children: inspectionData.actual_dates },
                              { key: 'expected', label: '预期天数', children: inspectionData.expected_dates || '-' },
                              { key: 'missing', label: '缺失天数', children: <Tag color="red">{inspectionData.missing_count || 0}</Tag> },
                              { key: 'coverage', label: '覆盖率', children: (
                                <Progress
                                  percent={inspectionData.coverage_percent || 0}
                                  strokeColor={inspectionData.coverage_percent >= 95 ? 'var(--color-gain)' : 'var(--color-loss)'}
                                  style={{ width: 200, marginLeft: 8 }}
                                  size="middle"
                                />
                              )},
                            ]} />

                            {inspectionData.missing_count > 0 && inspectionData.missing_dates && (
                              <div style={{ marginTop: 16 }}>
                                <div style={{ marginBottom: 8, fontWeight: 600 }}>缺失的交易日：</div>
                                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                                  {inspectionData.missing_dates.slice(0, 20).map((date: string) => (
                                    <Tag key={date} color="red">{date}</Tag>
                                  ))}
                                  {inspectionData.missing_count > 20 && (
                                    <Tag>... 还有 {inspectionData.missing_count - 20} 天</Tag>
                                  )}
                                </div>
                              </div>
                            )}

                            {!inspectionData.trading_calendar_available && (
                              <Alert
                                type="warning"
                                message="交易日历数据不可用，无法检查缺失日期。请先同步 sync_trade_cal 任务。"
                                style={{ marginTop: 16 }}
                                closable={false}
                              />
                            )}
                          </>),
                        }]} />
                      )}
                    </div>
                  )}

                  {/* 历史调度表格 */}
                  <Table
                    dataSource={syncHistory}
                    rowKey={(record: any) => `${record.sync_date}-${record.created_at}`}
                    size="middle"
                    pagination={{ pageSize: 10 }}
                    locale={{ emptyText: <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-secondary)' }}>暂无调度记录</div> }}
                    columns={[
                      { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 100 },
                      {
                        title: '同步行数',
                        dataIndex: 'rows_synced',
                        key: 'rows_synced',
                        width: 100,
                        render: (v: number) => v?.toLocaleString() || '-',
                      },
                      {
                        title: '状态',
                        dataIndex: 'status',
                        key: 'status',
                        width: 80,
                        render: (v: string, record: any) => {
                          const tag = <Tag color={v === 'success' ? 'green' : 'red'}>{v}</Tag>;
                          return v !== 'success' && record.error_message ? (
                            <Tooltip title={record.error_message}>{tag}</Tooltip>
                          ) : (
                            tag
                          );
                        },
                      },
                      {
                        title: '执行时间',
                        dataIndex: 'created_at',
                        key: 'created_at',
                        render: (v: string) => (v ? new Date(v).toLocaleString() : '-'),
                      },
                    ]}
                  />
                </div>
              ),
            }] : []),
          ]} />
        </div>

        {/* 底部按钮 */}
        <div
          style={{
            display: 'flex',
            justifyContent: 'flex-end',
            gap: 8,
            borderTop: '1px solid var(--border-color)',
            padding: '12px 24px',
          }}
        >
          <Button onClick={onClose}>取消</Button>
          <Button type="primary" onClick={() => handleSave(false)}>
            {isNew ? '创建' : '保存'}
          </Button>
        </div>
      </div>
    </Drawer>
    </>
  );
};
