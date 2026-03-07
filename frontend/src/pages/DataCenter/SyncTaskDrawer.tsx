/**
 * 同步任务配置抽屉组件
 * 匹配原始实现：可视化编辑 + JSON编辑 + 历史调度
 */
import React, { useState, useEffect, useRef } from 'react';
import {
  SideSheet,
  Button,
  Toast,
  Tabs,
  TabPane,
  Input,
  Select,
  Table,
  Collapse,
  Tag,
  Tooltip,
} from '@douyinfe/semi-ui';
import { IconHistory, IconCode, IconPlus, IconDelete } from '@douyinfe/semi-icons';
import Editor from '@monaco-editor/react';
import { dataApi } from '../../api';
import { useThemeStore } from '../../store';
import type { SyncTask } from '../../types';

interface SyncTaskDrawerProps {
  visible: boolean;
  task: SyncTask | null;
  isNew: boolean;
  onClose: () => void;
  onSave: () => void;
  onOpenVersionHistory?: (taskId: string) => void;
}

export const SyncTaskDrawer: React.FC<SyncTaskDrawerProps> = ({
  visible,
  task,
  isNew,
  onClose,
  onSave,
  onOpenVersionHistory,
}) => {
  const { mode } = useThemeStore();
  const [activeTab, setActiveTab] = useState('visual');
  const [config, setConfig] = useState<any>(null);
  const [jsonText, setJsonText] = useState('');
  const [syncHistory, setSyncHistory] = useState<any[]>([]);
  const [taskStatus, setTaskStatus] = useState<any>(null);
  const jsonEditorRef = useRef<any>(null);

  // 加载任务配置
  useEffect(() => {
    if (visible && task && !isNew) {
      loadTaskConfig();
      loadSyncHistory();
      loadTaskStatus();
    } else if (visible && isNew) {
      // 新建任务的默认配置
      const defaultConfig = {
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
      setConfig(defaultConfig);
      setJsonText(JSON.stringify(defaultConfig, null, 2));
    }
  }, [visible, task, isNew]);

  const loadTaskConfig = async () => {
    if (!task) return;
    try {
      const res = await dataApi.getTaskConfig(task.task_id);
      const cfg = res.data.config;
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
      const res = await dataApi.getSyncStatus(undefined, task.task_id, undefined, undefined, 50);
      setSyncHistory(res.data.logs || []);
    } catch (error) {
      console.error('Failed to load sync history:', error);
      setSyncHistory([]);
    }
  };

  const loadTaskStatus = async () => {
    if (!task) return;
    try {
      const res = await dataApi.getTaskStatus(task.task_id);
      setTaskStatus(res.data);
    } catch (error) {
      console.error('Failed to load task status:', error);
    }
  };

  const updateConfig = (key: string, value: any) => {
    const newConfig = { ...config, [key]: value };
    setConfig(newConfig);
    setJsonText(JSON.stringify(newConfig, null, 2));
  };

  const updateSchemaField = (fieldName: string, prop: string, value: any) => {
    const newSchema = { ...config.schema };
    if (!newSchema[fieldName]) newSchema[fieldName] = {};
    newSchema[fieldName] = { ...newSchema[fieldName], [prop]: value };
    updateConfig('schema', newSchema);
  };

  const updateParamsField = (key: string, value: string) => {
    const newParams = { ...config.params, [key]: value };
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
      Toast.error('JSON 格式无效');
    }
  };

  const handleSave = async () => {
    try {
      const configToSave = JSON.parse(jsonText);

      if (isNew) {
        await dataApi.createSyncTask(configToSave);
        Toast.success(`任务 ${configToSave.task_id} 创建成功`);
      } else {
        await dataApi.updateSyncTask(configToSave.task_id, configToSave);
        Toast.success(`任务 ${configToSave.task_id} 更新成功`);
      }

      onSave();
      onClose();
    } catch (error: any) {
      if (error instanceof SyntaxError) {
        Toast.error('JSON 格式无效');
      } else {
        Toast.error(error.response?.data?.detail || '保存配置失败');
      }
    }
  };

  const handleAddSchemaField = () => {
    const newSchema = { ...config.schema };
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
    const newSchema = { ...config.schema };
    delete newSchema[fieldName];
    updateConfig('schema', newSchema);
  };

  if (!config) return null;

  return (
    <SideSheet
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ color: 'var(--semi-color-primary)' }}>
            {isNew ? '新建任务' : task?.task_id}
          </span>
          {!isNew && onOpenVersionHistory && (
            <Button
              icon={<IconHistory />}
              size="small"
              theme="borderless"
              onClick={() => onOpenVersionHistory(task?.task_id || '')}
            >
              版本历史
            </Button>
          )}
        </div>
      }
      visible={visible}
      onCancel={onClose}
      width={720}
      bodyStyle={{ padding: 0 }}
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
              background: 'var(--semi-color-fill-0)',
              borderBottom: '1px solid var(--semi-color-border)',
            }}
          >
            <div>
              <div style={{ color: 'var(--semi-color-text-2)', fontSize: '12px', marginBottom: 4 }}>
                最新数据
              </div>
              <span style={{ color: 'var(--semi-color-success)', fontSize: '13px', fontWeight: 500 }}>
                {taskStatus.table_latest_date || '-'}
              </span>
            </div>
            <div>
              <div style={{ color: 'var(--semi-color-text-2)', fontSize: '12px', marginBottom: 4 }}>
                上次同步
              </div>
              <span style={{ color: 'var(--semi-color-text-2)', fontSize: '13px' }}>
                {taskStatus.last_sync_time || '-'}
              </span>
            </div>
          </div>
        )}

        {/* 标签页内容 */}
        <div style={{ flex: 1, overflow: 'auto', padding: '16px 24px' }}>
          <Tabs activeKey={activeTab} onChange={setActiveTab} size="small">
            {/* 可视化编辑 */}
            <TabPane tab="可视化编辑" itemKey="visual">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingTop: 8 }}>
                {/* 基本信息 */}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      任务ID
                    </div>
                    <Input
                      size="small"
                      prefix="sync_"
                      value={(config.task_id || '').replace(/^sync_/, '')}
                      onChange={(v) => updateConfig('task_id', `sync_${v}`)}
                      disabled={!isNew}
                      style={!isNew ? { background: 'var(--semi-color-fill-0)' } : undefined}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      API名称
                    </div>
                    <Input
                      size="small"
                      value={config.api_name || ''}
                      onChange={(v) => updateConfig('api_name', v)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      描述
                    </div>
                    <Input
                      size="small"
                      value={config.description || ''}
                      onChange={(v) => updateConfig('description', v)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      数据表
                    </div>
                    <Input
                      size="small"
                      prefix="sync_"
                      value={(config.table_name || '').replace(/^sync_/, '')}
                      onChange={(v) => updateConfig('table_name', `sync_${v}`)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      同步类型
                    </div>
                    <Select
                      size="small"
                      value={config.sync_type}
                      onChange={(v) => updateConfig('sync_type', v)}
                      style={{ width: '100%' }}
                    >
                      <Select.Option value="incremental">增量</Select.Option>
                      <Select.Option value="full">全量</Select.Option>
                    </Select>
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      来源
                    </div>
                    <Input
                      size="small"
                      value={config.source || 'tushare'}
                      onChange={(v) => updateConfig('source', v)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      日期字段
                    </div>
                    <Input
                      size="small"
                      value={config.date_field || ''}
                      onChange={(v) => updateConfig('date_field', v)}
                    />
                  </div>
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      API限制
                    </div>
                    <Input
                      size="small"
                      type="number"
                      value={config.api_limit || ''}
                      onChange={(v) => updateConfig('api_limit', parseInt(v) || 0)}
                    />
                  </div>
                  <div style={{ gridColumn: 'span 2' }}>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      主键（逗号分隔）
                    </div>
                    <Input
                      size="small"
                      value={config.primary_keys?.join(', ') || ''}
                      onChange={(v) =>
                        updateConfig(
                          'primary_keys',
                          v.split(',').map((s: string) => s.trim()).filter(Boolean)
                        )
                      }
                    />
                  </div>
                </div>

                {/* API 参数 */}
                {config.params && Object.keys(config.params).length > 0 && (
                  <Collapse defaultActiveKey={['params']}>
                    <Collapse.Panel
                      header={<span style={{ fontSize: '13px', fontWeight: 500 }}>API 参数</span>}
                      itemKey="params"
                    >
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                        {Object.entries(config.params).map(([key, value]) => (
                          <div key={key}>
                            <div
                              style={{
                                marginBottom: 4,
                                fontSize: '12px',
                                color: 'var(--semi-color-text-2)',
                              }}
                            >
                              {key}
                            </div>
                            <Input
                              size="small"
                              value={String(value)}
                              onChange={(v) => updateParamsField(key, v)}
                            />
                          </div>
                        ))}
                      </div>
                    </Collapse.Panel>
                  </Collapse>
                )}

                {/* Schema 字段表格 */}
                {config.schema && (
                  <Collapse defaultActiveKey={['schema']}>
                    <Collapse.Panel
                      header={<span style={{ fontSize: '13px', fontWeight: 500 }}>字段定义 (Schema)</span>}
                      itemKey="schema"
                    >
                      <Table
                        dataSource={Object.entries(config.schema).map(([name, props]: [string, any]) => ({
                          name,
                          type: props?.type || '',
                          nullable: props?.nullable,
                          comment: props?.comment || '',
                        }))}
                        rowKey="name"
                        size="small"
                        pagination={false}
                        columns={[
                          {
                            title: '字段名',
                            dataIndex: 'name',
                            key: 'name',
                            width: 140,
                            render: (v: string) => (
                              <Input
                                size="small"
                                value={v}
                                onChange={(val) => {
                                  const newSchema = { ...config.schema };
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
                                size="small"
                                value={v}
                                onChange={(val) => updateSchemaField(r.name, 'type', val)}
                                style={{ width: 120 }}
                              >
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
                            ),
                          },
                          {
                            title: '可空',
                            dataIndex: 'nullable',
                            key: 'nullable',
                            width: 60,
                            render: (v: boolean, r: any) => (
                              <Input
                                size="small"
                                type="checkbox"
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
                                size="small"
                                value={v}
                                onChange={(val) => updateSchemaField(r.name, 'comment', val)}
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
                                type="danger"
                                theme="borderless"
                                icon={<IconDelete />}
                                size="small"
                                onClick={() => handleDeleteSchemaField(r.name)}
                              />
                            ),
                          },
                        ]}
                      />
                      <Button
                        icon={<IconPlus />}
                        size="small"
                        onClick={handleAddSchemaField}
                        style={{ marginTop: 8 }}
                      >
                        新增字段
                      </Button>
                    </Collapse.Panel>
                  </Collapse>
                )}
              </div>
            </TabPane>

            {/* JSON 编辑 */}
            <TabPane tab="JSON 编辑" itemKey="json">
              <div style={{ paddingTop: 8 }}>
                <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
                  <Button size="small" icon={<IconCode />} onClick={handleFormatJson}>
                    格式化
                  </Button>
                </div>
                <div
                  style={{
                    border: '1px solid var(--semi-color-border)',
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
                <div style={{ marginTop: 6, color: 'var(--semi-color-text-2)', fontSize: 11 }}>
                  直接编辑 JSON 配置，修改会同步到可视化编辑界面
                </div>
              </div>
            </TabPane>

            {/* 历史调度 */}
            {!isNew && (
              <TabPane tab="历史调度" itemKey="history">
                <div style={{ paddingTop: 8 }}>
                  <Table
                    dataSource={syncHistory}
                    rowKey={(record: any) => `${record.sync_date}-${record.created_at}`}
                    size="small"
                    pagination={{ pageSize: 10 }}
                    empty={
                      <div style={{ padding: 20, textAlign: 'center', color: 'var(--semi-color-text-2)' }}>
                        暂无调度记录
                      </div>
                    }
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
                            <Tooltip content={record.error_message}>{tag}</Tooltip>
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
              </TabPane>
            )}
          </Tabs>
        </div>

        {/* 底部按钮 */}
        <div
          style={{
            display: 'flex',
            justifyContent: 'flex-end',
            gap: 8,
            borderTop: '1px solid var(--semi-color-border)',
            padding: '12px 24px',
          }}
        >
          <Button onClick={onClose}>取消</Button>
          <Button theme="solid" type="primary" onClick={handleSave}>
            {isNew ? '创建' : '保存'}
          </Button>
        </div>
      </div>
    </SideSheet>
  );
};
