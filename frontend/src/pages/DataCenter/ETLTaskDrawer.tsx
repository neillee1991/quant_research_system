/**
 * ETL 任务配置抽屉组件
 * 匹配原始实现：配置 + 脚本测试 + 字段定义 + 历史记录
 */
import React, { useState, useEffect, useRef } from 'react';
import {
  SideSheet,
  Button,
  Toast,
  Tabs,
  TabPane,
  Input,
  RadioGroup,
  Radio,
  Table,
  Select,
  DatePicker,
  Tag,
  Tooltip,
  Modal,
} from '@douyinfe/semi-ui';
import { IconCode } from '@douyinfe/semi-icons';
import Editor from '@monaco-editor/react';
import dayjs from 'dayjs';
import { dataApi } from '../../api';
import { useThemeStore } from '../../store';
import type { ETLTask } from '../../types';

interface ETLTaskDrawerProps {
  visible: boolean;
  task: ETLTask | null;
  isNew: boolean;
  onClose: () => void;
  onSave: () => void;
}

interface FieldType {
  name: string;
  type: string;
}

interface TestResult {
  status: 'success' | 'error';
  rows?: number;
  preview?: any[];
  error?: string;
}

export const ETLTaskDrawer: React.FC<ETLTaskDrawerProps> = ({
  visible,
  task,
  isNew,
  onClose,
  onSave,
}) => {
  const { mode } = useThemeStore();
  const [activeTab, setActiveTab] = useState('config');
  const [config, setConfig] = useState<any>({
    task_id: 'etl_',
    description: '',
    sync_type: 'incremental',
    date_field: '',
    script: '',
  });
  const [taskStatus, setTaskStatus] = useState<any>(null);
  const [fieldTypes, setFieldTypes] = useState<FieldType[]>([]);
  const [selectedPrimaryKeys, setSelectedPrimaryKeys] = useState<string[]>([]);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [testLoading, setTestLoading] = useState(false);
  const [testDate, setTestDate] = useState('');
  const [etlLogs, setEtlLogs] = useState<any[]>([]);
  const etlEditorRef = useRef<any>(null);

  // 加载任务配置
  useEffect(() => {
    if (visible && task && !isNew) {
      loadTaskConfig();
      loadTaskStatus();
      loadEtlLogs();
    } else if (visible && isNew && !task) {
      // 新建任务的默认配置
      setConfig({
        task_id: 'etl_',
        description: '',
        sync_type: 'incremental',
        date_field: '',
        script: '',
      });
      setFieldTypes([]);
      setSelectedPrimaryKeys([]);
      setTestResult(null);
      setEtlLogs([]);
      setTaskStatus(null);
    }
  }, [visible, task, isNew]);

  const loadTaskConfig = async () => {
    if (!task) return;
    setConfig({
      task_id: task.task_id,
      description: task.description,
      sync_type: task.sync_type || 'incremental',
      date_field: task.date_field || '',
      script: task.script,
      enabled: task.enabled !== undefined ? task.enabled : true,
    });

    // 加载字段定义
    try {
      const res = await dataApi.getEtlTableSchema(task.task_id);
      const fields = res.data.fields || [];
      setFieldTypes(fields.map((f: any) => ({ name: f.name, type: f.type })));
    } catch (error) {
      console.error('Failed to load ETL table schema:', error);
    }
  };

  const loadTaskStatus = async () => {
    if (!task) return;
    try {
      const res = await dataApi.getEtlTaskStatus(task.task_id);
      setTaskStatus(res.data);
    } catch (error) {
      console.error('Failed to load ETL task status:', error);
    }
  };

  const loadEtlLogs = async () => {
    if (!task) return;
    try {
      const res = await dataApi.getEtlLogs(task.task_id, undefined, undefined, 50);
      setEtlLogs(res.data.logs || []);
    } catch (error) {
      console.error('Failed to load ETL logs:', error);
      setEtlLogs([]);
    }
  };

  const handleTestScript = async () => {
    if (!config.script) {
      Toast.warning('请先输入 ETL 脚本');
      return;
    }

    setTestLoading(true);
    setTestResult(null);

    try {
      const res = await dataApi.testEtlScript(config.script, testDate || undefined);
      const data = res.data;

      // 提取字段类型（使用后端返回的 field_types）
      if (data.field_types && data.field_types.length > 0) {
        setFieldTypes(data.field_types);
      } else if (data.columns && data.columns.length > 0) {
        const fields = data.columns.map((col: string) => ({
          name: col,
          type: 'STRING', // 默认类型
        }));
        setFieldTypes(fields);
      }

      setTestResult({
        status: 'success',
        rows: data.rows || 0,
        preview: data.preview || [],
      });

      Toast.success(`测试通过: ${data.rows || 0} 行`);
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '脚本测试失败';
      setTestResult({
        status: 'error',
        error: errorMsg,
      });
      Toast.error(errorMsg);
    } finally {
      setTestLoading(false);
    }
  };

  const handleFormatScript = () => {
    if (etlEditorRef.current) {
      etlEditorRef.current.getAction('editor.action.formatDocument').run();
    }
  };

  const handleSave = async (confirmSchemaChange = false) => {
    if (!config.task_id || !config.task_id.startsWith('etl_')) {
      Toast.error('任务ID必须以 etl_ 开头');
      return;
    }

    if (!config.script) {
      Toast.error('请输入 ETL 脚本');
      return;
    }

    try {
      // 将 fieldTypes 数组转换为 schema 对象格式
      const schemaObject = fieldTypes.reduce((acc, field) => {
        acc[field.name] = { type: field.type };
        return acc;
      }, {} as Record<string, { type: string }>);

      const saveConfig = {
        task_id: config.task_id,
        description: config.description,
        table_name: config.task_id, // 目标表名 = 任务ID
        script: config.script,
        schema: schemaObject, // 转换为对象格式
        primary_keys: selectedPrimaryKeys,
        sync_type: config.sync_type,
        date_field: config.date_field,
        enabled: config.enabled !== undefined ? config.enabled : true,
        confirm_schema_change: confirmSchemaChange, // 添加确认标志
      };

      if (isNew) {
        const response = await dataApi.createEtlTask(saveConfig);
        Toast.success(`ETL 任务 ${config.task_id} 创建成功`);
        onSave();
        onClose();
      } else {
        const response = await dataApi.updateEtlTask(config.task_id, saveConfig);
        const result = response.data; // axios 响应数据在 data 中

        // 检查响应状态
        if (result.status === 'warning' && result.require_confirmation) {
          // 显示确认对话框
          Modal.confirm({
            title: '表结构已变更',
            content: (
              <div>
                <p>{result.message}</p>
                {result.changes && (
                  <div style={{ marginTop: 12 }}>
                    <p><strong>变更详情：</strong></p>
                    {result.changes.script_changed && <p>• ETL 脚本已修改</p>}
                    {result.changes.primary_keys_changed && <p>• 主键配置已修改</p>}
                    {result.changes.old_columns && result.changes.new_columns && (
                      <>
                        <p>• 旧字段: {result.changes.old_columns.join(', ')}</p>
                        <p>• 新字段: {result.changes.new_columns.join(', ')}</p>
                        {result.changes.removed && result.changes.removed.length > 0 && (
                          <p style={{ color: 'red' }}>• 删除字段: {result.changes.removed.join(', ')}</p>
                        )}
                        {result.changes.added && result.changes.added.length > 0 && (
                          <p style={{ color: 'green' }}>• 新增字段: {result.changes.added.join(', ')}</p>
                        )}
                      </>
                    )}
                    <p style={{ marginTop: 12, color: 'orange' }}>
                      <strong>警告：</strong>确认后将删除表 {result.table_name} 的所有历史数据！
                    </p>
                  </div>
                )}
              </div>
            ),
            okText: '确认并清空数据',
            cancelText: '取消',
            okType: 'danger',
            onOk: () => {
              // 用户确认，重新保存并带上确认标志
              handleSave(true);
            },
          });
        } else if (result.status === 'success') {
          Toast.success(`ETL 任务 ${config.task_id} 更新成功`);
          onSave();
          onClose();
        }
      }
    } catch (error: any) {
      Toast.error(error.response?.data?.detail || '保存配置失败');
    }
  };

  return (
    <SideSheet
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span>
            {isNew ? '新建 ETL 任务' : `编辑 ETL 任务: ${config.task_id}`}
          </span>
        </div>
      }
      visible={visible}
      onCancel={() => {
        onClose();
        setTestResult(null);
      }}
      width={720}
      footer={
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <Button
            onClick={() => {
              onClose();
              setTestResult(null);
            }}
          >
            取消
          </Button>
          <Button theme="solid" type="primary" onClick={() => handleSave(false)}>
            保存
          </Button>
        </div>
      }
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: '8px 0' }}>
        {/* 状态信息栏 - 仅编辑模式显示 */}
        {!isNew && taskStatus && (
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 16,
              padding: '12px',
              background: 'var(--semi-color-fill-0)',
              borderRadius: '6px',
            }}
          >
            <div>
              <div style={{ color: 'var(--semi-color-text-2)', fontSize: '12px', marginBottom: 4 }}>
                最新数据
              </div>
              <span style={{ color: 'var(--semi-color-success)', fontSize: '13px', fontWeight: 500 }}>
                {taskStatus.last_date || '-'}
              </span>
            </div>
            <div>
              <div style={{ color: 'var(--semi-color-text-2)', fontSize: '12px', marginBottom: 4 }}>
                上次同步
              </div>
              <span style={{ color: 'var(--semi-color-text-2)', fontSize: '13px' }}>
                {taskStatus.last_sync_time ? new Date(taskStatus.last_sync_time).toLocaleString() : '-'}
              </span>
            </div>
            {selectedPrimaryKeys.length > 0 && (
              <div>
                <div style={{ color: 'var(--semi-color-text-2)', fontSize: '12px', marginBottom: 4 }}>
                  主键
                </div>
                <span style={{ fontSize: '13px' }}>
                  {selectedPrimaryKeys.map((k) => (
                    <Tag key={k} size="small" style={{ marginRight: 4 }}>
                      {k}
                    </Tag>
                  ))}
                </span>
              </div>
            )}
          </div>
        )}

        <Tabs activeKey={activeTab} onChange={setActiveTab} size="small">
          {/* 配置标签页 */}
          <TabPane tab="配置" itemKey="config">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12, paddingTop: 8 }}>
              {/* 基本信息 */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                    任务 ID
                  </div>
                  <Input
                    size="small"
                    prefix="etl_"
                    value={(config.task_id || '').replace(/^etl_/, '')}
                    disabled={!isNew}
                    onChange={(v) => setConfig({ ...config, task_id: `etl_${v}` })}
                  />
                </div>
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                    目标表名（= 任务ID）
                  </div>
                  <Input size="small" value={config.task_id} disabled />
                </div>
              </div>

              <div>
                <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                  描述
                </div>
                <Input
                  size="small"
                  value={config.description}
                  onChange={(v) => setConfig({ ...config, description: v })}
                />
              </div>

              {/* 同步类型 */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                    同步类型
                  </div>
                  <RadioGroup
                    value={config.sync_type || 'incremental'}
                    onChange={(e: any) => setConfig({ ...config, sync_type: e.target.value })}
                  >
                    <Radio value="incremental">增量</Radio>
                    <Radio value="full">全量</Radio>
                  </RadioGroup>
                </div>
                {config.sync_type === 'incremental' && (
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                      日期字段
                    </div>
                    <Input
                      size="small"
                      value={config.date_field}
                      placeholder="如 trade_date"
                      onChange={(v) => setConfig({ ...config, date_field: v })}
                    />
                  </div>
                )}
              </div>

              {/* ETL 脚本 */}
              <div>
                <div
                  style={{
                    marginBottom: 4,
                    fontSize: '12px',
                    color: 'var(--semi-color-text-2)',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                  }}
                >
                  <div>
                    DolphinDB ETL 脚本
                    <span style={{ marginLeft: 8, color: 'var(--semi-color-text-3)', fontSize: '11px' }}>
                      使用 {'{date}'} 变量表示执行日期，格式为 YYYY.MM.DD
                    </span>
                  </div>
                  <Button size="small" icon={<IconCode />} onClick={handleFormatScript}>
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
                    height="300px"
                    language="sql"
                    theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
                    value={config.script}
                    onChange={(v) => {
                      setConfig({ ...config, script: v || '' });
                      setTestResult(null);
                    }}
                    onMount={(editor) => {
                      etlEditorRef.current = editor;
                    }}
                    options={{
                      minimap: { enabled: false },
                      fontSize: 13,
                      scrollBeyondLastLine: false,
                      automaticLayout: true,
                      tabSize: 4,
                      wordWrap: 'on',
                    }}
                  />
                </div>
                <div style={{ marginTop: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
                  {config.sync_type === 'incremental' && (
                    <DatePicker
                      size="small"
                      placeholder="测试日期（可选）"
                      style={{ width: 160 }}
                      onChange={(date: any) => setTestDate(date ? dayjs(date).format('YYYYMMDD') : '')}
                    />
                  )}
                  <Button size="small" theme="light" onClick={handleTestScript} loading={testLoading}>
                    测试脚本
                  </Button>
                  {testResult && testResult.status === 'success' && (
                    <span style={{ fontSize: '12px', color: 'var(--semi-color-success)' }}>
                      测试通过: {testResult.rows} 行
                    </span>
                  )}
                  {testResult && testResult.status === 'error' && (
                    <span style={{ fontSize: '12px', color: 'var(--semi-color-danger)' }}>
                      {testResult.error}
                    </span>
                  )}
                </div>
              </div>

              {/* 字段定义 */}
              {fieldTypes.length > 0 && (
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                    字段定义（勾选主键，可修改类型）
                  </div>
                  <Table
                    dataSource={fieldTypes}
                    rowKey="name"
                    size="small"
                    pagination={false}
                    rowSelection={{
                      selectedRowKeys: selectedPrimaryKeys,
                      onChange: (keys) => setSelectedPrimaryKeys(keys as string[]),
                    }}
                    columns={[
                      {
                        title: '字段名',
                        dataIndex: 'name',
                        key: 'name',
                        width: 180,
                        render: (v: string) => (
                          <code style={{ fontSize: '12px', color: 'var(--semi-color-primary)' }}>{v}</code>
                        ),
                      },
                      {
                        title: '类型',
                        dataIndex: 'type',
                        key: 'type',
                        render: (v: string, record: FieldType) => (
                          <Select
                            size="small"
                            value={v}
                            style={{ width: 130 }}
                            onChange={(val) =>
                              setFieldTypes((prev) =>
                                prev.map((f) => (f.name === record.name ? { ...f, type: val as string } : f))
                              )
                            }
                            optionList={[
                              { label: 'STRING', value: 'STRING' },
                              { label: 'SYMBOL', value: 'SYMBOL' },
                              { label: 'INT', value: 'INT' },
                              { label: 'LONG', value: 'LONG' },
                              { label: 'SHORT', value: 'SHORT' },
                              { label: 'DOUBLE', value: 'DOUBLE' },
                              { label: 'FLOAT', value: 'FLOAT' },
                              { label: 'DATE', value: 'DATE' },
                              { label: 'TIMESTAMP', value: 'TIMESTAMP' },
                              { label: 'DATETIME', value: 'DATETIME' },
                              { label: 'BOOL', value: 'BOOL' },
                            ]}
                          />
                        ),
                      },
                    ]}
                  />
                  {selectedPrimaryKeys.length > 0 && (
                    <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--semi-color-text-3)' }}>
                      主键: {selectedPrimaryKeys.join(', ')}
                    </div>
                  )}
                </div>
              )}

              {/* 测试结果预览 */}
              {testResult?.preview && testResult.preview.length > 0 && (
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--semi-color-text-2)' }}>
                    数据预览（前 10 行）
                  </div>
                  <div
                    style={{
                      maxHeight: 300,
                      overflow: 'auto',
                      border: '1px solid var(--semi-color-border)',
                      borderRadius: 4,
                    }}
                  >
                    <Table
                      dataSource={testResult.preview.slice(0, 10)}
                      columns={Object.keys(testResult.preview[0] || {}).map((key) => ({
                        title: key,
                        dataIndex: key,
                        key,
                        width: 120,
                        render: (v: any) => (
                          <span style={{ fontSize: '12px' }}>{v !== null && v !== undefined ? String(v) : '-'}</span>
                        ),
                      }))}
                      size="small"
                      pagination={false}
                    />
                  </div>
                </div>
              )}
            </div>
          </TabPane>

          {/* 历史记录标签页 */}
          {!isNew && (
            <TabPane tab="历史记录" itemKey="history">
              <div style={{ paddingTop: 8 }}>
                <Table
                  dataSource={etlLogs}
                  rowKey={(record: any) => `${record.sync_date}-${record.created_at}`}
                  size="small"
                  pagination={{ pageSize: 10 }}
                  empty={
                    <div style={{ padding: 20, textAlign: 'center', color: 'var(--semi-color-text-3)' }}>
                      暂无执行记录
                    </div>
                  }
                  columns={[
                    { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 100 },
                    {
                      title: '参数',
                      dataIndex: 'params',
                      key: 'params',
                      width: 180,
                      render: (v: string) =>
                        v ? (
                          <code style={{ fontSize: '11px', color: 'var(--semi-color-text-2)' }}>{v}</code>
                        ) : (
                          '-'
                        ),
                    },
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
    </SideSheet>
  );
};
