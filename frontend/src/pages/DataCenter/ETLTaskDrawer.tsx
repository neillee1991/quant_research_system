/**
 * ETL 任务配置抽屉组件
 * 匹配原始实现：配置 + 脚本测试 + 字段定义 + 历史记录
 */
import React, { useState, useEffect, useRef } from 'react';
import {
  Drawer,
  Button,
  Tabs,
  Input,
  Radio,
  Table,
  Select,
  Tag,
  Tooltip,
  Modal,
} from 'antd';
import { CodeOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import Editor from '@monaco-editor/react';
import QuantDatePicker from '../../components/QuantDatePicker';
import { dataApi, taskMonitorApi } from '../../api';
import { useThemeStore } from '../../store';
import type { ETLTask } from '../../types';
import { DataInspection } from '../../components/DataInspection';

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

interface FieldDiff {
  name: string;
  currentType?: string;  // 当前表中的类型
  newType?: string;      // 脚本测试产出的类型
  status: 'added' | 'removed' | 'modified' | 'unchanged';
}

export const ETLTaskDrawer: React.FC<ETLTaskDrawerProps> = ({
  visible,
  task,
  isNew,
  onClose,
  onSave,
}) => {
  const { mode } = useThemeStore();
  const message = useMessage();
  const [activeTab, setActiveTab] = useState('config');
  const [config, setConfig] = useState<any>({
    task_id: 'etl_',
    description: '',
    sync_type: 'incremental',
    date_field: '',
    script: '',
  });
  const [taskStatus, setTaskStatus] = useState<any>(null);
  const [tableExists, setTableExists] = useState(false);  // 表是否存在
  const [currentFields, setCurrentFields] = useState<FieldType[]>([]);  // 当前表的字段
  const [testFields, setTestFields] = useState<FieldType[]>([]);  // 测试脚本产出的字段
  const [fieldDiffs, setFieldDiffs] = useState<FieldDiff[]>([]);  // 字段差异
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
      setTableExists(false);
      setCurrentFields([]);
      setTestFields([]);
      setFieldDiffs([]);
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

    // 加载字段定义（检查表是否存在）
    try {
      const res = await dataApi.getEtlTableSchema(task.task_id);
      const exists = res.data.table_exists || false;
      const fields = res.data.schema || [];

      setTableExists(exists);
      if (exists) {
        // 表存在，显示实际表结构
        setCurrentFields(fields.map((f: any) => ({ name: f.name, type: f.type })));
      } else {
        // 表不存在，不显示字段
        setCurrentFields([]);
      }
      setTestFields([]);
      setFieldDiffs([]);
    } catch (error) {
      console.error('Failed to load ETL table schema:', error);
      setTableExists(false);
      setCurrentFields([]);
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
      // 使用统一的 taskMonitorApi 获取任务历史
      const res = await taskMonitorApi.getTaskHistory(50, 'etl', task.task_id);
      // 转换格式以兼容现有代码
      const logs = (res.data.tasks || []).map(taskRun => ({
        source: 'etl',
        data_type: taskRun.task_id,
        last_date: taskRun.params ? JSON.parse(taskRun.params).date || '' : '',
        sync_date: taskRun.params ? JSON.parse(taskRun.params).date || '' : '',
        rows_synced: taskRun.rows || 0,
        status: taskRun.status,
        error_message: taskRun.error || '',
        params: taskRun.params || '',
        created_at: taskRun.started_at || ''
      }));
      setEtlLogs(logs);
    } catch (error) {
      console.error('Failed to load ETL logs:', error);
      setEtlLogs([]);
    }
  };

  const handleTestScript = async () => {
    if (!config.script) {
      message.warning('请先输入 ETL 脚本');
      return;
    }

    setTestLoading(true);
    setTestResult(null);

    try {
      const res = await dataApi.testEtlScript(config.script, testDate || undefined);
      const data = res.data;

      // 提取字段类型（使用后端返回的 field_types）
      let newFields: FieldType[] = [];
      if (data.field_types && data.field_types.length > 0) {
        newFields = data.field_types;
      } else if (data.columns && data.columns.length > 0) {
        newFields = data.columns.map((col: string) => ({
          name: col,
          type: 'STRING', // 默认类型
        }));
      }

      setTestFields(newFields);

      // 如果表已存在，计算字段差异
      if (tableExists && currentFields.length > 0) {
        const diffs = calculateFieldDiffs(currentFields, newFields);
        setFieldDiffs(diffs);
      } else {
        // 表不存在，直接使用测试结果
        setFieldDiffs([]);
      }

      setTestResult({
        status: 'success',
        rows: data.rows || 0,
        preview: data.preview || [],
      });

      message.success(`测试通过: ${data.rows || 0} 行`);
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '脚本测试失败';
      setTestResult({
        status: 'error',
        error: errorMsg,
      });
      message.error(errorMsg);
    } finally {
      setTestLoading(false);
    }
  };

  // 计算字段差异
  const calculateFieldDiffs = (current: FieldType[], test: FieldType[]): FieldDiff[] => {
    const diffs: FieldDiff[] = [];
    const currentMap = new Map(current.map(f => [f.name, f.type]));
    const testMap = new Map(test.map(f => [f.name, f.type]));

    // 检查所有字段
    const allFields = new Set([...currentMap.keys(), ...testMap.keys()]);

    allFields.forEach(name => {
      const currentType = currentMap.get(name);
      const newType = testMap.get(name);

      if (currentType && newType) {
        if (currentType === newType) {
          diffs.push({ name, currentType, newType, status: 'unchanged' });
        } else {
          diffs.push({ name, currentType, newType, status: 'modified' });
        }
      } else if (currentType && !newType) {
        diffs.push({ name, currentType, status: 'removed' });
      } else if (!currentType && newType) {
        diffs.push({ name, newType, status: 'added' });
      }
    });

    return diffs;
  };

  const handleFormatScript = () => {
    if (etlEditorRef.current) {
      etlEditorRef.current.getAction('editor.action.formatDocument').run();
    }
  };

  const handleSave = async (confirmSchemaChange = false) => {
    if (!config.task_id || !config.task_id.startsWith('etl_')) {
      message.error('任务ID必须以 etl_ 开头');
      return;
    }

    if (!config.script) {
      message.error('请输入 ETL 脚本');
      return;
    }

    try {
      // 确定使用哪个字段列表：优先使用测试结果，否则使用当前表字段
      const fieldsToSave = testFields.length > 0 ? testFields : currentFields;

      // 将字段数组转换为 schema 对象格式
      const schemaObject = fieldsToSave.reduce((acc, field) => {
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
        message.success(`ETL 任务 ${config.task_id} 创建成功`);
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
            okButtonProps: { danger: true },
            onOk: () => {
              // 用户确认，重新保存并带上确认标志
              handleSave(true);
            },
          });
        } else if (result.status === 'success') {
          message.success(`ETL 任务 ${config.task_id} 更新成功`);
          onSave();
          onClose();
        }
      }
    } catch (error: any) {
      message.error(error.response?.data?.detail || '保存配置失败');
    }
  };

  return (
    <>
      <Drawer
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span>
              {isNew ? '新建 ETL 任务' : `编辑 ETL 任务: ${config.task_id}`}
            </span>
          </div>
        }
        open={visible}
        onClose={() => {
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
            <Button type="primary" onClick={() => handleSave(false)}>
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
              background: 'var(--bg-tertiary)',
              borderRadius: '6px',
            }}
          >
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>
                最新数据
              </div>
              <span style={{ color: 'var(--color-gain)', fontSize: '13px', fontWeight: 500 }}>
                {taskStatus.last_date || '-'}
              </span>
            </div>
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>
                上次同步
              </div>
              <span style={{ color: 'var(--text-secondary)', fontSize: '13px' }}>
                {taskStatus.last_sync_time ? new Date(taskStatus.last_sync_time).toLocaleString() : '-'}
              </span>
            </div>
            {selectedPrimaryKeys.length > 0 && (
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '12px', marginBottom: 4 }}>
                  主键
                </div>
                <span style={{ fontSize: '13px' }}>
                  {selectedPrimaryKeys.map((k) => (
                    <Tag key={k} style={{ marginRight: 4 }}>
                      {k}
                    </Tag>
                  ))}
                </span>
              </div>
            )}
          </div>
        )}

        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
         
          items={[
            {
              key: 'config',
              label: '配置',
              children: (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 12, paddingTop: 8 }}>
              {/* 基本信息 */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    任务 ID
                  </div>
                  <Input
                   
                    prefix="etl_"
                    value={(config.task_id || '').replace(/^etl_/, '')}
                    disabled={!isNew}
                    onChange={(e) => setConfig({ ...config, task_id: `etl_${e.target.value}` })}
                  />
                </div>
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    目标表名（= 任务ID）
                  </div>
                  <Input value={config.task_id} disabled />
                </div>
              </div>

              <div>
                <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                  描述
                </div>
                <Input
                 
                  value={config.description}
                  onChange={(e) => setConfig({ ...config, description: e.target.value })}
                />
              </div>

              {/* 同步类型 */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    同步类型
                  </div>
                  <Radio.Group
                    value={config.sync_type || 'incremental'}
                    onChange={(e: any) => setConfig({ ...config, sync_type: e.target.value })}
                  >
                    <Radio value="incremental">增量</Radio>
                    <Radio value="full">全量</Radio>
                  </Radio.Group>
                </div>
                {config.sync_type === 'incremental' && (
                  <div>
                    <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                      日期字段
                    </div>
                    <Input
                     
                      value={config.date_field}
                      placeholder="如 trade_date"
                      onChange={(e) => setConfig({ ...config, date_field: e.target.value })}
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
                    color: 'var(--text-secondary)',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                  }}
                >
                  <div>
                    DolphinDB ETL 脚本
                    <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontSize: '11px' }}>
                      使用 {'{date}'} 变量表示执行日期，格式为 YYYY.MM.DD
                    </span>
                  </div>
                  <Button icon={<CodeOutlined />} onClick={handleFormatScript}>
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
                    <QuantDatePicker
                      mode="single"
                      placeholder="测试日期（可选）"
                      style={{ width: 160 }}
                      disableFuture={false}
                      onChange={(d) => setTestDate(d)}
                    />
                  )}
                  <Button onClick={handleTestScript} loading={testLoading}>
                    测试脚本
                  </Button>
                  {testResult && testResult.status === 'success' && (
                    <span style={{ fontSize: '12px', color: 'var(--color-gain)' }}>
                      测试通过: {testResult.rows} 行
                    </span>
                  )}
                  {testResult && testResult.status === 'error' && (
                    <span style={{ fontSize: '12px', color: 'var(--color-loss)' }}>
                      {testResult.error}
                    </span>
                  )}
                </div>
              </div>

              {/* 字段定义 */}
              {/* 情况1: 表不存在且未测试 - 不显示 */}
              {/* 情况2: 表存在且未测试 - 显示当前表字段 */}
              {tableExists && currentFields.length > 0 && fieldDiffs.length === 0 && (
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    当前表字段定义（勾选主键，可修改类型）
                    <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontSize: '11px' }}>
                      💡 显示数据库实际表结构，修改后需删除表重建
                    </span>
                  </div>
                  <Table
                    dataSource={currentFields}
                    rowKey="name"
                   
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
                          <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
                        ),
                      },
                      {
                        title: '类型',
                        dataIndex: 'type',
                        key: 'type',
                        render: (v: string, record: FieldType) => (
                          <Select
                           
                            value={v}
                            style={{ width: 130 }}
                            onChange={(val) =>
                              setCurrentFields((prev) =>
                                prev.map((f) => (f.name === record.name ? { ...f, type: val as string } : f))
                              )
                            }
                            options={[
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
                    <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>
                      主键: {selectedPrimaryKeys.join(', ')}
                    </div>
                  )}
                </div>
              )}

              {/* 情况3: 表不存在但已测试 - 显示测试结果字段 */}
              {!tableExists && testFields.length > 0 && (
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    脚本测试字段定义（勾选主键，可修改类型）
                    <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontSize: '11px' }}>
                      💡 保存后将作为建表模板，表创建后以实际表结构为准
                    </span>
                  </div>
                  <Table
                    dataSource={testFields}
                    rowKey="name"
                   
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
                          <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
                        ),
                      },
                      {
                        title: '类型',
                        dataIndex: 'type',
                        key: 'type',
                        render: (v: string, record: FieldType) => (
                          <Select
                           
                            value={v}
                            style={{ width: 130 }}
                            onChange={(val) =>
                              setTestFields((prev) =>
                                prev.map((f) => (f.name === record.name ? { ...f, type: val as string } : f))
                              )
                            }
                            options={[
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
                    <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>
                      主键: {selectedPrimaryKeys.join(', ')}
                    </div>
                  )}
                </div>
              )}

              {/* 情况4: 表存在且已测试，显示字段差异对比（合并为单表四列） */}
              {tableExists && fieldDiffs.length > 0 && (
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    字段对比（勾选主键，可修改测试类型）
                  </div>
                  <Table
                    dataSource={fieldDiffs}
                    rowKey="name"
                   
                    pagination={false}
                    rowSelection={{
                      selectedRowKeys: selectedPrimaryKeys,
                      onChange: (keys) => setSelectedPrimaryKeys(keys as string[]),
                      getCheckboxProps: (record: FieldDiff) => ({
                        disabled: record.status === 'removed',
                      }),
                    }}
                    columns={[
                      {
                        title: '字段名',
                        dataIndex: 'name',
                        key: 'name',
                        width: 150,
                        render: (v: string) => (
                          <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
                        ),
                      },
                      {
                        title: '当前类型',
                        dataIndex: 'currentType',
                        key: 'currentType',
                        width: 120,
                        render: (v?: string) => (
                          <span style={{ fontSize: '12px', color: v ? 'inherit' : 'var(--text-secondary)' }}>
                            {v || '-'}
                          </span>
                        ),
                      },
                      {
                        title: '测试类型',
                        dataIndex: 'newType',
                        key: 'newType',
                        width: 150,
                        render: (_v: string | undefined, record: FieldDiff) => {
                          const v = record.newType;
                          if (!v) {
                            return <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>-</span>;
                          }
                          return (
                            <Select
                             
                              value={v}
                              style={{ width: 130 }}
                              onChange={(val) => {
                                setTestFields((prev) =>
                                  prev.map((f) => (f.name === record.name ? { ...f, type: val as string } : f))
                                );
                                setFieldDiffs((prev) =>
                                  prev.map((d) =>
                                    d.name === record.name
                                      ? {
                                          ...d,
                                          newType: val as string,
                                          status:
                                            !d.currentType
                                              ? 'added'
                                              : d.currentType === val
                                              ? 'unchanged'
                                              : 'modified',
                                        }
                                      : d
                                  )
                                );
                              }}
                              options={[
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
                          );
                        },
                      },
                      {
                        title: '状态',
                        dataIndex: 'status',
                        key: 'status',
                        width: 100,
                        render: (status: 'added' | 'removed' | 'modified' | 'unchanged') => {
                          const tagConfig: Record<string, { color: 'green' | 'red' | 'orange' | 'blue'; text: string }> = {
                            added: { color: 'green', text: '新增' },
                            removed: { color: 'red', text: '已删除' },
                            modified: { color: 'orange', text: '类型变更' },
                            unchanged: { color: 'blue', text: '一致' },
                          };
                          const config = tagConfig[status];
                          return <Tag color={config.color}>{config.text}</Tag>;
                        },
                      },
                    ]}
                  />
                  {selectedPrimaryKeys.length > 0 && (
                    <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>
                      主键: {selectedPrimaryKeys.join(', ')}
                    </div>
                  )}
                </div>
              )}

              {/* 测试结果预览 */}
              {testResult?.preview && testResult.preview.length > 0 && (
                <div>
                  <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
                    数据预览（前 10 行）
                  </div>
                  <div
                    style={{
                      maxHeight: 300,
                      overflow: 'auto',
                      border: '1px solid var(--border-color)',
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
                     
                      pagination={false}
                    />
                  </div>
                </div>
              )}
            </div>
          ),
        },
        ...(!isNew ? [{
          key: 'history',
          label: '历史记录',
          children: (
            <div style={{ paddingTop: 8 }}>
              {/* 数据探查组件 - 仅增量任务显示 */}
              {task && <DataInspection taskType="etl" taskId={task.task_id} syncType={task.sync_type} />}

              <Table
                dataSource={etlLogs}
                rowKey={(record: any) => `${record.sync_date}-${record.created_at}`}
               
                pagination={{ pageSize: 10 }}
                locale={{
                  emptyText: (
                    <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)' }}>
                      暂无执行记录
                    </div>
                  ),
                }}
                columns={[
                  { title: '同步日期', dataIndex: 'sync_date', key: 'sync_date', width: 100 },
                  {
                    title: '参数',
                    dataIndex: 'params',
                    key: 'params',
                    width: 180,
                    render: (v: string) =>
                      v ? (
                        <code style={{ fontSize: '11px', color: 'var(--text-secondary)' }}>{v}</code>
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
          ]}
        />
      </div>
    </Drawer>
    </>
  );
};
