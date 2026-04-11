import { notify } from '../../../utils/notify';
/**
 * ETL 任务配置抽屉 - 组装层
 */
import React, { useState, useEffect } from 'react';
import { Tabs, Modal } from 'antd';
import { useMessage } from '../../../hooks/useMessage';
import { dataApi } from '../../../api';
import type { ETLTask } from '../../../types';
import { BaseTaskDrawer } from '../../../components/TaskDrawer/BaseTaskDrawer';
import { UniversalJsonEditorTab } from '../../../components/TaskDrawer/tabs/UniversalJsonEditorTab';
import { useTaskLogs } from '../../../hooks/useTaskLogs';
import { TaskLogTable } from '../../../components/TaskLogTable';
import { ETLVisualEditorTab } from './VisualEditorTab';
import { ETLScriptTestTab } from './ScriptTestTab';

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

interface FieldDiff {
  name: string;
  currentType?: string;
  newType?: string;
  status: 'added' | 'removed' | 'modified' | 'unchanged';
}

interface TestResult {
  status: 'success' | 'error';
  rows?: number;
  preview?: any[];
  error?: string;
}

const DEFAULT_CONFIG = {
  task_id: 'etl_',
  description: '',
  sync_type: 'incremental',
  date_field: '',
  script: '',
};

export const ETLTaskDrawer: React.FC<ETLTaskDrawerProps> = ({
  visible,
  task,
  isNew,
  onClose,
  onSave,
}) => {
  const message = useMessage();
  const [activeTab, setActiveTab] = useState('config');
  const [config, setConfig] = useState<any>(DEFAULT_CONFIG);
  const [tableExists, setTableExists] = useState(false);
  const [currentFields, setCurrentFields] = useState<FieldType[]>([]);
  const [testFields, setTestFields] = useState<FieldType[]>([]);
  const [fieldDiffs, setFieldDiffs] = useState<FieldDiff[]>([]);
  const [selectedPrimaryKeys, setSelectedPrimaryKeys] = useState<string[]>([]);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [testLoading, setTestLoading] = useState(false);
  const [testDate, setTestDate] = useState('');

  const { logs: etlLogs, loading: etlLogsLoading, loadLogs: loadEtlLogs } = useTaskLogs('etl', 50);

  useEffect(() => {
    if (visible && task && !isNew) {
      loadTaskConfig();
    } else if (visible && isNew && !task) {
      setConfig(DEFAULT_CONFIG);
      setTableExists(false);
      setCurrentFields([]);
      setTestFields([]);
      setFieldDiffs([]);
      setSelectedPrimaryKeys([]);
      setTestResult(null);
    }
  }, [visible, task, isNew]);

  useEffect(() => {
    if (activeTab === 'history' && config.task_id && !isNew) {
      loadEtlLogs({ taskId: config.task_id });
    }
  }, [activeTab, config.task_id, isNew]);

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
    try {
      const res = await dataApi.getEtlTableSchema(task.task_id);
      const exists = res.data.table_exists || false;
      const fields = res.data.schema || [];
      setTableExists(exists);
      setCurrentFields(exists ? fields.map((f: any) => ({ name: f.name, type: f.type })) : []);
      setTestFields([]);
      setFieldDiffs([]);
    } catch (error) {
      console.error('Failed to load ETL table schema:', error);
      setTableExists(false);
      setCurrentFields([]);
    }
  };

  const calculateFieldDiffs = (current: FieldType[], test: FieldType[]): FieldDiff[] => {
    const currentMap = new Map(current.map((f) => [f.name, f.type]));
    const testMap = new Map(test.map((f) => [f.name, f.type]));
    const allFields = new Set([...currentMap.keys(), ...testMap.keys()]);
    const diffs: FieldDiff[] = [];
    allFields.forEach((name) => {
      const currentType = currentMap.get(name);
      const newType = testMap.get(name);
      if (currentType && newType) {
        diffs.push({ name, currentType, newType, status: currentType === newType ? 'unchanged' : 'modified' });
      } else if (currentType && !newType) {
        diffs.push({ name, currentType, status: 'removed' });
      } else if (!currentType && newType) {
        diffs.push({ name, newType, status: 'added' });
      }
    });
    return diffs;
  };

  const handleTestScript = async () => {
    if (!config.script) {
      notify.warning('请先输入 ETL 脚本');
      return;
    }
    setTestLoading(true);
    setTestResult(null);
    try {
      const res = await dataApi.testEtlScript(config.script, testDate || undefined);
      const data = res.data;
      let newFields: FieldType[] = [];
      if (data.field_types && data.field_types.length > 0) {
        newFields = data.field_types;
      } else if (data.columns && data.columns.length > 0) {
        newFields = data.columns.map((col: string) => ({ name: col, type: 'STRING' }));
      }
      setTestFields(newFields);
      if (tableExists && currentFields.length > 0) {
        setFieldDiffs(calculateFieldDiffs(currentFields, newFields));
      } else {
        setFieldDiffs([]);
      }
      setTestResult({ status: 'success', rows: data.rows || 0, preview: data.preview || [] });
      notify.success(`测试通过: ${data.rows || 0} 行`);
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || '脚本测试失败';
      setTestResult({ status: 'error', error: errorMsg });
      notify.error(errorMsg);
    } finally {
      setTestLoading(false);
    }
  };

  const handleSave = async (confirmSchemaChange = false) => {
    if (!config.task_id || !config.task_id.startsWith('etl_')) {
      notify.error('任务ID必须以 etl_ 开头');
      return;
    }
    if (!config.script) {
      notify.error('请输入 ETL 脚本');
      return;
    }
    try {
      const fieldsToSave = testFields.length > 0 ? testFields : currentFields;
      const schemaObject = fieldsToSave.reduce((acc, field) => {
        acc[field.name] = { type: field.type };
        return acc;
      }, {} as Record<string, { type: string }>);

      const saveConfig = {
        task_id: config.task_id,
        description: config.description,
        table_name: config.task_id,
        script: config.script,
        schema: schemaObject,
        primary_keys: selectedPrimaryKeys,
        sync_type: config.sync_type,
        date_field: config.date_field,
        enabled: config.enabled !== undefined ? config.enabled : true,
        confirm_schema_change: confirmSchemaChange,
      };

      if (isNew) {
        await dataApi.createEtlTask(saveConfig);
        notify.success(`ETL 任务 ${config.task_id} 创建成功`);
        onSave();
        onClose();
      } else {
        const response = await dataApi.updateEtlTask(config.task_id, saveConfig);
        const result = response.data;
        if (result.status === 'warning' && result.require_confirmation) {
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
                        {result.changes.removed?.length > 0 && <p style={{ color: 'red' }}>• 删除字段: {result.changes.removed.join(', ')}</p>}
                        {result.changes.added?.length > 0 && <p style={{ color: 'green' }}>• 新增字段: {result.changes.added.join(', ')}</p>}
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
            onOk: () => handleSave(true),
          });
        } else if (result.status === 'success') {
          notify.success(`ETL 任务 ${config.task_id} 更新成功`);
          onSave();
          onClose();
        }
      }
    } catch (error: any) {
      notify.error(error.response?.data?.detail || '保存配置失败');
    }
  };

  const handleClose = () => {
    onClose();
    setTestResult(null);
  };

  const drawerTitle = isNew ? '新建 ETL 任务' : `编辑 ETL 任务: ${config.task_id}`;

  const tabItems = [
    {
      key: 'config',
      label: '配置',
      children: (
        <ETLVisualEditorTab
          config={config}
          onChange={setConfig}
          isNew={isNew}
        />
      ),
    },
    {
      key: 'script',
      label: '脚本测试',
      children: (
        <ETLScriptTestTab
          taskId={config.task_id}
          script={config.script || ''}
          onScriptChange={(script) => setConfig({ ...config, script })}
          syncType={config.sync_type}
          tableExists={tableExists}
          currentFields={currentFields}
          fields={testFields}
          onFieldsChange={setTestFields}
          fieldDiffs={fieldDiffs}
          onFieldDiffsChange={setFieldDiffs}
          selectedPrimaryKeys={selectedPrimaryKeys}
          onPrimaryKeysChange={setSelectedPrimaryKeys}
          testResult={testResult}
          testLoading={testLoading}
          testDate={testDate}
          onTestDateChange={setTestDate}
          onTestScript={handleTestScript}
        />
      ),
    },
    {
      key: 'json',
      label: 'JSON编辑',
      children: (
        <UniversalJsonEditorTab
          value={config}
          onChange={(text) => {
            try {
              setConfig(JSON.parse(text));
            } catch (_) {}
          }}
        />
      ),
    },
    ...(isNew ? [] : [{
      key: 'history',
      label: '历史记录',
      children: (
        <TaskLogTable
          logs={etlLogs}
          loading={etlLogsLoading}
          taskIdLabel="任务ID"
          onFilter={(f) => loadEtlLogs({ ...f, taskId: f.taskId || config.task_id })}
        />
      ),
    }]),
  ];

  return (
    <BaseTaskDrawer
      visible={visible}
      title={drawerTitle}
      onClose={handleClose}
      onSave={() => handleSave(false)}
      width={720}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: '8px 0' }}>
        <Tabs activeKey={activeTab} onChange={setActiveTab} items={tabItems} />
      </div>
    </BaseTaskDrawer>
  );
};
