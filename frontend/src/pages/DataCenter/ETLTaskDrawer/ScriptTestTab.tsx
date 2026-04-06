/**
 * ETL 脚本测试 Tab
 * 包含脚本编辑器、测试运行、预览结果、字段差异对比
 */
import React, { useRef } from 'react';
import { Button, Table, Select, Tag } from 'antd';
import { CodeOutlined } from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import QuantDatePicker from '../../../components/QuantDatePicker';
import { useThemeStore } from '../../../store';

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

export interface ETLScriptTestTabProps {
  taskId: string;
  script: string;
  onScriptChange: (script: string) => void;
  syncType?: string;
  tableExists: boolean;
  currentFields: FieldType[];
  fields: FieldType[];
  onFieldsChange: (fields: FieldType[]) => void;
  fieldDiffs: FieldDiff[];
  onFieldDiffsChange: (diffs: FieldDiff[]) => void;
  selectedPrimaryKeys: string[];
  onPrimaryKeysChange: (keys: string[]) => void;
  testResult: TestResult | null;
  testLoading: boolean;
  testDate: string;
  onTestDateChange: (date: string) => void;
  onTestScript: () => void;
}

const TYPE_OPTIONS = [
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
];

export const ETLScriptTestTab: React.FC<ETLScriptTestTabProps> = ({
  script,
  onScriptChange,
  syncType,
  tableExists,
  currentFields,
  fields,
  onFieldsChange,
  fieldDiffs,
  onFieldDiffsChange,
  selectedPrimaryKeys,
  onPrimaryKeysChange,
  testResult,
  testLoading,
  testDate,
  onTestDateChange,
  onTestScript,
}) => {
  const { mode } = useThemeStore();
  const editorRef = useRef<any>(null);

  const handleFormatScript = () => {
    if (editorRef.current) {
      editorRef.current.getAction('editor.action.formatDocument').run();
    }
  };

  const fieldNameCol = {
    title: '字段名',
    dataIndex: 'name',
    key: 'name',
    width: 180,
    render: (v: string) => (
      <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code>
    ),
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, paddingTop: 8 }}>
      {/* 脚本编辑器 */}
      <div>
        <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            DolphinDB ETL 脚本
            <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontSize: '11px' }}>
              使用 {'{date}'} 变量表示执行日期，格式为 YYYY.MM.DD
            </span>
          </div>
          <Button icon={<CodeOutlined />} onClick={handleFormatScript}>格式化</Button>
        </div>
        <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
          <Editor
            height="300px"
            language="sql"
            theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
            value={script}
            onChange={(v) => onScriptChange(v || '')}
            onMount={(editor) => { editorRef.current = editor; }}
            options={{ minimap: { enabled: false }, fontSize: 13, scrollBeyondLastLine: false, automaticLayout: true, tabSize: 4, wordWrap: 'on' }}
          />
        </div>
        <div style={{ marginTop: 8, display: 'flex', gap: 8, alignItems: 'center' }}>
          {syncType === 'incremental' && (
            <QuantDatePicker
              mode="single"
              placeholder="测试日期（可选）"
              style={{ width: 160 }}
              disableFuture={false}
              onChange={(d) => onTestDateChange(d)}
            />
          )}
          <Button onClick={onTestScript} loading={testLoading}>测试脚本</Button>
          {testResult?.status === 'success' && (
            <span style={{ fontSize: '12px', color: 'var(--color-gain)' }}>测试通过: {testResult.rows} 行</span>
          )}
          {testResult?.status === 'error' && (
            <span style={{ fontSize: '12px', color: 'var(--color-loss)' }}>{testResult.error}</span>
          )}
        </div>
      </div>

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
            size="small"
            pagination={false}
            rowSelection={{ selectedRowKeys: selectedPrimaryKeys, onChange: (keys) => onPrimaryKeysChange(keys as string[]) }}
            columns={[
              fieldNameCol,
              {
                title: '类型', dataIndex: 'type', key: 'type',
                render: (v: string, record: FieldType) => (
                  <Select value={v} style={{ width: 130 }} options={TYPE_OPTIONS}
                    onChange={(val) => onFieldsChange(currentFields.map((f) => f.name === record.name ? { ...f, type: val } : f))}
                  />
                ),
              },
            ]}
          />
          {selectedPrimaryKeys.length > 0 && (
            <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>主键: {selectedPrimaryKeys.join(', ')}</div>
          )}
        </div>
      )}

      {/* 情况3: 表不存在但已测试 - 显示测试结果字段 */}
      {!tableExists && fields.length > 0 && (
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>
            脚本测试字段定义（勾选主键，可修改类型）
            <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontSize: '11px' }}>
              💡 保存后将作为建表模板，表创建后以实际表结构为准
            </span>
          </div>
          <Table
            dataSource={fields}
            rowKey="name"
            size="small"
            pagination={false}
            rowSelection={{ selectedRowKeys: selectedPrimaryKeys, onChange: (keys) => onPrimaryKeysChange(keys as string[]) }}
            columns={[
              fieldNameCol,
              {
                title: '类型', dataIndex: 'type', key: 'type',
                render: (v: string, record: FieldType) => (
                  <Select value={v} style={{ width: 130 }} options={TYPE_OPTIONS}
                    onChange={(val) => onFieldsChange(fields.map((f) => f.name === record.name ? { ...f, type: val } : f))}
                  />
                ),
              },
            ]}
          />
          {selectedPrimaryKeys.length > 0 && (
            <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>主键: {selectedPrimaryKeys.join(', ')}</div>
          )}
        </div>
      )}

      {/* 情况4: 表存在且已测试，字段差异对比 */}
      {tableExists && fieldDiffs.length > 0 && (
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>字段对比（勾选主键，可修改测试类型）</div>
          <Table
            dataSource={fieldDiffs}
            rowKey="name"
            size="small"
            pagination={false}
            rowSelection={{
              selectedRowKeys: selectedPrimaryKeys,
              onChange: (keys) => onPrimaryKeysChange(keys as string[]),
              getCheckboxProps: (record: FieldDiff) => ({ disabled: record.status === 'removed' }),
            }}
            columns={[
              { title: '字段名', dataIndex: 'name', key: 'name', width: 150, render: (v: string) => <code style={{ fontSize: '12px', color: 'var(--color-primary)' }}>{v}</code> },
              { title: '当前类型', dataIndex: 'currentType', key: 'currentType', width: 120, render: (v?: string) => <span style={{ fontSize: '12px', color: v ? 'inherit' : 'var(--text-secondary)' }}>{v || '-'}</span> },
              {
                title: '测试类型', dataIndex: 'newType', key: 'newType', width: 150,
                render: (_v: string | undefined, record: FieldDiff) => {
                  if (!record.newType) return <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>-</span>;
                  return (
                    <Select value={record.newType} style={{ width: 130 }} options={TYPE_OPTIONS}
                      onChange={(val) => {
                        onFieldsChange(fields.map((f) => f.name === record.name ? { ...f, type: val } : f));
                        onFieldDiffsChange(fieldDiffs.map((d) => d.name === record.name
                          ? { ...d, newType: val, status: !d.currentType ? 'added' : d.currentType === val ? 'unchanged' : 'modified' }
                          : d
                        ));
                      }}
                    />
                  );
                },
              },
              {
                title: '状态', dataIndex: 'status', key: 'status', width: 100,
                render: (status: 'added' | 'removed' | 'modified' | 'unchanged') => {
                  const tagConfig: Record<string, { color: 'green' | 'red' | 'orange' | 'blue'; text: string }> = {
                    added: { color: 'green', text: '新增' },
                    removed: { color: 'red', text: '已删除' },
                    modified: { color: 'orange', text: '类型变更' },
                    unchanged: { color: 'blue', text: '一致' },
                  };
                  const cfg = tagConfig[status];
                  return <Tag color={cfg.color}>{cfg.text}</Tag>;
                },
              },
            ]}
          />
          {selectedPrimaryKeys.length > 0 && (
            <div style={{ marginTop: 4, fontSize: '11px', color: 'var(--text-muted)' }}>主键: {selectedPrimaryKeys.join(', ')}</div>
          )}
        </div>
      )}

      {/* 测试结果预览 */}
      {testResult?.preview && testResult.preview.length > 0 && (
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>数据预览（前 10 行）</div>
          <div style={{ maxHeight: 300, overflow: 'auto', border: '1px solid var(--border-color)', borderRadius: 4 }}>
            <Table
              dataSource={testResult.preview.slice(0, 10)}
              columns={Object.keys(testResult.preview[0] || {}).map((key) => ({
                title: key, dataIndex: key, key, width: 120,
                render: (v: any) => <span style={{ fontSize: '12px' }}>{v !== null && v !== undefined ? String(v) : '-'}</span>,
              }))}
              size="small"
              pagination={false}
            />
          </div>
        </div>
      )}
    </div>
  );
};
