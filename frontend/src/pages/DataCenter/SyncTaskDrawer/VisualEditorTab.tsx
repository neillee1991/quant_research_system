/**
 * 同步任务可视化编辑 Tab
 */
import React from 'react';
import { Input, Select, Table, Collapse, Checkbox, Button } from 'antd';
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons';

interface SyncVisualEditorTabProps {
  config: any;
  onChange: (config: any) => void;
  isNew: boolean;
}

const SCHEMA_TYPES = [
  'BOOL','CHAR','SHORT','INT','LONG','FLOAT','DOUBLE',
  'DATE','MONTH','TIME','MINUTE','SECOND','DATETIME',
  'TIMESTAMP','NANOTIME','NANOTIMESTAMP','SYMBOL','STRING','UUID','BLOB',
].map(t => ({ value: t, label: t }));

export const SyncVisualEditorTab: React.FC<SyncVisualEditorTabProps> = ({
  config,
  onChange,
  isNew,
}) => {
  const updateField = (key: string, value: any) => {
    onChange({ ...config, [key]: value });
  };

  const updateSchemaField = (fieldName: string, prop: string, value: any) => {
    const newSchema = { ...(config.schema || {}) };
    newSchema[fieldName] = { ...(newSchema[fieldName] || {}), [prop]: value };
    updateField('schema', newSchema);
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
    updateField('schema', newSchema);
  };

  const handleDeleteSchemaField = (fieldName: string) => {
    const newSchema = { ...(config.schema || {}) };
    delete newSchema[fieldName];
    updateField('schema', newSchema);
  };

  const updateParamsField = (key: string, value: string) => {
    updateField('params', { ...(config.params || {}), [key]: value });
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingTop: 8 }}>
      {/* 基本信息 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>任务ID</div>
          <Input
            size="middle"
            prefix="sync_"
            value={(config.task_id || '').replace(/^sync_/, '')}
            onChange={(e) => updateField('task_id', `sync_${e.target.value}`)}
            disabled={!isNew}
            style={!isNew ? { background: 'var(--bg-tertiary)' } : undefined}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>API名称</div>
          <Input
            size="middle"
            value={config.api_name || ''}
            onChange={(e) => updateField('api_name', e.target.value)}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>描述</div>
          <Input
            size="middle"
            value={config.description || ''}
            onChange={(e) => updateField('description', e.target.value)}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>数据表</div>
          <Input
            size="middle"
            prefix="sync_"
            value={(config.table_name || '').replace(/^sync_/, '')}
            onChange={(e) => updateField('table_name', `sync_${e.target.value}`)}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>同步类型</div>
          <Select
            size="middle"
            value={config.sync_type}
            onChange={(v) => updateField('sync_type', v)}
            style={{ width: '100%' }}
            options={[
              { value: 'incremental', label: '增量' },
              { value: 'full', label: '全量' },
            ]}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>来源</div>
          <Input
            size="middle"
            value={config.source || 'tushare'}
            onChange={(e) => updateField('source', e.target.value)}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>日期字段</div>
          <Input
            size="middle"
            value={config.date_field || ''}
            onChange={(e) => updateField('date_field', e.target.value)}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>API限制</div>
          <Input
            size="middle"
            type="number"
            value={config.api_limit || ''}
            onChange={(e) => updateField('api_limit', parseInt(e.target.value) || 0)}
          />
        </div>
        <div style={{ gridColumn: 'span 2' }}>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>主键（逗号分隔）</div>
          <Input
            size="middle"
            value={config.primary_keys?.join(', ') || ''}
            onChange={(e) =>
              updateField(
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
                <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>{key}</div>
                <Input
                  size="middle"
                  value={String(value)}
                  onChange={(e) => updateParamsField(key, e.target.value)}
                />
              </div>
            ))}
          </div>
        ) : (
          <div style={{ padding: 12, color: 'var(--text-secondary)', fontSize: '12px' }}>暂无 API 参数配置</div>
        ),
      }]} />

      {/* Schema 字段表格 */}
      {config.schema && (
        <Collapse defaultActiveKey={['schema']} items={[{
          key: 'schema',
          label: <span style={{ fontSize: '13px', fontWeight: 500 }}>字段定义 (Schema)</span>,
          children: (
            <>
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
                          const rebuilt: Record<string, any> = {};
                          Object.entries(newSchema).forEach(([key, props]) => {
                            rebuilt[key === v ? val : key] = props;
                          });
                          updateField('schema', rebuilt);
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
                        options={SCHEMA_TYPES}
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
                        onChange={(e: any) => updateSchemaField(r.name, 'nullable', e.target.checked)}
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
            </>
          ),
        }]} />
      )}
    </div>
  );
};
