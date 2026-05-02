/**
 * 字段映射表组件
 * 通用的字段映射显示和配置组件
 */
import React from 'react';
import { Button, Spin, Select, Tag } from 'antd';
import { ReloadOutlined, SaveOutlined } from '@ant-design/icons';
import type { DataFieldMapping } from '../../../types';

interface FieldMappingTableProps {
  title: string;
  description: string;
  mappings: DataFieldMapping[];
  loading: boolean;
  saving: boolean;
  tables: string[];
  tableColumns: Record<string, string[]>;
  changed: boolean;
  onLoadConfig: () => void;
  onLoadColumnsForTable: (tableName: string) => void;
  onUpdateMapping: (idx: number, field: Partial<DataFieldMapping>) => void;
  onSaveConfig: () => void;
}

export const FieldMappingTable: React.FC<FieldMappingTableProps> = ({
  title,
  description,
  mappings,
  loading,
  saving,
  tables,
  tableColumns,
  changed,
  onLoadConfig,
  onLoadColumnsForTable,
  onUpdateMapping,
  onSaveConfig,
}) => {
  // 渲染使用模块的标签
  const renderUsedByTags = (usedBy: string[] = []) => {
    return usedBy.map(module => {
      let color = 'default';
      let text = '未指定';

      if (module === 'factor') {
        color = 'blue';
        text = '因子分析';
      } else if (module === 'backtest') {
        color = 'orange';
        text = '回测';
      } else if (module === 'both') {
        color = 'green';
        text = '两者都用';
      }

      return (
        <Tag key={module} color={color} style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>
          {text}
        </Tag>
      );
    });
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>
            {title}
          </span>
          <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
            {description}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Button icon={<ReloadOutlined />} onClick={onLoadConfig}>
            刷新
          </Button>
          <Button
            type="primary"
            icon={<SaveOutlined />}
            disabled={!changed}
            loading={saving}
            onClick={onSaveConfig}
          >
            保存配置
          </Button>
        </div>
      </div>

      <Spin spinning={loading}>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
          {mappings.map((m, idx) => {
            const extra = (() => {
              try { return JSON.parse(m.extra_config || '{}'); } catch { return {}; }
            })();
            const hasTable = !!m.table_name;
            const enumValues: Record<string, string> | undefined = extra.values;

            return (
              <div
                key={m.field_key}
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 8,
                  padding: '10px 14px',
                  background: 'var(--bg-surface)',
                  borderRadius: 6,
                  border: '1px solid var(--border-color)',
                }}
              >
                {/* 第一行: 字段信息 + 枚举标签 + 使用模块 + 状态 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <code style={{ color: 'var(--color-primary)', fontSize: 12, fontWeight: 600 }}>
                    {m.field_key}
                  </code>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>
                    {m.description}
                  </span>
                  {enumValues && (
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {Object.entries(enumValues).map(([k, v]) => (
                        <Tag
                          key={k}
                          style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}
                          color={k === '0' ? 'green' : k === '1' ? 'red' : k === '-1' ? 'blue' : 'grey'}
                        >
                          {k}={v}
                        </Tag>
                      ))}
                    </div>
                  )}
                  {/* 显示使用模块标签 */}
                  <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                    {renderUsedByTags(m.used_by)}
                  </div>
                  <div style={{ marginLeft: 'auto' }}>
                    {hasTable ? (
                      <Tag color="green" style={{ fontSize: 11 }}>
                        {m.table_name}{m.column_name ? `.${m.column_name}` : ''}
                      </Tag>
                    ) : extra.mode ? (
                      <Tag color="blue" style={{ fontSize: 11 }}>{extra.mode}</Tag>
                    ) : (
                      <Tag color="grey" style={{ fontSize: 11 }}>未配置</Tag>
                    )}
                  </div>
                </div>

                {/* 第二行: 数据表 + 列选择器 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11, flexShrink: 0 }}>数据表</span>
                  <Select
                    style={{ width: 240 }}
                    placeholder="选择数据表"
                    allowClear
                    showSearch
                    value={m.table_name || undefined}
                    onChange={(v) => {
                      onUpdateMapping(idx, { table_name: (v as string) || '', column_name: '' });
                      if (v) onLoadColumnsForTable(v as string);
                    }}
                    options={tables.map(t => ({ label: t, value: t }))}
                  />
                  <span style={{ color: 'var(--text-muted)', fontSize: 11, flexShrink: 0 }}>列</span>
                  <Select
                    style={{ width: 200 }}
                    placeholder="选择列"
                    allowClear
                    showSearch
                    value={m.column_name || undefined}
                    disabled={!m.table_name}
                    onFocus={() => { if (m.table_name) onLoadColumnsForTable(m.table_name); }}
                    onChange={(v) => onUpdateMapping(idx, { column_name: (v as string) || '' })}
                    options={(m.table_name ? (tableColumns[m.table_name] || []) : []).map(c => ({ label: c, value: c }))}
                  />
                </div>
              </div>
            );
          })}
        </div>
      </Spin>
    </div>
  );
};
