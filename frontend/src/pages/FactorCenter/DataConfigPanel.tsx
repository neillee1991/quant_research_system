/**
 * 数据配置面板
 */

import React from 'react';
import {
  Card, Button, Spin, Empty, Select, Tag,
} from '@douyinfe/semi-ui';
import { IconRefresh, IconSave } from '@douyinfe/semi-icons';
import { useDataConfig } from './hooks/useDataConfig';

const DataConfigPanel: React.FC = () => {
  const {
    mappings,
    loading,
    saving,
    tables,
    tableColumns,
    changed,
    loadConfig,
    loadColumnsForTable,
    updateMapping,
    saveConfig,
  } = useDataConfig();

  return (
    <Card style={{ background: 'var(--bg-card)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <div>
          <span style={{ color: 'var(--color-primary)', fontWeight: 600, fontSize: 15 }}>数据字段映射</span>
          <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
            配置因子计算引擎使用的数据表和字段映射。主键: trade_date + ts_code。留空表示使用引擎内置逻辑。
          </span>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Button size="small" icon={<IconRefresh />} onClick={loadConfig}>
            刷新
          </Button>
          <Button
            size="small"
            theme="solid"
            icon={<IconSave />}
            disabled={!changed}
            loading={saving}
            onClick={saveConfig}
          >
            保存配置
          </Button>
        </div>
      </div>

      <Spin spinning={loading}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {mappings.map((m, idx) => {
            const extra = (() => {
              try {
                return JSON.parse(m.extra_config || '{}');
              } catch {
                return {};
              }
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
                {/* 第一行: 字段信息 + 枚举标签 + 状态 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <code style={{ color: 'var(--color-primary)', fontSize: 12, fontWeight: 600 }}>
                    {m.field_key}
                  </code>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>
                    {m.description?.split('。')[0]}
                  </span>
                  {enumValues && (
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {Object.entries(enumValues).map(([k, v]) => (
                        <Tag
                          key={k}
                          size="small"
                          style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}
                          color={k === '0' ? 'green' : k === '1' ? 'red' : k === '-1' ? 'blue' : 'grey'}
                        >
                          {k}={v}
                        </Tag>
                      ))}
                    </div>
                  )}
                  <div style={{ marginLeft: 'auto' }}>
                    {hasTable ? (
                      <Tag color="green" style={{ fontSize: 11 }}>
                        {m.table_name}{m.column_name ? `.${m.column_name}` : ''}
                      </Tag>
                    ) : extra.mode ? (
                      <Tag color="blue" style={{ fontSize: 11 }}>
                        {extra.mode === 'infer_from_gaps'
                          ? '从交易日缺失推断'
                          : extra.mode === 'compute_from_ohlcv'
                          ? '从OHLCV计算'
                          : extra.mode}
                      </Tag>
                    ) : (
                      <Tag color="grey" style={{ fontSize: 11 }}>
                        未配置
                      </Tag>
                    )}
                  </div>
                </div>

                {/* 第二行: 数据表 + 列选择器 */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: 11, flexShrink: 0 }}>数据表</span>
                  <Select
                    size="small"
                    style={{ width: 240 }}
                    placeholder="选择数据表"
                    showClear
                    filter
                    value={m.table_name || undefined}
                    onChange={(v) => {
                      updateMapping(idx, { table_name: (v as string) || '', column_name: '' });
                      if (v) loadColumnsForTable(v as string);
                    }}
                    optionList={tables.map(t => ({ label: t, value: t }))}
                  />
                  <span style={{ color: 'var(--text-muted)', fontSize: 11, flexShrink: 0 }}>列</span>
                  <Select
                    size="small"
                    style={{ width: 200 }}
                    placeholder="选择列"
                    showClear
                    filter
                    value={m.column_name || undefined}
                    disabled={!m.table_name}
                    onFocus={() => {
                      if (m.table_name) loadColumnsForTable(m.table_name);
                    }}
                    onChange={(v) => updateMapping(idx, { column_name: (v as string) || '' })}
                    optionList={(tableColumns[m.table_name] || []).map(c => ({ label: c, value: c }))}
                  />
                </div>
              </div>
            );
          })}
        </div>
        {mappings.length === 0 && !loading && <Empty description="暂无配置数据" />}
      </Spin>
    </Card>
  );
};

export default DataConfigPanel;
