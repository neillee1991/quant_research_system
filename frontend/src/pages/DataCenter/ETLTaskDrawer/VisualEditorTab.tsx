/**
 * ETL 任务可视化配置 Tab（基本信息表单）
 */
import React from 'react';
import { Input, Radio } from 'antd';

export interface ETLVisualEditorTabProps {
  config: any;
  onChange: (config: any) => void;
  isNew: boolean;
}

export const ETLVisualEditorTab: React.FC<ETLVisualEditorTabProps> = ({
  config,
  onChange,
  isNew,
}) => {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, paddingTop: 8 }}>
      {/* 基本信息 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>任务 ID</div>
          <Input
            prefix="etl_"
            value={(config.task_id || '').replace(/^etl_/, '')}
            disabled={!isNew}
            onChange={(e) => onChange({ ...config, task_id: `etl_${e.target.value}` })}
          />
        </div>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>目标表名（= 任务ID）</div>
          <Input value={config.task_id} disabled />
        </div>
      </div>

      <div>
        <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>描述</div>
        <Input
          value={config.description}
          onChange={(e) => onChange({ ...config, description: e.target.value })}
        />
      </div>

      {/* 同步类型 */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
        <div>
          <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>同步类型</div>
          <Radio.Group
            value={config.sync_type || 'incremental'}
            onChange={(e: any) => onChange({ ...config, sync_type: e.target.value })}
          >
            <Radio value="incremental">增量</Radio>
            <Radio value="full">全量</Radio>
          </Radio.Group>
        </div>
        {config.sync_type === 'incremental' && (
          <div>
            <div style={{ marginBottom: 4, fontSize: '12px', color: 'var(--text-secondary)' }}>日期字段</div>
            <Input
              value={config.date_field}
              placeholder="如 trade_date"
              onChange={(e) => onChange({ ...config, date_field: e.target.value })}
            />
          </div>
        )}
      </div>
    </div>
  );
};
