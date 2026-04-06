/**
 * 通用任务状态 Tab
 */
import React from 'react';
import { Card, Descriptions, Alert, Tag, Spin } from 'antd';
import { CheckCircleOutlined, ClockCircleOutlined, ExclamationCircleOutlined } from '@ant-design/icons';

interface UniversalStatusTabProps {
  taskType: 'sync' | 'etl' | 'factor';
  taskId: string;
  status?: {
    last_date?: string | null;
    last_sync_time?: string | null;
    table_latest_date?: string | null;
    table_name?: string;
    description?: string;
    enabled?: boolean;
    [key: string]: unknown;
  } | null;
  loading?: boolean;
}

const formatDate = (dateStr: string | null | undefined): string => {
  if (!dateStr) return '-';
  if (typeof dateStr === 'string' && dateStr.length === 8) {
    return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
  }
  return String(dateStr).slice(0, 10);
};

const formatDateTime = (dateTimeStr: string | null | undefined): string => {
  if (!dateTimeStr) return '-';
  return dateTimeStr.slice(0, 19);
};

export const UniversalStatusTab: React.FC<UniversalStatusTabProps> = ({
  taskType,
  taskId,
  status,
  loading = false,
}) => {
  if (loading) {
    return (
      <div style={{ padding: '40px 0', textAlign: 'center' }}>
        <Spin size="large" />
        <div style={{ marginTop: 12, color: 'var(--text-secondary)' }}>加载状态中...</div>
      </div>
    );
  }

  if (!taskId) {
    return (
      <Alert
        message="请先保存任务"
        description="任务保存后才能查看状态信息"
        type="info"
        showIcon
      />
    );
  }

  if (!status) {
    return (
      <Alert
        message="暂无状态信息"
        description="该任务还没有执行记录"
        type="info"
        showIcon
      />
    );
  }

  const hasLatestData = !!status.table_latest_date || !!status.last_date;
  const hasLastSync = !!status.last_sync_time;

  return (
    <Card style={{ border: 'none' }}>
      <Descriptions column={1} size="small" bordered>
        <Descriptions.Item label="任务ID">
          <code style={{ color: 'var(--color-primary)' }}>{taskId}</code>
        </Descriptions.Item>

        <Descriptions.Item label="任务描述">
          {status.description || '-'}
        </Descriptions.Item>

        <Descriptions.Item label="数据表">
          {status.table_name ? (
            <code style={{ color: 'var(--color-gain)' }}>{status.table_name}</code>
          ) : (
            '-'
          )}
        </Descriptions.Item>

        <Descriptions.Item label="启用状态">
          {status.enabled !== false ? (
            <Tag color="green" icon={<CheckCircleOutlined />}>已启用</Tag>
          ) : (
            <Tag color="default" icon={<ExclamationCircleOutlined />}>已禁用</Tag>
          )}
        </Descriptions.Item>

        <Descriptions.Item
          label={
            <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <CheckCircleOutlined style={{ color: 'var(--color-gain)' }} />
              最新数据
            </span>
          }
        >
          {hasLatestData ? (
            <span style={{ color: 'var(--color-gain)', fontWeight: 500 }}>
              {formatDate(status.table_latest_date || status.last_date)}
            </span>
          ) : (
            <span style={{ color: 'var(--text-muted)' }}>-</span>
          )}
        </Descriptions.Item>

        <Descriptions.Item
          label={
            <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <ClockCircleOutlined />
              上次执行
            </span>
          }
        >
          {hasLastSync ? (
            <span style={{ color: 'var(--text-secondary)' }}>
              {formatDateTime(status.last_sync_time)}
            </span>
          ) : (
            <span style={{ color: 'var(--text-muted)' }}>未执行</span>
          )}
        </Descriptions.Item>
      </Descriptions>
    </Card>
  );
};
