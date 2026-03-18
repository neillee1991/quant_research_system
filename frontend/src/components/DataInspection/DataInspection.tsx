/**
 * 数据探查组件
 * 用于检查任务表的数据完整性
 */
import React, { useState } from 'react';
import {
  Button,
  Collapse,
  Tag,
  Descriptions,
  Progress,
  Alert,
  Spin,
} from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';

interface DataInspectionProps {
  taskType: 'sync' | 'etl' | 'factor';
  taskId: string;
  syncType?: 'incremental' | 'full';
}

interface InspectionResult {
  table_name: string;
  exists: boolean;
  has_data?: boolean;
  date_field?: string;
  min_date?: string;
  max_date?: string;
  actual_dates?: number;
  expected_dates?: number;
  missing_dates?: string[];
  missing_count?: number;
  coverage_percent?: number;
  trading_calendar_available?: boolean;
  message?: string;
}

export const DataInspection: React.FC<DataInspectionProps> = ({ taskType, taskId, syncType }) => {
  const message = useMessage();
  const [inspectionData, setInspectionData] = useState<InspectionResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [showResult, setShowResult] = useState(false);

  if (syncType === 'full') {
    return null;
  }

  const handleInspect = async () => {
    setLoading(true);
    try {
      const response = await fetch(
        `http://localhost:8000/api/v1/tasks/${taskType}/${taskId}/inspect`
      );
      const data = await response.json();
      setInspectionData(data);
      setShowResult(true);
    } catch (error) {
      console.error('Data inspection failed:', error);
      message.error('数据探查失败');
    } finally {
      setLoading(false);
    }
  };

  const coveragePct = inspectionData?.coverage_percent ?? 0;

  const descItems = inspectionData ? [
    { key: 'table', label: '表名', children: inspectionData.table_name },
    { key: 'field', label: '日期字段', children: inspectionData.date_field },
    { key: 'min', label: '最早日期', children: inspectionData.min_date },
    { key: 'max', label: '最晚日期', children: inspectionData.max_date },
    { key: 'actual', label: '实际天数', children: inspectionData.actual_dates },
    { key: 'expected', label: '预期天数', children: inspectionData.expected_dates || '-' },
    { key: 'missing', label: '缺失天数', children: <Tag color="red">{inspectionData.missing_count || 0}</Tag> },
    {
      key: 'coverage',
      label: '覆盖率',
      children: (
        <Progress
          percent={coveragePct}
          strokeColor={coveragePct >= 95 ? 'var(--color-gain)' : 'var(--color-loss)'}
          style={{ width: 200 }}
          size="small"
        />
      ),
    },
  ] : [];

  const collapseItems = inspectionData?.has_data ? [
    {
      key: '1',
      label: '数据完整性报告',
      children: (
        <div>
          <Descriptions items={descItems} size="small" column={2} />
          {(inspectionData?.missing_count ?? 0) > 0 && inspectionData?.missing_dates && (
            <div style={{ marginTop: 16 }}>
              <div style={{ marginBottom: 8, fontWeight: 600 }}>缺失的交易日：</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                {inspectionData.missing_dates.slice(0, 20).map((date: string) => (
                  <Tag key={date} color="red">{date}</Tag>
                ))}
                {(inspectionData.missing_count ?? 0) > 20 && (
                  <Tag>... 还有 {(inspectionData.missing_count ?? 0) - 20} 天</Tag>
                )}
              </div>
            </div>
          )}
          {!inspectionData?.trading_calendar_available && (
            <Alert
              type="warning"
              message="交易日历数据不可用，无法检查缺失日期。请先同步 sync_trade_cal 任务。"
              style={{ marginTop: 16 }}
              closable={false}
            />
          )}
        </div>
      ),
    },
  ] : [];

  return (
    <div>
      <div style={{ marginBottom: 16 }}>
        <Button
          icon={<SearchOutlined />}
          onClick={handleInspect}
          loading={loading}
          type="primary"
        >
          数据探查
        </Button>
      </div>

      {showResult && inspectionData && (
        <div style={{ marginBottom: 16 }}>
          {!inspectionData.exists ? (
            <Alert type="warning" message={inspectionData.message} closable={false} />
          ) : !inspectionData.has_data ? (
            <Alert type="info" message={inspectionData.message} closable={false} />
          ) : (
            <Collapse defaultActiveKey={['1']} items={collapseItems} />
          )}
        </div>
      )}
    </div>
  );
};
