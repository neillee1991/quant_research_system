/**
 * 数据探查组件
 * 用于检查任务表的数据完整性
 */
import React, { useState } from 'react';
import {
  Button,
  Toast,
  Collapse,
  Tag,
  Descriptions,
  Progress,
  Banner,
  Spin,
} from '@douyinfe/semi-ui';
import { IconSearch } from '@douyinfe/semi-icons';

interface DataInspectionProps {
  taskType: 'sync' | 'etl' | 'factor';
  taskId: string;
  syncType?: 'incremental' | 'full';  // 任务类型，用于判断是否显示
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
  const [inspectionData, setInspectionData] = useState<InspectionResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [showResult, setShowResult] = useState(false);

  // 如果是全量任务，不显示数据探查功能
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
      Toast.error('数据探查失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      {/* 数据探查按钮 */}
      <div style={{ marginBottom: 16 }}>
        <Button
          icon={<IconSearch />}
          onClick={handleInspect}
          loading={loading}
          theme="solid"
          type="primary"
        >
          数据探查
        </Button>
      </div>

      {/* 数据探查结果 */}
      {showResult && inspectionData && (
        <div style={{ marginBottom: 16 }}>
          {!inspectionData.exists ? (
            <Banner
              type="warning"
              description={inspectionData.message}
              closeIcon={null}
            />
          ) : !inspectionData.has_data ? (
            <Banner
              type="info"
              description={inspectionData.message}
              closeIcon={null}
            />
          ) : (
            <Collapse defaultActiveKey={['1']}>
              <Collapse.Panel header="数据完整性报告" itemKey="1">
                <Descriptions row size="small">
                  <Descriptions.Item itemKey="table">
                    表名: {inspectionData.table_name}
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="field">
                    日期字段: {inspectionData.date_field}
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="min">
                    最早日期: {inspectionData.min_date}
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="max">
                    最晚日期: {inspectionData.max_date}
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="actual">
                    实际天数: {inspectionData.actual_dates}
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="expected">
                    预期天数: {inspectionData.expected_dates || '-'}
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="missing">
                    缺失天数: <Tag color="red">{inspectionData.missing_count || 0}</Tag>
                  </Descriptions.Item>
                  <Descriptions.Item itemKey="coverage">
                    覆盖率:
                    <Progress
                      percent={inspectionData.coverage_percent ?? 0}
                      stroke={
                        (inspectionData.coverage_percent ?? 0) >= 95
                          ? 'var(--semi-color-success)'
                          : 'var(--semi-color-danger)'
                      }
                      style={{ width: 200, marginLeft: 8 }}
                      showInfo
                      size="small"
                    />
                  </Descriptions.Item>
                </Descriptions>

                {(inspectionData.missing_count ?? 0) > 0 && inspectionData.missing_dates && (
                  <div style={{ marginTop: 16 }}>
                    <div style={{ marginBottom: 8, fontWeight: 600 }}>
                      缺失的交易日：
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                      {inspectionData.missing_dates.slice(0, 20).map((date: string) => (
                        <Tag key={date} color="red" size="small">
                          {date}
                        </Tag>
                      ))}
                      {(inspectionData.missing_count ?? 0) > 20 && (
                        <Tag size="small">
                          ... 还有 {(inspectionData.missing_count ?? 0) - 20} 天
                        </Tag>
                      )}
                    </div>
                  </div>
                )}

                {!inspectionData.trading_calendar_available && (
                  <Banner
                    type="warning"
                    description="交易日历数据不可用，无法检查缺失日期。请先同步 sync_trade_cal 任务。"
                    style={{ marginTop: 16 }}
                    closeIcon={null}
                  />
                )}
              </Collapse.Panel>
            </Collapse>
          )}
        </div>
      )}
    </div>
  );
};
