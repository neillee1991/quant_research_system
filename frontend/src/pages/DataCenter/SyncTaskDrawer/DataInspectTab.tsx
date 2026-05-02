import { notify } from '../../../utils/notify';
/**
 * 同步任务数据探查 Tab
 */
import React, { useState } from 'react';
import axios from 'axios';
import { Button, Alert, Collapse, Descriptions, Tag, Progress } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { useMessage } from '../../../hooks/useMessage';

interface SyncDataInspectTabProps {
  taskId: string;
}

export const SyncDataInspectTab: React.FC<SyncDataInspectTabProps> = ({ taskId }) => {
  const message = useMessage();
  const [inspectionData, setInspectionData] = useState<any>(null);
  const [inspectionLoading, setInspectionLoading] = useState(false);

  const handleInspectData = async () => {
    setInspectionLoading(true);
    try {
      const response = await axios.get(`/api/v1/tasks/sync/${taskId}/inspect`);
      const data = response.data;
      setInspectionData(data);
    } catch (error) {
      console.error('Data inspection failed:', error);
      notify.error('数据探查失败');
    } finally {
      setInspectionLoading(false);
    }
  };

  return (
    <div style={{ paddingTop: 8 }}>
      <div style={{ marginBottom: 16 }}>
        <Button
          icon={<SearchOutlined />}
          onClick={handleInspectData}
          loading={inspectionLoading}
          type="primary"
        >
          数据探查
        </Button>
      </div>

      {inspectionData && (
        <>
          {!inspectionData.exists ? (
            <Alert type="warning" message={inspectionData.message} closable={false} />
          ) : !inspectionData.has_data ? (
            <Alert type="info" message={inspectionData.message} closable={false} />
          ) : (
            <Collapse defaultActiveKey={['1']} items={[{
              key: '1',
              label: '数据完整性报告',
              children: (
                <>
                  <Descriptions size="middle" items={[
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
                          percent={inspectionData.coverage_percent || 0}
                          strokeColor={inspectionData.coverage_percent >= 95 ? 'var(--color-gain)' : 'var(--color-loss)'}
                          style={{ width: 200, marginLeft: 8 }}
                          size="middle"
                        />
                      ),
                    },
                  ]} />

                  {inspectionData.missing_count > 0 && inspectionData.missing_dates && (
                    <div style={{ marginTop: 16 }}>
                      <div style={{ marginBottom: 8, fontWeight: 600 }}>缺失的交易日：</div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                        {inspectionData.missing_dates.slice(0, 20).map((date: string) => (
                          <Tag key={date} color="red">{date}</Tag>
                        ))}
                        {inspectionData.missing_count > 20 && (
                          <Tag>... 还有 {inspectionData.missing_count - 20} 天</Tag>
                        )}
                      </div>
                    </div>
                  )}

                  {!inspectionData.trading_calendar_available && (
                    <Alert
                      type="warning"
                      message="交易日历数据不可用，无法检查缺失日期。请先同步 sync_trade_cal 任务。"
                      style={{ marginTop: 16 }}
                      closable={false}
                    />
                  )}
                </>
              ),
            }]} />
          )}
        </>
      )}
    </div>
  );
};
