import React, { useState, useEffect } from 'react';
import { Card, Button, Table, Tag, message } from 'antd';
import { PlayCircleOutlined, BarChartOutlined } from '@ant-design/icons';
import { BacktestModal } from './BacktestModal';
import { BacktestResult } from './BacktestResult';
import { extractApiError } from '../utils/notify';
import './index.css';

interface BacktestHistoryItem {
  id: string;
  strategy: string;
  engine: string;
  startDate: string;
  endDate: string;
  status: string;
  returnRate: string;
}

interface BacktestConfig {
  [key: string]: any;
}

const BacktestCenter: React.FC = () => {
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [selectedBacktest, setSelectedBacktest] = useState<any>(null);
  const [backtestHistory, setBacktestHistory] = useState<BacktestHistoryItem[]>([]);

  const handleRunBacktest = async (config: BacktestConfig) => {
    setIsModalVisible(false);
    const hideLoading = message.loading('正在执行回测，请稍候...', 0);

    try {
      const response = await fetch('/api/v1/backtest/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
      });

      if (response.ok) {
        const result = await response.json();
        setSelectedBacktest(result);
        hideLoading();
        message.success('回测完成');
      } else {
        const errorData = await response.json();
        hideLoading();
        message.error(`回测失败: ${extractApiError(errorData.detail)}`);
      }
    } catch (error) {
      hideLoading();
      const errorMessage = error instanceof Error ? error.message : '未知错误';
      message.error(`回测失败: ${errorMessage}`);
    }
  };

  useEffect(() => {
    const loadBacktestHistory = async () => {
      const mockData: BacktestHistoryItem[] = [
        {
          id: '1',
          strategy: '简单策略',
          engine: 'VectorBT',
          startDate: '20100101',
          endDate: '20240101',
          status: 'completed',
          returnRate: '125.6%',
        },
      ];
      setBacktestHistory(mockData);
    };

    loadBacktestHistory();
  }, []);

  return (
    <div className="backtest-center">
      <div className="page-header">
        <h1>回测中心</h1>
        <Button
          type="primary"
          icon={<PlayCircleOutlined />}
          onClick={() => setIsModalVisible(true)}
          size="large"
        >
          新建回测
        </Button>
      </div>

      <Card title="回测历史" className="history-card">
        <Table<BacktestHistoryItem>
          columns={[
            {
              title: '策略',
              dataIndex: 'strategy',
              key: 'strategy',
            },
            {
              title: '引擎',
              dataIndex: 'engine',
              key: 'engine',
              render: (engine: string) => <Tag color={engine === 'VectorBT' ? 'blue' : 'orange'}>{engine}</Tag>,
            },
            {
              title: '开始日期',
              dataIndex: 'startDate',
              key: 'startDate',
            },
            {
              title: '结束日期',
              dataIndex: 'endDate',
              key: 'endDate',
            },
            {
              title: '状态',
              dataIndex: 'status',
              key: 'status',
              render: (status: string) => (
                <Tag color={status === 'completed' ? 'green' : 'yellow'}>
                  {status === 'completed' ? '已完成' : '进行中'}
                </Tag>
              ),
            },
            {
              title: '收益率',
              dataIndex: 'returnRate',
              key: 'returnRate',
              render: (rate: string) => (
                <span style={{ color: rate.startsWith('-') ? '#ff4d4f' : '#52c41a' }}>
                  {rate}
                </span>
              ),
            },
            {
              title: '操作',
              key: 'action',
              render: (_, record) => (
                <Button
                  type="link"
                  size="small"
                  icon={<BarChartOutlined />}
                  onClick={() => setSelectedBacktest(record)}
                >
                  查看详情
                </Button>
              ),
            },
          ]}
          dataSource={backtestHistory}
          rowKey="id"
          pagination={{ pageSize: 10 }}
        />
      </Card>

      <BacktestModal
        visible={isModalVisible}
        onCancel={() => setIsModalVisible(false)}
        onOk={handleRunBacktest}
      />

      {selectedBacktest && (
        <BacktestResult
          result={selectedBacktest}
          onClose={() => setSelectedBacktest(null)}
        />
      )}
    </div>
  );
};

export default BacktestCenter;
