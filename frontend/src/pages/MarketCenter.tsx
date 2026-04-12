import React, { useEffect, useState } from 'react';
import { Table, Select, Card } from 'antd';
import { FundOutlined } from '@ant-design/icons';
import { dataApi } from '../api';
import TradingViewChart from '../components/Charts/TradingViewChart';

const MarketCenter: React.FC = () => {
  const [stocks, setStocks] = useState<string[]>([]);
  const [selected, setSelected] = useState<string>('');
  const [dailyData, setDailyData] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadStocks();
  }, []);

  const loadStocks = async () => {
    try {
      const res = await dataApi.listStocks();
      setStocks(res.data.stocks || []);
    } catch (err) {
      console.error('Failed to load stocks:', err);
    }
  };

  const loadDaily = async (tsCode: string) => {
    setLoading(true);
    try {
      const res = await dataApi.getDaily(tsCode);
      setDailyData(res.data.data || []);
    } catch (err) {
      console.error('Failed to load daily data:', err);
    } finally {
      setLoading(false);
    }
  };

  const dailyColumns = [
    { title: '日期', dataIndex: 'trade_date', key: 'trade_date', width: 100 },
    { title: '开盘', dataIndex: 'open', key: 'open', width: 80, render: (v: number) => v?.toFixed(2) },
    { title: '最高', dataIndex: 'high', key: 'high', width: 80, render: (v: number) => v?.toFixed(2) },
    { title: '最低', dataIndex: 'low', key: 'low', width: 80, render: (v: number) => v?.toFixed(2) },
    { title: '收盘', dataIndex: 'close', key: 'close', width: 80, render: (v: number) => v?.toFixed(2) },
    { title: '成交量', dataIndex: 'vol', key: 'vol', width: 100, render: (v: number) => v?.toFixed(0) },
    { title: '成交额', dataIndex: 'amount', key: 'amount', width: 100, render: (v: number) => v?.toFixed(0) },
  ];

  return (
    <div style={{ padding: '8px', maxWidth: '1600px', margin: '0 auto' }}>
      <Card
        style={{
          background: 'var(--bg-card)',
          borderRadius: 12,
          border: '1px solid var(--border-color)',
          boxShadow: 'var(--shadow-sm)'
        }}
        styles={{ body: { padding: 24 } }}
      >
        <div style={{ marginBottom: 20 }}>
          <Select
            showSearch
            style={{ width: 280 }}
            placeholder="选择股票代码"
            options={stocks.map((s) => ({ label: s, value: s }))}
            onChange={(v) => {
              setSelected(v as string);
              loadDaily(v as string);
            }}
            size="large"
          />
        </div>

        {dailyData.length > 0 && (
          <div style={{
            marginBottom: 24,
            padding: 20,
            background: 'var(--bg-surface)',
            borderRadius: 12,
            border: '1px solid var(--border-color)',
            position: 'relative',
            overflow: 'hidden'
          }}>
            <div style={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              height: 3,
              background: 'var(--gradient-accent)',
              opacity: 0.6
            }}></div>
            <TradingViewChart data={dailyData.slice().reverse()} />
          </div>
        )}

        <Table
          dataSource={dailyData}
          columns={dailyColumns}
          rowKey="trade_date"
          loading={loading}
          size="middle"
          pagination={{ pageSize: 20 }}
          scroll={{ x: 800 }}
          style={{}}
        />
      </Card>
    </div>
  );
};

export default MarketCenter;
