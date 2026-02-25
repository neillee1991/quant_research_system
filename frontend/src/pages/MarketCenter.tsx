import React, { useEffect, useState } from 'react';
import { Table, Select, Card } from '@douyinfe/semi-ui';
import { IconCandlestickChartStroked } from '@douyinfe/semi-icons';
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
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{ marginBottom: '16px' }}>
        <h1 style={{
          color: 'var(--color-primary)',
          fontSize: '24px',
          fontWeight: 700,
          margin: 0,
          letterSpacing: '1px'
        }}>
          <IconCandlestickChartStroked style={{ marginRight: '8px' }} />
          行情
        </h1>
        <p style={{
          color: 'var(--text-secondary)',
          margin: '4px 0 0 0',
          fontSize: '12px'
        }}>
          实时行情数据管理与分析
        </p>
      </div>

      <Card
        style={{
          background: 'var(--bg-card)',
          borderRadius: 12,
          border: '1px solid var(--border-color)',
          boxShadow: 'var(--shadow-sm)'
        }}
        bodyStyle={{ padding: 24 }}
      >
        <div style={{ marginBottom: 20 }}>
          <Select
            filter
            style={{ width: 280 }}
            placeholder="选择股票代码"
            optionList={stocks.map((s) => ({ label: s, value: s }))}
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
          size="small"
          pagination={{ pageSize: 20 }}
          scroll={{ x: 800 }}
          style={{
            '--semi-color-primary': 'var(--color-primary)'
          } as React.CSSProperties}
        />
      </Card>
    </div>
  );
};

export default MarketCenter;
