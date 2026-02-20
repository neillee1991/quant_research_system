import React from 'react';
import ReactECharts from 'echarts-for-react';

interface KLineProps {
  data: Array<{
    trade_date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    vol: number;
  }>;
  buyPoints?: string[];
  sellPoints?: string[];
}

const KLineChart: React.FC<KLineProps> = ({ data, buyPoints = [], sellPoints = [] }) => {
  const dates = data.map((d) => d.trade_date);
  const ohlc = data.map((d) => [d.open, d.close, d.low, d.high]);
  const vols = data.map((d) => d.vol);

  const markPoints = [
    ...buyPoints.map((d) => ({ name: 'Buy', coord: [d, data.find((x) => x.trade_date === d)?.close], itemStyle: { color: '#52c41a' }, symbol: 'arrow', symbolSize: 12 })),
    ...sellPoints.map((d) => ({ name: 'Sell', coord: [d, data.find((x) => x.trade_date === d)?.close], itemStyle: { color: '#ff4d4f' }, symbol: 'arrow', symbolRotate: 180, symbolSize: 12 })),
  ];

  const option = {
    backgroundColor: '#141414',
    tooltip: { trigger: 'axis', axisPointer: { type: 'cross' } },
    legend: { data: ['K-Line', 'Volume'], textStyle: { color: '#ccc' } },
    grid: [{ left: '5%', right: '5%', top: '5%', height: '60%' }, { left: '5%', right: '5%', top: '72%', height: '18%' }],
    xAxis: [
      { type: 'category', data: dates, axisLabel: { color: '#aaa' }, gridIndex: 0 },
      { type: 'category', data: dates, axisLabel: { show: false }, gridIndex: 1 },
    ],
    yAxis: [
      { scale: true, axisLabel: { color: '#aaa' }, gridIndex: 0 },
      { scale: true, axisLabel: { color: '#aaa' }, gridIndex: 1 },
    ],
    series: [
      {
        name: 'K-Line', type: 'candlestick', data: ohlc, xAxisIndex: 0, yAxisIndex: 0,
        itemStyle: { color: '#ef232a', color0: '#14b143', borderColor: '#ef232a', borderColor0: '#14b143' },
        markPoint: { data: markPoints },
      },
      {
        name: 'Volume', type: 'bar', data: vols, xAxisIndex: 1, yAxisIndex: 1,
        itemStyle: { color: '#1677ff', opacity: 0.6 },
      },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 60, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start: 60, end: 100, textStyle: { color: '#aaa' } },
    ],
  };

  return <ReactECharts option={option} style={{ height: 500 }} theme="dark" />;
};

export default KLineChart;
