import React from 'react';
import ReactECharts from 'echarts-for-react';

interface EquityCurveProps {
  data: Array<{ trade_date: string; equity: number; port_return: number }>;
}

const EquityCurveChart: React.FC<EquityCurveProps> = ({ data }) => {
  const dates = data.map((d) => d.trade_date);
  const equity = data.map((d) => d.equity);

  // Drawdown
  let peak = equity[0] || 1;
  const drawdown = equity.map((v) => {
    if (v > peak) peak = v;
    return +((v / peak - 1) * 100).toFixed(2);
  });

  const option = {
    backgroundColor: '#141414',
    tooltip: { trigger: 'axis' },
    legend: { data: ['Equity', 'Drawdown %'], textStyle: { color: '#ccc' } },
    grid: [{ left: '5%', right: '5%', top: '8%', height: '55%' }, { left: '5%', right: '5%', top: '70%', height: '20%' }],
    xAxis: [
      { type: 'category', data: dates, axisLabel: { color: '#aaa' }, gridIndex: 0 },
      { type: 'category', data: dates, axisLabel: { show: false }, gridIndex: 1 },
    ],
    yAxis: [
      { scale: true, axisLabel: { color: '#aaa' }, gridIndex: 0 },
      { axisLabel: { color: '#aaa', formatter: '{value}%' }, gridIndex: 1 },
    ],
    series: [
      { name: 'Equity', type: 'line', data: equity, xAxisIndex: 0, yAxisIndex: 0, smooth: true, lineStyle: { color: '#52c41a', width: 2 }, areaStyle: { color: 'rgba(82,196,26,0.1)' } },
      { name: 'Drawdown %', type: 'line', data: drawdown, xAxisIndex: 1, yAxisIndex: 1, lineStyle: { color: '#ff4d4f', width: 1 }, areaStyle: { color: 'rgba(255,77,79,0.2)' } },
    ],
    dataZoom: [{ type: 'inside', xAxisIndex: [0, 1] }, { type: 'slider', xAxisIndex: [0, 1], textStyle: { color: '#aaa' } }],
  };

  return <ReactECharts option={option} style={{ height: 480 }} theme="dark" />;
};

export default EquityCurveChart;
