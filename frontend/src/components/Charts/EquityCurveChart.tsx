import React from 'react';
import ReactECharts from 'echarts-for-react';

interface EquityCurveProps {
  data: Array<{ trade_date: string; equity: number; port_return: number }>;
}

const EquityCurveChart: React.FC<EquityCurveProps> = ({ data }) => {
  const dates = data.map((d) => d.trade_date);
  const equity = data.map((d) => d.equity);

  let peak = equity[0] || 1;
  const drawdown = equity.map((v) => {
    if (v > peak) peak = v;
    return +((v / peak - 1) * 100).toFixed(2);
  });

  const option = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { data: ['Equity', 'Drawdown %'], textStyle: { color: 'var(--text-secondary)' } },
    grid: [{ left: '5%', right: '5%', top: '8%', height: '55%' }, { left: '5%', right: '5%', top: '70%', height: '20%' }],
    xAxis: [
      { type: 'category', data: dates, axisLabel: { color: '#94a3b8' }, gridIndex: 0 },
      { type: 'category', data: dates, axisLabel: { show: false }, gridIndex: 1 },
    ],
    yAxis: [
      { scale: true, axisLabel: { color: '#94a3b8' }, gridIndex: 0 },
      { axisLabel: { color: '#94a3b8', formatter: '{value}%' }, gridIndex: 1 },
    ],
    series: [
      { name: 'Equity', type: 'line', data: equity, xAxisIndex: 0, yAxisIndex: 0, smooth: true, lineStyle: { color: '#00B42A', width: 2 }, areaStyle: { color: 'rgba(0,180,42,0.1)' } },
      { name: 'Drawdown %', type: 'line', data: drawdown, xAxisIndex: 1, yAxisIndex: 1, lineStyle: { color: '#F53F3F', width: 1 }, areaStyle: { color: 'rgba(245,63,63,0.2)' } },
    ],
    dataZoom: [{ type: 'inside', xAxisIndex: [0, 1] }, { type: 'slider', xAxisIndex: [0, 1], textStyle: { color: '#94a3b8' } }],
  };

  return <ReactECharts option={option} style={{ height: 480 }} />;
};

export default EquityCurveChart;
