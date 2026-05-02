/**
 * 因子计算流程可视化组件
 * 展示8步 Pipeline 流程图
 */

import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import { FlowFormulaRenderer } from './FlowFormulaRenderer';

interface FactorCalculationFlowProps {
  height?: number;
}

const FactorCalculationFlow: React.FC<FactorCalculationFlowProps> = ({ height = 400 }) => {
  const chartOption = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c}',
    },
    animationDurationUpdate: 1500,
    animationEasingUpdate: 'quinticInOut',
    series: [
      {
        type: 'graph',
        layout: 'none',
        symbolSize: 60,
        roam: true,
        label: {
          show: true,
          fontSize: 12,
          color: '#fff',
        },
        edgeSymbol: ['circle', 'arrow'],
        edgeSymbolSize: [4, 10],
        edgeLabel: {
          fontSize: 10,
        },
        data: [
          { name: '1. 日期解析', x: 100, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '2. 数据加载', x: 250, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '3. 价格复权', x: 400, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '4. 状态过滤', x: 550, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '5. 因子计算', x: 700, y: 100, itemStyle: { color: '#10B981' } },
          { name: '6. 停牌处理', x: 850, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '7. 质量检查', x: 550, y: 220, itemStyle: { color: '#0077FA' } },
          { name: '8. 结果保存', x: 700, y: 220, itemStyle: { color: '#0077FA' } },
        ],
        links: [
          { source: '1. 日期解析', target: '2. 数据加载' },
          { source: '2. 数据加载', target: '3. 价格复权' },
          { source: '3. 价格复权', target: '4. 状态过滤' },
          { source: '4. 状态过滤', target: '5. 因子计算' },
          { source: '5. 因子计算', target: '6. 停牌处理' },
          { source: '6. 停牌处理', target: '7. 质量检查' },
          { source: '7. 质量检查', target: '8. 结果保存' },
        ],
        lineStyle: {
          opacity: 0.9,
          width: 2,
          curveness: 0,
        },
      },
    ],
  };

  const steps = [
    {
      title: '1. 日期解析',
      desc: '确定数据加载范围：data_start = start_date - lookback_days',
    },
    {
      title: '2. 数据加载',
      desc: '按 depends_on 从 DolphinDB 查询原始数据',
    },
    {
      title: '3. 价格复权',
      desc: '使用复权因子调整 OHLCV 价格',
    },
    {
      title: '4. 状态过滤',
      desc: '过滤 ST、新股，标记涨跌停',
    },
    {
      title: '5. 因子计算',
      desc: '执行用户定义的因子函数（Polars 向量化）',
    },
    {
      title: '6. 停牌处理',
      desc: '停牌期间因子值置空',
    },
    {
      title: '7. 质量检查',
      desc: '计算空值率、极端值标记',
    },
    {
      title: '8. 结果保存',
      desc: 'Upsert 到 factor_values 表',
    },
  ];

  return (
    <div>
      <Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>8步 Pipeline 流程</span>}
      >
        <ReactECharts option={chartOption} style={{ height }} />
      </Card>

      <Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>流程说明</span>}
      >
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 12 }}>
          {steps.map((step, idx) => (
            <div
              key={idx}
              style={{
                padding: 12,
                background: 'var(--bg-tertiary)',
                borderRadius: 6,
                borderLeft: `3px solid ${idx === 4 ? '#10B981' : '#0077FA'}`,
              }}
            >
              <div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}>
                {step.title}
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
                {step.desc}
              </div>
            </div>
          ))}
        </div>
      </Card>

      <Card
        style={{ background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>核心公式</span>}
      >
        <FlowFormulaRenderer type="calculation" />
      </Card>
    </div>
  );
};

export default FactorCalculationFlow;
