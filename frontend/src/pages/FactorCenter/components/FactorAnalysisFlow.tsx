/**
 * 因子分析流程可视化组件
 * 展示因子分析完整流程
 */

import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import { FlowFormulaRenderer } from './FlowFormulaRenderer';

interface FactorAnalysisFlowProps {
  height?: number;
}

const FactorAnalysisFlow: React.FC<FactorAnalysisFlowProps> = ({ height = 350 }) => {
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
        symbolSize: 55,
        roam: true,
        label: {
          show: true,
          fontSize: 11,
          color: '#fff',
        },
        edgeSymbol: ['circle', 'arrow'],
        edgeSymbolSize: [4, 10],
        edgeLabel: {
          fontSize: 10,
        },
        data: [
          { name: '加载因子数据', x: 150, y: 80, itemStyle: { color: '#0077FA' } },
          { name: '加载价格数据', x: 350, y: 80, itemStyle: { color: '#0077FA' } },
          { name: '股票池过滤', x: 250, y: 180, itemStyle: { color: '#0077FA' } },
          { name: '中性化处理', x: 250, y: 280, itemStyle: { color: '#0077FA' } },
          { name: '计算远期收益', x: 450, y: 280, itemStyle: { color: '#0077FA' } },
          { name: 'IC 分析', x: 150, y: 380, itemStyle: { color: '#10B981' } },
          { name: '分组回测', x: 350, y: 380, itemStyle: { color: '#10B981' } },
          { name: '换手率分析', x: 550, y: 380, itemStyle: { color: '#10B981' } },
          { name: '衰减分析', x: 750, y: 380, itemStyle: { color: '#10B981' } },
        ],
        links: [
          { source: '加载因子数据', target: '股票池过滤' },
          { source: '加载价格数据', target: '股票池过滤' },
          { source: '股票池过滤', target: '中性化处理' },
          { source: '中性化处理', target: '计算远期收益' },
          { source: '计算远期收益', target: 'IC 分析' },
          { source: '计算远期收益', target: '分组回测' },
          { source: '计算远期收益', target: '换手率分析' },
          { source: '计算远期收益', target: '衰减分析' },
        ],
        lineStyle: {
          opacity: 0.9,
          width: 2,
          curveness: 0.2,
        },
      },
    ],
  };

  const analysisModules = [
    {
      title: 'IC 分析',
      desc: '计算 IC 均值、ICIR、胜率、t 统计量等',
      color: '#10B981',
    },
    {
      title: '分组回测',
      desc: '按因子值分层，计算各组收益及多空 Spread',
      color: '#0077FA',
    },
    {
      title: '换手率分析',
      desc: '计算各分层的换手率，衡量策略稳定性',
      color: '#8B5CF6',
    },
    {
      title: '衰减分析',
      desc: '分析因子排名自相关性的衰减速度',
      color: '#F97316',
    },
    {
      title: '行业分析',
      desc: '分行业计算 IC 和收益，查看行业暴露',
      color: '#06B6D4',
    },
    {
      title: '事件研究',
      desc: '分析因子形成前后的累积收益',
      color: '#EC4899',
    },
  ];

  return (
    <div>
      <Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>分析流程图</span>}
      >
        <ReactECharts option={chartOption} style={{ height }} />
      </Card>

      <Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>分析模块说明</span>}
      >
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 12 }}>
          {analysisModules.map((module, idx) => (
            <div
              key={idx}
              style={{
                padding: 12,
                background: 'var(--bg-tertiary)',
                borderRadius: 6,
                borderLeft: `3px solid ${module.color}`,
              }}
            >
              <div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}>
                {module.title}
              </div>
              <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
                {module.desc}
              </div>
            </div>
          ))}
        </div>
      </Card>

      <Card
        style={{ background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>核心公式</span>}
      >
        <FlowFormulaRenderer type="analysis" />
      </Card>
    </div>
  );
};

export default FactorAnalysisFlow;
