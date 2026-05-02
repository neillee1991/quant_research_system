/**
 * 因子流程可视化抽屉
 * 展示因子计算和分析的完整流程
 */

import React from 'react';
import { Drawer, Tabs } from 'antd';
import { ApartmentOutlined, ApiOutlined, BarChartOutlined } from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import FactorCalculationFlow from './components/FactorCalculationFlow';
import FactorAnalysisFlow from './components/FactorAnalysisFlow';

interface FactorFlowDrawerProps {
  open: boolean;
  onClose: () => void;
  factorId?: string;
}

const FactorFlowDrawer: React.FC<FactorFlowDrawerProps> = ({ open, onClose, factorId }) => {
  // 概览流程图
  const overviewOption = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      formatter: '{b}',
    },
    animationDurationUpdate: 1500,
    animationEasingUpdate: 'quinticInOut',
    series: [
      {
        type: 'graph',
        layout: 'none',
        symbolSize: 65,
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
          { name: '数据加载', x: 100, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '价格复权', x: 250, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '状态过滤', x: 400, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '因子计算', x: 550, y: 120, itemStyle: { color: '#10B981' } },
          { name: '停牌处理', x: 700, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '质量检查', x: 850, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '结果保存', x: 550, y: 240, itemStyle: { color: '#0077FA' } },
          { name: '因子分析', x: 550, y: 360, itemStyle: { color: '#8B5CF6' } },
          { name: 'IC 分析', x: 300, y: 460, itemStyle: { color: '#10B981' } },
          { name: '分组回测', x: 500, y: 460, itemStyle: { color: '#10B981' } },
          { name: '换手率分析', x: 700, y: 460, itemStyle: { color: '#10B981' } },
        ],
        links: [
          { source: '数据加载', target: '价格复权' },
          { source: '价格复权', target: '状态过滤' },
          { source: '状态过滤', target: '因子计算' },
          { source: '因子计算', target: '停牌处理' },
          { source: '停牌处理', target: '质量检查' },
          { source: '质量检查', target: '结果保存' },
          { source: '结果保存', target: '因子分析' },
          { source: '因子分析', target: 'IC 分析' },
          { source: '因子分析', target: '分组回测' },
          { source: '因子分析', target: '换手率分析' },
        ],
        lineStyle: {
          opacity: 0.9,
          width: 2,
          curveness: 0.2,
        },
      },
    ],
  };

  const OverviewTab = () => (
    <div>
      <Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>端到端流程图</span>}
      >
        <ReactECharts option={overviewOption} style={{ height: 520 }} />
      </Card>

      <Card
        style={{ background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>快速说明</span>}
      >
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}>
          <div style={{ padding: 12, background: 'var(--bg-tertiary)', borderRadius: 6 }}>
            <div style={{ fontWeight: 600, color: '#0077FA', marginBottom: 8 }}>
              🔵 因子计算阶段
            </div>
            <ul style={{ margin: 0, paddingLeft: 16, color: 'var(--text-secondary)', fontSize: 12 }}>
              <li>数据加载和预处理</li>
              <li>价格复权和状态过滤</li>
              <li>用户定义因子计算</li>
              <li>质量检查和结果保存</li>
            </ul>
          </div>
          <div style={{ padding: 12, background: 'var(--bg-tertiary)', borderRadius: 6 }}>
            <div style={{ fontWeight: 600, color: '#10B981', marginBottom: 8 }}>
              🟢 因子分析阶段
            </div>
            <ul style={{ margin: 0, paddingLeft: 16, color: 'var(--text-secondary)', fontSize: 12 }}>
              <li>IC/IR 计算</li>
              <li>分组回测</li>
              <li>换手率分析</li>
              <li>行业和衰减分析</li>
            </ul>
          </div>
        </div>
      </Card>
    </div>
  );

  const tabItems = [
    {
      key: 'overview',
      label: <span><ApartmentOutlined /> 概览</span>,
      children: <OverviewTab />,
    },
    {
      key: 'calculation',
      label: <span><ApiOutlined /> 计算流程</span>,
      children: <FactorCalculationFlow />,
    },
    {
      key: 'analysis',
      label: <span><BarChartOutlined /> 分析流程</span>,
      children: <FactorAnalysisFlow />,
    },
  ];

  return (
    <Drawer
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <ApartmentOutlined style={{ color: 'var(--color-primary)' }} />
          <span>因子流程框架</span>
          {factorId && (
            <span style={{ fontSize: 13, color: 'var(--text-muted)' }}>
              · {factorId}
            </span>
          )}
        </div>
      }
      open={open}
      onClose={onClose}
      width={900}
      styles={{ body: { padding: '16px 24px' } }}
    >
      <Tabs defaultActiveKey="overview" items={tabItems} size="middle" />
    </Drawer>
  );
};

export default FactorFlowDrawer;
