/**
 * 因子中心主页面
 */

import React from 'react';
import { Tabs } from 'antd';
import { BarChartOutlined, UnorderedListOutlined } from '@ant-design/icons';
import FactorManageTab from './FactorManageTab';
import AnalysisPanel from './AnalysisPanel';

const FactorCenter: React.FC = () => (
  <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
    <div style={{ marginBottom: '16px' }}>
      <h1
        style={{
          color: 'var(--color-primary)',
          fontSize: '24px',
          fontWeight: 700,
          margin: 0,
          letterSpacing: '1px',
        }}
      >
        <UnorderedListOutlined style={{ marginRight: '8px' }} />
        因子
      </h1>
      <p style={{ color: 'var(--text-secondary)', margin: '4px 0 0 0', fontSize: '12px' }}>
        因子注册管理与 IC 分析
      </p>
    </div>

    <Tabs defaultActiveKey="factors" items={[
      {
        key: 'factors',
        label: <span><UnorderedListOutlined /> 因子管理</span>,
        children: <FactorManageTab />,
      },
      {
        key: 'analysis',
        label: <span><BarChartOutlined /> 因子分析</span>,
        children: <AnalysisPanel />,
      },
    ]} />
  </div>
);

export default FactorCenter;
