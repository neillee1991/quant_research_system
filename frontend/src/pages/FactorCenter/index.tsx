/**
 * 因子中心主页面
 */

import React from 'react';
import { Tabs } from 'antd';
import { BarChartOutlined, UnorderedListOutlined } from '@ant-design/icons';
import FactorManageTab from './FactorManageTab';
import AnalysisPanel from './AnalysisPanel';

const FactorCenter: React.FC = () => (
  <div style={{ padding: '8px', maxWidth: '1600px', margin: '0 auto' }}>
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
