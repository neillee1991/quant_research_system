/**
 * 因子中心主页面
 */

import React from 'react';
import { Tabs, TabPane } from '@douyinfe/semi-ui';
import { IconTestScoreStroked, IconBarChartHStroked, IconSetting } from '@douyinfe/semi-icons';
import FactorManageTab from './FactorManageTab';
import AnalysisPanel from './AnalysisPanel';
import DataConfigPanel from './DataConfigPanel';

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
        <IconTestScoreStroked style={{ marginRight: '8px' }} />
        因子
      </h1>
      <p style={{ color: 'var(--text-secondary)', margin: '4px 0 0 0', fontSize: '12px' }}>
        因子注册管理与 IC 分析
      </p>
    </div>

    <Tabs defaultActiveKey="factors">
      <TabPane
        itemKey="factors"
        tab={
          <span>
            <IconTestScoreStroked /> 因子管理
          </span>
        }
      >
        <FactorManageTab />
      </TabPane>
      <TabPane
        itemKey="analysis"
        tab={
          <span>
            <IconBarChartHStroked /> 因子分析
          </span>
        }
      >
        <AnalysisPanel />
      </TabPane>
      <TabPane
        itemKey="dataconfig"
        tab={
          <span>
            <IconSetting /> 数据配置
          </span>
        }
      >
        <DataConfigPanel />
      </TabPane>
    </Tabs>
  </div>
);

export default FactorCenter;
