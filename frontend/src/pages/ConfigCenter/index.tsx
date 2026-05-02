/**
 * 统一配置中心
 * 整合数据配置、指数订阅、导入导出三个配置模块
 * 数据配置同时管理因子分析和回测所需的字段映射
 */
import React, { useState } from 'react';
import { Tabs } from 'antd';
import DataMappingsTab from './DataMappingsTab';
import IndexSubscribeTab from './IndexSubscribeTab';
import ImportExportTab from './ImportExportTab';

const ConfigCenter: React.FC = () => {
  const [activeKey, setActiveKey] = useState('data-mappings');

  return (
    <div style={{ height: '100%', overflow: 'auto' }}>
      <Tabs
        activeKey={activeKey}
        onChange={setActiveKey}
        style={{ padding: '0 24px' }}
        items={[
          {
            key: 'data-mappings',
            label: '数据配置',
            children: <DataMappingsTab />,
          },
          {
            key: 'index-subscribe',
            label: '指数订阅',
            children: <IndexSubscribeTab />,
          },
          {
            key: 'import-export',
            label: '导入导出',
            children: <ImportExportTab />,
          },
        ]}
      />
    </div>
  );
};

export default ConfigCenter;
