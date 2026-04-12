/**
 * 统一配置中心
 * 整合字段映射、指数订阅、导入导出三个配置模块
 */
import React, { useState } from 'react';
import { Tabs } from 'antd';
import FieldMappingsTab from './FieldMappingsTab';
import IndexSubscribeTab from './IndexSubscribeTab';
import ImportExportTab from './ImportExportTab';

const ConfigCenter: React.FC = () => {
  const [activeKey, setActiveKey] = useState('field-mappings');

  return (
    <div style={{ height: '100%', overflow: 'auto' }}>
      <Tabs
        activeKey={activeKey}
        onChange={setActiveKey}
        style={{ padding: '0 24px' }}
        items={[
          {
            key: 'field-mappings',
            label: '字段映射',
            children: <FieldMappingsTab />,
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
