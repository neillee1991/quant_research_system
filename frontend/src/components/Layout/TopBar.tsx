import React, { useState } from 'react';
import { Breadcrumb, Button, Tooltip, Space, Drawer } from 'antd';
import { SunOutlined, MoonOutlined, SettingOutlined } from '@ant-design/icons';
import { useLocation } from 'react-router-dom';
import { useThemeStore } from '../../store';
import TaskMonitor from './TaskMonitor';
import ConfigManagement from '../../pages/ConfigManagement';

const routeNameMap: Record<string, string> = {
  '/': '数据中心',
  '/market': '行情中心',
  '/factor': '因子中心',
  '/strategy': '策略中心',
  '/scheduler': '调度中心',
  '/index-pool': '股票池',
};

const TopBar: React.FC = () => {
  const location = useLocation();
  const { mode, toggle } = useThemeStore();
  const [configOpen, setConfigOpen] = useState(false);

  const currentName = () => {
    const path = location.pathname;
    const match = Object.keys(routeNameMap).find(
      (key) => key !== '/' && path.startsWith(key)
    );
    return routeNameMap[match || '/'];
  };

  const barStyle: React.CSSProperties = {
    height: 48,
    background: 'var(--bg-toolbar)',
    borderBottom: '1px solid var(--border-default)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '0 20px',
    flexShrink: 0,
  };

  return (
    <div style={barStyle}>
      <Breadcrumb
        items={[
          { title: '量化研究系统' },
          { title: currentName() },
        ]}
      />
      <Space size="small">
        <TaskMonitor />
        <Tooltip title="配置管理">
          <Button
            type="text"
            icon={<SettingOutlined />}
            onClick={() => setConfigOpen(true)}
            style={{ color: 'var(--text-primary)' }}
          />
        </Tooltip>
        <Tooltip title={mode === 'dark' ? '切换亮色模式' : '切换暗色模式'}>
          <Button
            type="text"
            icon={mode === 'dark' ? <SunOutlined /> : <MoonOutlined />}
            onClick={toggle}
            style={{ color: 'var(--text-primary)' }}
          />
        </Tooltip>
      </Space>
      <Drawer
        title="配置管理"
        open={configOpen}
        onClose={() => setConfigOpen(false)}
        width={900}
        destroyOnClose
      >
        <ConfigManagement />
      </Drawer>
    </div>
  );
};

export default TopBar;
