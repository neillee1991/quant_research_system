import React from 'react';
import { Breadcrumb, Button, Tooltip, Space } from 'antd';
import { SunOutlined, MoonOutlined, SettingOutlined } from '@ant-design/icons';
import { useLocation, useNavigate } from 'react-router-dom';
import { useThemeStore } from '../../store';
import TaskMonitor from './TaskMonitor';

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
  const navigate = useNavigate();
  const { mode, toggle } = useThemeStore();

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
        <Tooltip title="配置中心">
          <Button
            type="text"
            icon={<SettingOutlined />}
            onClick={() => navigate('/config')}
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
    </div>
  );
};

export default TopBar;
