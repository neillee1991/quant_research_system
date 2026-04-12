import React, { useState } from 'react';
import { Menu } from 'antd';
import {
  ExperimentOutlined,
  LineChartOutlined,
  ScheduleOutlined,
  FundOutlined,
  SettingOutlined,
  SyncOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { useThemeStore, useNavStore } from '../../store';

const Sidebar: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { mode } = useThemeStore();
  const { dataTab, setDataTab } = useNavStore();

  const menuItems = [
    {
      type: 'group' as const,
      label: '数据',
      children: [
        { key: 'data-sync', label: '同步任务', icon: <SyncOutlined /> },
        { key: 'data-etl',  label: 'ETL 任务', icon: <PlayCircleOutlined /> },
        { key: 'data-scheduler', label: '调度任务', icon: <ScheduleOutlined /> },
      ],
    },
    {
      type: 'group' as const,
      label: '研究',
      children: [
        { key: '/market',   label: '行情', icon: <FundOutlined /> },
        { key: '/factor',   label: '因子', icon: <ExperimentOutlined /> },
        { key: '/strategy', label: '策略', icon: <LineChartOutlined /> },
      ],
    },
    {
      type: 'group' as const,
      label: '系统',
      children: [
        { key: '/config', label: '系统配置', icon: <SettingOutlined /> },
      ],
    },
  ];

  const selectedKey = (): string => {
    const path = location.pathname;
    if (path === '/') return dataTab === '1' ? 'data-sync' : dataTab === '2' ? 'data-etl' : 'data-scheduler';
    const routes = ['/market', '/factor', '/strategy', '/scheduler', '/config'];
    const match = routes.find((r) => path.startsWith(r));
    return match || 'data-sync';
  };

  const handleClick = ({ key }: { key: string }) => {
    if (key === 'data-sync') {
      setDataTab('1');
      navigate('/');
    } else if (key === 'data-etl') {
      setDataTab('2');
      navigate('/');
    } else if (key === 'data-scheduler') {
      setDataTab('3');
      navigate('/');
    } else {
      navigate(key);
    }
  };

  const sidebarStyle: React.CSSProperties = {
    width: collapsed ? 60 : 200,
    height: '100vh',
    background: 'var(--bg-sidebar)',
    borderRight: '1px solid var(--border-default)',
    display: 'flex',
    flexDirection: 'column',
    flexShrink: 0,
    transition: 'width 200ms ease',
    overflow: 'hidden',
  };

  const logoStyle: React.CSSProperties = {
    height: 48,
    display: 'flex',
    alignItems: 'center',
    justifyContent: collapsed ? 'center' : 'flex-start',
    padding: collapsed ? '0' : '0 20px',
    color: 'var(--text-primary)',
    fontWeight: 700,
    fontSize: 15,
    whiteSpace: 'nowrap',
    borderBottom: '1px solid var(--border-default)',
  };

  const toggleBtnStyle: React.CSSProperties = {
    height: 40,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    cursor: 'pointer',
    color: 'var(--text-secondary)',
    borderTop: '1px solid var(--border-default)',
    fontSize: 16,
    userSelect: 'none',
  };

  return (
    <div style={sidebarStyle}>
      <div style={logoStyle}>
        {collapsed ? '⚡' : '⚡ 量化研究系统'}
      </div>
      <div style={{ flex: 1, overflow: 'hidden' }}>
        <Menu
          mode="inline"
          inlineCollapsed={collapsed}
          selectedKeys={[selectedKey()]}
          items={menuItems}
          onClick={handleClick}
          style={{ background: 'transparent', border: 'none', height: '100%' }}
          theme={mode === 'dark' ? 'dark' : 'light'}
        />
      </div>
      <div style={toggleBtnStyle} onClick={() => setCollapsed(!collapsed)}>
        {collapsed ? '»' : '« 收起'}
      </div>
    </div>
  );
};

export default Sidebar;
