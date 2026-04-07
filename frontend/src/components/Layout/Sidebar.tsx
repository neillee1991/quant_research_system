import React, { useState } from 'react';
import { Menu } from 'antd';
import {
  DatabaseOutlined,
  ExperimentOutlined,
  LineChartOutlined,
  ScheduleOutlined,
  FundOutlined,
  AppstoreOutlined,
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';

const menuItems = [
  { key: '/market', label: '行情', icon: <FundOutlined /> },
  { key: '/', label: '数据', icon: <DatabaseOutlined /> },
  { key: '/factor', label: '因子', icon: <ExperimentOutlined /> },
  { key: '/strategy', label: '策略', icon: <LineChartOutlined /> },
  { key: '/scheduler', label: '调度', icon: <ScheduleOutlined /> },
];

const Sidebar: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();

  const selectedKey = () => {
    const path = location.pathname;
    if (path === '/') return '/';
    if (path.startsWith('/market')) return '/market';
    const match = menuItems.find(
      (item) => item.key !== '/' && item.key !== '/market' && path.startsWith(item.key)
    );
    return match ? match.key : '/';
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
          onClick={({ key }) => navigate(key)}
          style={{ background: 'transparent', border: 'none', height: '100%' }}
          theme="dark"
        />
      </div>
      <div style={toggleBtnStyle} onClick={() => setCollapsed(!collapsed)}>
        {collapsed ? '»' : '« 收起'}
      </div>
    </div>
  );
};

export default Sidebar;
