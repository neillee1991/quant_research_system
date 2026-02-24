import React, { useState } from 'react';
import { Nav } from '@douyinfe/semi-ui';
import { IconServer, IconTestScoreStroked, IconLineChartStroked, IconCalendarClock } from '@douyinfe/semi-icons';
import { useNavigate, useLocation } from 'react-router-dom';

const navItems = [
  { itemKey: '/', text: '数据中心', icon: <IconServer /> },
  { itemKey: '/factor', text: '因子中心', icon: <IconTestScoreStroked /> },
  { itemKey: '/strategy', text: '策略中心', icon: <IconLineChartStroked /> },
  { itemKey: '/scheduler', text: '调度中心', icon: <IconCalendarClock /> },
];

const Sidebar: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();

  const selectedKey = () => {
    const path = location.pathname;
    const match = navItems.find(
      (item) => item.itemKey !== '/' && path.startsWith(item.itemKey)
    );
    return match ? match.itemKey : '/';
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
        <Nav
          isCollapsed={collapsed}
          selectedKeys={[selectedKey()]}
          items={navItems}
          onSelect={({ itemKey }) => navigate(itemKey as string)}
          style={{ background: 'transparent', border: 'none' }}
          footer={{ collapseButton: false }}
        />
      </div>
      <div style={toggleBtnStyle} onClick={() => setCollapsed(!collapsed)}>
        {collapsed ? '»' : '« 收起'}
      </div>
    </div>
  );
};

export default Sidebar;
