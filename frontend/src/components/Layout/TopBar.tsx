import React from 'react';
import { Breadcrumb, Button, Tooltip } from '@douyinfe/semi-ui';
import { IconSun, IconMoon } from '@douyinfe/semi-icons';
import { useLocation } from 'react-router-dom';
import { useThemeStore } from '../../store';

const routeNameMap: Record<string, string> = {
  '/': '数据中心',
  '/factor': '因子中心',
  '/strategy': '策略中心',
  '/scheduler': '调度中心',
};

const TopBar: React.FC = () => {
  const location = useLocation();
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
      <Breadcrumb>
        <Breadcrumb.Item>量化研究系统</Breadcrumb.Item>
        <Breadcrumb.Item>{currentName()}</Breadcrumb.Item>
      </Breadcrumb>
      <Tooltip content={mode === 'dark' ? '切换亮色模式' : '切换暗色模式'}>
        <Button
          theme="borderless"
          icon={mode === 'dark' ? <IconSun /> : <IconMoon />}
          onClick={toggle}
          style={{ color: 'var(--text-primary)' }}
        />
      </Tooltip>
    </div>
  );
};

export default TopBar;
