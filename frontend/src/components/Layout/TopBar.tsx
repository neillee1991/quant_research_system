import React from 'react';
import { Breadcrumb, Button, Tooltip, Space } from 'antd';
import { SunOutlined, MoonOutlined } from '@ant-design/icons';
import { useLocation } from 'react-router-dom';
import { useThemeStore, useNavStore } from '../../store';

const routeMap: Record<string, { group: string; name: string }> = {
  '/market':    { group: '研究', name: '行情' },
  '/factor':    { group: '研究', name: '因子' },
  '/strategy':  { group: '研究', name: '策略' },
  '/scheduler': { group: '数据', name: '调度' },
  '/config':    { group: '系统', name: '系统配置' },
};

const TopBar: React.FC = () => {
  const location = useLocation();
  const { mode, toggle } = useThemeStore();
  const { dataTab } = useNavStore();

  const currentRoute = (): { group: string; name: string } => {
    const path = location.pathname;
    if (path === '/') {
      const nameMap = { '1': '同步任务', '2': 'ETL 任务', '3': '调度任务' };
      return { group: '数据', name: nameMap[dataTab] ?? '同步任务' };
    }
    const match = Object.keys(routeMap).find((key) => path.startsWith(key));
    return match ? routeMap[match] : { group: '数据', name: '同步任务' };
  };

  const route = currentRoute();

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
          { title: route.group },
          { title: route.name },
        ]}
      />
      <Space size="small">
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
