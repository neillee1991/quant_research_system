import React from 'react';
import { Button, Tooltip } from '@douyinfe/semi-ui';
import { IconRefresh, IconExternalOpen } from '@douyinfe/semi-icons';

const SchedulerCenter: React.FC = () => {
  const prefectUrl = process.env.REACT_APP_PREFECT_URL || 'http://localhost:4200';

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header with title + action buttons */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        marginBottom: 16,
      }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 600, color: 'var(--text-primary)' }}>调度中心</h2>
          <p style={{ margin: '4px 0 0', fontSize: 13, color: 'var(--text-secondary)' }}>Prefect 工作流调度管理</p>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Tooltip content="刷新">
            <Button theme="borderless" icon={<IconRefresh />} onClick={() => {
              const iframe = document.querySelector('iframe');
              if (iframe) iframe.src = iframe.src;
            }} />
          </Tooltip>
          <Tooltip content="在新标签页打开">
            <Button theme="borderless" icon={<IconExternalOpen />} onClick={() => window.open(prefectUrl, '_blank')} />
          </Tooltip>
        </div>
      </div>

      {/* Iframe */}
      <div style={{
        flex: 1, borderRadius: 8, overflow: 'hidden',
        border: '1px solid var(--border-color)',
        minHeight: 'calc(100vh - 180px)',
      }}>
        <iframe src={prefectUrl} style={{ width: '100%', height: '100%', border: 'none', minHeight: 'calc(100vh - 180px)' }} title="Prefect Dashboard" />
      </div>
    </div>
  );
};

export default SchedulerCenter;
