import React, { useState } from 'react';
import { Select, Spin, Tooltip } from '@douyinfe/semi-ui';
import { IconHistory } from '@douyinfe/semi-icons';

interface VersionSelectorProps {
  currentVersion: number;
  versions: Array<{
    version: number;
    changed_by?: string;
    change_reason?: string;
    created_at?: string;
  }>;
  onChange: (version: number) => void;
  loading?: boolean;
  disabled?: boolean;
  style?: React.CSSProperties;
}

export const VersionSelector: React.FC<VersionSelectorProps> = ({
  currentVersion,
  versions,
  onChange,
  loading = false,
  disabled = false,
  style,
}) => {
  const options = versions.map((v) => ({
    value: v.version,
    label: `v${v.version}${v.version === currentVersion ? ' (当前)' : ''}`,
    otherKey: v,
  }));

  return (
    <Select
      value={currentVersion}
      onChange={(value) => onChange(value as number)}
      style={{ width: 150, ...style }}
      disabled={disabled || loading}
      prefix={<IconHistory />}
      optionList={options}
      renderSelectedItem={(optionNode: any) => (
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          {loading && <Spin size="small" />}
          <span>版本 v{currentVersion}</span>
        </div>
      )}
      renderOptionItem={(renderProps) => {
        const { version, changed_by, change_reason } = renderProps.otherKey;
        return (
          <Tooltip
            content={
              <div>
                {changed_by && <div>修改人: {changed_by}</div>}
                {change_reason && <div>原因: {change_reason}</div>}
              </div>
            }
            position="right"
          >
            <div>
              <div style={{ fontWeight: version === currentVersion ? 'bold' : 'normal' }}>
                v{version} {version === currentVersion && '(当前)'}
              </div>
              {change_reason && (
                <div style={{ fontSize: 12, color: 'var(--semi-color-text-2)', marginTop: 2 }}>
                  {change_reason.length > 30 ? `${change_reason.slice(0, 30)}...` : change_reason}
                </div>
              )}
            </div>
          </Tooltip>
        );
      }}
    />
  );
};

export default VersionSelector;
