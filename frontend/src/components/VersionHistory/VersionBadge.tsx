import React from 'react';
import { Tag, Tooltip } from '@douyinfe/semi-ui';
import { IconHistory } from '@douyinfe/semi-icons';
import dayjs from 'dayjs';

interface VersionBadgeProps {
  version: number;
  changedBy?: string;
  changeReason?: string;
  createdAt?: string;
  size?: 'small' | 'default' | 'large';
  showIcon?: boolean;
  onClick?: () => void;
}

export const VersionBadge: React.FC<VersionBadgeProps> = ({
  version,
  changedBy,
  changeReason,
  createdAt,
  size = 'default',
  showIcon = false,
  onClick,
}) => {
  const tooltipContent = (
    <div>
      <div><strong>版本:</strong> v{version}</div>
      {changedBy && <div><strong>修改人:</strong> {changedBy}</div>}
      {changeReason && <div><strong>修改原因:</strong> {changeReason}</div>}
      {createdAt && <div><strong>时间:</strong> {dayjs(createdAt).format('YYYY-MM-DD HH:mm:ss')}</div>}
    </div>
  );

  const badge = (
    <Tag
      color="blue"
      size={size}
      style={{ cursor: onClick ? 'pointer' : 'default' }}
      onClick={onClick}
    >
      {showIcon && <IconHistory style={{ marginRight: 4 }} />}
      v{version}
    </Tag>
  );

  if (changedBy || changeReason || createdAt) {
    return <Tooltip content={tooltipContent}>{badge}</Tooltip>;
  }

  return badge;
};

export default VersionBadge;
