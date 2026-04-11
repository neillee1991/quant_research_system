/**
 * 统一通知工具 — 右下角小卡片，success/info 1.5s，error/warning 3s
 */
import type { CSSProperties } from 'react';
import { notification } from 'antd';

const baseStyle: CSSProperties = {
  padding: '8px 12px',
  minWidth: 0,
  width: 'auto',
  maxWidth: 320,
};

const open = (
  type: 'success' | 'error' | 'info' | 'warning',
  content: string,
  duration: number,
) => {
  notification[type]({
    message: content,
    description: null,
    placement: 'bottomRight',
    duration,
    style: baseStyle,
  });
};

export const notify = {
  success: (content: string) => open('success', content, 1.5),
  error:   (content: string) => open('error',   content, 3),
  info:    (content: string) => open('info',    content, 1.5),
  warning: (content: string) => open('warning', content, 1.5),
};
