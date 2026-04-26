/**
 * 统一通知工具 — 右下角一行小字，success/info 1.5s，error/warning 3s
 */
import { notification } from 'antd';

const COLORS = {
  success: '#52c41a',
  error:   '#ff4d4f',
  info:    '#1677ff',
  warning: '#faad14',
};

const BG = {
  success: 'rgba(82,196,26,0.08)',
  error:   'rgba(255,77,79,0.08)',
  info:    'rgba(22,119,255,0.08)',
  warning: 'rgba(250,173,20,0.08)',
};

const open = (type: 'success' | 'error' | 'info' | 'warning', content: string, duration: number) => {
  notification.open({
    message: null,
    description: content,
    placement: 'bottomRight',
    duration,
    icon: null,
    closeIcon: null,
    className: `notify-${type}`,
    style: {
      // CSS 变量传给 global.css 使用
      ['--notify-color' as string]: COLORS[type],
      ['--notify-bg' as string]: BG[type],
      ['--notify-border' as string]: `${COLORS[type]}44`,
    },
  });
};

// 处理 pydantic v2 校验错误数组或普通字符串
export const extractApiError = (detail: unknown, fallback = '操作失败'): string => {
  if (!detail) return fallback;
  if (typeof detail === 'string') return detail;
  if (Array.isArray(detail)) {
    return detail.map((e: any) => e?.msg ?? JSON.stringify(e)).join('; ');
  }
  return fallback;
};

export const notify = {
  success: (content: string) => open('success', content, 1.5),
  error:   (content: string) => open('error',   content, 3),
  info:    (content: string) => open('info',    content, 1.5),
  warning: (content: string) => open('warning', content, 1.5),
};
