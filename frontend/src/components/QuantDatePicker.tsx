import React from 'react';
import { DatePicker } from '@douyinfe/semi-ui';
import dayjs from 'dayjs';

// ---- 类型定义 ----

interface RangeProps {
  mode?: 'range';
  value?: [string, string];           // YYYYMMDD
  onChange?: (start: string, end: string) => void;
  presets?: boolean;
  disableFuture?: boolean;
  size?: 'small' | 'default' | 'large';
  style?: React.CSSProperties;
  placeholder?: [string, string];
}

interface SingleProps {
  mode: 'single';
  value?: string;                     // YYYYMMDD
  onChange?: (date: string) => void;
  disableFuture?: boolean;
  size?: 'small' | 'default' | 'large';
  style?: React.CSSProperties;
  placeholder?: string;
}

type QuantDatePickerProps = RangeProps | SingleProps;

// ---- 预设区间 ----

const PRESETS = [
  { text: '最近7天',  start: () => dayjs().subtract(6, 'day'),  end: () => dayjs() },
  { text: '最近30天', start: () => dayjs().subtract(29, 'day'), end: () => dayjs() },
  { text: '最近3个月',start: () => dayjs().subtract(89, 'day'), end: () => dayjs() },
  { text: '今年',     start: () => dayjs().startOf('year'),     end: () => dayjs() },
  { text: '去年',     start: () => dayjs().subtract(1, 'year').startOf('year'),
                      end:   () => dayjs().subtract(1, 'year').endOf('year') },
];

// ---- 工具函数 ----

function toDate(yyyymmdd: string): Date | undefined {
  if (!yyyymmdd || yyyymmdd.length !== 8) return undefined;
  const d = dayjs(yyyymmdd, 'YYYYMMDD');
  return d.isValid() ? d.toDate() : undefined;
}

function toYYYYMMDD(dateStr: string): string {
  if (!dateStr) return '';
  const d = dayjs(dateStr);
  return d.isValid() ? d.format('YYYYMMDD') : '';
}

function disabledFuture(current: Date | null | undefined): boolean {
  return current ? dayjs(current).isAfter(dayjs().endOf('day')) : false;
}

// ---- 组件 ----

const QuantDatePicker: React.FC<QuantDatePickerProps> = (props) => {
  if (props.mode === 'single') {
    const { value, onChange, disableFuture = false, size = 'small', style, placeholder } = props;
    return (
      <DatePicker
        type="date"
        size={size}
        style={style}
        placeholder={placeholder}
        value={toDate(value || '')}
        defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
        disabledDate={disableFuture ? disabledFuture : undefined}
        onChange={(_date, dateStr) => {
          onChange?.(toYYYYMMDD(typeof dateStr === 'string' ? dateStr : ''));
        }}
      />
    );
  }

  // range mode (default)
  const {
    value,
    onChange,
    presets = true,
    disableFuture: df = true,
    size = 'small',
    style,
    placeholder = ['开始日期', '结束日期'],
  } = props as RangeProps;

  const rangeValue: [Date, Date] | undefined =
    value?.[0] && value?.[1]
      ? [dayjs(value[0], 'YYYYMMDD').toDate(), dayjs(value[1], 'YYYYMMDD').toDate()]
      : undefined;

  const presetList = presets
    ? PRESETS.map((p) => ({
        text: p.text,
        start: p.start().toDate(),
        end: p.end().toDate(),
      }))
    : undefined;

  return (
    <DatePicker
      type="dateRange"
      size={size}
      style={style}
      placeholder={placeholder}
      value={rangeValue}
      defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
      disabledDate={df ? disabledFuture : undefined}
      presets={presetList}
      onChange={(date, dateStr) => {
        // Semi UI returns string[] for dateRange type, but typings are loose
        // When using presets, dateStr may be undefined — fall back to date array
        const strs = Array.isArray(dateStr) ? (dateStr as string[]) : [];
        if (strs[0] && strs[1]) {
          onChange?.(toYYYYMMDD(strs[0]), toYYYYMMDD(strs[1]));
        } else if (Array.isArray(date) && date[0] && date[1]) {
          onChange?.(dayjs(date[0]).format('YYYYMMDD'), dayjs(date[1]).format('YYYYMMDD'));
        } else {
          onChange?.('', '');
        }
      }}
    />
  );
};

export default QuantDatePicker;
