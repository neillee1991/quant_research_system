import React, { useState, useEffect } from 'react';
import { DatePicker } from 'antd';
import dayjs, { Dayjs } from 'dayjs';

// ---- 类型定义 ----

interface RangeProps {
  mode?: 'range';
  value?: [string, string];           // YYYYMMDD
  onChange?: (start: string, end: string) => void;
  presets?: boolean;
  disableFuture?: boolean;
  size?: 'small' | 'middle' | 'large';
  style?: React.CSSProperties;
  placeholder?: [string, string];
}

interface SingleProps {
  mode: 'single';
  value?: string;                     // YYYYMMDD
  onChange?: (date: string) => void;
  disableFuture?: boolean;
  size?: 'small' | 'middle' | 'large';
  style?: React.CSSProperties;
  placeholder?: string;
}

type QuantDatePickerProps = RangeProps | SingleProps;

// ---- 预设区间 ----

const PRESETS = [
  { label: '最近7天',  value: [dayjs().subtract(6, 'day'), dayjs()] as [Dayjs, Dayjs] },
  { label: '最近30天', value: [dayjs().subtract(29, 'day'), dayjs()] as [Dayjs, Dayjs] },
  { label: '最近3个月',value: [dayjs().subtract(89, 'day'), dayjs()] as [Dayjs, Dayjs] },
  { label: '今年',     value: [dayjs().startOf('year'), dayjs()] as [Dayjs, Dayjs] },
  { label: '去年',     value: [dayjs().subtract(1, 'year').startOf('year'), dayjs().subtract(1, 'year').endOf('year')] as [Dayjs, Dayjs] },
];

// ---- 工具函数 ----

function toDayjs(yyyymmdd: string): Dayjs | null {
  if (!yyyymmdd || yyyymmdd.length !== 8) return null;
  const d = dayjs(yyyymmdd, 'YYYYMMDD');
  return d.isValid() ? d : null;
}

function toYYYYMMDD(d: Dayjs | null): string {
  return d?.isValid() ? d.format('YYYYMMDD') : '';
}

// ---- 组件 ----

const QuantDatePicker: React.FC<QuantDatePickerProps> = (props) => {
  if (props.mode === 'single') {
    const { value, onChange, disableFuture = false, size = 'middle', style, placeholder } = props;
    return (
      <DatePicker
        size={size}
        style={style}
        placeholder={placeholder}
        value={toDayjs(value || '')}
        disabledDate={disableFuture ? (current) => current && current.isAfter(dayjs().endOf('day')) : undefined}
        onChange={(date) => {
          onChange?.(toYYYYMMDD(date));
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
    size = 'middle',
    style,
    placeholder = ['开始日期', '结束日期'],
  } = props as RangeProps;

  const rangeValue: [Dayjs | null, Dayjs | null] | undefined = value
    ? [toDayjs(value[0]), toDayjs(value[1])]
    : undefined;

  return (
    <DatePicker.RangePicker
      size={size}
      style={style}
      placeholder={placeholder}
      value={rangeValue}
      disabledDate={df ? (current) => current && current.isAfter(dayjs().endOf('day')) : undefined}
      presets={presets ? PRESETS : undefined}
      onChange={(dates) => {
        if (dates && dates[0] && dates[1]) {
          onChange?.(toYYYYMMDD(dates[0]), toYYYYMMDD(dates[1]));
        }
      }}
    />
  );
};

export default QuantDatePicker;
