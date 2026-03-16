# QuantDatePicker 统一封装实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 封装统一的 QuantDatePicker 组件，内置快捷预设、禁用未来日期、YYYYMMDD 格式转换，替换项目中全部 15 处 DatePicker 用法。

**Architecture:** 新建 `src/components/QuantDatePicker.tsx`，支持 range（默认）和 single 两种模式。所有格式转换、disabledDate、defaultPickerValue、presets 逻辑内聚在组件内部，调用方只传 YYYYMMDD 字符串。逐文件替换现有 DatePicker，AnalysisPanel 的两个单独 DatePicker 合并为一个 range。

**Tech Stack:** React 18, TypeScript, Semi UI DatePicker, dayjs

---

### Task 1: 创建 QuantDatePicker 组件

**Files:**
- Create: `frontend/src/components/QuantDatePicker.tsx`

**Step 1: 创建组件文件**

```tsx
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
  return dayjs(yyyymmdd, 'YYYYMMDD').toDate();
}

function toYYYYMMDD(dateStr: string): string {
  return dateStr ? dateStr.replace(/-/g, '') : '';
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
        start: p.start(),
        end: p.end(),
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
      onChange={(_date, dateStr) => {
        const strs = dateStr as unknown as string[];
        if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
          onChange?.(toYYYYMMDD(strs[0]), toYYYYMMDD(strs[1]));
        } else {
          onChange?.('', '');
        }
      }}
    />
  );
};

export default QuantDatePicker;
```

**Step 2: 确认文件存在**

```bash
ls frontend/src/components/QuantDatePicker.tsx
```

Expected: 文件存在

**Step 3: Commit**

```bash
git add frontend/src/components/QuantDatePicker.tsx
git commit -m "feat: add QuantDatePicker component with presets and YYYYMMDD format"
```

---

### Task 2: 替换 DataCenter/Modals.tsx（3处）

**Files:**
- Modify: `frontend/src/pages/DataCenter/Modals.tsx`

**Context:** 文件中有 3 处 `type="dateRange"` 的 DatePicker，分别在增量同步、批量同步、ETL 回溯模态框中。每处的 onChange 都是把 dateStr 数组转成 YYYYMMDD 后调用 `onStartDateChange` / `onEndDateChange`。

**Step 1: 在文件顶部添加 import**

找到现有的 DatePicker import 行：
```tsx
import { ..., DatePicker, ... } from '@douyinfe/semi-ui';
```
在其下方添加：
```tsx
import QuantDatePicker from '../../components/QuantDatePicker';
```

**Step 2: 替换第1处（增量同步，约第71行）**

将：
```tsx
              <DatePicker
                type="dateRange"
                placeholder={['开始日期', '结束日期']}
                defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
                value={
                  startDate && endDate
                    ? [
                        dayjs(startDate, 'YYYYMMDD').toDate(),
                        dayjs(endDate, 'YYYYMMDD').toDate(),
                      ]
                    : undefined
                }
                onChange={(date, dateStr) => {
                  const strs = dateStr as unknown as string[];
                  if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                    onStartDateChange(strs[0].replace(/-/g, ''));
                    onEndDateChange(strs[1].replace(/-/g, ''));
                  } else {
                    onStartDateChange('');
                    onEndDateChange('');
                  }
                }}
                style={{ width: '100%' }}
                size="small"
              />
```
替换为：
```tsx
              <QuantDatePicker
                value={[startDate, endDate]}
                onChange={(s, e) => { onStartDateChange(s); onEndDateChange(e); }}
                style={{ width: '100%' }}
              />
```

**Step 3: 替换第2处（批量同步，约第223行）和第3处（ETL回溯，约第390行）**

同样模式，找到对应的 DatePicker 块，替换为：
```tsx
              <QuantDatePicker
                value={[startDate, endDate]}
                onChange={(s, e) => { onStartDateChange(s); onEndDateChange(e); }}
                style={{ width: '100%' }}
              />
```

**Step 4: 删除不再使用的 dayjs import（如果 dayjs 在该文件其他地方没有用到则删除）**

检查文件中是否还有其他 `dayjs(` 用法，如果没有则删除 `import dayjs from 'dayjs'`。

**Step 5: Commit**

```bash
git add frontend/src/pages/DataCenter/Modals.tsx
git commit -m "refactor: replace DatePicker with QuantDatePicker in DataCenter/Modals"
```

---

### Task 3: 替换 DataCenter/ETLPanel.tsx 和 SyncPanel.tsx

**Files:**
- Modify: `frontend/src/pages/DataCenter/ETLPanel.tsx`
- Modify: `frontend/src/pages/DataCenter/SyncPanel.tsx`

**Context:** 两个文件各有 1 处 `type="dateRange"` 的 DatePicker，用于日志筛选，size="small"，style={{ width: 280 }}。

**Step 1: ETLPanel.tsx — 添加 import**

```tsx
import QuantDatePicker from '../../components/QuantDatePicker';
```

**Step 2: ETLPanel.tsx — 替换 DatePicker（约第395行）**

找到：
```tsx
            <DatePicker
              type="dateRange"
              placeholder={['开始日期', '结束日期']}
              defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
              size="small"
              style={{ width: 280 }}
              onChange={(date, dateStr) => {
                const strs = dateStr as unknown as string[];
                if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                  setLogFilter(prev => ({ ...prev, start_date: strs[0].replace(/-/g, ''), end_date: strs[1].replace(/-/g, '') }));
                } else {
                  setLogFilter(prev => ({ ...prev, start_date: undefined, end_date: undefined }));
                }
              }}
            />
```
替换为：
```tsx
            <QuantDatePicker
              style={{ width: 280 }}
              onChange={(s, e) => setLogFilter(prev => ({
                ...prev,
                start_date: s || undefined,
                end_date: e || undefined,
              }))}
            />
```

**Step 3: SyncPanel.tsx — 同样操作**

添加 import，找到约第419行的 DatePicker，替换为：
```tsx
            <QuantDatePicker
              style={{ width: 280 }}
              onChange={(s, e) => setLogFilter(prev => ({
                ...prev,
                start_date: s || undefined,
                end_date: e || undefined,
              }))}
            />
```

**Step 4: Commit**

```bash
git add frontend/src/pages/DataCenter/ETLPanel.tsx frontend/src/pages/DataCenter/SyncPanel.tsx
git commit -m "refactor: replace DatePicker with QuantDatePicker in ETLPanel and SyncPanel"
```

---

### Task 4: 替换 DataCenter/ETLTaskDrawer.tsx（单日期）

**Files:**
- Modify: `frontend/src/pages/DataCenter/ETLTaskDrawer.tsx`

**Context:** 约第545行，单日期选择，用于 ETL 脚本测试的测试日期，可选，仅增量任务显示。

**Step 1: 添加 import**

```tsx
import QuantDatePicker from '../../components/QuantDatePicker';
```

**Step 2: 替换 DatePicker（约第545行）**

找到：
```tsx
                  <DatePicker
                    size="small"
                    placeholder="测试日期（可选）"
                    style={{ width: 160 }}
                    onChange={(date, dateStr) => {
                      setTestDate(typeof dateStr === 'string' ? dateStr.replace(/-/g, '') : '');
                    }}
                  />
```
替换为：
```tsx
                  <QuantDatePicker
                    mode="single"
                    placeholder="测试日期（可选）"
                    style={{ width: 160 }}
                    disableFuture={false}
                    onChange={(d) => setTestDate(d)}
                  />
```

**Step 3: Commit**

```bash
git add frontend/src/pages/DataCenter/ETLTaskDrawer.tsx
git commit -m "refactor: replace DatePicker with QuantDatePicker in ETLTaskDrawer"
```

---

### Task 5: 替换 FactorCenter/AnalysisPanel.tsx（2个单独 → 1个 range）

**Files:**
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`

**Context:** 约第307-324行，有"开始日期"和"结束日期"两个独立的 `type="date"` DatePicker，分别调用 `setStartDate` / `setEndDate`。改为一个 QuantDatePicker range，在 onChange 里同时设置两个 state。

**Step 1: 添加 import**

```tsx
import QuantDatePicker from '../../components/QuantDatePicker';
```

**Step 2: 替换两个 DatePicker 为一个 QuantDatePicker**

将：
```tsx
          <div style={{ width: 140 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>开始日期</div>
            <DatePicker
              size="small"
              type="date"
              style={{ width: '100%' }}
              onChange={(date, dateStr) => setStartDate(typeof dateStr === 'string' ? dateStr.replace(/-/g, '') : '')}
            />
          </div>
          <div style={{ width: 140 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>结束日期</div>
            <DatePicker
              size="small"
              type="date"
              style={{ width: '100%' }}
              onChange={(date, dateStr) => setEndDate(typeof dateStr === 'string' ? dateStr.replace(/-/g, '') : '')}
            />
          </div>
```
替换为：
```tsx
          <div style={{ width: 240 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分析区间</div>
            <QuantDatePicker
              value={[startDate, endDate]}
              onChange={(s, e) => { setStartDate(s); setEndDate(e); }}
              disableFuture={false}
              style={{ width: '100%' }}
            />
          </div>
```

**Step 3: Commit**

```bash
git add frontend/src/pages/FactorCenter/AnalysisPanel.tsx
git commit -m "refactor: merge two DatePickers into one QuantDatePicker in AnalysisPanel"
```

---

### Task 6: 替换 FactorCenter/TestPanel.tsx

**Files:**
- Modify: `frontend/src/pages/FactorCenter/TestPanel.tsx`

**Context:** 约第118行，`type="dateRange"`，已有 `disabledDate` 禁用未来日期，onChange 设置 `setDateRange([start, end])`。

**Step 1: 添加 import**

```tsx
import QuantDatePicker from '../../components/QuantDatePicker';
```

**Step 2: 替换 DatePicker（约第118行）**

将：
```tsx
        <DatePicker
          type="dateRange"
          size="small"
          style={{ flex: 1 }}
          defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
          disabledDate={(current) => current ? dayjs(current).isAfter(dayjs().endOf('day')) : false}
          onChange={(date, dateStr) => {
            const strs = dateStr as unknown as string[];
            if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
              setDateRange([strs[0].replace(/-/g, ''), strs[1].replace(/-/g, '')]);
            } else {
              setDateRange(['', '']);
            }
          }}
          placeholder={['开始日期', '结束日期']}
        />
```
替换为：
```tsx
        <QuantDatePicker
          style={{ flex: 1 }}
          onChange={(s, e) => setDateRange([s, e])}
        />
```

**Step 3: Commit**

```bash
git add frontend/src/pages/FactorCenter/TestPanel.tsx
git commit -m "refactor: replace DatePicker with QuantDatePicker in TestPanel"
```

---

### Task 7: 替换 FactorCenter/FactorManageTab.tsx

**Files:**
- Modify: `frontend/src/pages/FactorCenter/FactorManageTab.tsx`

**Context:** 约第459行，`type="dateRange"`，onChange 设置 `setComputeDateRange([start, end])`。

**Step 1: 添加 import**

```tsx
import QuantDatePicker from '../../components/QuantDatePicker';
```

**Step 2: 替换 DatePicker（约第459行）**

找到对应的 DatePicker 块，替换为：
```tsx
                <QuantDatePicker
                  value={computeDateRange}
                  onChange={(s, e) => setComputeDateRange([s, e])}
                  style={{ width: '100%' }}
                />
```

**Step 3: Commit**

```bash
git add frontend/src/pages/FactorCenter/FactorManageTab.tsx
git commit -m "refactor: replace DatePicker with QuantDatePicker in FactorManageTab"
```

---

### Task 8: 替换 FactorCenter.tsx（4处）

**Files:**
- Modify: `frontend/src/pages/FactorCenter.tsx`

**Context:** 4处 dateRange DatePicker，分别在：
- 约第830行：因子测试区间（有 disabledDate）
- 约第1515行：因子全量计算日期范围
- 约第1583行：因子批量计算日期范围
- 约第1816行：因子分析日期范围（无 disabledDate，style={{ width: 240 }}）

**Step 1: 添加 import**

```tsx
import QuantDatePicker from '../components/QuantDatePicker';
```

**Step 2: 替换第1处（约第830行，因子测试区间）**

找到：
```tsx
              <DatePicker
                type="dateRange"
                size="small"
                style={{ flex: 1 }}
                defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
                disabledDate={(current) => current ? dayjs(current).isAfter(dayjs().endOf('day')) : false}
                ...onChange...
              />
```
替换为：
```tsx
              <QuantDatePicker
                style={{ flex: 1 }}
                onChange={(s, e) => { /* 对应原来的 onChange 逻辑 */ }}
              />
```

**Step 3: 替换第2处（约第1515行，全量计算）**

```tsx
                <QuantDatePicker
                  value={[fullStartDate, fullEndDate]}
                  onChange={(s, e) => { setFullStartDate(s); setFullEndDate(e); }}
                  style={{ width: '100%' }}
                />
```

**Step 4: 替换第3处（约第1583行，批量计算）**

```tsx
                <QuantDatePicker
                  value={[batchStartDate, batchEndDate]}
                  onChange={(s, e) => { setBatchStartDate(s); setBatchEndDate(e); }}
                  style={{ width: '100%' }}
                />
```

**Step 5: 替换第4处（约第1816行，因子分析，不禁用未来）**

```tsx
              <QuantDatePicker
                disableFuture={false}
                style={{ width: 240 }}
                onChange={(s, e) => { /* 对应原来的 onChange 逻辑 */ }}
              />
```

> **注意：** FactorCenter.tsx 文件较大，替换时需要先读取每处的实际 state 变量名和 onChange 逻辑，确保对应正确。

**Step 6: Commit**

```bash
git add frontend/src/pages/FactorCenter.tsx
git commit -m "refactor: replace all DatePickers with QuantDatePicker in FactorCenter"
```

---

### Task 9: 清理不再使用的 DatePicker import

**Files:** 所有被修改的文件

**Step 1: 检查每个文件是否还有直接使用 DatePicker 的地方**

```bash
grep -n "DatePicker" frontend/src/pages/DataCenter/Modals.tsx
grep -n "DatePicker" frontend/src/pages/DataCenter/ETLPanel.tsx
grep -n "DatePicker" frontend/src/pages/DataCenter/SyncPanel.tsx
grep -n "DatePicker" frontend/src/pages/DataCenter/ETLTaskDrawer.tsx
grep -n "DatePicker" frontend/src/pages/FactorCenter/AnalysisPanel.tsx
grep -n "DatePicker" frontend/src/pages/FactorCenter/TestPanel.tsx
grep -n "DatePicker" frontend/src/pages/FactorCenter/FactorManageTab.tsx
grep -n "DatePicker" frontend/src/pages/FactorCenter.tsx
```

**Step 2: 对于只剩 import 行的文件，从 Semi UI import 中删除 `DatePicker`**

例如，如果 Modals.tsx 的 import 变成：
```tsx
import { Button, Modal, Select, DatePicker, ... } from '@douyinfe/semi-ui';
```
改为：
```tsx
import { Button, Modal, Select, ... } from '@douyinfe/semi-ui';
```

**Step 3: 同样检查 dayjs import，如果文件中不再使用 dayjs 则删除**

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: remove unused DatePicker and dayjs imports after QuantDatePicker migration"
```
