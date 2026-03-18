# Semi Design → Ant Design 迁移实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将前端 UI 库从 @douyinfe/semi-ui 全量迁移至 antd 5.x，同时升级视觉风格为现代金融 Dashboard

**Architecture:** 一次性全量迁移，通过 ConfigProvider 统一管理双主题 token，保留现有 CSS 变量体系并与 Ant Design token 对齐。迁移顺序：基础设施 → Layout → 公共组件 → 页面。

**Tech Stack:** React 18, TypeScript, Ant Design 5.x, @ant-design/icons, dayjs

---

## Task 1: 安装依赖

**Files:** `package.json`

**Steps:**
1. Run: `cd quant_research_system/frontend && npm install antd @ant-design/icons`
2. Run: `npm uninstall @douyinfe/semi-ui @douyinfe/semi-icons`
3. Verify: `cat package.json | grep -E "antd|semi"`
4. Commit: `chore: replace semi-ui with antd`

---

## Task 2: 配置 ConfigProvider 双主题

**Files:** `src/App.tsx`, `src/theme/index.ts` (create)

**Steps:**
1. Create `src/theme/index.ts` with dark and light theme token configs:
   ```typescript
   import { theme } from 'antd';

   export const darkTheme = {
     algorithm: theme.darkAlgorithm,
     token: {
       colorPrimary: '#1677FF',
       colorBgBase: '#0D1117',
       colorBgContainer: '#161B22',
       colorBgElevated: '#1C2128',
       colorBorder: '#30363D',
       colorText: '#E6EDF3',
       colorTextSecondary: '#8B949E',
       colorSuccess: '#3FB950',
       colorError: '#F85149',
       colorWarning: '#D29922',
       borderRadius: 6,
       fontFamily: "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Microsoft YaHei', sans-serif",
     },
     components: {
       Menu: { darkItemBg: '#080C15', darkSubMenuItemBg: '#0D1117' },
       Table: { headerBg: '#161B22', rowHoverBg: '#1C2128' },
       Drawer: { colorBgElevated: '#1C2128' },
     },
   };

   export const lightTheme = {
     algorithm: theme.defaultAlgorithm,
     token: {
       colorPrimary: '#1677FF',
       colorBgBase: '#F6F8FA',
       colorBgContainer: '#FFFFFF',
       colorBorder: '#D0D7DE',
       colorText: '#1F2328',
       colorTextSecondary: '#656D76',
       borderRadius: 6,
       fontFamily: "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Microsoft YaHei', sans-serif",
     },
   };
   ```
2. Modify `src/App.tsx` to wrap with `ConfigProvider` and `App` (from antd):
   ```typescript
   import { ConfigProvider, App as AntApp } from 'antd';
   import { darkTheme, lightTheme } from './theme';
   import { useThemeStore } from './store';

   // Inside component:
   const { mode } = useThemeStore();
   const themeConfig = mode === 'dark' ? darkTheme : lightTheme;

   return (
     <ConfigProvider theme={themeConfig}>
       <AntApp>
         {/* existing router */}
       </AntApp>
     </ConfigProvider>
   );
   ```
3. Create `src/hooks/useMessage.ts`:
   ```typescript
   import { App } from 'antd';
   export const useMessage = () => {
     const { message } = App.useApp();
     return message;
   };
   ```
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `feat: add antd ConfigProvider with dual theme tokens`

---

## Task 3: 更新 global.css

**Files:** `src/styles/global.css`

**Steps:**
1. Update dark theme CSS variables to align with new token values:
   - `--bg-primary: #0D1117`
   - `--bg-secondary: #161B22`
   - `--bg-tertiary: #1C2128`
   - `--bg-card: #161B22`
   - `--bg-sidebar: #080C15`
   - `--bg-app: #0D1117`
   - `--color-primary: #1677FF`
   - `--color-gain: #3FB950`
   - `--color-loss: #F85149`
   - `--border-color: #30363D`
   - `--border-default: #30363D`
   - `--text-primary: #E6EDF3`
   - `--text-secondary: #8B949E`
2. Update light theme CSS variables similarly
3. Remove any `--semi-color-*` variables if present
4. Commit: `style: align CSS variables with antd token values`

---

## Task 4: 迁移 AppLayout

**Files:** `src/components/Layout/AppLayout.tsx`

**Steps:**
1. Replace `import { Layout } from '@douyinfe/semi-ui'` with `import { Layout } from 'antd'`
2. `Layout.Content` → `Layout.Content` (same API)
3. Verify TypeScript: `npx tsc --noEmit`
4. Commit: `refactor: migrate AppLayout to antd`

---

## Task 5: 迁移 Sidebar

**Files:** `src/components/Layout/Sidebar.tsx`

**Steps:**
1. Replace imports:
   ```typescript
   import { Menu } from 'antd';
   import { DatabaseOutlined, ExperimentOutlined, LineChartOutlined, ScheduleOutlined, FundOutlined, AppstoreOutlined } from '@ant-design/icons';
   ```
2. Replace `Nav` with `Menu`:
   ```typescript
   const menuItems = [
     { key: '/market', label: '行情', icon: <FundOutlined /> },
     { key: '/', label: '数据', icon: <DatabaseOutlined /> },
     { key: '/factor', label: '因子', icon: <ExperimentOutlined /> },
     { key: '/index-pool', label: '股票池', icon: <AppstoreOutlined /> },
     { key: '/strategy', label: '策略', icon: <LineChartOutlined /> },
     { key: '/scheduler', label: '调度', icon: <ScheduleOutlined /> },
   ];

   <Menu
     mode="inline"
     inlineCollapsed={collapsed}
     selectedKeys={[selectedKey()]}
     items={menuItems}
     onClick={({ key }) => navigate(key)}
     style={{ background: 'transparent', border: 'none', flex: 1 }}
     theme="dark"
   />
   ```
3. Verify TypeScript: `npx tsc --noEmit`
4. Commit: `refactor: migrate Sidebar Nav to antd Menu`

---

## Task 6: 迁移 TopBar

**Files:** `src/components/Layout/TopBar.tsx`

**Steps:**
1. Replace imports:
   ```typescript
   import { Breadcrumb, Button, Tooltip } from 'antd';
   import { SunOutlined, MoonOutlined } from '@ant-design/icons';
   ```
2. `Tooltip content` → `Tooltip title`
3. Verify TypeScript: `npx tsc --noEmit`
4. Commit: `refactor: migrate TopBar to antd`

---

## Task 7: 迁移 QuantDatePicker

**Files:** `src/components/QuantDatePicker.tsx`

**Steps:**
1. Replace `import { DatePicker } from '@douyinfe/semi-ui'` with `import { DatePicker } from 'antd'`
2. Adapt props — Ant Design DatePicker uses dayjs values, check existing usage in `FactorDrawer`
3. Verify TypeScript: `npx tsc --noEmit`
4. Commit: `refactor: migrate QuantDatePicker to antd`

---

## Task 8: 迁移 TaskList

**Files:** `src/components/TaskList/TaskList.tsx`

**Steps:**
1. Replace all `@douyinfe/semi-ui` imports with `antd` equivalents
2. Replace all `@douyinfe/semi-icons` imports with `@ant-design/icons` equivalents
3. Key changes: `Toast` → use `useMessage` hook, `Tag color` check compatibility
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate TaskList to antd`

---

## Task 9: 迁移 DataInspection

**Files:** `src/components/DataInspection/DataInspection.tsx`

**Steps:**
1. Replace all `@douyinfe/semi-ui` imports with `antd` equivalents
2. Replace `@douyinfe/semi-icons` with `@ant-design/icons`
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate DataInspection to antd`

---

## Task 10: 迁移 Charts 组件

**Files:** `src/components/Charts/TradingViewChart.tsx`, `KLineChart.tsx`, `EquityCurveChart.tsx`

**Steps:**
1. `TradingViewChart.tsx`: Replace Semi imports with antd, replace Semi icons with antd icons
   - `Tooltip content` → `Tooltip title`
   - `Modal.confirm okButtonProps.type='danger'` → `okButtonProps.danger=true`
2. `KLineChart.tsx` and `EquityCurveChart.tsx`: Check for any Semi imports (likely none)
3. Verify TypeScript: `npx tsc --noEmit`
4. Commit: `refactor: migrate Charts components to antd`

---

## Task 11: 迁移 FlowEditor 组件

**Files:** `src/components/FlowEditor/Toolbar.tsx`, `index.tsx`, `nodes/DataInputNode.tsx`, `nodes/OperatorNode.tsx`, `nodes/SignalNode.tsx`, `nodes/BacktestOutputNode.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents in each file
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. `Card` API is similar in antd
5. `Form` in antd is different from Semi — check if Semi Form is used or just Card+Form layout
6. Verify TypeScript: `npx tsc --noEmit`
7. Commit: `refactor: migrate FlowEditor components to antd`

---

## Task 12: 迁移 SchedulerFlowEditor 组件

**Files:** `src/components/SchedulerFlowEditor/TaskSelector.tsx`, `DAGEditor.tsx`, `index.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. `Checkbox`, `CheckboxGroup`, `Spin`, `Typography` → antd equivalents
3. Replace Semi icons with antd icons
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate SchedulerFlowEditor to antd`

---

## Task 13: 迁移 DataCenter/DataTable

**Files:** `src/pages/DataCenter/DataTable.tsx`

**Steps:**
1. Replace imports: `Card, Table, Button, Input` from antd; `PlayCircleOutlined, ReloadOutlined, LinkOutlined` from @ant-design/icons
2. `Input showClear` → `Input allowClear`
3. `Table empty` → `Table locale={{ emptyText: ... }}`
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate DataTable to antd`

---

## Task 14: 迁移 DataCenter/Modals

**Files:** `src/pages/DataCenter/Modals.tsx`

**Steps:**
1. Replace `Modal, Tag` from antd
2. Verify TypeScript: `npx tsc --noEmit`
3. Commit: `refactor: migrate DataCenter Modals to antd`

---

## Task 15: 迁移 DataCenter/SyncPanel

**Files:** `src/pages/DataCenter/SyncPanel.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate SyncPanel to antd`

---

## Task 16: 迁移 DataCenter/ETLPanel

**Files:** `src/pages/DataCenter/ETLPanel.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate ETLPanel to antd`

---

## Task 17: 迁移 DataCenter/SyncTaskDrawer (最复杂)

**Files:** `src/pages/DataCenter/SyncTaskDrawer.tsx`

**Steps:**
1. Replace component imports:
   - `SideSheet` → `Drawer` (`visible` → `open`, `onCancel` → `onClose`, `bodyStyle` → `styles={{ body: ... }}`)
   - `Banner` → `Alert` (`closeIcon={null}` → `closable={false}`)
   - `Collapse.Panel` → `Collapse items={[]}` array format
   - `Descriptions.Item` → `Descriptions items={[]}` array format
   - `Progress stroke` → `Progress strokeColor`
   - `Select.Option` → `Select options={[]}` array format
   - `Button type="danger"` → `Button danger`
   - `Button theme="solid" type="primary"` → `Button type="primary"`
2. Replace all `var(--semi-color-*)` inline styles:
   - `var(--semi-color-primary)` → `var(--color-primary)`
   - `var(--semi-color-success)` → `var(--color-gain)`
   - `var(--semi-color-danger)` → `var(--color-loss)`
   - `var(--semi-color-warning)` → `var(--color-warning)`
   - `var(--semi-color-text-1)` → `var(--text-primary)`
   - `var(--semi-color-text-2)` → `var(--text-secondary)`
   - `var(--semi-color-fill-0)` → `var(--bg-tertiary)`
   - `var(--semi-color-border)` → `var(--border-color)`
   - `var(--semi-color-danger-light-default)` → `rgba(248, 81, 73, 0.1)`
3. Replace Semi icons with antd icons
4. `Toast` → `useMessage` hook
5. `Modal.confirm okButtonProps: { type: 'danger' }` → `okButtonProps: { danger: true }`
6. Verify TypeScript: `npx tsc --noEmit`
7. Commit: `refactor: migrate SyncTaskDrawer to antd`

---

## Task 18: 迁移 DataCenter/ETLTaskDrawer

**Files:** `src/pages/DataCenter/ETLTaskDrawer.tsx`

**Steps:**
1. Same pattern as SyncTaskDrawer: `SideSheet` → `Drawer`, replace all Semi imports
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate ETLTaskDrawer to antd`

---

## Task 19: 迁移 DataCenter/index

**Files:** `src/pages/DataCenter/index.tsx`

**Steps:**
1. Replace `Tabs, TabPane, Toast, Modal` from antd
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. `TabPane` → `Tabs items={[]}` array format
5. Verify TypeScript: `npx tsc --noEmit`
6. Commit: `refactor: migrate DataCenter index to antd`

---

## Task 20: 迁移 FactorCenter/FactorManageTab

**Files:** `src/pages/FactorCenter/FactorManageTab.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate FactorManageTab to antd`

---

## Task 21: 迁移 FactorCenter/DataConfigPanel

**Files:** `src/pages/FactorCenter/DataConfigPanel.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. `ReloadOutlined, SaveOutlined` from @ant-design/icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate DataConfigPanel to antd`

---

## Task 22: 迁移 FactorCenter/TestPanel

**Files:** `src/pages/FactorCenter/TestPanel.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. `WarningOutlined` from @ant-design/icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate TestPanel to antd`

---

## Task 23: 迁移 FactorCenter/AnalysisPanel

**Files:** `src/pages/FactorCenter/AnalysisPanel.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. `BarChartOutlined` from @ant-design/icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate AnalysisPanel to antd`

---

## Task 24: 迁移 FactorCenter/FactorDrawer (最复杂因子页面)

**Files:** `src/pages/FactorCenter/FactorDrawer.tsx`

**Steps:**
1. Replace component imports:
   - `SideSheet` → `Drawer`
   - `Empty` → `Empty` (antd, same name)
   - `Spin` → `Spin` (antd, same name, `spinning` prop same)
   - `Collapse.Panel` → `Collapse items={[]}` array format
   - `Select optionList` → `Select options`
   - `InputNumber` → `InputNumber` (antd, similar API)
2. Replace Semi icons: `EditOutlined, SaveOutlined, CodeOutlined, DatabaseOutlined, SearchOutlined, BarChartOutlined`
3. `Toast` → `useMessage` hook
4. `Tooltip content` → `Tooltip title`
5. `Table empty` → `Table locale={{ emptyText: ... }}`
6. Verify TypeScript: `npx tsc --noEmit`
7. Commit: `refactor: migrate FactorDrawer to antd`

---

## Task 25: 迁移 FactorCenter/index 和 FactorCenter.tsx

**Files:** `src/pages/FactorCenter/index.tsx`, `src/pages/FactorCenter.tsx`

**Steps:**
1. Replace `Tabs, TabPane` → `Tabs items={[]}` in both files
2. Replace Semi icons with antd icons
3. `Toast` → `useMessage` hook (in FactorCenter.tsx)
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate FactorCenter pages to antd`

---

## Task 26: 迁移 MarketCenter

**Files:** `src/pages/MarketCenter.tsx`

**Steps:**
1. Replace `Table, Select, Card` from antd; `FundOutlined` from @ant-design/icons
2. Verify TypeScript: `npx tsc --noEmit`
3. Commit: `refactor: migrate MarketCenter to antd`

---

## Task 27: 迁移 IndexPoolCenter

**Files:** `src/pages/IndexPoolCenter.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. Replace Semi icons: `UploadOutlined, DeleteOutlined, DownloadOutlined, PlusOutlined`
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate IndexPoolCenter to antd`

---

## Task 28: 迁移 StrategyCenter

**Files:** `src/pages/StrategyCenter.tsx`

**Steps:**
1. Replace `Tabs, TabPane, Select, Button, Tag, Spin, Progress, Toast` from antd
2. `TabPane` → `Tabs items={[]}` array format
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate StrategyCenter to antd`

---

## Task 29: 迁移 SchedulerCenter

**Files:** `src/pages/SchedulerCenter.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. Replace all Semi icons with antd icons
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate SchedulerCenter to antd`

---

## Task 30: 迁移 TaskManagementExample

**Files:** `src/pages/TaskManagementExample.tsx`

**Steps:**
1. Replace all Semi imports with antd equivalents
2. Replace Semi icons: `SyncOutlined, CodeOutlined`
3. `Toast` → `useMessage` hook
4. Verify TypeScript: `npx tsc --noEmit`
5. Commit: `refactor: migrate TaskManagementExample to antd`

---

## Task 31: 全局清理与验收

**Files:** All `src/` files

**Steps:**
1. Search for any remaining Semi references: `grep -r "@douyinfe/semi" src/`
   Expected: no output
2. Search for `var(--semi-color-` in all files: `grep -r "semi-color" src/`
   Expected: no output
3. Run TypeScript check: `npx tsc --noEmit`
   Expected: no errors
4. Start dev server and visually verify each page in dark mode and light mode
5. Final commit: `chore: complete semi-ui to antd migration`
