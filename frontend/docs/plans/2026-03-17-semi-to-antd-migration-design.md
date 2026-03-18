# Semi Design → Ant Design 迁移设计文档

**日期**: 2026-03-17
**范围**: `frontend/src/` 全量迁移
**策略**: 一次性全量迁移 + UI 风格升级（方向 B：现代金融 Dashboard）

---

## 背景

当前前端使用 `@douyinfe/semi-ui` (v2.91.0)，社区规模较小，存在长期维护风险。迁移至 Ant Design 5.x，同时借机升级整体视觉风格。

**迁移规模**: 34 个文件，59 处 Semi 引用

---

## 设计方向：现代金融 Dashboard

以 Ant Design 5.x 暗色算法为基础，采用 GitHub 风格深色三层背景 + Ant Design 标准蓝主色，整体更精致、层次更清晰。

### 主题 Token

#### 暗色主题
```
colorBgBase:       #0D1117   // 最深背景（页面底色）
colorBgContainer:  #161B22   // 容器背景（Card、Table）
colorBgElevated:   #1C2128   // 浮层背景（Drawer、Modal、Dropdown）
colorPrimary:      #1677FF   // 主色（Ant Design 标准蓝）
colorSuccess:      #3FB950   // 成功（GitHub 绿）
colorError:        #F85149   // 错误（GitHub 红）
colorWarning:      #D29922   // 警告
colorBorder:       #30363D   // 边框
colorText:         #E6EDF3   // 主文字
colorTextSecondary:#8B949E   // 次要文字
borderRadius:      6
```

#### 亮色主题
```
colorBgBase:       #F6F8FA
colorBgContainer:  #FFFFFF
colorBgElevated:   #FFFFFF
colorPrimary:      #1677FF
colorBorder:       #D0D7DE
colorText:         #1F2328
colorTextSecondary:#656D76
borderRadius:      6
```

### 保留的自定义 CSS 变量

以下变量继续保留在 `global.css`，值与 Ant Design token 对齐：
- `--color-gain` / `--color-loss`：涨跌颜色（金融专用，Ant Design 无对应 token）
- `--font-mono`：等宽字体（代码、数字展示）
- `--bg-sidebar`、`--bg-app` 等布局变量

**清理**：移除所有 `var(--semi-color-*)` 引用，替换为自定义变量或 Ant Design token。

---

## 组件 API 差异映射

### 布局 & 导航

| Semi | Ant Design | 变更说明 |
|------|-----------|---------|
| `Layout` | `Layout` | API 基本一致 |
| `Nav` | `Menu mode="inline"` | items 结构重写，`isCollapsed` → `inlineCollapsed` |
| `Breadcrumb` | `Breadcrumb` | API 基本一致 |

### 数据展示

| Semi | Ant Design | 变更说明 |
|------|-----------|---------|
| `Table` | `Table` | `empty` → `locale={{ emptyText }}` |
| `Tag` | `Tag` | `color` prop 基本一致 |
| `Descriptions` + `Descriptions.Item` | `Descriptions items={[]}` | 改为 items 数组写法 |
| `Progress` | `Progress` | `stroke` → `strokeColor`，`showInfo` → `showInfo` |
| `Collapse` + `Collapse.Panel` | `Collapse items={[]}` | 改为 items 数组写法 |

### 表单 & 输入

| Semi | Ant Design | 变更说明 |
|------|-----------|---------|
| `Input` | `Input` | `showClear` → `allowClear` |
| `Select` + `Select.Option` / `optionList` | `Select options={[]}` | 改为 options 数组写法 |
| `InputNumber` | `InputNumber` | API 基本一致 |
| `Checkbox` | `Checkbox` | `onChange(e)` 中 `e.target.checked` 一致 |
| `DatePicker` | `DatePicker` | 需适配 dayjs（Ant Design 5.x 默认） |

### 反馈 & 浮层

| Semi | Ant Design | 变更说明 |
|------|-----------|---------|
| `SideSheet` | `Drawer` | `visible` → `open`，`onCancel` → `onClose`，`bodyStyle` → `styles.body` |
| `Modal.confirm` | `Modal.confirm` | `okButtonProps.type='danger'` → `okButtonProps.danger=true` |
| `Toast.success/error/info` | `message.success/error/info` | 通过 `App.useApp()` 获取，需 `App` 包裹根组件 |
| `Banner` | `Alert` | `closeIcon={null}` → `closable={false}` |
| `Spin` | `Spin` | `spinning` prop 一致 |
| `Tooltip` | `Tooltip` | `content` → `title` |

### 按钮

| Semi | Ant Design | 变更说明 |
|------|-----------|---------|
| `Button type="danger"` | `Button danger` | 变为独立 boolean prop |
| `Button theme="solid" type="primary"` | `Button type="primary"` | 简化 |
| `Button theme="borderless"` | `Button type="text"` | |

---

## 图标替换映射

`@douyinfe/semi-icons` → `@ant-design/icons`

| Semi 图标 | Ant Design 图标 |
|-----------|----------------|
| `IconServer` | `DatabaseOutlined` |
| `IconTestScoreStroked` | `ExperimentOutlined` |
| `IconLineChartStroked` | `LineChartOutlined` |
| `IconCalendarClock` | `ScheduleOutlined` |
| `IconCandlestickChartStroked` | `FundOutlined` |
| `IconGridStroked` | `AppstoreOutlined` |
| `IconEdit` | `EditOutlined` |
| `IconSave` | `SaveOutlined` |
| `IconCode` | `CodeOutlined` |
| `IconSearch` | `SearchOutlined` |
| `IconHistogram` | `BarChartOutlined` |
| `IconPlus` | `PlusOutlined` |
| `IconDelete` | `DeleteOutlined` |
| `IconRefresh` | `ReloadOutlined` |
| `IconPlay` | `PlayCircleOutlined` |
| `IconSync` | `SyncOutlined` |
| `IconUpload` | `UploadOutlined` |
| `IconDownload` | `DownloadOutlined` |
| `IconSun` | `SunOutlined` |
| `IconMoon` | `MoonOutlined` |
| `IconAlertTriangle` | `WarningOutlined` |
| `IconBarChartHStroked` | `BarChartOutlined` |
| `IconSetting` | `SettingOutlined` |
| `IconLink` | `LinkOutlined` |

---

## 实施顺序

### Phase 1：基础设施（不影响业务逻辑）
1. 安装 `antd` + `@ant-design/icons`，卸载 Semi 包
2. 配置 `ConfigProvider` + 双主题 token（`App.tsx`）
3. 封装 `useMessage` hook（替代 `Toast`）
4. 更新 `global.css`：移除 `--semi-color-*`，对齐新 token 值

### Phase 2：Layout 层（全局影响，优先验证）
5. `AppLayout.tsx` — `Layout` 迁移
6. `Sidebar.tsx` — `Nav` → `Menu`
7. `TopBar.tsx` — `Breadcrumb` + 主题切换按钮

### Phase 3：公共组件
8. `QuantDatePicker.tsx` — `DatePicker` 适配 dayjs
9. `TaskList/TaskList.tsx`
10. `DataInspection/DataInspection.tsx`
11. `Charts/` — 仅替换 UI 组件，ECharts/TradingView 不变

### Phase 4：页面（按复杂度排序）
12. `DataCenter/DataTable.tsx`
13. `DataCenter/Modals.tsx`
14. `DataCenter/SyncPanel.tsx`
15. `DataCenter/ETLPanel.tsx`
16. `DataCenter/SyncTaskDrawer.tsx`（最复杂，含 `var(--semi-color-*)` 内联样式）
17. `DataCenter/ETLTaskDrawer.tsx`
18. `DataCenter/index.tsx`
19. `FactorCenter/FactorManageTab.tsx`
20. `FactorCenter/DataConfigPanel.tsx`
21. `FactorCenter/TestPanel.tsx`
22. `FactorCenter/AnalysisPanel.tsx`
23. `FactorCenter/FactorDrawer.tsx`（最复杂因子页面）
24. `FactorCenter/index.tsx`
25. `FactorCenter.tsx`（旧入口文件）
26. `MarketCenter.tsx`
27. `IndexPoolCenter.tsx`
28. `StrategyCenter.tsx`
29. `SchedulerCenter.tsx`
30. `SchedulerFlowEditor/TaskSelector.tsx`
31. `SchedulerFlowEditor/DAGEditor.tsx`
32. `SchedulerFlowEditor/index.tsx`
33. `FlowEditor/` nodes + index + Toolbar
34. `TaskManagementExample.tsx`

### Phase 5：收尾
35. 全局搜索 `var(--semi-color-*)` 残留，清理
36. 全局搜索 `@douyinfe/semi` 残留，确认清零
37. 视觉验收：暗色/亮色主题切换，各页面走查

---

## 风险点

1. **`SyncTaskDrawer` 内联样式**：大量 `var(--semi-color-*)` 内联在 JSX 中，需逐一替换
2. **`Toast` → `message`**：需要 `App` 组件包裹，且 `message` 实例需通过 hook 获取，影响所有使用 Toast 的文件
3. **`Nav` → `Menu`**：Semi 的 `Nav` 和 Ant Design 的 `Menu` 数据结构差异较大，Sidebar 需要重写
4. **`DatePicker`**：Ant Design 5.x 使用 dayjs，项目已有 dayjs 依赖，但 `QuantDatePicker` 的自定义逻辑需验证
5. **`Collapse` / `Descriptions`**：Ant Design 5.x 推荐 items 数组写法，旧的 `Panel` 子组件写法仍支持但不推荐

---

## 依赖变更

```json
// 新增
"antd": "^5.x",
"@ant-design/icons": "^5.x"

// 移除
"@douyinfe/semi-ui": "^2.91.0",
"@douyinfe/semi-icons": "^2.91.0"
```
