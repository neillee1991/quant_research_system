# Phase 3: 抽屉扩展层完善 - 架构设计文档

## 概述

本文档描述 Phase 3 的完整实现方案：**抽屉扩展层完善**。由于 SyncTaskDrawer 和 ETLTaskDrawer 各 900+ 行，完全拆分会需要谨慎进行，建议采用渐进式迁移策略。

## 当前状态

### 已完成 ✅
- **Phase 1**: Panel + Hook 统一
- **Phase 2**: 抽屉基础层抽象
  - `BaseTaskDrawer` - 基础抽屉壳
  - `UniversalJsonEditorTab` - JSON 编辑器 Tab
  - `UniversalHistoryTab` - 执行历史 Tab
  - `UniversalStatusTab` - 状态显示 Tab

### 待进行 ⏳
- **Phase 3**: 抽屉扩展层完善
  - 提取 Sync/ETL 独有的 Tab 组件
  - 重构抽屉使用分层架构
  - 保留现有功能完整

## 架构设计

### 目标目录结构

```
frontend/src/pages/DataCenter/
├── SyncTaskDrawer/
│   ├── index.tsx                    # 组装层（通用 Tab + 独有 Tab）
│   ├── VisualEditorTab.tsx           # Sync 可视化编辑（独有）
│   └── DataInspectTab.tsx            # Sync 数据探查（独有）
├── ETLTaskDrawer/
│   ├── index.tsx                    # 组装层（通用 Tab + 独有 Tab）
│   ├── VisualEditorTab.tsx           # ETL 可视化编辑（独有）
│   └── ScriptTestTab.tsx             # ETL 脚本测试（独有）
└── legacy/                          # 旧代码归档（可选）
    ├── SyncTaskDrawer.old.tsx
    └── ETLTaskDrawer.old.tsx
```

### SyncTaskDrawer 分层设计

#### 1. SyncVisualEditorTab.tsx（独有）

**职责**：
- Sync 任务的可视化编辑
- 基本信息（任务ID、API名称、描述等）
- API 参数编辑
- Schema 字段定义表格

**Props 接口**：
```typescript
interface SyncVisualEditorTabProps {
  config: SyncTaskConfig;
  onChange: (config: SyncTaskConfig) => void;
  isNew: boolean;
}
```

**提取内容**：
- 第 469-719 行：可视化编辑的完整表单
- `updateConfig()`, `updateSchemaField()`, `updateParamsField()`
- `handleAddSchemaField()`, `handleDeleteSchemaField()`

#### 2. SyncDataInspectTab.tsx（独有）

**职责**：
- 数据探查功能
- 完整性检查
- 缺失日期显示
- 覆盖率统计

**Props 接口**：
```typescript
interface SyncDataInspectTabProps {
  taskId: string;
  task?: SyncTask;
}
```

**提取内容**：
- 第 194-210 行：`handleInspectData()` 函数
- 第 770-848 行：数据探查 UI
- `inspectionData`, `inspectionLoading`, `showInspection` 状态

#### 3. SyncTaskDrawer/index.tsx（组装层）

**职责**：
- 组装通用 Tab 和独有 Tab
- 管理整体状态（config, jsonText 等）
- 处理保存逻辑（包括 schema 变更确认）
- 协调各 Tab 之间的数据共享

**使用的通用组件**：
- `BaseTaskDrawer` - 抽屉壳
- `UniversalJsonEditorTab` - JSON 编辑
- `UniversalHistoryTab` - 历史记录
- `UniversalStatusTab` - 状态显示
- `SyncVisualEditorTab` - Sync 可视化编辑（独有）
- `SyncDataInspectTab` - Sync 数据探查（独有）

### ETLTaskDrawer 分层设计

#### 1. ETLVisualEditorTab.tsx（独有）

**职责**：
- ETL 任务的可视化编辑
- 基本信息（任务ID、描述等）
- 脚本编辑器
- 字段定义

**Props 接口**：
```typescript
interface ETLVisualEditorTabProps {
  config: ETLTaskConfig;
  onChange: (config: ETLTaskConfig) => void;
  isNew: boolean;
}
```

#### 2. ETLScriptTestTab.tsx（独有）

**职责**：
- 脚本测试功能
- 试运行脚本
- 预览结果数据
- 字段差异对比

**Props 接口**：
```typescript
interface ETLScriptTestTabProps {
  taskId: string;
  script: string;
  onScriptChange: (script: string) => void;
}
```

#### 3. ETLTaskDrawer/index.tsx（组装层）

**职责**：
- 组装通用 Tab 和独有 Tab
- 管理整体状态
- 处理保存逻辑
- 协调各 Tab 之间的数据共享

## 渐进式迁移策略

### 阶段 1：创建目录结构和占位文件

```bash
# 创建新目录结构
mkdir -p frontend/src/pages/DataCenter/SyncTaskDrawer
mkdir -p frontend/src/pages/DataCenter/ETLTaskDrawer

# 创建占位文件
touch frontend/src/pages/DataCenter/SyncTaskDrawer/index.tsx
touch frontend/src/pages/DataCenter/SyncTaskDrawer/VisualEditorTab.tsx
touch frontend/src/pages/DataCenter/SyncTaskDrawer/DataInspectTab.tsx
touch frontend/src/pages/DataCenter/ETLTaskDrawer/index.tsx
touch frontend/src/pages/DataCenter/ETLTaskDrawer/VisualEditorTab.tsx
touch frontend/src/pages/DataCenter/ETLTaskDrawer/ScriptTestTab.tsx
```

### 阶段 2：实现组装层（使用旧组件）

在 `SyncTaskDrawer/index.tsx` 中：
```typescript
// 阶段 2：直接使用旧组件作为临时方案
import { SyncTaskDrawer as LegacySyncTaskDrawer } from '../SyncTaskDrawer';

export const SyncTaskDrawer: React.FC<SyncTaskDrawerProps> = (props) => {
  // 暂时直接渲染旧组件
  return <LegacySyncTaskDrawer {...props} />;
};
```

这样可以确保功能完整，同时为后续拆分做准备。

### 阶段 3：逐个提取独有 Tab

**优先顺序**：
1. **SyncDataInspectTab** - 相对独立，容易提取
2. **ETLScriptTestTab** - 相对独立，容易提取
3. **SyncVisualEditorTab** - 较复杂，需要更多测试
4. **ETLVisualEditorTab** - 较复杂，需要更多测试

**每个 Tab 的提取步骤**：
1. 从旧文件中复制相关代码到新文件
2. 调整 props 接口，使其独立
3. 从组装层传入必要的状态和回调
4. 测试功能完整性
5. 从旧文件中删除已提取的代码

### 阶段 4：重构组装层使用通用组件

当所有独有 Tab 提取完成后：
1. 替换 `Drawer` 为 `BaseTaskDrawer`
2. 替换 JSON 编辑为 `UniversalJsonEditorTab`
3. 替换历史记录为 `UniversalHistoryTab`
4. 替换状态显示为 `UniversalStatusTab`
5. 保留独有 Tab 的使用
6. 测试完整功能

### 阶段 5：归档旧代码（可选）

```bash
# 创建归档目录
mkdir -p frontend/src/pages/DataCenter/legacy

# 移动旧文件
mv frontend/src/pages/DataCenter/SyncTaskDrawer.tsx \
   frontend/src/pages/DataCenter/legacy/SyncTaskDrawer.old.tsx
mv frontend/src/pages/DataCenter/ETLTaskDrawer.tsx \
   frontend/src/pages/DataCenter/legacy/ETLTaskDrawer.old.tsx
```

## 风险控制

### 主要风险

1. **功能回归** - 拆分过程中可能破坏现有功能
2. **时间消耗** - 完整拆分需要大量时间和测试
3. **用户体验** - 中间阶段可能有 UI 不一致

### 缓解措施

1. **保持旧组件可用** - 不要立即删除旧文件
2. **功能对比测试** - 拆分前后对比功能完整性
3. **渐进式发布** - 可以先在开发环境验证
4. **保留回滚能力** - 随时可以切回旧实现

## 组件接口规范

### Tab 组件通用规范

所有 Tab 组件应该遵循：

```typescript
// 1. 明确的 Props 接口
interface TabProps {
  // 输入数据
  data?: SomeDataType;
  taskId?: string;

  // 回调函数
  onChange?: (data: SomeDataType) => void;
  onRefresh?: () => void;

  // 状态控制
  readOnly?: boolean;
  isNew?: boolean;
}

// 2. 内部状态管理
// 尽可能使用传入的 props，减少内部状态
// 需要内部状态时，提供同步回调

// 3. 错误处理
// 捕获并合理展示错误
// 提供重试机制（如需要）
```

### 组装层规范

组装层应该遵循：

```typescript
// 1. 集中管理共享状态
const [config, setConfig] = useState<ConfigType>();
const [jsonText, setJsonText] = useState<string>();

// 2. 提供统一的更新函数
const updateConfig = useCallback((updates: Partial<ConfigType>) => {
  const newConfig = { ...config, ...updates };
  setConfig(newConfig);
  setJsonText(JSON.stringify(newConfig, null, 2));
}, [config]);

// 3. 协调 Tab 间的数据共享
// 例如：JSON 编辑 ↔ 可视化编辑 的双向同步

// 4. 处理跨 Tab 的复杂逻辑
// 例如：保存时的 schema 变更确认
```

## 测试计划

### 单元测试

每个提取的 Tab 组件应该有：
- Props 接口验证
- 主要功能场景测试
- 错误处理测试

### 集成测试

组装层应该有：
- Tab 切换测试
- 数据同步测试
- 保存流程测试

### E2E 测试

关键用户流程：
1. 创建新 Sync 任务
2. 编辑现有 Sync 任务
3. 使用数据探查功能
4. 创建新 ETL 任务
5. 编辑现有 ETL 任务
6. 使用脚本测试功能

## 总结

Phase 3 是最复杂的阶段，建议：

1. **不要急于完全拆分** - 先确保 Phase 1 和 2 稳定
2. **采用渐进式策略** - 按阶段逐步迁移
3. **保持回滚能力** - 旧组件保留一段时间
4. **充分测试** - 每个阶段都要有完整测试

当前 Phase 1 和 2 已经提供了很好的基础，可以显著减少代码重复。Phase 3 可以根据实际需求和时间安排来决定是否进行完整实现。
