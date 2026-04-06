# 统一任务管理模块 - 前后端讨论会

**日期**: 2026-03-29
**参会人员**: 前端开发者、后端开发者、架构师
**目标**: 讨论并确定前端 Hook/Panel/抽屉的统一方案

---

## 一、会前准备（请会前阅读）

### 1.1 现有代码位置

**后端**（已统一）:
- `backend/app/models/base_task.py` - 基础模型
- `backend/app/services/task_service.py` - 通用服务
- `backend/app/api/v1/generic_task.py` - 通用 API 路由
- `backend/app/services/task_runner.py` - 统一任务执行器

**前端**（待统一）:
- `frontend/src/pages/DataCenter/hooks/useSyncTasks.ts` - Sync Hook
- `frontend/src/pages/DataCenter/hooks/useETLTasks.ts` - ETL Hook
- `frontend/src/pages/DataCenter/SyncPanel.tsx` - Sync Panel
- `frontend/src/pages/DataCenter/ETLPanel.tsx` - ETL Panel
- `frontend/src/pages/DataCenter/SyncTaskDrawer.tsx` - Sync 抽屉（900+ 行）
- `frontend/src/pages/DataCenter/ETLTaskDrawer.tsx` - ETL 抽屉（800+ 行）

### 1.2 现状对比

| 层级 | 后端 | 前端 |
|------|------|------|
| 数据模型 | ✅ 统一 `BaseTaskConfig` | ⚠️ 类型定义重复 |
| 服务层 | ✅ 统一 `TaskService<T>` | ⚠️ `useSyncTasks` / `useETLTasks` 90% 重复 |
| API 层 | ✅ 统一 `create_task_router()` | ⚠️ API 调用分散 |
| Panel 层 | - | ⚠️ `SyncPanel` / `ETLPanel` 85% 重复 |
| 抽屉层 | - | ⚠️ 高度定制，差异较大 |

### 1.3 抽屉功能差异表

| 功能模块 | SyncTaskDrawer | ETLTaskDrawer | 通用性 |
|---------|----------------|---------------|--------|
| **基础信息编辑** | ✅ 任务ID/描述/表名等 | ✅ 任务ID/描述/表名等 | 🔵 高 |
| **可视化编辑** | ✅ 表单式编辑 | ✅ 表单式编辑 | 🟡 中（字段不同） |
| **JSON 编辑** | ✅ Monaco Editor | ✅ Monaco Editor | 🔵 高 |
| **执行历史** | ✅ `task_runs` 表查询 | ✅ `task_runs` 表查询 | 🔵 高 |
| **状态显示** | ✅ 最新数据/上次同步 | ✅ 最新数据/上次同步 | 🔵 高 |
| **数据探查** | ✅ 完整性检查/缺失日期 | ❌ 无 | 🔴 Sync 独有 |
| **Schema 变更确认** | ✅ 删除字段警告/清空确认 | ❌ 无 | 🔴 Sync 独有 |
| **脚本测试** | ❌ 无 | ✅ 脚本试运行/预览 | 🔴 ETL 独有 |
| **字段差异对比** | ❌ 无 | ✅ 现有字段 vs 测试产出 | 🔴 ETL 独有 |

---

## 二、会议议程

### 议题 1：抽屉抽象策略选择（30分钟）

**目标**: 确定抽屉的抽象方式

**选项 A：完全抽象 + 插槽模式**
```
<GenericTaskDrawer
  taskType="sync"
  open={visible}
  onClose={onClose}
  tabs={[
    {
      key: 'visual',
      label: '可视化编辑',
      component: SyncVisualEditor,
      props: { task, onChange }
    },
    {
      key: 'json',
      label: 'JSON 编辑',
      component: UniversalJsonEditorTab
    },
    {
      key: 'history',
      label: '历史记录',
      component: UniversalHistoryTab
    },
  ]}
  extraFooterActions={<DataInspectButton />}
/>
```
- **优点**: 高度统一，新增任务类型只需配置
- **缺点**: 现有复杂抽屉重构工作量大，需要拆解组件

**选项 B：分层抽象 - 基础层 + 扩展层**（推荐）
```
通用层（所有任务共用）:
├── BaseTaskDrawer.tsx          # 抽屉壳（标题/关闭/底部按钮）
├── tabs/
│   ├── UniversalJsonEditorTab.tsx    # JSON 编辑器
│   ├── UniversalHistoryTab.tsx       # 执行历史
│   └── UniversalStatusTab.tsx        # 状态显示

扩展层（各任务独有）:
├── SyncTaskDrawer/
│   ├── index.tsx               # 组装通用 Tab + 独有 Tab
│   ├── VisualEditorTab.tsx     # Sync 可视化编辑
│   └── DataInspectTab.tsx      # Sync 数据探查
└── ETLTaskDrawer/
    ├── index.tsx               # 组装通用 Tab + 独有 Tab
    ├── VisualEditorTab.tsx     # ETL 可视化编辑
    └── ScriptTestTab.tsx       # ETL 脚本测试
```
- **优点**: 渐进式迁移，风险低，可复用现有代码 80%
- **缺点**: 仍有少量组装代码重复

**选项 C：混合策略 - Panel统一，抽屉保留**
- Panel 和 Hook 按之前设计统一
- 抽屉暂时保留现有实现，后续再考虑统一
- **优点**: 快速见效，风险最低
- **缺点**: 抽屉仍有重复，技术债未偿还

**🔹 讨论问题**:
1. 大家更倾向于哪个选项？为什么？
2. 选项 B 的分层抽象是否能满足需求？
3. 是否有中间方案？

---

### 议题 2：通用 Tab 组件定义（20分钟）

**目标**: 确定哪些 Tab 可以通用，以及它们的 Props 接口

**建议的通用 Tab**:

#### 2.1 UniversalJsonEditorTab
```typescript
interface UniversalJsonEditorTabProps {
  value: any;
  onChange: (value: any) => void;
  readOnly?: boolean;
}
```
- 功能: JSON 格式化、语法高亮、错误提示
- 复用: Sync / ETL / Factor 都需要

#### 2.2 UniversalHistoryTab
```typescript
interface UniversalHistoryTabProps {
  taskType: 'sync' | 'etl' | 'factor';
  taskId: string;
  limit?: number;
}
```
- 功能: 调用 `taskMonitorApi.getTaskHistory()` 展示
- 复用: Sync / ETL / Factor 都需要

#### 2.3 UniversalStatusTab
```typescript
interface UniversalStatusTabProps {
  taskType: 'sync' | 'etl' | 'factor';
  taskId: string;
}
```
- 功能: 显示最新数据日期、上次执行时间等
- 复用: Sync / ETL / Factor 都需要

**🔹 讨论问题**:
1. 这三个通用 Tab 是否够用？
2. 还需要提取其他通用 Tab 吗？
3. Props 接口设计是否合理？

---

### 议题 3：执行参数抽屉设计（15分钟）

**目标**: 确定任务执行时的参数输入方式

**现状**:
- Sync: 需要日期范围（target_date / start_date / end_date）
- ETL: 需要日期范围（回溯用）
- Factor: 需要日期范围 + 预处理选项

**设计选项**:

**选项 A：通用 RunTaskParamsModal + 配置驱动**
```typescript
interface RunTaskParamField {
  key: string;
  label: string;
  type: 'date' | 'daterange' | 'select' | 'checkbox';
  required?: boolean;
  options?: Array<{ label: string; value: any }>;
}

const syncRunParams: RunTaskParamField[] = [
  { key: 'dateRange', label: '日期范围', type: 'daterange', required: true },
];

const factorRunParams: RunTaskParamField[] = [
  { key: 'dateRange', label: '日期范围', type: 'daterange', required: true },
  { key: 'adjustPrice', label: '复权方式', type: 'select', options: [...] },
];
```

**选项 B：各任务独立实现，但统一打开方式**
- Panel 中通过配置传入 `openRunParamsModal: (task) => void`
- 各任务自己实现参数 Modal
- 优点: 灵活，无限制
- 缺点: 有重复代码

**🔹 讨论问题**:
1. 执行参数的复杂度如何？是否值得抽象？
2. 更倾向于哪个选项？

---

### 议题 4：后端接口补充确认（10分钟）

**目标**: 确认后端接口是否满足统一需求

**现有接口清单**:

| 接口 | Sync | ETL | Factor | 备注 |
|------|------|-----|--------|
| `listTasks` | ✅ | ✅ | ✅ | 已统一 |
| `getTask` | ✅ | ✅ | ✅ | 已统一 |
| `createTask` | ✅ | ✅ | ✅ | 已统一 |
| `updateTask` | ✅ | ✅ | ✅ | 已统一 |
| `deleteTask` | ✅ | ✅ | ✅ | 已统一 |
| `getTaskStatus` | ✅ | ✅ | ❓ | Factor 需要确认 |
| `getTaskHistory` | ✅ | ✅ | ✅ | 已统一（taskMonitorApi） |
| `runTask` | ✅ | ✅ | ✅ | 各有独立实现 |
| `inspectData` | ✅ | ❌ | ❌ | Sync 独有 |

**🔹 讨论问题**（后端）:
1. Factor 任务是否需要 `getTaskStatus` 接口？
2. `inspectData` 接口是否应该抽象为通用接口？
3. 还有哪些接口可以进一步统一？

---

### 议题 5：迁移优先级与时间规划（15分钟）

**目标**: 确定迁移步骤和时间安排

**建议的迁移路线图**:

**Phase 1: Panel + Hook 统一**（预估 1-2 天）
- [ ] 实现 `config/taskTypes.ts` 配置体系
- [ ] 实现 `hooks/useTasks.ts` 通用 Hook
- [ ] 实现 `components/TaskPanel/` 系列组件
- [ ] 迁移 Sync 任务到新架构
- [ ] 迁移 ETL 任务到新架构
- [ ] 清理旧代码（归档到 `legacy/` 目录）

**Phase 2: 抽屉基础层抽象**（预估 2-3 天）
- [ ] 实现 `components/TaskDrawer/BaseTaskDrawer.tsx`
- [ ] 实现 `components/TaskDrawer/tabs/UniversalJsonEditorTab.tsx`
- [ ] 实现 `components/TaskDrawer/tabs/UniversalHistoryTab.tsx`
- [ ] 实现 `components/TaskDrawer/tabs/UniversalStatusTab.tsx`
- [ ] Sync 抽屉迁移到分层架构
- [ ] ETL 抽屉迁移到分层架构

**Phase 3: 抽屉扩展层完善**（预估 2-3 天）
- [ ] 提取 `SyncTaskDrawer/VisualEditorTab.tsx`
- [ ] 提取 `SyncTaskDrawer/DataInspectTab.tsx`
- [ ] 提取 `ETLTaskDrawer/VisualEditorTab.tsx`
- [ ] 提取 `ETLTaskDrawer/ScriptTestTab.tsx`
- [ ] 完善文档和示例
- [ ] 代码审查和优化

**🔹 讨论问题**:
1. 这个时间规划是否合理？
2. 优先级是否正确？
3. 是否需要调整或增加缓冲时间？

---

## 三、待决策事项汇总

| 编号 | 决策项 | 选项 | 推荐 | 最终决策 |
|------|--------|------|------|----------|
| D1 | 抽屉抽象策略 | A / B / C | B | |
| D2 | 通用 Tab 范围 | 按提议 / 调整 | 按提议 | |
| D3 | 执行参数设计 | A / B | B | |
| D4 | 接口补充 | 确认清单 | | |
| D5 | 迁移时间线 | 按提议 / 调整 | 按提议 | |

---

## 四、会后行动项

- [ ] 记录会议决策，更新本文档
- [ ] 创建详细的实现任务清单
- [ ] 分配开发任务
- [ ] 确定 Code Review 节点

---

**文档位置**: `docs/discussions/2026-03-29-unified-task-management-discussion.md`
**上次更新**: 2026-03-29
