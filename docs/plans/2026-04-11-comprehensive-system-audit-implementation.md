# 全面系统诊断实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 生成完整的 UI → API → 后端端点追踪报告，识别所有活跃功能和死代码

**Architecture:** 四层诊断方法 - UI 功能清单 → API 调用追踪 → 后端端点追踪 → 生成对应表

**Tech Stack:** Bash grep、Python 脚本、Markdown 报告

---

## Task 1: 扫描所有 UI 页面和功能

**Files:**
- Scan: `frontend/src/pages/`
- Output: `docs/audit/ui-functions.md`

**Step 1: 列出所有页面组件**

Run:
```bash
find frontend/src/pages -name "*.tsx" -type f | sort
```

Expected: 列出所有页面文件路径

**Step 2: 为每个主要模块提取功能**

对以下模块逐个分析：
- DataCenter (SyncTaskPanel, ETLTaskPanel, etc.)
- FactorCenter (FactorList, FactorCompute, FactorAnalysis, etc.)
- SchedulerCenter (FlowList, FlowEditor, etc.)
- BacktestCenter (BacktestForm, BacktestResult, etc.)
- ConfigManagement (ConfigImport, ConfigExport, etc.)
- 其他 (DataQuery, IndexManagement, etc.)

对每个模块，记录：
- 页面路径
- 主要功能（列表、创建、编辑、删除、执行等）
- 关键用户交互点

**Step 3: 生成 UI 功能清单**

创建 `docs/audit/ui-functions.md`，格式：

```markdown
## DataCenter 模块

### 功能 1: 同步任务列表
- 页面: `frontend/src/pages/DataCenter/index.tsx`
- 组件: `SyncTaskPanel`
- 功能: 列表、创建、编辑、删除、执行、查看日志
- 关键交互: 
  - 点击"新建"按钮 → 打开创建对话框
  - 点击"编辑"按钮 → 打开编辑对话框
  - 点击"执行"按钮 → 执行同步任务
  - 点击"删除"按钮 → 删除任务
  - 点击"查看日志"按钮 → 查看执行日志

### 功能 2: ETL 任务列表
...
```

**Step 4: 提交**

```bash
git add docs/audit/ui-functions.md
git commit -m "audit: scan UI pages and extract functions"
```

---

## Task 2: 追踪每个 UI 功能使用的前端 API 方法

**Files:**
- Scan: `frontend/src/pages/`, `frontend/src/api/index.ts`
- Output: `docs/audit/ui-api-mapping.md`

**Step 1: 提取前端 API 方法定义**

Run:
```bash
grep -E "^\s+(list|create|update|delete|get|run|execute|test|backfill)" frontend/src/api/index.ts | head -50
```

Expected: 列出所有 API 方法

**Step 2: 对每个 UI 功能，追踪它调用的 API 方法**

对 Task 1 中的每个功能，使用 grep 追踪：

```bash
# 例如，对于 SyncTaskPanel，查找它使用的 API 方法
grep -r "listSyncTasks\|createSyncTask\|updateSyncTask\|deleteSyncTask\|syncTask" frontend/src/pages/DataCenter --include="*.tsx"
```

记录：
- UI 功能名称
- 使用的 API 方法列表
- 调用位置（文件:行号）

**Step 3: 标记 API 方法的状态**

对 `frontend/src/api/index.ts` 中的每个 API 方法，检查：
- ✅ 活跃 - 被至少一个 UI 功能调用
- ❌ 死亡 - 定义但未被任何 UI 功能调用
- ⚠️ 测试专用 - 仅在测试中使用

**Step 4: 生成 UI → API 映射表**

创建 `docs/audit/ui-api-mapping.md`，格式：

```markdown
## DataCenter 模块

### 功能 1: 同步任务列表
| 操作 | 前端 API 方法 | 状态 | 调用位置 |
|------|-------------|------|---------|
| 列表 | listSyncTasks | ✅ 活跃 | SyncTaskPanel.tsx:45 |
| 创建 | createSyncTask | ✅ 活跃 | SyncTaskDrawer.tsx:120 |
| 编辑 | updateSyncTask | ✅ 活跃 | SyncTaskDrawer.tsx:150 |
| 删除 | deleteTask | ✅ 活跃 | SyncTaskPanel.tsx:80 |
| 执行 | syncTask | ✅ 活跃 | SyncTaskPanel.tsx:100 |

## 死亡的 API 方法
| API 方法 | 定义位置 | 原因 |
|---------|---------|------|
| getProductionHistory | api/index.ts:XXX | 未被任何 UI 功能调用 |
| getFactorMissingDates | api/index.ts:XXX | 未被任何 UI 功能调用 |
```

**Step 5: 提交**

```bash
git add docs/audit/ui-api-mapping.md
git commit -m "audit: trace UI functions to frontend API methods"
```

---

## Task 3: 追踪每个前端 API 方法调用的后端端点

**Files:**
- Scan: `frontend/src/api/index.ts`, `backend/app/api/v1/`
- Output: `docs/audit/api-endpoint-mapping.md`

**Step 1: 提取所有后端端点**

Run:
```bash
grep -r "@router\." backend/app/api/v1 --include="*.py" | grep -E "(get|post|put|delete)" | sort
```

Expected: 列出所有后端端点

**Step 2: 对每个前端 API 方法，找出它调用的后端端点**

从 `frontend/src/api/index.ts` 中提取每个 API 方法的 URL 路径，例如：

```typescript
listSyncTasks: () => api.get('/tasks/sync')  // → GET /api/v1/tasks/sync
syncTask: (taskId) => api.post(`/tasks/sync/${taskId}/execute`, ...)  // → POST /api/v1/tasks/sync/{taskId}/execute
```

**Step 3: 标记后端端点的状态**

对每个后端端点，检查：
- ✅ 活跃 - 被至少一个前端 API 方法调用
- ❌ 死亡 - 定义但未被任何前端 API 方法调用
- ⚠️ 内部使用 - 仅被其他后端端点调用

**Step 4: 标记端点来自哪个 System**

对每个后端端点，标记：
- System A: `/api/v1/tasks/{type}/*`
- System B: `/api/v1/{type}/tasks`, `/api/v1/factors/tasks` 等
- System C: `/api/v1/data/sync/*`, `/api/v1/data/etl/*` 等

**Step 5: 生成 API → 后端端点映射表**

创建 `docs/audit/api-endpoint-mapping.md`，格式：

```markdown
## 同步任务相关

| 前端 API 方法 | 后端端点 | HTTP 方法 | 状态 | System |
|-------------|---------|---------|------|--------|
| listSyncTasks | /tasks/sync | GET | ✅ 活跃 | A |
| createSyncTask | /tasks/sync | POST | ✅ 活跃 | A |
| updateSyncTask | /tasks/sync/{id} | PUT | ✅ 活跃 | A |
| deleteTask | /tasks/sync/{id} | DELETE | ✅ 活跃 | A |
| syncTask | /tasks/sync/{id}/execute | POST | ✅ 活跃 | A |

## 死亡的后端端点
| 后端端点 | HTTP 方法 | 文件 | 原因 | System |
|---------|---------|------|------|--------|
| /data/sync/scheduler/start | POST | sync_api.py | 未被前端调用 | C |
| /data/sync/scheduler/stop | POST | sync_api.py | 未被前端调用 | C |
```

**Step 6: 提交**

```bash
git add docs/audit/api-endpoint-mapping.md
git commit -m "audit: trace frontend API methods to backend endpoints"
```

---

## Task 4: 生成完整的 UI → API → 后端端点对应表

**Files:**
- Input: `docs/audit/ui-functions.md`, `docs/audit/ui-api-mapping.md`, `docs/audit/api-endpoint-mapping.md`
- Output: `docs/audit/complete-tracing-map.md`

**Step 1: 合并三层映射**

创建 `docs/audit/complete-tracing-map.md`，格式：

```markdown
## DataCenter 模块 - 同步任务列表功能

### 功能链路追踪

| 层级 | 组件/方法 | 详情 | 状态 | System |
|------|---------|------|------|--------|
| UI | SyncTaskPanel | 显示同步任务列表 | ✅ 活跃 | - |
| 前端 API | listSyncTasks() | 调用后端获取任务列表 | ✅ 活跃 | - |
| 后端端点 | GET /tasks/sync | 返回所有同步任务 | ✅ 活跃 | A |

### 功能链路追踪

| 层级 | 组件/方法 | 详情 | 状态 | System |
|------|---------|------|------|--------|
| UI | SyncTaskPanel | 点击"执行"按钮 | ✅ 活跃 | - |
| 前端 API | syncTask(taskId) | 调用后端执行任务 | ✅ 活跃 | - |
| 后端端点 | POST /tasks/sync/{id}/execute | 执行同步任务 | ✅ 活跃 | A |

...（其他功能）
```

**Step 2: 提交**

```bash
git add docs/audit/complete-tracing-map.md
git commit -m "audit: generate complete UI-API-endpoint tracing map"
```

---

## Task 5: 生成死代码汇总报告

**Files:**
- Input: `docs/audit/ui-api-mapping.md`, `docs/audit/api-endpoint-mapping.md`
- Output: `docs/audit/dead-code-summary.md`

**Step 1: 汇总所有死亡的前端 API 方法**

创建表格，列出：
- API 方法名称
- 定义文件和行号
- 对应的后端端点
- 为什么是死代码（未被任何 UI 功能调用）

**Step 2: 汇总所有死亡的后端端点**

创建表格，列出：
- 后端端点
- 定义文件和行号
- 对应的前端 API 方法（如果有）
- 来自哪个 System
- 为什么是死代码（未被前端调用）

**Step 3: 汇总所有孤立的后端端点**

列出：
- 定义了但前端没有对应 API 方法的后端端点
- 这些端点是否被其他后端端点调用

**Step 4: 生成死代码汇总报告**

创建 `docs/audit/dead-code-summary.md`，格式：

```markdown
# 死代码汇总报告

## 统计

- 总前端 API 方法数: X
- 活跃前端 API 方法数: Y
- 死亡前端 API 方法数: Z (占比 Z/X%)

- 总后端端点数: X
- 活跃后端端点数: Y
- 死亡后端端点数: Z (占比 Z/X%)

## 死亡的前端 API 方法

| API 方法 | 文件 | 行号 | 后端端点 | 原因 |
|---------|------|------|---------|------|
| getProductionHistory | api/index.ts | 250 | GET /factor/history | 未被任何 UI 功能调用 |
| getFactorMissingDates | api/index.ts | 260 | GET /factor/factors/{id}/missing-dates | 未被任何 UI 功能调用 |

## 死亡的后端端点

| 后端端点 | 文件 | 行号 | System | 原因 |
|---------|------|------|--------|------|
| POST /data/sync/scheduler/start | sync_api.py | 100 | C | 未被前端调用 |
| POST /data/sync/scheduler/stop | sync_api.py | 110 | C | 未被前端调用 |

## 孤立的后端端点

| 后端端点 | 文件 | 原因 |
|---------|------|------|
| ... | ... | ... |
```

**Step 5: 提交**

```bash
git add docs/audit/dead-code-summary.md
git commit -m "audit: generate dead code summary report"
```

---

## Task 6: 生成 System 分布统计报告

**Files:**
- Input: `docs/audit/api-endpoint-mapping.md`, `docs/audit/dead-code-summary.md`
- Output: `docs/audit/system-distribution.md`

**Step 1: 统计每个 System 的端点分布**

对每个 System（A/B/C），统计：
- 总端点数
- 活跃端点数
- 死亡端点数
- 占比

**Step 2: 分析 System 迁移情况**

检查：
- 前端是否完全迁移到 System A？
- System B 和 System C 中还有多少活跃端点？
- 是否有重复的功能实现？

**Step 3: 生成 System 分布报告**

创建 `docs/audit/system-distribution.md`，格式：

```markdown
# System 分布统计报告

## 总体统计

| System | 总端点数 | 活跃端点数 | 死亡端点数 | 占比 |
|--------|---------|----------|----------|------|
| A | 30 | 28 | 2 | 93% |
| B | 15 | 0 | 15 | 0% |
| C | 20 | 5 | 15 | 25% |

## System A (新系统)
- 路由模式: `/api/v1/tasks/{type}/*`
- 活跃端点: 28 个
- 死亡端点: 2 个
- 前端迁移状态: ✅ 完全迁移

## System B (中间系统)
- 路由模式: `/api/v1/{type}/tasks`, `/api/v1/factors/tasks`
- 活跃端点: 0 个
- 死亡端点: 15 个
- 前端迁移状态: ✅ 已完全迁移到 System A
- 建议: 删除所有 System B 端点

## System C (旧系统)
- 路由模式: `/api/v1/data/sync/*`, `/api/v1/data/etl/*`
- 活跃端点: 5 个
- 死亡端点: 15 个
- 前端迁移状态: ⚠️ 部分迁移
- 建议: 迁移剩余 5 个活跃端点到 System A，删除所有 System C 端点

## 迁移建议

1. **立即删除**: System B 的所有 15 个端点（0% 使用率）
2. **逐步迁移**: System C 的 5 个活跃端点迁移到 System A
3. **删除**: System C 的所有 15 个死亡端点
```

**Step 4: 提交**

```bash
git add docs/audit/system-distribution.md
git commit -m "audit: generate system distribution statistics"
```

---

## Task 7: 生成最终诊断报告

**Files:**
- Input: 所有 audit 文件
- Output: `docs/audit/COMPREHENSIVE_AUDIT_REPORT.md`

**Step 1: 整合所有诊断结果**

创建 `docs/audit/COMPREHENSIVE_AUDIT_REPORT.md`，包含：
1. 执行摘要
2. 诊断方法论
3. UI 功能清单（来自 Task 1）
4. 完整的 UI → API → 后端端点追踪（来自 Task 4）
5. 死代码汇总（来自 Task 5）
6. System 分布统计（来自 Task 6）
7. 关键发现和建议

**Step 2: 生成最终报告**

格式：

```markdown
# 全面系统诊断报告

**诊断日期**: 2026-04-11  
**诊断范围**: UI、前端 API、后端端点  
**诊断方法**: 四层追踪法

---

## 执行摘要

### 关键发现

1. **前端迁移状态**: ✅ 前端已完全迁移到 System A
2. **死代码规模**: 
   - 前端 API 方法: X 个死亡（占比 Y%）
   - 后端端点: Z 个死亡（占比 W%）
3. **System 分布**:
   - System A: 28 个活跃端点（93%）
   - System B: 0 个活跃端点（0%）- 建议删除
   - System C: 5 个活跃端点（25%）- 建议迁移

### 建议行动

1. **立即删除** System B 的所有 15 个端点
2. **逐步迁移** System C 的 5 个活跃端点到 System A
3. **删除** System C 的所有 15 个死亡端点
4. **清理** 前端 API 中的 X 个死亡方法

---

## 详细诊断结果

[包含 Task 1-6 的所有内容]

---

## 后续行动清单

- [ ] 审核死代码汇总报告
- [ ] 确认删除 System B 的计划
- [ ] 制定 System C 迁移计划
- [ ] 清理前端 API 死亡方法
- [ ] 执行清理工作
```

**Step 3: 提交**

```bash
git add docs/audit/COMPREHENSIVE_AUDIT_REPORT.md
git commit -m "audit: generate comprehensive system audit report"
```

---

## Task 8: 验证诊断结果

**Files:**
- Verify: `docs/audit/COMPREHENSIVE_AUDIT_REPORT.md`

**Step 1: 手动验证关键发现**

- [ ] 验证前端是否真的完全迁移到 System A
- [ ] 验证 System B 是否真的没有活跃端点
- [ ] 验证死代码列表是否准确

**Step 2: 与用户讨论诊断结果**

准备讨论：
- 是否同意死代码的定义？
- 是否同意删除 System B 的计划？
- 是否同意迁移 System C 的计划？

**Step 3: 提交最终报告**

```bash
git add -A
git commit -m "audit: comprehensive system audit complete"
```

---

## 预期输出

完成后，应该有以下文件：

```
docs/audit/
├── ui-functions.md                    # UI 功能清单
├── ui-api-mapping.md                  # UI → API 映射
├── api-endpoint-mapping.md            # API → 后端端点映射
├── complete-tracing-map.md            # 完整的三层追踪
├── dead-code-summary.md               # 死代码汇总
├── system-distribution.md             # System 分布统计
└── COMPREHENSIVE_AUDIT_REPORT.md      # 最终诊断报告
```

以及一份清晰的、可以直接用于下一步清理工作的行动清单。
