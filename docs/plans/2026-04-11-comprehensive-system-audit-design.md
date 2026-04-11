# 全面系统诊断设计文档

**日期**: 2026-04-11  
**目标**: 验证上次清理是否彻底，理清当前系统中哪些功能是真正被使用的，哪些是冗余的

---

## 一、诊断目标

1. **验证清理彻底性** - 找出上次清理遗漏的死代码
2. **功能使用情况** - 理清哪些功能是真正被使用的，哪些是冗余的
3. **完整链路追踪** - 建立 UI → API → 后端端点的一一对应关系
4. **System 归类** - 标记每个功能来自哪个 System（A/B/C）

---

## 二、诊断范围

### UI 层（6 个主要模块）
1. **DataCenter** - 同步任务、ETL 任务管理
2. **FactorCenter** - 因子管理、因子计算、因子分析
3. **SchedulerCenter** - 调度管理、Flow 配置
4. **BacktestCenter** - 回测执行、结果分析
5. **ConfigManagement** - 配置导入导出、字段映射
6. **其他** - 数据查询、指数管理、监控等

### 前端 API 层
- `src/api/index.ts` 中的所有 API 对象和方法
- 包括：dataApi、productionApi、flowApi、strategyApi、configApi 等

### 后端端点层
- `app/api/v1/` 下的所有路由
- 包括：tasks.py、factor/、data/、flows.py 等

---

## 三、诊断方法

### 第 1 步：UI 功能清单
- 扫描 `frontend/src/pages/` 下的所有页面组件
- 列出每个页面的主要功能
- 记录每个功能的用户交互点（按钮、表单、菜单等）

### 第 2 步：API 调用追踪
- 对每个 UI 功能，使用 grep 追踪它调用了哪些前端 API 方法
- 标记 API 方法的状态：
  - ✅ 活跃 - 被 UI 功能调用
  - ❌ 死亡 - 定义但未被任何 UI 功能调用
  - ⚠️ 测试专用 - 仅在测试中使用

### 第 3 步：后端端点追踪
- 对每个前端 API 方法，追踪它调用的后端端点
- 标记后端端点的状态：
  - ✅ 活跃 - 被前端调用
  - ❌ 死亡 - 定义但未被前端调用
  - ⚠️ 内部使用 - 仅被其他后端端点调用
- 标记端点来自哪个 System：
  - System A - `/api/v1/tasks/{type}/*`
  - System B - `/api/v1/{type}/tasks`, `/api/v1/factors/tasks` 等
  - System C - `/api/v1/data/sync/*`, `/api/v1/data/etl/*` 等

### 第 4 步：生成对应表
生成格式：
```
UI 功能 | 前端 API 方法 | 后端端点 | 状态 | System
```

---

## 四、输出形式

### 4.1 按模块分类的功能清单
```markdown
## DataCenter 模块

### 功能 1: 同步任务列表
- UI 组件: SyncTaskPanel
- 主要操作: 列表、创建、编辑、删除、执行
- 使用的 API 方法: listSyncTasks, createSyncTask, updateSyncTask, deleteSyncTask, syncTask
- 使用的后端端点: GET /tasks/sync, POST /tasks/sync, PUT /tasks/sync/{id}, DELETE /tasks/sync/{id}, POST /tasks/sync/{id}/execute
- 状态: ✅ 活跃
- System: A
```

### 4.2 死代码汇总表
```markdown
## 死代码汇总

### 前端 API 方法（未被使用）
| API 方法 | 文件 | 后端端点 | 原因 |
|---------|------|---------|------|
| ... | ... | ... | ... |

### 后端端点（未被前端调用）
| 后端端点 | 文件 | 前端 API 方法 | 原因 |
|---------|------|-------------|------|
| ... | ... | ... | ... |
```

### 4.3 System 分布统计
```markdown
## System 分布

- System A: X 个活跃端点，Y 个死亡端点
- System B: X 个活跃端点，Y 个死亡端点
- System C: X 个活跃端点，Y 个死亡端点
```

---

## 五、关键问题

1. **前端是否完全迁移到 System A？** - 需要验证是否还有旧路由调用
2. **后端是否有孤立的端点？** - 需要找出未被前端使用的端点
3. **是否有重复的功能实现？** - 需要找出多个端点实现同一功能的情况
4. **测试代码中的 API 调用是否应该计入？** - 不计入（实用定义）

---

## 六、成功标准

✅ 完成诊断时应该能够：
1. 列出所有 UI 功能及其对应的 API 调用链路
2. 识别出所有死代码（前端 API 方法和后端端点）
3. 确认前端是否完全迁移到 System A
4. 为下一步清理提供明确的目标清单
