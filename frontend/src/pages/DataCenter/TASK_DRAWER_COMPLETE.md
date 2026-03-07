# 任务配置抽屉实现 - 完整版

## 概述

为数据中心实现了与重构前一致的任务配置抽屉，分别为同步任务和 ETL 任务提供专门的配置界面。

## 实现的组件

### 1. SyncTaskDrawer.tsx - 同步任务配置抽屉

**功能特性：**
- ✅ 可视化编辑 + JSON 编辑双模式
- ✅ 任务ID和表名自动添加 `sync_` 前缀
- ✅ 完整的 DolphinDB 字段类型支持（20+ 种类型）
- ✅ Schema 字段表格编辑（字段名、类型、可空、注释）
- ✅ API 参数配置（动态表单）
- ✅ 主键配置（逗号分隔）
- ✅ 状态信息栏（最新数据、上次同步）
- ✅ 历史调度记录查看
- ✅ 版本历史按钮（可选）
- ✅ JSON 格式化功能
- ✅ Monaco Editor 集成

**字段类型支持：**
```
BOOL, CHAR, SHORT, INT, LONG, FLOAT, DOUBLE,
DATE, MONTH, TIME, MINUTE, SECOND, DATETIME,
TIMESTAMP, NANOTIME, NANOTIMESTAMP,
SYMBOL, STRING, UUID, BLOB
```

### 2. ETLTaskDrawer.tsx - ETL 任务配置抽屉

**功能特性：**
- ✅ 配置 + 历史记录双标签页
- ✅ 任务ID自动添加 `etl_` 前缀
- ✅ 目标表名自动等于任务ID
- ✅ 同步类型选择（增量/全量）
- ✅ 日期字段配置（增量模式）
- ✅ DolphinDB 脚本编辑器（Monaco Editor）
- ✅ 脚本测试功能（支持指定测试日期）
- ✅ 字段定义表格（从测试结果自动提取）
- ✅ 主键选择（多选）
- ✅ 字段类型修改
- ✅ 数据预览（前10行）
- ✅ 状态信息栏（最新数据、上次同步、主键）
- ✅ 历史执行记录查看
- ✅ 版本历史按钮（可选）
- ✅ 脚本格式化功能

**脚本变量支持：**
- `{date}` - 执行日期，格式为 YYYY.MM.DD

## 主要改进

### 与原始实现的一致性

1. **UI/UX 完全一致**
   - 相同的布局和样式
   - 相同的字段顺序
   - 相同的交互逻辑

2. **功能完全一致**
   - 可视化编辑 + JSON 编辑
   - 字段类型完整支持
   - 脚本测试和预览
   - 历史记录查看

3. **命名规范一致**
   - 同步任务：`sync_` 前缀
   - ETL 任务：`etl_` 前缀

### 代码结构优化

1. **组件分离**
   - 同步任务和 ETL 任务使用独立组件
   - 更清晰的职责划分
   - 更易于维护和扩展

2. **状态管理**
   - 独立的状态管理
   - 清晰的数据流
   - 避免状态混乱

3. **类型安全**
   - 完整的 TypeScript 类型定义
   - 编译时类型检查
   - 更好的 IDE 支持

## 使用方式

### 同步任务

#### 新建任务
```typescript
handleNewTask() ->
  setTaskDrawerTask(null) ->
  setTaskDrawerIsNew(true) ->
  setTaskDrawerVisible(true)
```

#### 编辑任务
```typescript
handleOpenTaskDrawer(task) ->
  setTaskDrawerTask(task) ->
  setTaskDrawerIsNew(false) ->
  setTaskDrawerVisible(true)
```

#### 复制任务
```typescript
handleCopyTask(task) ->
  创建副本（清空ID，添加"副本"后缀）->
  setTaskDrawerIsNew(true) ->
  setTaskDrawerVisible(true)
```

### ETL 任务

#### 新建任务
```typescript
handleNewEtlTask() ->
  setEtlDrawerTask(null) ->
  setEtlDrawerIsNew(true) ->
  setEtlDrawerVisible(true)
```

#### 编辑任务
```typescript
handleEditEtlTask(task) ->
  setEtlDrawerTask(task) ->
  setEtlDrawerIsNew(false) ->
  setEtlDrawerVisible(true)
```

#### 复制任务
```typescript
handleCopyEtlTask(task) ->
  创建副本（清空ID，添加"副本"后缀）->
  setEtlDrawerIsNew(true) ->
  setEtlDrawerVisible(true)
```

## 数据流

### 同步任务
```
用户操作
  ↓
SyncTaskDrawer
  ↓
加载配置 (getTaskConfig)
  ↓
可视化编辑 ←→ JSON 编辑（双向同步）
  ↓
保存配置 (createSyncTask / updateSyncTask)
  ↓
刷新任务列表
```

### ETL 任务
```
用户操作
  ↓
ETLTaskDrawer
  ↓
编辑脚本
  ↓
测试脚本 (testEtlScript)
  ↓
提取字段定义
  ↓
选择主键、修改类型
  ↓
保存配置 (createEtlTask / updateEtlTask)
  ↓
刷新任务列表
```

## 关键特性

### 1. 自动前缀处理

**同步任务：**
```typescript
// 输入框显示
<Input prefix="sync_" value={taskId.replace(/^sync_/, '')} />

// 保存时自动添加
task_id: `sync_${userInput}`
table_name: `sync_${userInput}`
```

**ETL 任务：**
```typescript
// 输入框显示
<Input prefix="etl_" value={taskId.replace(/^etl_/, '')} />

// 保存时自动添加
task_id: `etl_${userInput}`
table_name: task_id  // 目标表名 = 任务ID
```

### 2. Schema 字段管理

**同步任务：**
- 表格形式编辑
- 支持字段重命名
- 支持添加/删除字段
- 支持修改字段属性（类型、可空、注释）

**ETL 任务：**
- 从脚本测试结果自动提取
- 支持修改字段类型
- 支持选择主键（多选）
- 显示数据预览

### 3. 双向同步（同步任务）

可视化编辑和 JSON 编辑实时同步：
```typescript
// 可视化 → JSON
updateConfig(key, value) ->
  setConfig(newConfig) ->
  setJsonText(JSON.stringify(newConfig))

// JSON → 可视化
handleJsonChange(value) ->
  setJsonText(value) ->
  setConfig(JSON.parse(value))
```

### 4. 脚本测试（ETL 任务）

```typescript
handleTestScript() ->
  testEtlScript(script, testDate) ->
  提取字段类型 ->
  显示数据预览 ->
  显示测试结果
```

## API 端点

### 同步任务
- 获取配置：`GET /api/v1/data/sync/task/{task_id}/config`
- 创建任务：`POST /api/v1/data/sync/tasks`
- 更新任务：`PUT /api/v1/data/sync/task/{task_id}/config`
- 获取状态：`GET /api/v1/data/sync/status/{task_id}`
- 获取历史：`GET /api/v1/data/sync/status`

### ETL 任务
- 创建任务：`POST /api/v1/data/etl/tasks`
- 更新任务：`PUT /api/v1/data/etl/task/{task_id}`
- 获取状态：`GET /api/v1/data/etl/task/{task_id}/status`
- 获取字段：`GET /api/v1/data/etl/task/{task_id}/schema`
- 测试脚本：`POST /api/v1/data/etl/test`
- 获取历史：`GET /api/v1/data/etl/logs`

## 文件结构

```
src/pages/DataCenter/
├── index.tsx                    # 主页面（集成抽屉）
├── SyncTaskDrawer.tsx          # 同步任务抽屉
├── ETLTaskDrawer.tsx           # ETL 任务抽屉
├── SyncPanel.tsx               # 同步任务面板
├── ETLPanel.tsx                # ETL 任务面板
├── Modals.tsx                  # 模态框组件
├── hooks/
│   ├── useSyncTasks.ts        # 同步任务 Hook
│   └── useETLTasks.ts         # ETL 任务 Hook
└── types.ts                    # 本地类型定义
```

## 验证清单

- [x] 同步任务抽屉创建
- [x] ETL 任务抽屉创建
- [x] 主页面集成
- [x] 旧 TaskDrawer 删除
- [x] 导入更新
- [x] 状态管理更新
- [x] 处理函数更新
- [x] 文件验证通过

## 测试建议

### 同步任务
1. 新建任务（验证前缀自动添加）
2. 编辑任务（验证配置加载）
3. 复制任务（验证副本创建）
4. 可视化编辑（验证字段修改）
5. JSON 编辑（验证双向同步）
6. Schema 管理（添加/删除/修改字段）
7. 查看历史记录

### ETL 任务
1. 新建任务（验证前缀自动添加）
2. 编辑任务（验证配置加载）
3. 复制任务（验证副本创建）
4. 脚本编辑（验证 Monaco Editor）
5. 脚本测试（验证字段提取）
6. 字段类型修改
7. 主键选择
8. 数据预览
9. 查看历史记录

## 注意事项

1. **任务ID唯一性**：新建时需确保ID不重复
2. **前缀规范**：同步任务 `sync_`，ETL 任务 `etl_`
3. **表名规范**：同步任务可自定义，ETL 任务等于任务ID
4. **JSON 格式**：JSON 编辑时需保持格式正确
5. **脚本语法**：ETL 脚本需符合 DolphinDB 语法
6. **字段类型**：选择正确的 DolphinDB 类型
7. **主键配置**：至少选择一个主键字段

## 后续优化

1. **表单验证增强**
   - 任务ID重复检查
   - 表名存在性检查
   - 脚本语法检查

2. **用户体验优化**
   - 保存前确认对话框
   - 表单自动保存草稿
   - 配置导入/导出

3. **功能增强**
   - 脚本模板库
   - 字段映射模板
   - 批量创建任务
   - 任务依赖关系图
