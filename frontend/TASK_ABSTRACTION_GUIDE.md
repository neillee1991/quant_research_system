# 任务管理抽象层使用指南

## 概述

任务管理抽象层提供了统一的接口来管理不同类型的任务（同步任务、ETL任务、因子），具有以下特性：

- **统一的CRUD接口**：所有任务类型使用相同的方法签名
- **版本控制支持**：自动管理版本历史和回滚
- **类型安全**：完整的TypeScript类型定义
- **错误处理**：统一的错误提示和异常处理
- **可复用组件**：通用的TaskList组件

## 文件结构

```
frontend/src/
├── types/
│   └── task.ts                    # 任务类型定义
├── services/
│   └── taskService.ts             # 任务服务层
├── components/
│   └── TaskList/
│       ├── TaskList.tsx           # 通用任务列表组件
│       └── index.ts
└── pages/
    └── TaskManagementExample.tsx  # 使用示例
```

## 快速开始

### 1. 使用预配置的服务实例

```typescript
import { syncService, etlService, factorService } from '../services/taskService';

// 列出所有同步任务
const tasks = await syncService.listTasks();

// 获取特定任务
const task = await syncService.getTask('daily_basic');

// 创建新任务
const newTask = await syncService.createTask({
  task_id: 'new_task',
  description: '新任务',
  api_name: 'daily',
  enabled: true,
}, 'user', '创建新任务');

// 更新任务
await syncService.updateTask('daily_basic', {
  description: '更新后的描述',
}, 'user', '更新描述');

// 删除任务
await syncService.deleteTask('daily_basic');

// 切换启用状态
await syncService.toggleEnabled('daily_basic', false);
```

### 2. 使用通用TaskList组件

```typescript
import { TaskList } from '../components/TaskList';
import { syncService } from '../services/taskService';
import type { SyncTaskConfig } from '../types/task';

// 定义列配置
const columns = [
  {
    title: '任务ID',
    dataIndex: 'task_id',
    width: 150,
  },
  {
    title: '描述',
    dataIndex: 'description',
  },
  // ... 更多列
];

// 使用组件
<TaskList
  taskType="sync"
  service={syncService}
  columns={columns}
  onEdit={handleEdit}
  onCreate={handleCreate}
  idField="task_id"
/>
```

## API参考

### TaskService<T>

泛型任务服务类，提供统一的CRUD操作。

#### 构造函数

```typescript
new TaskService<T>(taskType: TaskType)
```

- `taskType`: 任务类型 ('sync' | 'etl' | 'factor')

#### 方法

##### listTasks(enabledOnly?: boolean): Promise<T[]>

列出所有任务（仅当前版本）。

- `enabledOnly`: 是否只返回启用的任务（默认false）
- 返回：任务数组

##### getTask(taskId: string): Promise<T>

获取特定任务的当前版本。

- `taskId`: 任务ID
- 返回：任务配置对象

##### createTask(config, changedBy?, changeReason?): Promise<T>

创建新任务。

- `config`: 任务配置（不包含版本字段）
- `changedBy`: 修改人（默认'user'）
- `changeReason`: 修改原因（默认'创建任务'）
- 返回：创建的任务对象

##### updateTask(taskId, updates, changedBy?, changeReason?): Promise<T>

更新任务（创建新版本）。

- `taskId`: 任务ID
- `updates`: 要更新的字段
- `changedBy`: 修改人（默认'user'）
- `changeReason`: 修改原因（默认'更新任务'）
- 返回：更新后的任务对象

##### deleteTask(taskId: string): Promise<void>

删除任务（软删除）。

- `taskId`: 任务ID

##### toggleEnabled(taskId: string, enabled: boolean): Promise<T>

切换任务启用状态。

- `taskId`: 任务ID
- `enabled`: 是否启用
- 返回：更新后的任务对象

##### getVersionHistory(taskId: string): Promise<T[]>

获取任务的版本历史。

- `taskId`: 任务ID
- 返回：版本数组（按版本号降序）

##### rollbackToVersion(taskId, version, changedBy?, changeReason?): Promise<T>

回滚到指定版本。

- `taskId`: 任务ID
- `version`: 目标版本号
- `changedBy`: 修改人（默认'user'）
- `changeReason`: 修改原因（默认'版本回滚'）
- 返回：回滚后的任务对象

### TaskList<T>

通用任务列表组件。

#### Props

```typescript
interface TaskListProps<T extends BaseTaskConfig> {
  taskType: TaskType;              // 任务类型
  service: TaskService<T>;         // 服务实例
  columns: ColumnProps<T>[];       // 表格列配置
  onEdit?: (task: T) => void;      // 编辑回调
  onCreate?: () => void;           // 创建回调
  onRefresh?: () => void;          // 刷新回调
  idField: 'task_id' | 'factor_id'; // ID字段名
  showSearch?: boolean;            // 显示搜索框（默认true）
  showCreate?: boolean;            // 显示创建按钮（默认true）
  extraActions?: (task: T) => React.ReactNode; // 额外操作按钮
}
```

#### 内置功能

- 任务列表展示（分页、搜索）
- 启用/禁用开关
- 编辑、删除、版本历史按钮
- 版本历史弹窗（集成VersionHistory组件）
- 版本回滚功能

## 类型定义

### BaseTaskConfig

所有任务类型的基础接口，包含8个版本控制字段：

```typescript
interface BaseTaskConfig {
  version_number: number;    // 版本号
  is_current: boolean;       // 是否当前版本
  changed_by: string;        // 修改人
  change_reason: string;     // 修改原因
  created_at?: string;       // 创建时间
  updated_at?: string;       // 更新时间
  description: string;       // 描述
  enabled: boolean;          // 是否启用
}
```

### SyncTaskConfig

同步任务配置：

```typescript
interface SyncTaskConfig extends BaseTaskConfig {
  task_id: string;
  api_name: string;
  api_limit?: number;
  fields?: string;
  start_date?: string;
  end_date?: string;
  sync_type?: string;
  table_name?: string;
  source?: string;
  schedule?: string;
  cron_expression?: string;
}
```

### ETLTaskConfig

ETL任务配置：

```typescript
interface ETLTaskConfig extends BaseTaskConfig {
  task_id: string;
  source_table: string;
  target_table: string;
  script: string;
  schedule?: string;
  table_name?: string;
}
```

### FactorConfig

因子配置：

```typescript
interface FactorConfig extends BaseTaskConfig {
  factor_id: string;
  code: string;
  depends_on?: string;
  params?: string;
  lookback_days?: number;
  category?: string;
}
```

## 完整示例

查看 `frontend/src/pages/TaskManagementExample.tsx` 获取完整的使用示例，包括：

- 三种任务类型的列表展示
- 创建/编辑/删除操作
- 版本历史查看
- 自定义列配置
- 额外操作按钮

## 与后端API对接

服务层自动对接以下API端点：

### 同步任务
- `GET /api/v1/sync/tasks` - 列出任务
- `GET /api/v1/sync/tasks/{id}` - 获取任务
- `POST /api/v1/sync/tasks` - 创建任务
- `PUT /api/v1/sync/tasks/{id}` - 更新任务
- `DELETE /api/v1/sync/tasks/{id}` - 删除任务

### ETL任务
- `GET /api/v1/etl/tasks` - 列出任务
- `GET /api/v1/etl/tasks/{id}` - 获取任务
- `POST /api/v1/etl/tasks` - 创建任务
- `PUT /api/v1/etl/tasks/{id}` - 更新任务
- `DELETE /api/v1/etl/tasks/{id}` - 删除任务

### 因子
- `GET /api/v1/factors/tasks` - 列出因子
- `GET /api/v1/factors/tasks/{id}` - 获取因子
- `POST /api/v1/factors/tasks` - 创建因子
- `PUT /api/v1/factors/tasks/{id}` - 更新因子
- `DELETE /api/v1/factors/tasks/{id}` - 删除因子

### 版本控制（通用）
- `GET /api/v1/tasks/{type}/{id}/versions` - 获取版本历史
- `POST /api/v1/tasks/{type}/{id}/rollback/{version}` - 回滚版本

## 错误处理

所有服务方法都包含统一的错误处理：

- 自动显示Toast错误提示
- 抛出异常供调用方捕获
- HTTP错误自动解析并显示详细信息

```typescript
try {
  await syncService.createTask(config);
} catch (error) {
  // 错误已通过Toast显示，这里可以做额外处理
  console.error('创建失败', error);
}
```

## 扩展新任务类型

要添加新的任务类型（如backtest），只需：

1. 在 `types/task.ts` 中定义接口：
```typescript
export interface BacktestTaskConfig extends BaseTaskConfig {
  task_id: string;
  strategy_code: string;
  // ... 其他字段
}
```

2. 创建服务实例：
```typescript
export const backtestService = new TaskService<BacktestTaskConfig>('backtest');
```

3. 使用TaskList组件展示。

总计约50行代码即可完成新任务类型的前端实现。
