# 任务管理抽象层 - 快速参考

## 导入

```typescript
// 服务实例
import { syncService, etlService, factorService } from '../services/taskService';

// 组件
import { TaskList } from '../components/TaskList';

// 类型
import type { SyncTaskConfig, ETLTaskConfig, FactorConfig } from '../types/task';
```

## 常用操作

### 列出任务
```typescript
const tasks = await syncService.listTasks();
const enabledTasks = await syncService.listTasks(true); // 仅启用的
```

### 获取任务
```typescript
const task = await syncService.getTask('daily_basic');
```

### 创建任务
```typescript
await syncService.createTask({
  task_id: 'new_task',
  description: '新任务',
  api_name: 'daily',
  enabled: true,
}, 'user', '创建原因');
```

### 更新任务
```typescript
await syncService.updateTask('task_id', {
  description: '新描述',
  enabled: false,
}, 'user', '更新原因');
```

### 删除任务
```typescript
await syncService.deleteTask('task_id');
```

### 切换启用状态
```typescript
await syncService.toggleEnabled('task_id', false);
```

### 版本控制
```typescript
// 获取版本历史
const versions = await syncService.getVersionHistory('task_id');

// 回滚到指定版本
await syncService.rollbackToVersion('task_id', 1, 'user', '回滚原因');
```

## 使用TaskList组件

```typescript
// 1. 定义列
const columns = [
  { title: '任务ID', dataIndex: 'task_id', width: 150 },
  { title: '描述', dataIndex: 'description' },
  // ... 更多列
];

// 2. 使用组件
<TaskList
  taskType="sync"
  service={syncService}
  columns={columns}
  onEdit={(task) => console.log('编辑', task)}
  onCreate={() => console.log('创建')}
  idField="task_id"
/>
```

## 三种任务类型

| 任务类型 | 服务实例 | ID字段 | API路径 |
|---------|---------|--------|---------|
| 同步任务 | `syncService` | `task_id` | `/api/v1/sync/tasks` |
| ETL任务 | `etlService` | `task_id` | `/api/v1/etl/tasks` |
| 因子 | `factorService` | `factor_id` | `/api/v1/factors/tasks` |

## 错误处理

所有方法都会自动显示Toast错误提示，也可以手动捕获：

```typescript
try {
  await syncService.createTask(config);
} catch (error) {
  console.error('创建失败', error);
  // 自定义错误处理
}
```

## 完整示例

查看 `frontend/src/pages/TaskManagementExample.tsx`

## 详细文档

查看 `frontend/TASK_ABSTRACTION_GUIDE.md`
