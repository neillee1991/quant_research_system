# DataCenter 组件架构图

## 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         DataCenter/index.tsx                     │
│                          (主协调器)                              │
│                                                                   │
│  职责：                                                           │
│  - 初始化数据加载                                                 │
│  - 协调各个子组件                                                 │
│  - 管理模态框状态                                                 │
│  - 处理用户交互事件                                               │
└─────────────────────────────────────────────────────────────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
        ┌───────────────┐ ┌───────────┐ ┌──────────────┐
        │  useSyncTasks │ │ useETLTasks│ │ useDataQuery │
        │     Hook      │ │    Hook    │ │     Hook     │
        └───────────────┘ └───────────┘ └──────────────┘
                │                │              │
                │                │              │
                ▼                ▼              ▼
        ┌───────────────┐ ┌───────────┐ ┌──────────────┐
        │   SyncPanel   │ │  ETLPanel │ │  DataTable   │
        │   Component   │ │ Component │ │  Component   │
        └───────────────┘ └───────────┘ └──────────────┘
                                 │
                                 ▼
                        ┌────────────────┐
                        │     Modals     │
                        │   Components   │
                        └────────────────┘
```

## 数据流向

```
用户操作
   │
   ▼
index.tsx (事件处理)
   │
   ├─► Hook (业务逻辑)
   │      │
   │      ├─► API 调用
   │      │      │
   │      │      ▼
   │      │   Backend
   │      │      │
   │      │      ▼
   │      └─► 状态更新
   │             │
   │             ▼
   └─► Component (UI 渲染)
          │
          ▼
      用户界面更新
```

## 文件依赖关系

```
index.tsx
├── hooks/useSyncTasks.ts
│   ├── @/api (dataApi)
│   ├── @/types (SyncTask, TaskStatus, SyncLog, ScheduleInfo)
│   └── @douyinfe/semi-ui (Toast)
│
├── hooks/useETLTasks.ts
│   ├── @/api (dataApi)
│   ├── @/types (ETLTask, ETLTestResult, ETLFieldDefinition)
│   ├── ./types (ETLTaskStatus)
│   └── @douyinfe/semi-ui (Toast)
│
├── hooks/useDataQuery.ts
│   ├── @/api (dataApi)
│   ├── @/types (TableInfo, DailyData)
│   └── @douyinfe/semi-ui (Toast)
│
├── SyncPanel.tsx
│   ├── @/types (SyncTask, TaskStatus, SyncLog, ScheduleInfo)
│   ├── ./types (LogFilters)
│   └── @douyinfe/semi-ui (UI 组件)
│
├── ETLPanel.tsx
│   ├── @/types (ETLTask)
│   ├── ./types (ETLLogFilters)
│   └── @douyinfe/semi-ui (UI 组件)
│
├── DataTable.tsx
│   ├── @/types (TableInfo)
│   ├── @monaco-editor/react (Editor)
│   └── @douyinfe/semi-ui (UI 组件)
│
├── Modals.tsx
│   ├── @/types (SyncTask)
│   └── @douyinfe/semi-ui (Modal, DatePicker, Tag)
│
└── types.ts (本地类型定义)
```

## 组件通信模式

### 1. Props Down, Events Up

```
index.tsx
   │
   ├─► Props ──────────► SyncPanel
   │                        │
   │                        │
   └◄─ Callbacks ◄─────────┘
```

### 2. Hook 状态共享

```
useSyncTasks Hook
   │
   ├─► syncTasks ──────────► SyncPanel (显示)
   ├─► syncingTasks ───────► SyncPanel (加载状态)
   ├─► selectedTaskIds ────► SyncPanel (选中状态)
   │
   └─► loadSyncTasks() ────► index.tsx (调用)
```

## 关键接口定义

### SyncPanel Props
```typescript
interface SyncPanelProps {
  // 数据
  syncTasks: SyncTask[];
  taskStatuses: Record<string, TaskStatus>;
  syncLogs: SyncLog[];
  syncingTasks: Set<string>;
  selectedTaskIds: string[];
  scheduleInfo: Record<string, ScheduleInfo>;

  // 回调
  onSelectedTaskIdsChange: (ids: string[]) => void;
  onRefreshStatus: () => void;
  onNewTask: () => void;
  onBatchSync: () => void;
  onSyncTask: (taskId: string) => void;
  onCopyTask: (task: SyncTask) => void;
  onDeleteTask: (taskId: string) => void;
  onOpenTaskDrawer: (task: SyncTask) => void;
  onLoadSyncLogs: (source?, dataType?, startDate?, endDate?) => void;
}
```

### useSyncTasks 返回值
```typescript
{
  // 状态
  syncTasks: SyncTask[];
  taskStatuses: Record<string, TaskStatus>;
  syncLogs: SyncLog[];
  syncingTasks: Set<string>;
  selectedTaskIds: string[];
  scheduleInfo: Record<string, ScheduleInfo>;

  // 方法
  setSelectedTaskIds: (ids: string[]) => void;
  loadSyncTasks: () => Promise<void>;
  loadTaskStatus: (taskId: string) => Promise<void>;
  loadTaskScheduleInfo: (taskId: string) => Promise<void>;
  loadSyncLogs: (source?, dataType?, startDate?, endDate?) => Promise<void>;
  syncTask: (taskId, targetDate?, startDate?, endDate?) => Promise<void>;
  batchSyncTasks: (taskIds, startDate?, endDate?) => Promise<boolean>;
  deleteTask: (taskId: string, dropTable?: boolean) => Promise<void>;
  toggleSchedule: (taskId, enabled, schedule?, cronExpression?) => Promise<boolean>;
}
```

## 状态管理策略

### 1. 本地状态 (useState)
- 模态框显示/隐藏
- 表单输入值
- 临时 UI 状态

### 2. Hook 状态 (自定义 Hook)
- 业务数据 (tasks, logs, tables)
- 加载状态 (loading, syncing)
- 选中状态 (selectedIds)

### 3. 全局状态 (useThemeStore)
- 主题模式 (light/dark)

## 错误处理流程

```
用户操作
   │
   ▼
index.tsx (事件处理)
   │
   ▼
Hook (业务逻辑)
   │
   ├─► try {
   │      API 调用
   │      状态更新
   │      Toast.success()
   │   }
   │
   └─► catch (error) {
          console.error()
          Toast.error()
          throw error (可选)
       }
```

## 性能优化点

### 1. useCallback 优化
```typescript
const loadSyncTasks = useCallback(async () => {
  // 避免不必要的函数重建
}, []);
```

### 2. 条件渲染
```typescript
{selectedTaskIds.length > 0 && (
  <Button>批量操作</Button>
)}
```

### 3. 批量 API 调用
```typescript
// 避免 N+1 查询
const statusBatchRes = await dataApi.getTaskStatusBatch();
```

### 4. 虚拟滚动 (Table)
```typescript
<Table
  scroll={{ x: 'max-content', y: 500 }}
  pagination={{ pageSize: 50 }}
/>
```

## 扩展指南

### 添加新功能
1. 在对应 Hook 中添加业务逻辑
2. 在对应 Panel 组件中添加 UI
3. 在 index.tsx 中连接 props

### 添加新面板
1. 创建新的 Panel 组件
2. 创建对应的 Hook
3. 在 index.tsx 中添加 TabPane

### 添加新模态框
1. 在 Modals.tsx 中添加组件
2. 在 index.tsx 中添加状态和处理函数
3. 在对应 Panel 中触发

## 测试策略

### 1. Hook 测试
```typescript
import { renderHook, act } from '@testing-library/react-hooks';
import { useSyncTasks } from './useSyncTasks';

test('should load sync tasks', async () => {
  const { result } = renderHook(() => useSyncTasks());
  await act(async () => {
    await result.current.loadSyncTasks();
  });
  expect(result.current.syncTasks.length).toBeGreaterThan(0);
});
```

### 2. 组件测试
```typescript
import { render, screen } from '@testing-library/react';
import { SyncPanel } from './SyncPanel';

test('should render sync tasks', () => {
  render(<SyncPanel {...mockProps} />);
  expect(screen.getByText('同步任务管理')).toBeInTheDocument();
});
```

### 3. 集成测试
```typescript
import { render, screen, waitFor } from '@testing-library/react';
import DataCenter from './index';

test('should load and display data', async () => {
  render(<DataCenter />);
  await waitFor(() => {
    expect(screen.getByText('数据中心')).toBeInTheDocument();
  });
});
```
