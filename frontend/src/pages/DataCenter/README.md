# DataCenter 组件

数据中心模块 - 重构版本

## 目录结构

```
DataCenter/
├── index.tsx              # 主页面入口
├── SyncPanel.tsx          # 同步任务面板
├── ETLPanel.tsx           # ETL 任务面板
├── DataTable.tsx          # SQL 查询面板
├── Modals.tsx             # 模态框组件集合
├── types.ts               # 本地类型定义
├── hooks/                 # 自定义 Hooks
│   ├── useSyncTasks.ts    # 同步任务逻辑
│   ├── useETLTasks.ts     # ETL 任务逻辑
│   └── useDataQuery.ts    # 数据查询逻辑
├── REFACTORING_REPORT.md  # 重构报告
├── ARCHITECTURE.md        # 架构文档
└── README.md              # 本文件
```

## 快速开始

### 导入组件
```typescript
import DataCenter from '@/pages/DataCenter';
```

### 使用 Hooks
```typescript
import { useSyncTasks } from '@/pages/DataCenter/hooks/useSyncTasks';

const MyComponent = () => {
  const { syncTasks, loadSyncTasks } = useSyncTasks();
  // ...
};
```

## 功能模块

### 1. 同步任务管理
- 任务列表展示
- 单个/批量同步
- 任务状态监控
- 同步日志查询

### 2. ETL 任务管理
- ETL 任务列表
- 脚本测试
- 任务回溯
- 执行日志

### 3. SQL 查询
- 数据表管理
- SQL 编辑器
- 查询结果展示
- 表数据清空

## 开发指南

### 添加新功能
1. 在对应 Hook 中添加业务逻辑
2. 在对应 Panel 组件中添加 UI
3. 在 index.tsx 中连接 props

### 代码规范
- 使用 TypeScript 严格模式
- 所有函数使用 useCallback
- 错误处理必须包含用户提示
- 避免空 catch 块

### 性能优化
- 使用批量 API 避免 N+1 查询
- 条件渲染减少不必要的组件
- 表格使用虚拟滚动

## 相关文档

- [重构报告](./REFACTORING_REPORT.md) - 详细的重构过程和改进点
- [架构文档](./ARCHITECTURE.md) - 组件架构和数据流向
- [全局类型](../../types/data.ts) - 共享类型定义

## 待办事项

- [ ] 实现任务配置抽屉
- [ ] 添加单元测试
- [ ] 添加 E2E 测试
- [ ] 性能监控
- [ ] 国际化支持

## 版本历史

### v2.0.0 (2026-03-07)
- 重构：拆分 2356 行超大组件为 8 个模块化文件
- 改进：完善错误处理
- 优化：提取自定义 Hooks
- 增强：类型安全

### v1.0.0 (原始版本)
- 单文件实现所有功能
