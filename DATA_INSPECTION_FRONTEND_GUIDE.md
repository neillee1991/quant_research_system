# 数据探查功能 - 前端集成指南

> 日期: 2026-03-14
> 状态: 已实现通用组件

## 已完成的工作

### 1. 后端 API ✅
- 端点：`GET /api/v1/tasks/{task_type}/{task_id}/inspect`
- 支持：sync、etl、factor 三种任务类型
- 返回：完整的数据探查信息

### 2. 前端通用组件 ✅
- 位置：`frontend/src/components/DataInspection/DataInspection.tsx`
- 功能：可复用的数据探查组件

## 如何在任务详情页中使用

### 方法 1：使用通用组件（推荐）

在 SyncTaskDrawer.tsx 中：

```tsx
// 1. 导入组件
import { DataInspection } from '../../components/DataInspection';

// 2. 在历史调度 Tab 中使用
<TabPane tab="历史调度" itemKey="history">
  <div style={{ paddingTop: 8 }}>
    {/* 添加数据探查组件 */}
    {task && <DataInspection taskType="sync" taskId={task.task_id} />}

    {/* 原有的历史调度表格 */}
    <Table
      dataSource={syncHistory}
      ...
    />
  </div>
</TabPane>
```

### 方法 2：手动集成（已在 SyncTaskDrawer 中实现）

如果需要自定义样式或逻辑，可以参考 SyncTaskDrawer.tsx 中的实现：

```tsx
// 1. 添加状态
const [inspectionData, setInspectionData] = useState<any>(null);
const [inspectionLoading, setInspectionLoading] = useState(false);
const [showInspection, setShowInspection] = useState(false);

// 2. 添加处理函数
const handleInspectData = async () => {
  if (!task) return;
  setInspectionLoading(true);
  try {
    const response = await fetch(
      `http://localhost:8000/api/v1/tasks/sync/${task.task_id}/inspect`
    );
    const data = await response.json();
    setInspectionData(data);
    setShowInspection(true);
  } catch (error) {
    Toast.error('数据探查失败');
  } finally {
    setInspectionLoading(false);
  }
};

// 3. 在 UI 中添加按钮和结果展示
// （参考 SyncTaskDrawer.tsx 第 717-790 行）
```

## 需要完成的集成工作

### 1. SyncTaskDrawer.tsx ✅
- 已添加数据探查功能
- 位置：历史调度 Tab
- 状态：已实现（第 717-790 行）

### 2. ETLTaskDrawer.tsx ⏳
需要添加：
```tsx
import { DataInspection } from '../../components/DataInspection';

// 在历史记录 Tab 中添加
{task && <DataInspection taskType="etl" taskId={task.task_id} />}
```

### 3. 因子任务详情页 ⏳
如果有因子任务的详情页，也需要添加：
```tsx
{task && <DataInspection taskType="factor" taskId={task.factor_id} />}
```

## 组件 API

### DataInspection Props

```typescript
interface DataInspectionProps {
  taskType: 'sync' | 'etl' | 'factor';  // 任务类型
  taskId: string;                        // 任务 ID
}
```

### 返回的数据结构

```typescript
interface InspectionResult {
  table_name: string;              // 表名
  exists: boolean;                 // 表是否存在
  has_data?: boolean;              // 是否有数据
  date_field?: string;             // 日期字段名
  min_date?: string;               // 最早日期
  max_date?: string;               // 最晚日期
  actual_dates?: number;           // 实际天数
  expected_dates?: number;         // 预期天数
  missing_dates?: string[];        // 缺失日期列表
  missing_count?: number;          // 缺失天数
  coverage_percent?: number;       // 覆盖率
  trading_calendar_available?: boolean;  // 交易日历是否可用
  message?: string;                // 提示信息
}
```

## UI 展示效果

### 1. 表不存在
```
⚠️ Table sync_xxx does not exist yet
```

### 2. 表存在但无数据
```
ℹ️ Table sync_xxx exists but has no data
```

### 3. 完整的数据探查报告
```
┌─ 数据完整性报告 ─────────────────────┐
│ 表名: sync_daily_data                │
│ 日期字段: trade_date                 │
│ 最早日期: 2020-01-02                 │
│ 最晚日期: 2024-12-31                 │
│ 实际天数: 1200                       │
│ 预期天数: 1220                       │
│ 缺失天数: 20                         │
│ 覆盖率: ████████████░░ 98.36%       │
│                                      │
│ 缺失的交易日：                       │
│ 2020-03-15  2020-06-20  2021-01-10  │
│ ... 还有 17 天                       │
└──────────────────────────────────────┘
```

## 快速集成步骤

### 对于 ETLTaskDrawer.tsx

1. 添加导入：
```tsx
import { DataInspection } from '../../components/DataInspection';
```

2. 找到历史记录 Tab（约第 859 行）

3. 在 `<TabPane tab="历史记录" itemKey="history">` 内部添加：
```tsx
<div style={{ paddingTop: 8 }}>
  {/* 添加数据探查 */}
  {task && <DataInspection taskType="etl" taskId={task.task_id} />}

  {/* 原有内容 */}
  ...
</div>
```

## 测试

### 1. 启动前端
```bash
cd frontend
npm start
```

### 2. 打开任务详情
- 进入数据中心页面
- 点击任何同步任务
- 切换到"历史调度" Tab
- 点击"数据探查"按钮

### 3. 验证功能
- ✅ 按钮可点击
- ✅ 显示加载状态
- ✅ 返回数据正确展示
- ✅ 缺失日期列表正确显示
- ✅ 覆盖率进度条正确显示

## 文件清单

### 后端
- `backend/app/services/task_service.py:338-450` - 数据探查服务方法
- `backend/app/api/v1/tasks.py:80-98, 382-410` - API 端点

### 前端
- `frontend/src/components/DataInspection/DataInspection.tsx` - 通用组件
- `frontend/src/components/DataInspection/index.ts` - 导出文件
- `frontend/src/pages/DataCenter/SyncTaskDrawer.tsx` - 已集成（第 717-790 行）
- `frontend/src/pages/DataCenter/ETLTaskDrawer.tsx` - 待集成

### 文档
- `DATA_INSPECTION_FEATURE.md` - 功能文档
- `DATA_INSPECTION_FRONTEND_GUIDE.md` - 本文档

## 注意事项

1. **API 地址**：当前硬编码为 `http://localhost:8000`，生产环境需要改为环境变量

2. **错误处理**：组件已包含基本错误处理，但可以根据需要增强

3. **缓存策略**：建议在前端添加缓存，避免频繁查询

4. **权限控制**：如果需要权限控制，在调用 API 前检查用户权限

5. **样式定制**：可以通过 props 传递自定义样式

## 下一步优化

1. **添加刷新按钮**：允许用户手动刷新数据探查结果

2. **导出功能**：支持导出缺失日期列表为 CSV

3. **历史对比**：显示数据覆盖率的历史趋势

4. **自动检查**：在任务执行后自动触发数据探查

5. **告警功能**：当覆盖率低于阈值时发送告警

---

**最后更新**: 2026-03-14
**状态**: 通用组件已完成，SyncTaskDrawer 已集成，ETLTaskDrawer 待集成
