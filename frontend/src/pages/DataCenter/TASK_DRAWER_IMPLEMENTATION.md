# 任务配置抽屉实现说明

## 实现概述

为数据中心的同步任务和 ETL 任务添加了统一的任务配置抽屉组件，支持新建、编辑和复制任务。

## 新增文件

### 1. TaskDrawer.tsx
位置: `/src/pages/DataCenter/TaskDrawer.tsx`

**功能特性:**
- 统一的抽屉组件，支持同步任务和 ETL 任务
- 表单验证（任务ID、描述、表名等必填项）
- 字段映射编辑器（同步任务）
- DolphinDB 脚本编辑器（ETL 任务）
- 调度配置（启用/禁用、调度周期、Cron 表达式）

**组件结构:**
```typescript
<TaskDrawer
  visible={boolean}
  type={'sync' | 'etl'}
  task={SyncTask | ETLTask | null}
  onClose={() => void}
  onSave={(config: any) => Promise<void>}
/>
```

**字段映射编辑器:**
- 动态添加/删除字段
- 支持字段类型选择（STRING, INT, LONG, DOUBLE, DATE, TIMESTAMP）
- 源字段映射配置
- 字段描述

## 修改文件

### 1. index.tsx (主页面)
**新增状态:**
```typescript
const [taskDrawerVisible, setTaskDrawerVisible] = useState(false);
const [taskDrawerType, setTaskDrawerType] = useState<'sync' | 'etl'>('sync');
const [taskDrawerTask, setTaskDrawerTask] = useState<SyncTask | ETLTask | null>(null);
```

**新增处理函数:**
- `handleNewTask()` - 新建同步任务
- `handleCopyTask(task)` - 复制同步任务
- `handleOpenTaskDrawer(task)` - 编辑同步任务
- `handleNewEtlTask()` - 新建 ETL 任务
- `handleEditEtlTask(task)` - 编辑 ETL 任务
- `handleCopyEtlTask(task)` - 复制 ETL 任务
- `handleSaveTask(config)` - 保存任务配置

### 2. useSyncTasks.ts (同步任务 Hook)
**新增方法:**
```typescript
createTask: (config: any) => Promise<void>
updateTask: (config: any) => Promise<void>
```

### 3. useETLTasks.ts (ETL 任务 Hook)
**修改方法签名:**
```typescript
// 修改前
updateEtlTask: (taskId: string, config: any) => Promise<boolean>

// 修改后
updateEtlTask: (config: any) => Promise<boolean>
```

### 4. api/index.ts (API 层)
**新增方法:**
```typescript
createSyncTask: (config: any) => Promise<Response>
updateSyncTask: (taskId: string, config: any) => Promise<Response>
```

## 使用流程

### 新建任务
1. 点击"新建任务"按钮
2. 打开任务配置抽屉
3. 填写任务信息
4. 配置字段映射（同步任务）或编写脚本（ETL 任务）
5. 设置调度配置
6. 点击"保存"

### 编辑任务
1. 点击任务ID或编辑按钮
2. 打开任务配置抽屉，自动填充现有配置
3. 修改配置
4. 点击"保存"

### 复制任务
1. 点击复制按钮
2. 打开任务配置抽屉，自动填充源任务配置
3. 任务ID清空，描述添加"(副本)"后缀
4. 修改配置
5. 点击"保存"

## 表单验证规则

### 通用字段
- **任务ID**: 必填，只能包含字母、数字和下划线，编辑时禁用
- **描述**: 必填
- **数据表名**: 必填，只能包含字母、数字和下划线

### 同步任务特有
- **同步类型**: 必选（增量/全量）
- **数据源**: 必选（Tushare/AKShare/自定义）
- **API名称**: 必填

### ETL 任务特有
- **DolphinDB脚本**: 必填

## 数据流

```
用户操作
  ↓
TaskDrawer (表单验证)
  ↓
handleSaveTask (主页面)
  ↓
createTask/updateTask (Hook)
  ↓
createSyncTask/updateSyncTask (API)
  ↓
后端 API
  ↓
刷新任务列表
```

## 注意事项

1. **任务ID唯一性**: 新建任务时需确保任务ID不重复，后端会进行验证
2. **字段映射**: 同步任务的字段映射需要与 API 返回的字段对应
3. **脚本语法**: ETL 任务的 DolphinDB 脚本需要符合语法规范
4. **调度配置**: 调度周期和 Cron 表达式为可选项，留空则不启用自动调度
5. **复制任务**: 复制时会清空任务ID，需要重新输入唯一的ID

## 后续优化建议

1. **字段映射增强**
   - 从 API 文档自动获取可用字段
   - 字段类型自动推断
   - 字段映射模板

2. **脚本编辑器增强**
   - 语法高亮
   - 代码补全
   - 实时语法检查
   - 脚本模板

3. **表单验证增强**
   - 任务ID重复检查（前端）
   - 表名存在性检查
   - Cron 表达式验证

4. **用户体验优化**
   - 保存前确认对话框
   - 表单自动保存草稿
   - 配置导入/导出
   - 批量创建任务

## 测试建议

1. **功能测试**
   - 新建同步任务
   - 新建 ETL 任务
   - 编辑现有任务
   - 复制任务
   - 表单验证

2. **边界测试**
   - 空字段提交
   - 特殊字符输入
   - 超长文本输入
   - 重复任务ID

3. **集成测试**
   - 创建任务后立即同步
   - 编辑任务后验证配置
   - 删除任务后验证清理
