# 前端任务显示问题 - 调试指南

## 快速诊断

### 步骤 1: 检查浏览器控制台

打开浏览器开发者工具 (F12)，查看 Console 标签：

```javascript
// 在控制台运行以下命令
console.log('API Base URL:', '/api/v1')

// 测试 API 连接
fetch('/api/v1/tasks/sync')
  .then(r => {
    console.log('Status:', r.status)
    return r.json()
  })
  .then(d => {
    console.log('Response:', d)
    console.log('Tasks count:', d.tasks?.length)
  })
  .catch(e => console.error('Error:', e))
```

### 步骤 2: 检查网络请求

在浏览器开发者工具的 Network 标签中：

1. 刷新页面
2. 查找 `tasks/sync` 请求
3. 检查：
   - 状态码是否为 200？
   - 响应体是否包含 tasks 数组？
   - 响应大小是否 > 0？

### 步骤 3: 检查前端代码

在 React DevTools 中：

1. 找到 DataCenter 或 SyncPanel 组件
2. 查看 props 和 state：
   - `syncTasks` 是否为数组？
   - `syncTasks.length` 是否 > 0？
   - 是否有错误信息？

---

## 常见问题排查

### 问题 1: 网络请求返回 404

**原因**: API 端点不存在或路由不匹配

**解决方案**:
```bash
# 检查后端是否运行
curl http://localhost:8000/api/v1/tasks/sync

# 应该返回 200 和任务列表
# 如果返回 404，说明后端路由有问题
```

### 问题 2: 网络请求返回 401

**原因**: 认证失败

**解决方案**:
```javascript
// 检查 token 是否存在
console.log('Token:', localStorage.getItem('token'))

// 检查请求头
fetch('/api/v1/tasks/sync', {
  headers: {
    'Authorization': `Bearer ${localStorage.getItem('token')}`
  }
})
```

### 问题 3: 网络请求返回 500

**原因**: 后端错误

**解决方案**:
```bash
# 查看后端日志
tail -f backend/logs/app.log

# 或者直接测试后端
cd backend
source .venv/bin/activate
python -c "
import asyncio
from app.api.v1.tasks import list_tasks
asyncio.run(list_tasks('sync', False))
"
```

### 问题 4: 网络请求成功但前端不显示

**原因**: 前端组件问题

**解决方案**:
```javascript
// 检查数据是否正确解析
const response = await fetch('/api/v1/tasks/sync')
const data = await response.json()
console.log('Raw data:', data)
console.log('Tasks:', data.tasks)
console.log('Total:', data.total)

// 检查数据结构
if (Array.isArray(data.tasks)) {
  console.log('✓ tasks 是数组')
  console.log('✓ 任务数:', data.tasks.length)
} else {
  console.error('✗ tasks 不是数组:', typeof data.tasks)
}
```

---

## 完整的调试流程

### 1. 验证后端

```bash
cd backend
source .venv/bin/activate

# 测试数据库连接
python -c "
import asyncio
from scheduler.db import DatabasePool

async def test():
    await DatabasePool.init_pool()
    result = await DatabasePool.fetch('SELECT COUNT(*) as count FROM sync_task_configs')
    print(f'Sync tasks in DB: {result[0][\"count\"]}')
    await DatabasePool.close_pool()

asyncio.run(test())
"

# 测试 API 端点
python -c "
import asyncio
from app.api.v1.tasks import list_tasks

async def test():
    result = await list_tasks('sync', False)
    print(f'API returned {result.total} tasks')
    print(f'First task: {result.tasks[0].get(\"task_id\") if result.tasks else \"None\"}')

asyncio.run(test())
"
```

### 2. 验证前端 API 调用

```javascript
// 在浏览器控制台运行
import { dataApi } from './api/index'

dataApi.listSyncTasks()
  .then(res => {
    console.log('✓ API call successful')
    console.log('Tasks:', res.data.tasks)
    console.log('Total:', res.data.total)
  })
  .catch(err => {
    console.error('✗ API call failed:', err)
  })
```

### 3. 验证前端组件

```javascript
// 在 React DevTools 中检查组件状态
// 或在组件中添加调试日志

useEffect(() => {
  console.log('Loading sync tasks...')
  loadSyncTasks()
    .then(() => console.log('✓ Tasks loaded'))
    .catch(err => console.error('✗ Failed to load tasks:', err))
}, [])

// 在渲染时检查
console.log('Rendering with tasks:', syncTasks)
```

---

## 网络请求示例

### 成功的请求

```
GET /api/v1/tasks/sync HTTP/1.1
Host: localhost:8000
Authorization: Bearer eyJhbGc...
Accept: application/json

HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 2048

{
  "tasks": [
    {
      "task_id": "sync_stock_basic",
      "enabled": true,
      "sync_type": "incremental",
      ...
    },
    ...
  ],
  "total": 15,
  "task_type": "sync"
}
```

### 失败的请求

```
GET /api/v1/tasks/sync HTTP/1.1
Host: localhost:8000

HTTP/1.1 401 Unauthorized
Content-Type: application/json

{
  "detail": "Not authenticated"
}
```

---

## 快速修复清单

- [ ] 后端是否运行？ (`curl http://localhost:8000/docs`)
- [ ] 前端是否连接到正确的后端？ (检查 baseURL)
- [ ] 是否有认证 token？ (检查 localStorage)
- [ ] 数据库是否有数据？ (检查 PostgreSQL)
- [ ] API 是否返回 200？ (检查网络标签)
- [ ] 响应数据是否正确？ (检查 JSON 结构)
- [ ] 前端是否正确解析？ (检查 React DevTools)

---

## 联系方式

如果以上步骤都无法解决问题，请提供：

1. 浏览器控制台的完整错误信息
2. 网络请求的完整响应
3. 后端日志的相关错误
4. 前端组件的 props 和 state
