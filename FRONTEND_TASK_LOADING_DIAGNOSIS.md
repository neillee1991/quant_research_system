# 前端任务加载诊断

## 问题描述
前端 DataCenter 页面看不到同步任务和 ETL 任务。

## 诊断步骤

### 1. 检查后端 API 是否返回数据

在浏览器控制台运行：

```javascript
// 测试同步任务 API
fetch('/api/v1/tasks/sync')
  .then(r => r.json())
  .then(d => {
    console.log('Sync tasks response:', d);
    console.log('Tasks count:', d.tasks?.length);
    console.log('First task:', d.tasks?.[0]);
  })
  .catch(e => console.error('Error:', e));

// 测试 ETL 任务 API
fetch('/api/v1/tasks/etl')
  .then(r => r.json())
  .then(d => {
    console.log('ETL tasks response:', d);
    console.log('Tasks count:', d.tasks?.length);
  })
  .catch(e => console.error('Error:', e));
```

**预期结果：**
- 状态码 200
- 响应包含 `tasks` 数组
- `tasks` 数组不为空（应该有 15 个 sync 任务，3 个 ETL 任务）

**如果失败：**
- 检查后端是否运行
- 检查认证 token 是否有效
- 检查后端日志

### 2. 检查前端 Hook 是否正确加载

在浏览器 React DevTools 中：

1. 找到 `DataCenter` 组件
2. 查看 `syncTasksHook` 的状态：
   - `tasks` 是否为数组？
   - `tasks.length` 是否 > 0？
   - `isLoading` 是否为 false？

3. 查看 `etlTasksHook` 的状态：
   - `tasks` 是否为数组？
   - `tasks.length` 是否 > 0？

### 3. 检查 TaskPanel 是否正确渲染

在浏览器 React DevTools 中：

1. 找到 `TaskPanel` 组件
2. 查看 props：
   - `tasksHook.tasks` 是否为空？
   - `config` 是否正确？

### 4. 检查浏览器控制台错误

打开浏览器开发者工具 (F12)，查看 Console 标签：

- 是否有 JavaScript 错误？
- 是否有网络错误？
- 是否有 API 调用失败的日志？

### 5. 检查网络请求

在浏览器开发者工具的 Network 标签中：

1. 刷新页面
2. 查找 `tasks/sync` 和 `tasks/etl` 请求
3. 检查：
   - 状态码是否为 200？
   - 响应体是否包含 tasks 数组？
   - 响应大小是否 > 0？

## 可能的问题和解决方案

### 问题 1: API 返回 404

**原因**: 后端路由不存在或路径错误

**解决方案**:
```bash
# 检查后端是否运行
curl http://localhost:8000/api/v1/tasks/sync

# 应该返回 200 和任务列表
# 如果返回 404，说明后端路由有问题
```

### 问题 2: API 返回 401

**原因**: 认证失败

**解决方案**:
```javascript
// 检查 token 是否存在
console.log('Token:', localStorage.getItem('token'));

// 检查请求头
fetch('/api/v1/tasks/sync', {
  headers: {
    'Authorization': `Bearer ${localStorage.getItem('token')}`
  }
})
```

### 问题 3: API 返回 500

**原因**: 后端错误

**解决方案**:
```bash
# 查看后端日志
tail -f backend/logs/app.log

# 检查数据库连接
psql -h localhost -U quant -d quantsystem -c "SELECT COUNT(*) FROM sync_task_configs"
```

### 问题 4: API 返回 200 但 tasks 为空

**原因**: 数据库中没有任务数据

**解决方案**:
```bash
# 检查数据库中是否有任务
psql -h localhost -U quant -d quantsystem << EOF
SELECT COUNT(*) as sync_tasks FROM sync_task_configs;
SELECT COUNT(*) as etl_tasks FROM etl_task_configs;
EOF

# 如果为 0，需要导入初始数据
cd backend
python database/init_dolphindb.py
```

### 问题 5: 前端 Hook 没有加载数据

**原因**: `loadInitialData` 没有被调用或调用失败

**解决方案**:
```javascript
// 在浏览器控制台检查
// 1. 打开 React DevTools
// 2. 找到 DataCenter 组件
// 3. 查看 syncTasksHook.tasks 是否为空
// 4. 如果为空，手动调用 loadTasks()

// 在 React DevTools 中执行：
// $r.syncTasksHook.loadTasks()
```

## 完整的调试流程

1. **验证后端**
   ```bash
   curl http://localhost:8000/api/v1/tasks/sync
   ```

2. **验证前端 API 调用**
   ```javascript
   fetch('/api/v1/tasks/sync').then(r => r.json()).then(d => console.log(d))
   ```

3. **验证前端 Hook**
   - 打开 React DevTools
   - 查看 `DataCenter` 组件的 `syncTasksHook.tasks`

4. **验证前端组件**
   - 打开 React DevTools
   - 查看 `TaskPanel` 组件的 props

5. **查看浏览器控制台**
   - 检查是否有错误日志

## 预期的正常流程

1. 页面加载
2. `DataCenter` 组件挂载
3. `useEffect` 调用 `loadInitialData()`
4. `loadInitialData()` 调用 `syncTasksHook.loadTasks()` 和 `etlTasksHook.loadTasks()`
5. `loadTasks()` 调用 `config.api.listTasks()`
6. API 返回任务列表
7. `setTasks(taskList)` 更新状态
8. `TaskPanel` 组件接收 `tasks` 并渲染表格
9. 表格显示任务列表

## 如果以上步骤都正常，但仍然看不到任务

请检查：
1. 浏览器是否缓存了旧的 JavaScript 代码（Ctrl+Shift+Delete 清除缓存）
2. 前端是否编译成功（`npm run build`）
3. 后端是否重启（`./stop.sh && ./start.sh`）
4. 数据库是否有数据（`psql` 查询）

