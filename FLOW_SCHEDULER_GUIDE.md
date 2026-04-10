# Flow Scheduler 使用说明

## 快速开始

### 启动服务

```bash
# 方式 1: 使用自动重新加载版本（推荐）
cd backend
python flows/serve_with_reload.py --check-interval 10

# 方式 2: 使用原始版本（需要手动重启）
cd backend
python flows/serve.py
```

## 自动重新加载功能

`serve_with_reload.py` 提供以下功能：

1. **定期检查数据库变化**（默认每 10 秒）
2. **自动重新加载修改的 flows**
3. **在 Prefect Dashboard 中实时看到变化**

### 工作原理

```
数据库 (flow_config)
    ↓ 定期检查 (每 N 秒)
serve_with_reload.py
    ↓ 自动重新加载
Prefect Dashboard
```

### 配置检查间隔

```bash
# 每 5 秒检查一次
python flows/serve_with_reload.py --check-interval 5

# 每 30 秒检查一次（减少数据库压力）
python flows/serve_with_reload.py --check-interval 30
```

## 完整工作流

### 1. 创建 Flow

1. 在调度管理页面点击"新建 Flow"
2. 填写 Flow 信息，选择任务
3. 自动识别依赖或手动设置
4. 点击"保存"

### 2. 查看在 Prefect 中的状态

```bash
# 等最多 10 秒（检查间隔）
# 或者刷新 Prefect Dashboard: http://localhost:4200
```

### 3. 修改 Flow

1. 在调度管理页面编辑 Flow
2. 保存修改
3. 等待自动重新加载（最多 10 秒）

### 4. 删除 Flow

1. 在调度管理页面删除 Flow
2. 等待自动重新加载（最多 10 秒）

## 故障排除

### Prefect Dashboard 看不到变化？

1. **确认 serve_with_reload.py 正在运行**
   ```bash
   ps aux | grep serve_with_reload
   ```

2. **检查日志**
   ```bash
   # 查看 backend/logs/app.log
   tail -f backend/logs/app.log
   ```

3. **手动刷新**
   - 重启 serve_with_reload.py
   - 按 Ctrl+C，然后重新运行

### Flow 没有被调度？

1. 确认 Flow 是 `enabled: true`
2. 检查 Cron 表达式是否正确
3. 在 Prefect Dashboard 查看 Deployment 详情

## 架构说明

```
┌─────────────────────────────────────────────────────────┐
│              前端 (React)                               │
│  SchedulerCenter (新建/编辑/删除 Flow)                   │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│              后端 API (FastAPI)                          │
│  /api/v1/flows (CRUD + 依赖识别)                        │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           DolphinDB (flow_config 表)                    │
│  存储 Flow 配置，包含 updated_at 时间戳                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼ 定期检查 updated_at
┌─────────────────────────────────────────────────────────┐
│        serve_with_reload.py (新)                        │
│  - 每 N 秒检查数据库                                     │
│  - 发现变化自动重新加载                                  │
│  - 调用 Prefect API 更新 deployments                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Prefect Server + Dashboard                    │
│  UI: http://localhost:4200                              │
└─────────────────────────────────────────────────────────┘
```

## 对比：旧版 vs 新版

| 特性 | serve.py (旧版) | serve_with_reload.py (新版) |
|------|-------------------|-------------------------------|
| 启动时加载 | ✅ | ✅ |
| 自动检测变化 | ❌ | ✅ |
| 自动重新加载 | ❌ | ✅ |
| 需要手动重启 | ✅ | ❌ |
| Prefect 实时同步 | ❌ | ✅ |

## 从旧版迁移

1. 停止旧的 serve.py
2. 启动新的 serve_with_reload.py
3. 完成！所有现有 flows 会自动加载

```bash
# 1. 停止旧版本 (Ctrl+C)

# 2. 启动新版本
cd backend
python flows/serve_with_reload.py
```
