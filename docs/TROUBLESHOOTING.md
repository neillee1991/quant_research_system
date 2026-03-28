# 故障排查

## 服务无法启动

### 检查服务状态

```bash
# 使用提供的脚本
./check_status.sh

# 或手动检查
lsof -ti:8000  # 后端
lsof -ti:3000  # 前端
lsof -ti:8848  # DolphinDB
lsof -ti:4200  # Prefect
```

### 查看日志

```bash
# 后端日志
tail -f logs/backend.log

# 前端日志
tail -f logs/frontend.log

# Docker 日志
docker-compose logs -f dolphindb
docker-compose logs -f prefect-server
```

### 常见原因

1. **端口被占用**
   - 修改 `.env` 中的端口配置
   - 或停止占用端口的进程

2. **Docker 未运行**
   - 启动 Docker Desktop
   - 或运行 `systemctl start docker` (Linux)

3. **虚拟环境未激活**
   - 运行 `source backend/.venv/bin/activate`

---

## 数据库连接失败

### 检查 DolphinDB 状态

```bash
# 检查容器状态
docker ps | grep quant_dolphindb

# 查看数据库日志
docker-compose logs dolphindb

# 重启数据库
docker-compose restart dolphindb
```

### 检查连接配置

确认 `.env` 中的配置正确：
```bash
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USERNAME=admin
DOLPHINDB_PASSWORD=123456
```

### 测试连接

```python
from store.dolphindb_client import db_client

# 测试查询
df = db_client.query("SELECT 1")
print(df)
```

---

## 数据同步问题

### 检查同步任务

```bash
# 列出所有同步任务
curl http://localhost:8000/api/v1/data/sync/tasks

# 检查任务是否启用
# 确认 enabled: true
```

### 检查同步日志

```bash
# 查看同步日志
curl "http://localhost:8000/api/v1/data/sync/status?limit=100"
```

### 常见问题

1. **Tushare token 未设置**
   - 在 `.env` 中设置 `TUSHARE_TOKEN`

2. **API 限流**
   - 检查 `COLLECTOR__CALLS_PER_MINUTE` 配置
   - 默认: 120 次/分钟

3. **表不存在**
   - 运行 `python database/init_dolphindb.py` 初始化表

---

## Prefect 调度异常

### 检查 Prefect 状态

```bash
# 检查 Prefect Server 健康状态
curl http://localhost:4200/api/health

# 查看 Prefect 日志
docker-compose logs prefect-server

# 重启 Prefect
docker-compose restart prefect-server
```

---

## 查询性能问题

### 分区性能测试

```bash
cd backend
python scripts/performance/test_partition_performance.py
```

预期速度：> 10,000 行/秒表示分区优化生效。

### 检查查询是否命中分区

确保查询使用分区键：
- `factor_id`
- `trade_date`
- `ts_code`

### 重建分区表

如果需要重新优化分区：
```bash
cd backend
python scripts/performance/optimize_factor_values_partition.py
```

---

## 因子计算问题

### 检查因子配置

确认因子配置存在于 `factor_metadata` 表中：
```sql
SELECT * FROM factor_metadata WHERE factor_id = 'your_factor_id'
```

### 检查依赖数据

确认 `depends_on` 指定的表有数据：
```sql
SELECT count(*) FROM sync_daily_data WHERE trade_date >= '20240101'
```

### 查看计算日志

查看 `production_task_run` 表或 API 历史记录：
```bash
curl "http://localhost:8000/api/v1/production/history?factor_id=your_factor_id"
```

---

## 前端问题

### 前端无法连接后端

1. 确认后端正在运行
2. 检查 `frontend/package.json` 中的代理配置
3. 查看浏览器控制台错误

### 前端页面崩溃

1. 打开浏览器开发者工具 (F12)
2. 查看 Console 标签页的错误信息
3. 检查 Network 标签页的 API 请求

---

## 常见错误信息

### SQL 语法错误

**错误**: `Syntax Error`

**解决**:
- 使用 `%s` 不是 `?` 作为参数占位符
- 传递 tuple 不是 list
- 裸表名会通过 `_adapt_sql_syntax()` 自动解析

### 连接池耗尽

**错误**: `Connection pool exhausted`

**解决**:
- DolphinDB 使用单个持久连接
- 检查是否有会话泄漏
- 确保 `db_client.close()` 被正确调用

### 空表

**错误**: `Table not found` 或查询返回空

**解决**:
- 检查同步任务是否存在: `GET /api/v1/data/sync/tasks`
- 确认任务 `enabled: true`
- 运行同步: `POST /api/v1/data/sync/task/{task_id}`

---

## 获取帮助

如果以上方法无法解决问题：

1. 检查 [README.md](../README.md)
2. 检查 [ARCHITECTURE.md](./ARCHITECTURE.md)
3. 检查 [DEVELOPER_GUIDE.md](./DEVELOPER_GUIDE.md)
4. 查看 Git 历史记录
5. 提交 Issue 并包含：
   - 错误信息
   - 复现步骤
   - 日志内容
   - 环境信息
