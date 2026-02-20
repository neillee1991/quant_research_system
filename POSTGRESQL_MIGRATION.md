# PostgreSQL 迁移指南

## 概述

本项目已从 DuckDB 迁移到 PostgreSQL，以满足更高的并发性能、数据一致性和企业级需求。

## 主要变更

### 1. 数据库架构
- **旧版**: DuckDB (嵌入式数据库)
- **新版**: PostgreSQL 16 (客户端-服务器架构)

### 2. 核心优势
- ✅ 更好的并发支持（多用户同时访问）
- ✅ 事务隔离和 ACID 保证
- ✅ 连接池管理（提升性能）
- ✅ 丰富的索引和查询优化
- ✅ 企业级稳定性和可扩展性
- ✅ Docker 容器化部署

## 快速开始

### 1. 启动 PostgreSQL 数据库

使用 Docker Compose 启动数据库服务：

```bash
cd /Users/bytedance/Claude/quant_research_system
docker-compose up -d
```

这将启动两个服务：
- **PostgreSQL**: 端口 5432
- **pgAdmin**: 端口 5050 (Web 管理界面)

### 2. 验证数据库连接

```bash
# 检查容器状态
docker-compose ps

# 查看日志
docker-compose logs postgres

# 连接到数据库
docker exec -it quant_postgres psql -U quant_user -d quant_research
```

### 3. 安装 Python 依赖

```bash
cd backend
pip install -r requirements.txt
```

新增依赖：
- `psycopg2-binary>=2.9.9` - PostgreSQL 驱动
- `SQLAlchemy>=2.0.0` - ORM 和连接管理

### 4. 配置环境变量

编辑 `backend/.env` 文件：

```env
# Tushare Token
TUSHARE_TOKEN=your_token_here
DEBUG=false

# PostgreSQL Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=quant_research
POSTGRES_USER=quant_user
POSTGRES_PASSWORD=quant_pass_2024
```

### 5. 启动后端服务

```bash
cd backend
python main.py
```

## 数据库管理

### 使用 pgAdmin (推荐)

1. 访问 http://localhost:5050
2. 登录信息：
   - Email: `admin@quant.com`
   - Password: `admin123`
3. 添加服务器连接：
   - Host: `postgres` (Docker 内部) 或 `localhost` (本地)
   - Port: `5432`
   - Database: `quant_research`
   - Username: `quant_user`
   - Password: `quant_pass_2024`

### 使用命令行

```bash
# 进入 PostgreSQL 容器
docker exec -it quant_postgres psql -U quant_user -d quant_research

# 常用命令
\dt                    # 列出所有表
\d table_name          # 查看表结构
\l                     # 列出所有数据库
\q                     # 退出

# 查询示例
SELECT * FROM stock_basic LIMIT 10;
SELECT COUNT(*) FROM daily_basic;
```

## 数据库表结构

系统自动创建以下表：

| 表名 | 说明 | 主键 |
|------|------|------|
| `sync_log` | 数据同步日志 | (source, data_type) |
| `stock_basic` | 股票基础信息 | ts_code |
| `daily_basic` | 每日指标 | (ts_code, trade_date) |
| `adj_factor` | 复权因子 | (ts_code, trade_date) |
| `index_daily` | 指数日线行情 | (ts_code, trade_date) |
| `moneyflow` | 个股资金流向 | (ts_code, trade_date) |
| `daily_data` | 日线行情 (兼容旧版) | (trade_date, ts_code) |

## API 变更

### 客户端接口保持兼容

所有 API 接口保持不变，内部实现已切换到 PostgreSQL：

```python
# 旧代码（DuckDB）
from store.duckdb_client import db_client

# 新代码（PostgreSQL）
from store.postgres_client import db_client

# 使用方式完全相同
df = db_client.query("SELECT * FROM stock_basic LIMIT 10")
db_client.upsert("daily_basic", df, ["ts_code", "trade_date"])
```

### 主要方法

```python
# 查询数据
df = db_client.query(sql, params)

# 执行 SQL
db_client.execute(sql, params)

# 插入或更新
db_client.upsert(table_name, df, key_columns)

# 检查表是否存在
exists = db_client.table_exists(table_name)

# 创建表
db_client.create_table(table_name, schema, primary_keys)

# 同步日志
last_date = db_client.get_last_sync_date(source, data_type)
db_client.update_sync_log(source, data_type, last_date)
```

## 性能优化

### 1. 连接池配置

在 `backend/.env` 中调整：

```env
# 连接池大小（默认 10）
DATABASE__CONNECTION_POOL_SIZE=20

# 查询超时（秒，默认 300）
DATABASE__QUERY_TIMEOUT=600
```

### 2. 索引优化

所有表已自动创建索引：
- 主键索引（自动）
- 日期字段索引（trade_date）
- 股票代码索引（ts_code）
- 行业、市场分类索引

### 3. 批量操作

使用 `execute_values` 进行批量插入，性能提升 10-100 倍：

```python
# 自动使用批量插入
db_client.upsert(table_name, large_df, key_columns)
```

## 数据迁移（从 DuckDB）

如果需要从旧的 DuckDB 数据迁移到 PostgreSQL：

```python
import duckdb
import polars as pl
from store.postgres_client import db_client

# 连接到旧的 DuckDB
old_db = duckdb.connect("data/quant.duckdb")

# 迁移每个表
tables = ["stock_basic", "daily_basic", "adj_factor", "sync_log"]

for table in tables:
    print(f"Migrating {table}...")
    df = old_db.execute(f"SELECT * FROM {table}").pl()

    if not df.is_empty():
        # 获取主键（根据表定义）
        if table == "stock_basic":
            keys = ["ts_code"]
        elif table == "sync_log":
            keys = ["source", "data_type"]
        else:
            keys = ["ts_code", "trade_date"]

        db_client.upsert(table, df, keys)
        print(f"✓ Migrated {len(df)} rows")

print("Migration completed!")
```

## 备份与恢复

### 备份数据库

```bash
# 备份整个数据库
docker exec quant_postgres pg_dump -U quant_user quant_research > backup.sql

# 备份特定表
docker exec quant_postgres pg_dump -U quant_user -t stock_basic quant_research > stock_basic.sql
```

### 恢复数据库

```bash
# 恢复数据库
docker exec -i quant_postgres psql -U quant_user quant_research < backup.sql
```

## 故障排查

### 1. 连接失败

```bash
# 检查容器状态
docker-compose ps

# 查看日志
docker-compose logs postgres

# 重启服务
docker-compose restart postgres
```

### 2. 权限问题

```sql
-- 授予权限
GRANT ALL PRIVILEGES ON DATABASE quant_research TO quant_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO quant_user;
```

### 3. 性能问题

```sql
-- 查看慢查询
SELECT * FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;

-- 分析表
ANALYZE stock_basic;

-- 重建索引
REINDEX TABLE daily_basic;
```

### 4. 清理数据

```bash
# 停止并删除所有数据
docker-compose down -v

# 重新启动（会重新初始化）
docker-compose up -d
```

## 生产环境部署

### 1. 修改默认密码

编辑 `docker-compose.yml` 和 `backend/.env`，修改：
- `POSTGRES_PASSWORD`
- `PGADMIN_DEFAULT_PASSWORD`

### 2. 持久化存储

数据已自动持久化到 Docker volumes：
- `postgres_data`: 数据库文件
- `pgadmin_data`: pgAdmin 配置

### 3. 网络安全

生产环境建议：
- 不暴露 PostgreSQL 端口到公网
- 使用 SSL/TLS 连接
- 配置防火墙规则
- 定期备份数据

### 4. 监控

```sql
-- 查看连接数
SELECT count(*) FROM pg_stat_activity;

-- 查看数据库大小
SELECT pg_size_pretty(pg_database_size('quant_research'));

-- 查看表大小
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## 常见问题

### Q: 为什么要从 DuckDB 迁移到 PostgreSQL？

A: DuckDB 是优秀的分析型数据库，但在以下场景 PostgreSQL 更合适：
- 多用户并发访问
- 需要事务保证
- 需要远程访问
- 企业级部署

### Q: 性能会受影响吗？

A: 对于单用户分析查询，DuckDB 可能更快。但 PostgreSQL 在并发、稳定性和可扩展性方面更优。通过连接池和索引优化，性能完全满足需求。

### Q: 可以同时使用两个数据库吗？

A: 可以，但不推荐。如需保留 DuckDB 用于本地分析，可以保留 `duckdb_client.py` 并在特定场景使用。

### Q: 如何回滚到 DuckDB？

A:
1. 恢复 `requirements.txt` 中的 `duckdb>=1.4.0`
2. 将所有 `from store.postgres_client import` 改回 `from store.duckdb_client import`
3. 停止 Docker 容器：`docker-compose down`

## 技术支持

如遇问题，请检查：
1. Docker 容器日志：`docker-compose logs`
2. 后端日志：`backend/logs/`
3. PostgreSQL 日志：`docker exec quant_postgres tail -f /var/log/postgresql/postgresql.log`

---

**迁移完成日期**: 2026-02-20
**PostgreSQL 版本**: 16-alpine
**Python 驱动**: psycopg2-binary 2.9.9+
