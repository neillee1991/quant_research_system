# 运维手册 v2.0

## 目录
1. [系统概述](#系统概述)
2. [日常运维](#日常运维)
3. [监控告警](#监控告警)
4. [故障排查](#故障排查)
5. [性能调优](#性能调优)
6. [备份恢复](#备份恢复)
7. [安全管理](#安全管理)
8. [应急预案](#应急预案)

---

## 系统概述

### 架构组件
- **API 服务**: FastAPI (端口 8000)
- **数据库**: DolphinDB (端口 8848)
- **工作流引擎**: Prefect (端口 4200)
- **前端**: React (端口 3000)
- **反向代理**: Nginx (端口 80/443)

### 服务依赖关系
```
用户 → Nginx → FastAPI → DolphinDB
                    ↓
                 Prefect
```

### 关键路径
- **项目根目录**: `/Users/lisheng/Code/quantsystem/quant_research_system/backend`
- **配置文件**: `config/production.yaml`
- **日志目录**: `logs/`
- **备份目录**: `.backups/`
- **PID 文件**: `.pids/`

---

## 日常运维

### 服务管理

#### 启动服务
```bash
# 启动所有服务
cd /Users/lisheng/Code/quantsystem/quant_research_system
./start.sh

# 单独启动 Backend
cd backend
source .venv/bin/activate
python main.py &
echo $! > .pids/backend.pid

# 单独启动 Prefect
prefect server start &
```

#### 停止服务
```bash
# 停止所有服务
./stop.sh

# 单独停止 Backend
kill $(cat backend/.pids/backend.pid)

# 单独停止 Prefect
pkill -f "prefect server"
```

#### 重启服务
```bash
# 重启 Backend
cd backend
kill $(cat .pids/backend.pid)
source .venv/bin/activate
python main.py &
echo $! > .pids/backend.pid
```

#### 查看服务状态
```bash
# 检查所有服务
./check_status.sh

# 检查进程
ps aux | grep python
ps aux | grep prefect

# 检查端口
lsof -i :8000  # API
lsof -i :8848  # DolphinDB
lsof -i :4200  # Prefect
```

### 日志管理

#### 查看日志
```bash
# 实时查看应用日志
tail -f backend/logs/app.log

# 查看错误日志
grep -i "error\|exception" backend/logs/app.log | tail -50

# 查看 Nginx 日志
tail -f /var/log/nginx/access.log
tail -f /var/log/nginx/error.log

# 查看 DolphinDB 日志
tail -f /path/to/dolphindb/logs/dolphindb.log
```

#### 日志清理
```bash
# 清理 7 天前的日志
find backend/logs -name "*.log" -mtime +7 -delete

# 压缩旧日志
gzip backend/logs/*.log.$(date -d "yesterday" +%Y%m%d)

# 日志轮转（自动）
# 配置在 config/production.yaml 中
# rotation: "1 day"
# retention: "30 days"
```

### 配置管理

#### 查看当前配置
```bash
cd backend
source .venv/bin/activate
python -c "from app.core.config import settings; import json; print(json.dumps(settings.dict(), indent=2, default=str))"
```

#### 更新配置
```bash
# 1. 编辑配置文件
vim config/production.yaml

# 2. 验证配置
python -c "from app.core.config import settings; print('Config OK')"

# 3. 重启服务
kill $(cat .pids/backend.pid)
python main.py &
echo $! > .pids/backend.pid
```

#### 环境变量管理
```bash
# 查看环境变量
cat .env

# 更新环境变量
vim .env

# 重新加载环境变量
source .env
```

---

## 监控告警

### 监控面板

#### Grafana Dashboard
- **URL**: http://grafana.example.com
- **用户名**: admin
- **仪表盘**: Quant System v2.0

**关键指标**:
- API 请求量/错误率
- 响应时间 (P50/P95/P99)
- CPU/内存/磁盘使用率
- 数据库连接数
- 因子计算成功率

#### Prometheus
- **URL**: http://prometheus.example.com:9090
- **指标端点**: http://localhost:9090/metrics

**常用查询**:
```promql
# API 错误率
rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m])

# P95 响应时间
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# 内存使用率
process_resident_memory_bytes / node_memory_MemTotal_bytes
```

### 告警处理

#### 告警级别
- **Critical**: 立即处理（15分钟内）
- **Warning**: 尽快处理（1小时内）
- **Info**: 记录跟踪

#### 告警响应流程
1. **接收告警**: 邮件/钉钉/企业微信
2. **确认问题**: 查看监控面板和日志
3. **初步处理**: 根据故障排查手册操作
4. **升级**: 无法解决时联系相关负责人
5. **记录**: 在工单系统记录处理过程
6. **复盘**: 严重问题需要事后复盘

#### 常见告警处理

**告警: API 错误率过高**
```bash
# 1. 查看错误日志
tail -100 backend/logs/app.log | grep ERROR

# 2. 检查服务状态
curl http://localhost:8000/health

# 3. 检查数据库连接
python -c "from store.dolphindb_client import db_client; print(db_client.query('select 1'))"

# 4. 如果无法恢复，重启服务
kill $(cat backend/.pids/backend.pid)
python backend/main.py &
```

**告警: 响应时间过长**
```bash
# 1. 查看慢查询
grep "slow query" backend/logs/app.log

# 2. 检查数据库性能
# 在 DolphinDB 中执行
select * from getRecentJobs() order by endTime desc limit 10

# 3. 检查系统资源
top
df -h
```

**告警: 内存使用过高**
```bash
# 1. 查看进程内存
ps aux --sort=-%mem | head -10

# 2. 检查是否有内存泄漏
# 查看内存增长趋势

# 3. 重启服务释放内存
kill $(cat backend/.pids/backend.pid)
python backend/main.py &
```

---

## 故障排查

### 服务无法启动

#### 症状
- 执行 `python main.py` 后进程立即退出
- 无法访问 API 端点

#### 排查步骤
```bash
# 1. 查看启动日志
python main.py

# 2. 检查端口占用
lsof -i :8000

# 3. 检查配置文件
python -c "from app.core.config import settings"

# 4. 检查依赖
pip list | grep -E "fastapi|dolphindb|polars"

# 5. 检查权限
ls -la backend/logs
```

#### 常见原因
- 端口被占用
- 配置文件错误
- 依赖包缺失
- 日志目录权限不足
- 数据库连接失败

### 数据库连接失败

#### 症状
- API 返回 500 错误
- 日志显示 "Connection refused"

#### 排查步骤
```bash
# 1. 检查 DolphinDB 服务
docker ps | grep dolphindb
# 或
ps aux | grep dolphindb

# 2. 检查端口
lsof -i :8848

# 3. 测试连接
telnet localhost 8848

# 4. 检查配置
echo $DOLPHINDB_HOST
echo $DOLPHINDB_PORT

# 5. 手动连接测试
python -c "
import dolphindb as ddb
s = ddb.session()
s.connect('localhost', 8848, 'admin', '123456')
print(s.run('1+1'))
"
```

#### 解决方案
```bash
# 重启 DolphinDB
docker restart dolphindb
# 或
/path/to/dolphindb/dolphindb -console 0
```

### 因子计算失败

#### 症状
- 因子计算任务失败
- `factor_values` 表无数据

#### 排查步骤
```bash
# 1. 查看因子计算日志
grep "factor" backend/logs/app.log | tail -50

# 2. 检查因子注册
cd backend
python -c "
from engine.production.registry import discover_factors
factors = discover_factors()
print(f'Found {len(factors)} factors')
for name, factor in factors.items():
    print(f'  - {name}')
"

# 3. 检查数据源
python -c "
from store.dolphindb_client import db_client
result = db_client.query('select count(*) from daily_data')
print(result)
"

# 4. 手动执行因子计算
python -c "
from engine.production.engine import ProductionEngine
engine = ProductionEngine()
# 测试单个因子
"
```

### 数据同步失败

#### 症状
- 数据同步任务失败
- 数据不是最新的

#### 排查步骤
```bash
# 1. 查看同步日志
grep "sync" backend/logs/app.log | tail -50

# 2. 检查同步任务配置
python -c "
from store.dolphindb_client import db_client
result = db_client.query('select * from sync_task_config where enabled=true')
print(result)
"

# 3. 检查 Tushare Token
echo $TUSHARE_TOKEN

# 4. 测试 Tushare API
python -c "
import tushare as ts
ts.set_token('$TUSHARE_TOKEN')
pro = ts.pro_api()
df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
print(df.head())
"

# 5. 手动执行同步
curl -X POST http://localhost:8000/api/v1/data/sync/task/1
```

### 性能问题

#### 症状
- API 响应慢
- 因子计算耗时长

#### 排查步骤
```bash
# 1. 查看慢查询
grep "slow" backend/logs/app.log

# 2. 检查系统资源
top
htop
iostat -x 1

# 3. 检查数据库性能
# DolphinDB 中执行
select * from getRecentJobs() where endTime > now() - 3600000 order by endTime desc

# 4. 分析 API 性能
# 使用 profiling 工具
python -m cProfile -o profile.stats backend/main.py
```

---

## 性能调优

### API 性能优化

#### 增加 Worker 数量
```yaml
# config/production.yaml
server:
  workers: 8  # 根据 CPU 核心数调整
```

#### 启用缓存
```yaml
# config/production.yaml
cache:
  enabled: true
  ttl: 3600
  max_size: 1000
```

#### 连接池优化
```yaml
# config/production.yaml
database:
  pool_size: 50
  timeout: 30
```

### 数据库性能优化

#### 分区优化
```python
# 确保分区表配置正确
# daily_data 按日期分区
# factor_values 按日期分区
```

#### 索引优化
```python
# DolphinDB 通过分区实现索引
# 确保查询条件包含分区键
```

#### 查询优化
```python
# 1. 使用分区过滤
select * from daily_data where trade_date >= 20240101

# 2. 限制返回行数
select top 1000 * from daily_data

# 3. 使用列过滤
select ts_code, trade_date, close from daily_data
```

### 因子计算优化

#### 并行计算
```yaml
# config/production.yaml
factor:
  parallel_workers: 4
  batch_size: 500
```

#### 增量计算
```python
# 使用增量模式
engine.run_task(
    factor_id=1,
    mode="incremental",
    days=1
)
```

---

## 备份恢复

### 数据备份

#### 自动备份
```yaml
# config/production.yaml
backup:
  enabled: true
  schedule: "0 2 * * *"  # 每天凌晨2点
  retention_days: 30
  path: "/data/backups"
```

#### 手动备份
```bash
# 备份代码
cd /Users/lisheng/Code/quantsystem/quant_research_system
tar -czf backup_$(date +%Y%m%d).tar.gz \
    --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='logs' \
    backend/

# 备份配置
cp -r backend/config backup_config_$(date +%Y%m%d)

# 备份数据库
# DolphinDB 备份需要使用专用工具
```

### 数据恢复

#### 恢复代码
```bash
tar -xzf backup_20260307.tar.gz -C /path/to/restore
```

#### 恢复配置
```bash
cp -r backup_config_20260307/* backend/config/
```

#### 恢复数据库
```bash
# 使用 DolphinDB 恢复工具
# 或从备份表恢复
```

---

## 安全管理

### 访问控制

#### SSH 访问
```bash
# 仅允许密钥登录
# /etc/ssh/sshd_config
PasswordAuthentication no
PubkeyAuthentication yes
```

#### 防火墙规则
```bash
# 仅开放必要端口
sudo ufw allow 22/tcp   # SSH
sudo ufw allow 80/tcp   # HTTP
sudo ufw allow 443/tcp  # HTTPS
sudo ufw enable
```

### 密钥管理

#### 环境变量
```bash
# 敏感信息存储在 .env 文件
# 不要提交到 Git
echo ".env" >> .gitignore
```

#### 密钥轮换
```bash
# 定期更新密钥
# 1. 生成新密钥
# 2. 更新 .env 文件
# 3. 重启服务
# 4. 验证功能正常
# 5. 撤销旧密钥
```

### 日志审计

#### 访问日志
```bash
# 查看 API 访问日志
tail -f /var/log/nginx/access.log

# 分析异常访问
awk '{print $1}' /var/log/nginx/access.log | sort | uniq -c | sort -rn | head -20
```

#### 操作日志
```bash
# 查看系统操作日志
tail -f /var/log/auth.log

# 查看应用操作日志
grep "user_action" backend/logs/app.log
```

---

## 应急预案

### 服务完全不可用

#### 应急步骤
1. **立即通知**: 通知技术负责人和用户
2. **快速诊断**: 检查服务状态和日志
3. **尝试重启**: 重启所有服务
4. **回滚版本**: 如果是新版本问题，立即回滚
5. **启用备用**: 如果有备用服务器，切换流量

#### 执行命令
```bash
# 1. 检查状态
./check_status.sh

# 2. 重启服务
./stop.sh
./start.sh

# 3. 回滚版本
cd backend/scripts/deploy
bash rollback.sh

# 4. 验证恢复
bash health_check.sh
bash smoke_test.sh
```

### 数据库故障

#### 应急步骤
1. **检查 DolphinDB 状态**
2. **尝试重启 DolphinDB**
3. **检查磁盘空间**
4. **从备份恢复**

#### 执行命令
```bash
# 1. 重启 DolphinDB
docker restart dolphindb

# 2. 检查磁盘
df -h

# 3. 清理空间
find /path/to/dolphindb/logs -name "*.log" -mtime +7 -delete
```

### 性能严重退化

#### 应急步骤
1. **检查系统资源**
2. **识别性能瓶颈**
3. **临时扩容**
4. **限流降级**

#### 执行命令
```bash
# 1. 检查资源
top
df -h
iostat

# 2. 限流
# 在 Nginx 中配置
limit_req_zone $binary_remote_addr zone=api:10m rate=100r/s;

# 3. 降级
# 临时关闭非核心功能
```

---

## 联系方式

### 技术团队
| 角色 | 姓名 | 电话 | 邮箱 | 工作时间 |
|------|------|------|------|----------|
| 技术负责人 | ___ | ___ | ___ | 24/7 |
| 运维负责人 | ___ | ___ | ___ | 24/7 |
| 值班工程师 | ___ | ___ | ___ | 轮班 |
| DBA | ___ | ___ | ___ | 工作日 9-18 |

### 外部支持
| 服务 | 联系方式 | 说明 |
|------|---------|------|
| DolphinDB 技术支持 | support@dolphindb.com | 数据库问题 |
| Tushare 客服 | service@tushare.pro | 数据接口问题 |
| 云服务商 | ___ | 基础设施问题 |

---

**文档版本**: v1.0
**最后更新**: 2026-03-07
**维护人**: DevOps Team
