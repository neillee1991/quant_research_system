# 部署指南

## 环境要求

- Python 3.11+
- Node.js 18+
- Docker & Docker Compose
- Tushare Token（可选，用于数据同步）

## 快速开始

### 1. 配置环境变量

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑 .env 填入必要配置
# - TUSHARE_TOKEN
# - DOLPHINDB_PASSWORD
```

### 2. 运行安装脚本

```bash
./setup.sh
```

### 3. 启动所有服务

```bash
./start.sh
```

### 4. 检查服务状态

```bash
./check_status.sh
```

### 5. 停止服务

```bash
./stop.sh
```

## 服务访问

启动后访问：
- **前端界面**: http://localhost:3000
- **API 文档**: http://localhost:8000/docs
- **Prefect UI**: http://localhost:4200
- **DolphinDB Web UI**: http://localhost:8848

## 手动部署

### 后端部署

```bash
cd backend

# 创建虚拟环境
python3.11 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 初始化数据库
python database/init_dolphindb.py

# 启动开发服务器
python main.py
# 或
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 前端部署

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm start
```

### Docker 部署

```bash
# 启动 DolphinDB
docker-compose up -d dolphindb

# 启动 Prefect
docker-compose up -d prefect-server
```

## 配置说明

### 环境变量配置

所有配置项在 `.env` 文件中定义，支持两种命名方式：

```bash
# 扁平命名
TUSHARE_TOKEN=your_token_here
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848

# 嵌套命名（对应 settings.xxx.yyy）
DOLPHINDB__USERNAME=admin
DOLPHINDB__PASSWORD=123456
BACKTEST__INITIAL_CAPITAL=1000000
```

### 脚本配置

启动脚本的配置参数统一在 `config/scripts.config.sh` 中定义：
- 服务端口配置
- Docker 容器名称
- 日志和 PID 文件路径
- Python 版本要求
- 功能开关（Prefect Worker 等）

## 数据库管理

### DolphinDB

```bash
# 查看容器状态
docker ps | grep quant_dolphindb

# 查看日志
docker-compose logs -f dolphindb

# 重启数据库
docker-compose restart dolphindb
```

### Prefect 调度

```bash
# 部署 Prefect 流程
cd backend
python -m flows.serve

# 查看 Prefect UI
# 浏览器访问 http://localhost:4200
```

## 生产环境部署

### 安全检查清单

- [ ] 修改默认密码
- [ ] 配置 HTTPS
- [ ] 启用认证和授权
- [ ] 配置防火墙规则
- [ ] 定期备份数据库
- [ ] 配置日志轮转
- [ ] 监控系统资源

### 备份策略

```bash
# 备份配置
python scripts/maintenance/backup_configs.py

# 定期备份数据库
# 建议使用 cron 或其他定时任务
```

## 性能优化

系统已完成全面性能优化：
- DolphinDB TSDB 分区表（COMPO 分区策略）
- SQL 注入防护（参数化查询）
- 流式查询（大数据集支持）
- Prefect 任务编排与调度
- GZip 压缩（响应减少 60-80%）
- VectorBT 向量化回测
