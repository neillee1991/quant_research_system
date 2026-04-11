# 开发环境快速设置

## 前置要求

- Python 3.11（PyCaret 兼容性要求）
- Node.js 16+
- Docker & Docker Compose
- Git

## 一键启动（推荐）

```bash
# 在项目根目录执行

# 首次运行：配置环境
./setup.sh

# 启动所有服务
./start.sh

# 检查服务状态
./check_status.sh

# 停止所有服务
./stop.sh
```

启动后访问：
- **前端界面**: http://localhost:3000
- **API 文档**: http://localhost:8000/docs
- **Prefect UI**: http://localhost:4200
- **DolphinDB Web UI**: http://localhost:8848

---

## 手动设置

### 1. 后端环境

```bash
cd backend

# 创建虚拟环境
python3.11 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# 或
.venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env，设置必要的配置（如 TUSHARE_TOKEN）

# 启动开发服务器
python main.py
# 或
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 2. 前端环境

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm start  # 访问 http://localhost:3000
```

### 3. 数据库环境

```bash
# 启动 DolphinDB (Docker)
docker-compose up -d dolphindb

# 初始化数据库表结构
cd backend
python database/init_dolphindb.py

# DolphinDB Web UI: http://localhost:8848
# 默认用户名: admin
# 默认密码: 123456
```

### 4. Prefect 调度器

```bash
# 启动 Prefect Server
docker-compose up -d prefect-server

# Prefect UI: http://localhost:4200
```

---

## 环境变量配置

创建 `backend/.env` 文件：

```bash
# DolphinDB 配置
DOLPHINDB__HOST=localhost
DOLPHINDB__PORT=8848
DOLPHINDB__USERNAME=admin
DOLPHINDB__PASSWORD=123456

# Tushare API 配置（必需）
COLLECTOR__TUSHARE_TOKEN=your_token_here

# API 限流配置
COLLECTOR__CALLS_PER_MINUTE=120

# 回测配置
BACKTEST__INITIAL_CAPITAL=1000000
BACKTEST__COMMISSION_RATE=0.001

# 日志配置
LOG_LEVEL=INFO
```

---

## 验证安装

```bash
# 测试后端连接
cd backend
python -c "from store.dolphindb_client import db_client; print(db_client.query('SELECT 1'))"

# 测试前端构建
cd frontend
npm run build

# 运行测试
cd backend
pytest tests/ --cov=. --cov-report=term-missing
```

---

## 常见问题

**Q: Python 版本错误**
```bash
# 确保使用 Python 3.11
python3.11 --version

# 如果没有 Python 3.11，使用 pyenv 安装
pyenv install 3.11.9
pyenv local 3.11.9
```

**Q: DolphinDB 连接失败**
```bash
# 检查容器状态
docker ps | grep dolphindb

# 查看日志
docker-compose logs dolphindb

# 重启容器
docker-compose restart dolphindb
```

**Q: 端口被占用**
```bash
# 查看占用端口的进程
lsof -ti:8000  # 后端
lsof -ti:3000  # 前端
lsof -ti:8848  # DolphinDB

# 修改 .env 中的端口配置或停止占用进程
```

---

## 下一步

- 阅读 [DEVELOPER_GUIDE.md](./DEVELOPER_GUIDE.md) 了解开发规范
- 阅读 [ARCHITECTURE.md](./ARCHITECTURE.md) 了解系统架构
- 查看 [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) 解决常见问题
