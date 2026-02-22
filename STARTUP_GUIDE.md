# 量化研究系统 - 一键启动脚本

## 快速开始

### 启动所有服务

```bash
./start.sh
```

这个脚本会自动：
1. 检查 Docker 环境
2. 启动 PostgreSQL 数据库
3. 检查 Python 环境并创建虚拟环境（如果不存在）
4. 启动后端 FastAPI 服务
5. 检查 Node.js 环境并安装依赖（如果不存在）
6. 启动前端 React 服务

### 停止所有服务

```bash
./stop.sh
```

这个脚本会停止：
- 后端服务
- 前端服务
- PostgreSQL 数据库
- pgAdmin 管理界面

## 服务地址

启动成功后，可以访问以下地址：

- **前端界面**: http://localhost:3000
- **后端 API**: http://localhost:8000
- **API 文档**: http://localhost:8000/docs
- **PostgreSQL**: localhost:5432
- **pgAdmin**: http://localhost:5050

## 日志查看

### 实时查看后端日志
```bash
tail -f logs/backend.log
```

### 实时查看前端日志
```bash
tail -f logs/frontend.log
```

## 常见问题

### 1. 端口被占用

如果提示端口被占用，请检查：
- 3000 端口（前端）
- 8000 端口（后端）
- 5432 端口（PostgreSQL）
- 5050 端口（pgAdmin）

### 2. 服务启动失败

查看对应的日志文件：
```bash
# 后端日志
cat logs/backend.log

# 前端日志
cat logs/frontend.log
```

### 3. 重启服务

如果服务已在运行，脚本会提示是否重启：
```bash
./start.sh
# 提示：是否重启服务? (y/n)
```

### 4. 清理所有数据

如果需要完全清理数据库数据：
```bash
docker-compose down -v
```

## 手动启动（开发模式）

如果需要手动启动各个服务：

### 启动数据库
```bash
docker-compose up -d
```

### 启动后端
```bash
cd backend
source venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 启动前端
```bash
cd frontend
npm start
```

## 系统要求

- Docker Desktop
- Python 3.8+
- Node.js 14+
- npm 6+

## 目录结构

```
quant_research_system/
├── start.sh              # 一键启动脚本
├── stop.sh               # 停止服务脚本
├── logs/                 # 日志目录
│   ├── backend.log       # 后端日志
│   └── frontend.log      # 前端日志
├── .pids/                # PID 文件目录
│   ├── backend.pid       # 后端进程 ID
│   └── frontend.pid      # 前端进程 ID
├── backend/              # 后端代码
└── frontend/             # 前端代码
```

## 注意事项

1. 首次启动会自动创建 Python 虚拟环境和安装依赖，需要较长时间
2. 前端首次启动需要安装 npm 依赖，也需要较长时间
3. 服务在后台运行，关闭终端不会停止服务
4. 使用 `./stop.sh` 停止所有服务
5. 调度器会在后端启动时自动加载并运行配置的定时任务
