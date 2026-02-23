# 量化研究系统 (Quant Research System)

一个全栈量化交易研究平台，提供拖拽式策略建模、向量化回测和 AutoML 功能。

## 快速开始

### 环境要求

- Python 3.11+
- Node.js 18+
- Docker & Docker Compose
- Tushare Token（可选，用于数据同步）

### 一键启动

```bash
# 1. 配置环境变量（首次运行）
cp .env.example .env
# 编辑 .env 填入 TUSHARE_TOKEN 和 POSTGRES_PASSWORD

# 2. 启动所有服务
./start.sh

# 3. 检查服务状态
./check_status.sh

# 4. 停止服务
./stop.sh
```

启动后访问：
- **前端界面**: http://localhost:3000
- **API 文档**: http://localhost:8000/docs
- **健康检查**: `python backend/health_check.py`

## 核心功能

| 模块 | 功能 | 技术栈 |
|------|------|--------|
| **数据层** | PostgreSQL 16 + Redis 7 | 连接池 + 索引优化 + 缓存层 |
| **数据同步** | Tushare/AkShare | 增量同步 + DAG 编排 + 定时任务 |
| **因子库** | 技术指标计算 | Polars 向量化: MA, EMA, RSI, MACD, KDJ, Bollinger, ATR |
| **生产因子** | 因子注册与分析 | @factor 装饰器 + IC/IR 分析 + 分组收益 |
| **回测引擎** | 向量化回测 | Sharpe, MaxDD, WinRate, ProfitFactor |
| **策略建模** | 可视化拖拽 | React Flow + DSL 解析器 |
| **AutoML** | 模型训练优化 | PyCaret + Optuna 贝叶斯优化 |
| **前端** | 交互界面 | React 18 + TypeScript + Ant Design + ECharts |

## 项目架构

```
quant_research_system/
├── backend/                    # Python 后端
│   ├── app/
│   │   ├── api/v1/            # FastAPI 路由
│   │   ├── core/              # 配置、日志、异常
│   │   └── services/          # 业务逻辑层
│   ├── data_manager/          # 数据同步引擎
│   │   ├── collectors/        # Tushare/AkShare 采集器
│   │   ├── dag_executor.py    # DAG 任务编排
│   │   └── scheduler.py       # 定时任务调度
│   ├── engine/
│   │   ├── analysis/          # 因子分析（IC/IR）
│   │   ├── backtester/        # 向量化回测
│   │   ├── factors/           # 技术/财务因子库
│   │   ├── parser/            # 策略 DSL 解析
│   │   └── production/        # 生产因子框架
│   ├── ml_module/             # AutoML 模块
│   ├── store/                 # 数据库客户端
│   │   ├── postgres_client.py # PostgreSQL 连接池
│   │   └── redis_client.py    # Redis 缓存
│   ├── database/
│   │   └── migrations/        # 数据库迁移脚本
│   └── health_check.py        # 系统健康检查
├── frontend/                   # React 前端
│   └── src/
│       ├── components/        # 图表组件
│       ├── pages/             # 页面（数据中心/因子中心/策略中心）
│       └── api/               # API 客户端
├── config/                     # 配置文件
│   └── scripts.config.sh      # 脚本统一配置
├── docker-compose.yml         # Docker 服务编排
├── .env.example               # 环境变量模板
├── start.sh                   # 启动脚本
├── stop.sh                    # 停止脚本
└── check_status.sh            # 状态检查脚本
```

## 配置说明

### 环境变量配置

所有配置项在 `.env` 文件中定义，支持两种命名方式：

```bash
# 扁平命名
TUSHARE_TOKEN=your_token_here
POSTGRES_PASSWORD=your_password

# 嵌套命名（对应 settings.xxx.yyy）
DATABASE__CONNECTION_POOL_SIZE=50
CACHE_TTL_STOCK_LIST=3600
BACKTEST__INITIAL_CAPITAL=1000000
```

完整配置项参见 [.env.example](.env.example)

### 脚本配置

启动脚本的配置参数统一在 `config/scripts.config.sh` 中定义：

- 服务端口配置
- Docker 容器名称
- 日志和 PID 文件路径
- Python 版本要求
- 功能开关（Redis、索引检查等）

## 数据库管理

### PostgreSQL

```bash
# 连接数据库
docker exec -it quant_postgres psql -U quant_user -d quant_research

# 备份数据库
docker exec quant_postgres pg_dump -U quant_user quant_research > backup.sql

# 恢复数据库
docker exec -i quant_postgres psql -U quant_user quant_research < backup.sql

# 查看日志
docker-compose logs -f postgres
```

### Redis 缓存

```bash
# 连接 Redis
docker exec -it quant_redis redis-cli

# 查看缓存键
docker exec quant_redis redis-cli keys "*"

# 查看缓存统计
docker exec quant_redis redis-cli info stats

# 清空缓存
docker exec quant_redis redis-cli flushdb
```

## 性能优化

系统已完成全面性能优化，详见 [docs/PERFORMANCE.md](docs/PERFORMANCE.md)

### 优化成果

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| API 响应时间（P95） | 2-5秒 | < 100ms | **95%+** |
| 数据库查询时间 | 1-10秒 | < 50ms | **95%+** |
| 内存占用（峰值） | 8-10GB | < 2GB | **80%** |
| 并发支持（QPS） | 10-20 | 200+ | **10倍** |
| 缓存命中率 | 0% | 85%+ | **新增** |

### 优化措施

- ✅ 数据库索引优化（47个性能索引）
- ✅ SQL 注入防护（参数化查询）
- ✅ N+1 查询优化
- ✅ 流式查询（大数据集支持）
- ✅ 连接池优化（10-50连接）
- ✅ Redis 缓存层（Docker 部署）
- ✅ GZip 压缩（响应减少60-80%）
- ✅ 前端轮询优化

## 开发指南

### 后端开发

```bash
cd backend

# 创建虚拟环境
python3.11 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 启动开发服务器
python main.py
# 或
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 前端开发

```bash
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm start
```

### 数据库迁移

```bash
cd backend

# 应用性能索引
python database/migrations/apply_indexes_direct.py

# 初始化数据库表
python init_database.py
```

## API 文档

启动后端服务后访问：http://localhost:8000/docs

主要 API 端点：

- `/api/v1/data/*` - 数据查询和同步
- `/api/v1/factor/*` - 因子计算
- `/api/v1/strategy/*` - 回测执行
- `/api/v1/ml/*` - AutoML 训练
- `/api/v1/production/*` - 生产因子管理

## 故障排查

### 服务无法启动

```bash
# 检查服务状态
./check_status.sh

# 查看日志
tail -f logs/backend.log
tail -f logs/frontend.log

# 检查端口占用
lsof -ti:8000  # 后端
lsof -ti:3000  # 前端
```

### 数据库连接失败

```bash
# 检查 PostgreSQL 状态
docker ps | grep quant_postgres

# 查看数据库日志
docker-compose logs postgres

# 重启数据库
docker-compose restart postgres
```

### Redis 缓存不可用

```bash
# 检查 Redis 状态
docker ps | grep quant_redis

# 启动 Redis
docker-compose up -d redis

# 测试连接
docker exec quant_redis redis-cli ping
```

### 系统健康检查

```bash
cd backend
python health_check.py
```

## 技术特点

### PostgreSQL 优势
- 多用户并发访问
- 事务隔离和 ACID 保证
- 连接池管理（10-50连接）
- 丰富的索引和查询优化

### Redis 缓存策略
- 股票列表缓存：1小时
- 日线数据缓存：30分钟
- 因子元数据缓存：1小时
- 因子分析缓存：2小时
- 自动失效机制

### Polars 数据处理
- 比 Pandas 快 5-10 倍
- 更低的内存占用
- 惰性求值优化
- 向量化操作

### 向量化回测
- 处理整个价格序列（无循环）
- 使用 Polars 高效计算
- 支持复杂策略逻辑

## 相关文档

- [性能优化报告](docs/PERFORMANCE.md)
- [Claude AI 助手指南](CLAUDE.md)
- [待办事项](TODO.md)

## 许可证

MIT License
