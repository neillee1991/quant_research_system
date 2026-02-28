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
# 编辑 .env 填入 TUSHARE_TOKEN 和 DOLPHINDB_PASSWORD

./setup.sh

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
- **Prefect UI**: http://localhost:4200
- **DolphinDB**: http://localhost:8848

## 核心功能

| 模块 | 功能 | 技术栈 |
|------|------|--------|
| **数据层** | DolphinDB (TSDB) | 列存分区表 + 内存表缓存 |
| **数据同步** | Tushare/AkShare | 增量同步 + Prefect 编排 + 定时任务 |
| **因子库** | 技术指标计算 | Polars 向量化: MA, EMA, RSI, MACD, KDJ, Bollinger, ATR |
| **生产因子** | 因子注册与分析 | @factor 装饰器 + IC/IR 分析 + 分组收益 |
| **回测引擎** | 向量化回测 | VectorBT: Sharpe, MaxDD, WinRate, ProfitFactor |
| **策略建模** | 可视化拖拽 | React Flow + DSL 解析器 |
| **AutoML** | 模型训练优化 | PyCaret + Optuna 贝叶斯优化 |
| **前端** | 交互界面 | React 18 + TypeScript + Semi Design + ECharts |

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
│   │   └── sync_components.py # 同步组件
│   ├── flows/                 # Prefect 流程定义
│   │   ├── data_sync_flow.py  # 数据同步流
│   │   ├── dynamic_flow.py    # 动态流程
│   │   └── serve.py           # 流程部署入口
│   ├── engine/
│   │   ├── analysis/          # 因子分析（IC/IR）
│   │   ├── backtester/        # 向量化回测
│   │   ├── factors/           # 技术/财务因子库
│   │   ├── parser/            # 策略 DSL 解析
│   │   └── production/        # 生产因子框架
│   ├── ml_module/             # AutoML 模块
│   ├── store/                 # 数据库客户端
│   │   └── dolphindb_client.py # DolphinDB 连接管理
│   ├── database/
│   │   ├── init_dolphindb.dos # DolphinDB 初始化脚本
│   │   └── init_dolphindb.py  # 初始化执行器
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
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848

# 嵌套命名（对应 settings.xxx.yyy）
DOLPHINDB__USERNAME=admin
DOLPHINDB__PASSWORD=123456
BACKTEST__INITIAL_CAPITAL=1000000
```

完整配置项参见 [.env.example](.env.example)

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
# 初始化数据库（首次运行）
cd backend
python -m database.init_dolphindb

# 查看容器状态
docker ps | grep quant_dolphindb

# 查看日志
docker-compose logs -f dolphindb
```

### Prefect 调度

```bash
# 部署 Prefect 流程
cd backend
python -m flows.serve

# 查看 Prefect UI
# 浏览器访问 http://localhost:4200
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

- ✅ DolphinDB TSDB 分区表（COMPO 分区策略）
- ✅ SQL 注入防护（参数化查询）
- ✅ 流式查询（大数据集支持）
- ✅ Prefect 任务编排与调度
- ✅ GZip 压缩（响应减少60-80%）
- ✅ VectorBT 向量化回测

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

### 数据库初始化

```bash
cd backend

# 初始化 DolphinDB 表结构
python -m database.init_dolphindb
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
# 检查 DolphinDB 状态
docker ps | grep quant_dolphindb

# 查看数据库日志
docker-compose logs dolphindb

# 重启数据库
docker-compose restart dolphindb
```

### Prefect 调度异常

```bash
# 检查 Prefect Server 状态
curl http://localhost:4200/api/health

# 查看 Prefect 日志
docker-compose logs prefect-server

# 重启 Prefect
docker-compose restart prefect-server
```

### 系统健康检查

```bash
cd backend
python health_check.py
```

## 技术特点

### DolphinDB 优势
- TSDB 引擎，专为时序数据优化
- COMPO 分区策略（按月 + 按股票代码）
- 列式存储，高效聚合查询
- 内置流计算和分布式计算能力

### Prefect 调度
- 声明式流程定义
- 可视化 DAG 监控
- 自动重试与错误处理
- Cron 定时调度

### VectorBT 回测
- 向量化计算，高性能回测
- 丰富的指标库（Sharpe, MaxDD, WinRate）
- 支持组合优化与参数扫描

### Polars 数据处理
- 比 Pandas 快 5-10 倍
- 更低的内存占用
- 惰性求值优化
- 向量化操作

### 向量化回测 (VectorBT)
- 处理整个价格序列（无循环）
- 基于 VectorBT 高效计算
- 支持复杂策略逻辑与参数优化

## 相关文档

- [性能优化报告](docs/PERFORMANCE.md)
- [Claude AI 助手指南](CLAUDE.md)
- [待办事项](TODO.md)

## 许可证

MIT License
