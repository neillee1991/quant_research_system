# 量化研究系统 (Quant Research System)

一个全栈量化交易研究平台，提供拖拽式策略建模、向量化回测和 AutoML 功能。

## 快速开始

### 环境要求

- Python 3.11 (PyCaret 要求)
- Node.js 18+
- Docker & Docker Compose
- Tushare Token (可选，用于数据同步)

### 一键启动

```bash
# 启动所有服务（数据库、后端、前端）
./start.sh

# 检查服务状态
./check_status.sh

# 停止所有服务
./stop.sh
```

启动后访问：
- **前端界面**: http://localhost:3000
- **API 文档**: http://localhost:8000/docs
- **pgAdmin**: http://localhost:5050 (admin@quant.com / admin123)

### 手动启动

#### 1. 启动 PostgreSQL 数据库

```bash
docker-compose up -d
```

#### 2. 启动后端

```bash
# 配置环境变量（首次，在项目根目录）
cp .env.example .env
# 编辑 .env 填入 TUSHARE_TOKEN 和 POSTGRES_PASSWORD

cd backend

# 创建虚拟环境（首次）
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 启动服务
python main.py
```

#### 3. 启动前端

```bash
cd frontend
npm install
npm start
```

## 核心功能

| 功能 | 技术栈 |
|---|---|
| 数据库 | PostgreSQL 16 + 连接池 + Docker |
| 数据同步 | Tushare/AkShare + 增量同步 + DAG 编排 |
| 因子库 | Polars 向量化: MA, EMA, RSI, MACD, KDJ, Bollinger, ATR |
| 生产因子 | @factor 装饰器注册 + 沙箱测试 + IC/IR 分析 + 分组收益 |
| 回测引擎 | 向量化计算: Sharpe, MaxDD, WinRate, ProfitFactor |
| 策略建模 | React Flow 拖拽式可视化 + DSL 解析器 |
| AutoML | PyCaret 模型对比 + Optuna 贝叶斯优化 |
| 定时任务 | APScheduler + DAG 依赖调度 |
| 前端 | React 18 + TypeScript + Ant Design + ECharts + Monaco Editor |

## 项目架构

```
quant_research_system/
├── backend/
│   ├── app/
│   │   ├── api/v1/              # FastAPI 路由（data, factor, strategy, ml, production）
│   │   ├── core/                # 配置、日志、异常、依赖注入
│   │   └── services/            # 业务逻辑层
│   ├── data_manager/            # 数据同步引擎 + DAG 调度
│   │   ├── collectors/          # Tushare / AkShare 采集器
│   │   ├── dag_executor.py      # DAG 任务编排引擎
│   │   └── scheduler.py         # APScheduler 定时任务
│   ├── engine/
│   │   ├── analysis/            # 因子分析（IC/IR/分组收益）
│   │   ├── backtester/          # 向量化回测引擎
│   │   ├── factors/             # 技术/财务因子库
│   │   ├── parser/              # 策略 DSL 解析器
│   │   └── production/          # 生产因子框架（注册/计算/调度）
│   │       ├── registry.py      # @factor 装饰器注册
│   │       ├── engine.py        # 增量/全量计算引擎
│   │       └── factors/         # 已注册因子（momentum, value, volatility）
│   ├── ml_module/               # AutoML（PyCaret + Optuna）
│   └── store/                   # PostgreSQL 客户端 + Parquet 存储
├── frontend/
│   ├── src/
│   │   ├── components/          # Charts（K线/权益曲线）+ FlowEditor（拖拽策略）
│   │   ├── pages/               # DataCenter / FactorCenter / StrategyCenter
│   │   ├── api/                 # Axios API 客户端
│   │   └── store/               # Zustand 状态管理
├── docker-compose.yml           # PostgreSQL + pgAdmin
├── .env.example                 # 环境变量模板
├── start.sh                     # 一键启动脚本
├── stop.sh                      # 停止脚本
└── check_status.sh              # 状态检查脚本
```

## 数据库管理

### 连接信息

- Host: `localhost`
- Port: `5432`
- Database: `quant_research`
- User: `quant_user`
- Password: 见 `.env` 中的 `POSTGRES_PASSWORD`

### 常用命令

```bash
# 连接数据库
docker exec -it quant_postgres psql -U quant_user -d quant_research

# 备份数据库
docker exec quant_postgres pg_dump -U quant_user quant_research > backup.sql

# 恢复数据库
docker exec -i quant_postgres psql -U quant_user quant_research < backup.sql

# 查看日志
docker-compose logs -f postgres

# 使用数据库管理工具
python db_manager.py
```

### pgAdmin Web 界面

访问 http://localhost:5050
- Email: `admin@quant.com`
- Password: `admin123`

添加服务器连接：
- Host: `postgres` (Docker 内部) 或 `localhost` (本地)
- Port: `5432`
- Database: `quant_research`
- Username: `quant_user`
- Password: 见 `.env` 中的 `POSTGRES_PASSWORD`

## API 接口

所有 API 接口都在 `/api/v1/` 路径下：

### 数据管理 (`/data/*`)
- `GET /data/stocks` - 获取股票列表
- `GET /data/daily` - 获取日线数据
- `GET /data/tables` - 获取数据库表列表
- `POST /data/query` - 执行 SQL 查询
- `GET /data/sync/tasks` - 获取同步任务列表
- `POST /data/sync/task/{task_id}` - 执行同步任务
- `POST /data/sync/all` - 批量同步所有任务

### 生产因子 (`/production/*`)
- `GET /production/factors` - 因子列表
- `POST /production/factors` - 创建因子（支持代码定义）
- `PUT /production/factors/{factor_id}` - 更新因子
- `DELETE /production/factors/{factor_id}` - 删除因子
- `POST /production/factors/{factor_id}/run` - 运行因子计算（增量/全量）
- `POST /production/factors/test` - 沙箱测试因子代码
- `POST /production/factors/{factor_id}/analyze` - IC/IR 分析 + 分组收益
- `GET /production/factors/{factor_id}/data` - 查询因子数据

### DAG 编排 (`/dag/*`)
- `GET /dag/list` - DAG 列表
- `POST /dag/create` - 创建 DAG
- `POST /dag/run` - 运行 DAG（支持回填）
- `GET /dag/{dag_id}/history` - 运行历史

### 因子计算 (`/factor/*`)
- `POST /factor/compute` - 计算技术指标
- `POST /factor/ic` - IC 分析

### 策略回测 (`/strategy/*`)
- `POST /strategy/backtest` - 执行回测
- `GET /strategy/operators` - 获取算子列表

### AutoML (`/ml/*`)
- `POST /ml/train` - 训练模型
- `GET /ml/status/{job_id}` - 训练状态
- `GET /ml/weights` - 特征权重

完整 API 文档：http://localhost:8000/docs

## 数据同步

### 同步配置

同步任务配置在 `backend/data_manager/sync_config.json`：

```json
{
  "task_id": "daily_basic",
  "api_name": "daily_basic",
  "sync_type": "incremental",
  "params": {
    "trade_date": "{date}"
  },
  "table_name": "daily_basic",
  "primary_keys": ["ts_code", "trade_date"],
  "batch_size": 5000,
  "api_limit": 5000,
  "enabled": true
}
```

### 同步类型

- **incremental**: 增量同步，记录最后同步日期
- **full**: 全量同步，每次完全替换

### API 分页

当 Tushare API 有数据量限制时，设置 `api_limit` 参数自动分页：

```json
{
  "api_limit": 5000
}
```

系统会自动循环调用 API，使用 `limit` 和 `offset` 参数获取完整数据。

### 定时任务

后端启动时会自动加载定时任务（默认每日 18:00 执行）。

## 开发指南

### 添加新的同步任务

1. 编辑 `backend/data_manager/sync_config.json`
2. 添加任务配置（参考 `task_config_template.json`）
3. 定义表结构（使用 PostgreSQL 类型）
4. 设置主键用于 upsert 操作
5. 任务会在首次同步时自动创建表

### 添加新的 API 端点

1. 在 `backend/app/api/v1/` 添加路由
2. 使用 Pydantic 模型进行请求/响应验证
3. 通过依赖注入使用服务层
4. 抛出自定义异常（`app.core.exceptions`）
5. FastAPI 会自动生成 OpenAPI 文档

### 添加新的因子

#### 方式一：生产因子框架（推荐）

```python
# backend/engine/production/factors/my_factor.py
from engine.production.registry import factor

@factor(
    factor_id="my_alpha_01",
    description="自定义 Alpha 因子",
    category="alpha",
    depends_on=["daily_data"],
)
def compute(df):
    """接收 Polars DataFrame，返回含 factor_value 列的 DataFrame"""
    return df.with_columns(
        (pl.col("close") / pl.col("close").shift(5) - 1).alias("factor_value")
    )
```

因子会在导入时自动注册，支持通过 API 或前端运行增量/全量计算。

#### 方式二：技术因子库

1. 在 `backend/engine/factors/` 添加因子函数
2. 使用 Polars 表达式实现向量化计算
3. 在 `FactorService` 中注册因子

## 故障排查

### 端口被占用

检查以下端口是否被占用：
- 3000 (前端)
- 8000 (后端)
- 5432 (PostgreSQL)
- 5050 (pgAdmin)

```bash
# macOS/Linux
lsof -i :8000
kill -9 <PID>
```

### 数据库连接失败

```bash
# 检查 PostgreSQL 是否运行
docker ps | grep quant_postgres

# 查看日志
docker-compose logs postgres

# 重启数据库
docker-compose restart postgres
```

### 同步任务失败

1. 检查 Tushare Token 是否配置正确
2. 查看后端日志：`tail -f /tmp/backend.log`
3. 检查任务配置：`backend/data_manager/sync_config.json`
4. 验证任务是否启用：`"enabled": true`

### SQL 语法错误

PostgreSQL 使用 `%s` 占位符，不是 `?`：

```python
# 正确
df = db_client.query("SELECT * FROM daily_basic WHERE ts_code = %s", ("000001.SZ",))

# 错误
df = db_client.query("SELECT * FROM daily_basic WHERE ts_code = ?", ("000001.SZ",))
```

### 前端无法访问后端

检查前端代理配置 `frontend/package.json`：

```json
{
  "proxy": "http://localhost:8000"
}
```

## 配置说明

### 环境变量 (`.env`)

从项目根目录复制模板：

```bash
cp .env.example .env
# 编辑 .env 填入 Tushare Token 和数据库密码
```

支持两种命名方式：

```env
# 扁平（通过 env= 别名）
TUSHARE_TOKEN=your_token_here
POSTGRES_HOST=localhost

# 嵌套（通过 __ 分隔符，对应 settings.xxx.yyy）
COLLECTOR__CALLS_PER_MINUTE=120
DATABASE__CONNECTION_POOL_SIZE=10
BACKTEST__INITIAL_CAPITAL=1000000
```

完整配置项参见 `.env.example`。

## 技术特点

### PostgreSQL 优势

- 多用户并发访问
- 事务隔离和 ACID 保证
- 连接池管理
- 丰富的索引和查询优化
- 企业级稳定性

### Polars 数据处理

- 比 Pandas 快 5-10 倍
- 更低的内存占用
- 惰性求值优化
- 向量化操作

### 向量化回测

- 处理整个价格序列（无循环）
- 使用 Polars 高效计算
- 支持复杂策略逻辑

### 依赖注入架构

```python
from app.core.container import container

data_service = container.get_data_service()
factor_service = container.get_factor_service()
backtest_service = container.get_backtest_service()
```

## 许可证

MIT License

## 相关文档

- [CLAUDE.md](CLAUDE.md) - Claude Code AI 助手指南
- API 文档: http://localhost:8000/docs
- PostgreSQL 文档: https://www.postgresql.org/docs/
- Tushare 文档: https://tushare.pro/document/2
