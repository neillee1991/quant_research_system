# QuantSystem 综合指南

**版本**: v2.0
**更新日期**: 2026-03-08
**状态**: 生产就绪

---

## 目录

1. [系统概览](#系统概览)
2. [快速开始](#快速开始)
3. [架构设计](#架构设计)
4. [核心功能](#核心功能)
5. [API 参考](#api-参考)
6. [开发指南](#开发指南)
7. [配置管理](#配置管理)
8. [故障排查](#故障排查)
9. [测试指南](#测试指南)
10. [部署运维](#部署运维)

---

## 系统概览

### 简介

QuantSystem 是一个全栈量化研究平台，提供数据管理、因子计算、回测分析和策略研究功能。

### 核心特性

- **高性能数据处理**: DolphinDB (时间序列数据库) + Polars (向量化计算)
- **工作流编排**: Prefect 3.x 管理数据同步和计算任务
- **模块化架构**: 清晰的分层设计，职责分离
- **类型安全**: 完整的 TypeScript/Python 类型注解
- **不可变数据**: 函数式编程范式，避免副作用

### 技术栈

**后端**:
- FastAPI - Web 框架
- Polars - 数据处理
- DolphinDB - 时序数据库
- Prefect 3.x - 工作流编排
- Pydantic - 数据验证

**前端**:
- React 18 - UI 框架
- TypeScript - 类型安全
- Ant Design - 组件库
- Zustand - 状态管理
- ECharts - 数据可视化

---

## 快速开始

### 环境要求

- Python 3.11+
- Node.js 18+
- Docker (用于 DolphinDB)
- 8GB+ RAM

### 安装步骤

```bash
# 1. 克隆项目
git clone <repository-url>
cd quantsystem

# 2. 启动 DolphinDB
docker-compose up -d dolphindb

# 3. 后端设置
cd quant_research_system/backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 4. 配置环境变量
cp .env.example .env
# 编辑 .env 文件，设置 TUSHARE_TOKEN 等

# 5. 启动后端
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 6. 前端设置（新终端）
cd ../frontend
npm install
npm start
```

### 验证安装

```bash
# 检查服务状态
curl http://localhost:8000/docs        # API 文档
curl http://localhost:3000             # 前端界面
curl http://localhost:8848             # DolphinDB
```

### 首次使用

```bash
# 1. 同步基础数据
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_stock_basic"

# 2. 同步日线数据
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_daily_data"

# 3. 计算示例因子
curl -X POST "http://localhost:8000/api/v1/production/run" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "ma20",
    "start_date": "20240101",
    "end_date": "20240131",
    "mode": "full"
  }'
```

---

## 架构设计

### 系统架构图

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (React)                        │
│  DataCenter │ FactorCenter │ BacktestCenter │ StrategyCenter │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP/REST API
┌──────────────────────────▼──────────────────────────────────┐
│                    API Layer (FastAPI)                       │
│  /data/* │ /production/* │ /backtest/* │ /strategy/*        │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                   Service Layer (业务逻辑)                    │
│  DataService │ FactorService │ BacktestService              │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                  Engine Layer (计算引擎)                      │
│  ProductionEngine │ DataPipeline │ FactorAnalyzer           │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                   Data Layer (数据访问)                       │
│  DolphinDBClient │ DataProcessor │ Prefect Flows            │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                  Storage Layer (存储)                         │
│  DolphinDB (时序数据) │ File Storage (配置/日志)              │
└─────────────────────────────────────────────────────────────┘
```

### 分层架构

#### 1. API Layer (API 层)
- 处理 HTTP 请求，参数验证，响应格式化
- 统一响应格式: `{success, data, error, timestamp}`
- 完整的 Pydantic 模型验证

#### 2. Service Layer (服务层)
- 业务逻辑编排
- 跨模块协调
- 事务管理

#### 3. Engine Layer (引擎层)
- 因子计算引擎 (ProductionEngine)
- 数据处理管道 (DataPipeline)
- 因子分析器 (FactorAnalyzer)

#### 4. Data Layer (数据层)
- DolphinDB 客户端封装
- 数据预处理
- Prefect 工作流

#### 5. Storage Layer (存储层)
- DolphinDB 时序数据库
- 文件系统 (配置、日志)

### 核心模块

#### ProductionEngine (因子计算引擎)

8步计算流程:
1. **日期解析** - 判断 full/incremental，计算 lookback
2. **数据加载** - 按 depends_on 从 DolphinDB 加载
3. **复权处理** - forward/backward 复权
4. **状态过滤** - 过滤 ST、新股、停牌
5. **因子计算** - 执行 Polars 向量化计算
6. **停牌处理** - 停牌复牌后置空因子值
7. **质量标记** - null 率、极端值检测
8. **结果保存** - upsert 到 factor_values 表

#### DataPipeline (数据管道)

8个可组合处理器:
1. DataLoaderProcessor - 数据加载
2. AdjustmentProcessor - 复权处理
3. StatusFilterProcessor - 状态过滤
4. FactorComputeProcessor - 因子计算
5. SuspensionHandlerProcessor - 停牌处理
6. DateRangeFilterProcessor - 日期过滤
7. QualityCheckerProcessor - 质量检查
8. ResultWriterProcessor - 结果写入

---

## 核心功能

### 1. 数据管理

#### 数据同步

17个同步任务，覆盖:
- 股票基础信息
- 日线行情数据
- 复权因子
- 财务数据
- 指数成分股

```python
from data_manager.refactored_sync_engine import RefactoredSyncEngine

engine = RefactoredSyncEngine()
result = await engine.sync_task(
    task_id="sync_daily_data",
    start_date="20240101",
    end_date="20240131"
)
```

#### ETL 任务

3个 ETL 任务:
- etl_index_member - 指数成分股处理
- etl_index_member_daily - 每日成分股快照
- etl_stock_daily_info - 股票每日状态

### 2. 因子计算

#### 注册因子

```python
from engine.production.registry import factor

@factor(
    factor_id="momentum_20",
    factor_name="20日动量",
    depends_on=["close"],
    params={"window": 20},
    mode="incremental"
)
def compute_momentum_20(df: pl.DataFrame, params: dict) -> pl.Series:
    return df["close"] / df["close"].shift(params["window"]) - 1
```

#### 运行因子

```python
from engine.production.engine import ProductionEngine

engine = ProductionEngine()
result = await engine.run_task(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    mode="incremental",
    preprocess_options={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_new_stock": True,
        "handle_suspension": True,
        "mark_limit": True
    }
)
```

### 3. 因子分析

```python
from engine.analysis.analyzer import FactorAnalyzer

analyzer = FactorAnalyzer()
result = await analyzer.analyze_factor(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    forward_periods=[1, 5, 10, 20]
)

print(f"IC Mean: {result['ic_mean']:.4f}")
print(f"IR: {result['ir']:.4f}")
print(f"IC>0 Rate: {result['ic_positive_rate']:.2%}")
```

---

## API 参考

### 基础 URL

```
http://localhost:8000/api/v1
```

### 响应格式

```json
{
  "success": true,
  "data": { ... },
  "error": null,
  "timestamp": "2026-03-08T10:30:00Z"
}
```

### 数据端点

#### GET /data/daily
查询日线数据

```bash
curl "http://localhost:8000/api/v1/data/daily?ts_code=000001.SZ&start_date=20240101&end_date=20240131"
```

#### GET /data/sync/tasks
列出所有同步任务

```bash
curl "http://localhost:8000/api/v1/data/sync/tasks"
```

#### POST /data/sync/task/{task_id}
执行同步任务

```bash
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_daily_data"
```

### 因子端点

#### POST /production/run
运行因子计算

```bash
curl -X POST "http://localhost:8000/api/v1/production/run" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "ma20",
    "start_date": "20240101",
    "end_date": "20240131",
    "mode": "full"
  }'
```

#### POST /production/factors
注册新因子

```bash
curl -X POST "http://localhost:8000/api/v1/production/factors" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "custom_factor",
    "factor_name": "自定义因子",
    "category": "technical",
    "depends_on": ["close", "volume"],
    "params": {"window": 20},
    "enabled": true
  }'
```

#### POST /production/analysis/run
运行因子分析

```bash
curl -X POST "http://localhost:8000/api/v1/production/analysis/run" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "ma20",
    "start_date": "20240101",
    "end_date": "20240131",
    "forward_periods": [1, 5, 10]
  }'
```

---

## 开发指南

### 代码规范

#### 不可变性 (CRITICAL)

```python
# ❌ 错误 - 修改原始数据
df["new_col"] = df["old_col"] * 2

# ✅ 正确 - 创建新数据
df = df.with_columns((pl.col("old_col") * 2).alias("new_col"))
```

#### 文件组织

- 高内聚，低耦合
- 200-400 行典型，800 行最大
- 按功能/领域组织，不按类型

#### 错误处理

```python
# 始终处理错误
try:
    result = await engine.run_task(...)
except ValidationError as e:
    logger.error(f"参数验证失败: {e}")
    raise
except DatabaseError as e:
    logger.error(f"数据库错误: {e}")
    raise
```

### 添加新因子

1. **定义计算逻辑** (`engine/factors/technical.py`)

```python
class TechnicalFactors:
    @staticmethod
    def custom_indicator(df: pl.DataFrame, window: int) -> pl.Series:
        """自定义指标计算"""
        return df["close"].rolling_mean(window)
```

2. **注册因子** (`engine/production/registry.py`)

```python
@factor(
    factor_id="custom_ma",
    factor_name="自定义均线",
    depends_on=["close"],
    params={"window": 20},
    mode="incremental"
)
def compute_custom_ma(df: pl.DataFrame, params: dict) -> pl.Series:
    return TechnicalFactors.custom_indicator(df, params["window"])
```

3. **通过 API 注册到数据库**

```bash
curl -X POST "http://localhost:8000/api/v1/production/factors" \
  -H "Content-Type: application/json" \
  -d '{
    "factor_id": "custom_ma",
    "factor_name": "自定义均线",
    "category": "technical",
    "depends_on": ["close"],
    "params": {"window": 20},
    "enabled": true
  }'
```

### 添加新 API 端点

```python
# app/api/v1/custom.py
from fastapi import APIRouter, Depends
from app.models.response import ApiResponse

router = APIRouter(prefix="/custom", tags=["Custom"])

@router.get("/endpoint")
async def custom_endpoint(param: str):
    """自定义端点"""
    try:
        # 业务逻辑
        result = {"message": f"Hello {param}"}
        return ApiResponse.success(result)
    except Exception as e:
        logger.error(f"错误: {e}")
        return ApiResponse.error(str(e))
```

---

## 配置管理

### 配置类型

#### 1. 数据同步配置

存储: `sync_task_config` 表

```json
{
  "task_id": "sync_daily_data",
  "api_name": "daily",
  "table_name": "sync_daily_data",
  "fields": ["ts_code", "trade_date", "open", "high", "low", "close"],
  "date_field": "trade_date",
  "primary_keys": ["ts_code", "trade_date"],
  "api_limit": 5000,
  "enabled": true
}
```

#### 2. 因子配置

存储: `factor_data_config` 表

```json
{
  "factor_id": "momentum_20",
  "field_mappings": {
    "close": "close",
    "volume": "vol"
  },
  "preprocess_options": {
    "adjust_price": "forward",
    "filter_st": true,
    "filter_new_stock": true,
    "handle_suspension": true
  },
  "enabled": true
}
```

### 预处理配置

6种预设配置:

- **default**: 标准配置
- **conservative**: 保守配置 (严格过滤)
- **aggressive**: 激进配置 (最少过滤)
- **research**: 研究配置
- **backtest**: 回测配置
- **live**: 实盘配置

```python
# 使用预设配置
result = await engine.run_task(
    factor_id="ma20",
    preprocess_profile="conservative"
)
```

### 配置更新

⚠️ **重要**: 配置更新采用直接覆盖模式，无法自动回滚

```bash
# 1. 备份当前配置
curl "http://localhost:8000/api/v1/data/sync/tasks/sync_daily_data" > backup.json

# 2. 更新配置
curl -X PUT "http://localhost:8000/api/v1/data/sync/tasks/sync_daily_data" \
  -H "Content-Type: application/json" \
  -d @updated_config.json

# 3. 验证配置
curl "http://localhost:8000/api/v1/data/sync/tasks/sync_daily_data"
```

---

## 故障排查

### 数据库连接问题

**症状**: `ConnectionError: 无法连接 DolphinDB`

**解决方案**:

```bash
# 1. 检查 DolphinDB 是否运行
docker ps | grep dolphindb

# 2. 启动 DolphinDB
docker-compose up -d dolphindb

# 3. 验证连接
python -c "from store.dolphindb_client import db_client; print(db_client.query('SELECT 1'))"
```

### 空表问题

**症状**: 查询返回空数据

**解决方案**:

```bash
# 1. 检查同步任务状态
curl "http://localhost:8000/api/v1/data/sync/tasks"

# 2. 运行同步任务
curl -X POST "http://localhost:8000/api/v1/data/sync/task/sync_daily_data"

# 3. 验证数据
python -c "from store.dolphindb_client import db_client; print(db_client.query('SELECT COUNT(*) FROM sync_daily_data'))"
```

### SQL 语法错误

**常见错误**:

```python
# ❌ 错误 - 使用 ? 占位符
db_client.query("SELECT * FROM table WHERE id = ?", (1,))

# ✅ 正确 - 使用 %s 占位符
db_client.query("SELECT * FROM table WHERE id = %s", (1,))

# ❌ 错误 - 大写函数名
db_client.query("SELECT MAX(trade_date) FROM table")

# ✅ 正确 - 小写函数名
db_client.query("SELECT max(trade_date) FROM table")
```

### 因子计算失败

**检查清单**:

1. 因子是否已注册
2. 依赖数据是否存在
3. 日期范围是否有效
4. 参数是否正确
5. 查看日志: `backend/logs/app.log`

---

## 测试指南

### 测试结构

```
tests/
├── unit/                    # 单元测试
│   ├── test_technical_factors.py
│   └── test_analyzer.py
├── integration/             # 集成测试
│   ├── test_connection.py
│   └── test_pipeline_integration.py
└── api/                     # API 测试
    └── test_alphalens_api.py
```

### 运行测试

```bash
# 所有测试
pytest tests/

# 单元测试
pytest tests/unit/

# 集成测试
pytest tests/integration/

# 特定测试
pytest tests/unit/test_technical_factors.py::test_ma

# 覆盖率报告
pytest tests/ --cov=. --cov-report=html
open htmlcov/index.html
```

### 编写测试

```python
import pytest
import polars as pl
from engine.factors.technical import TechnicalFactors

def test_moving_average():
    """测试移动平均"""
    df = pl.DataFrame({
        "close": [10.0, 11.0, 12.0, 13.0, 14.0]
    })

    result = TechnicalFactors.ma(df["close"], window=3)

    assert result[2] == 11.0  # (10+11+12)/3
    assert result[3] == 12.0  # (11+12+13)/3
```

---

## 部署运维

### 开发环境

```
localhost:3000  → React Dev Server
localhost:8000  → FastAPI (uvicorn)
localhost:8848  → DolphinDB
localhost:4200  → Prefect UI
```

### 生产环境

```
Nginx (80/443)
  ↓
  ├─→ React (静态文件)
  └─→ FastAPI (Gunicorn + Uvicorn Workers)
        ↓
        ├─→ DolphinDB Cluster
        └─→ Prefect Server
```

### 启动服务

```bash
# 后端
cd backend
source .venv/bin/activate
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

# 前端
cd frontend
npm run build
npx serve -s build -l 3000

# DolphinDB
docker-compose up -d dolphindb

# Prefect
prefect server start
```

### 监控

#### 日志位置
- 应用日志: `backend/logs/app.log`
- Prefect 日志: Prefect UI
- DolphinDB 日志: Docker logs

#### 关键指标
- API 响应时间
- 因子计算耗时
- 数据库查询性能
- 内存使用情况

### 备份

```bash
# 导出配置
curl "http://localhost:8000/api/v1/data/sync/tasks" > sync_tasks_backup.json
curl "http://localhost:8000/api/v1/production/factors" > factors_backup.json

# DolphinDB 备份
# 参考 DolphinDB 官方文档
```

---

## 核心文件路径

| 功能 | 文件路径 |
|------|---------|
| 生产引擎 | `backend/engine/production/engine.py` |
| 因子注册 | `backend/engine/production/registry.py` |
| 字段映射 | `backend/engine/production/data_config.py` |
| 技术指标 | `backend/engine/factors/technical.py` |
| 因子分析 | `backend/engine/analysis/analyzer.py` |
| 数据预处理 | `backend/data_manager/processor.py` |
| 同步引擎 | `backend/data_manager/refactored_sync_engine.py` |
| DolphinDB客户端 | `backend/store/dolphindb_client.py` |
| 生产API | `backend/app/api/v1/production.py` |
| 数据API | `backend/app/api/v1/data_merged.py` |
| Prefect工作流 | `backend/flows/data_sync_flow.py` |
| 配置文件 | `.env` |
| 日志文件 | `backend/logs/app.log` |

---

## 常用命令

```bash
# 启动服务
docker-compose up -d
python -m uvicorn app.main:app --reload
npm start

# 停止服务
docker-compose down
pkill -f "uvicorn app.main:app"

# 查看日志
tail -f backend/logs/app.log
docker logs dolphindb

# 运行测试
pytest tests/
pytest tests/ --cov=.

# 代码格式化
black app/ engine/ data_manager/ store/
isort app/ engine/ data_manager/ store/

# 类型检查
mypy app/ engine/ data_manager/ store/
```

---

## 参考资源

### 官方文档
- [FastAPI](https://fastapi.tiangolo.com/)
- [Polars](https://docs.pola-rs.com/)
- [DolphinDB](https://www.dolphindb.com/docs/)
- [Prefect](https://docs.prefect.io/)

### 项目文档
- API 文档: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

**版本**: v2.0
**状态**: 生产就绪
**维护**: 开发团队
