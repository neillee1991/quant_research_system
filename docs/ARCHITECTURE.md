# 系统架构

## 系统概览

QuantResearchSystem 是一个全栈量化交易研究平台，提供拖拽式策略建模、向量化回测和 AutoML 功能。

### 技术栈

| 层级 | 技术 |
|------|------|
| 数据层 | DolphinDB (TSDB 时序数据库) |
| 计算层 | Polars (向量化数据处理) |
| 编排层 | Prefect 3.x (任务编排) |
| API 层 | FastAPI |
| 前端层 | React 18 + TypeScript + Ant Design |
| 回测引擎 | VectorBT |

---

## 架构分层

```
┌─────────────────────────────────────────────────────────┐
│                     Frontend (React)                     │
│  数据中心 / 因子中心 / 策略中心 / 市场中心 / 调度中心   │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTP/REST API
┌──────────────────────▼──────────────────────────────────┐
│              FastAPI Application Layer                  │
│  /api/v1/data, /production, /factor, /strategy, /ml   │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                   Service Layer                         │
│     DataService / FactorService / BacktestService      │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                  Engine Layer                           │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │
│  │   因子引擎    │ │   回测引擎    │ │   策略解析器   │  │
│  │ Production   │ │  VectorBT    │ │  FlowParser   │  │
│  └──────────────┘ └──────────────┘ └──────────────┘  │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                  Data Manager                          │
│         数据同步 / 数据处理 / Schema 生成              │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                  Store Layer                           │
│         DolphinDB Client (单例)                       │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              DolphinDB Database                        │
│  ┌──────────────────┐  ┌───────────────────────────┐  │
│  │   TSDB 表        │  │    维度表                 │  │
│  │  (时序数据)      │  │   (元数据/配置)          │  │
│  └──────────────────┘  └───────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## 核心模块说明

### 生产因子框架 (8步流程)

**位置**: `engine/production/`

**核心文件**:
- `engine.py` - 生产引擎主入口
- `registry.py` - 因子注册器 (@factor 装饰器)
- `data_config.py` - 数据配置加载器

**8步计算流程**:
1. `_resolve_dates()` - 判断全量/增量，计算 data_start (lookback_days 偏移)
2. `_load_data()` - 按 depends_on 从 DolphinDB 加载数据
3. `_apply_adjust()` - 复权 (forward/backward)，调整 OHLC
4. `_apply_stock_status()` - 过滤 ST、新股(上市<60天)，标记涨跌停
5. `definition.func(df, params)` - 执行因子计算 (Polars 向量化)
6. `_handle_suspension_from_status()` - 停牌复牌后置空 factor_value
7. `_build_quality_flag()` - 质量标记 (null 率、极端值)
8. `_save_results()` - upsert 到 factor_values 表

**预处理默认选项**:
```python
DEFAULT_PREPROCESS = {
    "adjust_price": "forward",   # 前复权
    "filter_st": True,           # 过滤 ST
    "filter_new_stock": True,    # 过滤新股(<60天)
    "handle_suspension": True,   # 停牌处理
    "mark_limit": True,          # 标记涨跌停
}
```

### 数据同步引擎

**位置**: `data_manager/`

**核心文件**:
- `refactored_sync_engine.py` - 重构同步引擎
- `sync_components.py` - 同步组件
- `processor.py` - 数据处理器
- `collectors/tushare_collector.py` - Tushare 采集器
- `collectors/akshare_collector.py` - AkShare 采集器

**数据库驱动设计**:
同步任务定义存储在 `sync_task_config` 表中，而非硬编码。

### 因子分析器

**位置**: `engine/analysis/`

**功能**:
- IC/IR 分析
- 分组收益分析
- Alphalens 适配器

### 回测引擎

**位置**: `engine/backtester/`

**核心文件**:
- `vector_engine.py` - 向量化回测引擎 (VectorBT)

**计算指标**:
- Sharpe 比率
- 最大回撤
- 胜率
- 盈亏比

---

## 数据库表设计

### TSDB 表 (时序数据)

| 表名 | 用途 |
|------|------|
| `sync_daily_data` | 日线行情 (OHLCV) |
| `sync_daily_basic` | 日线基础数据 (PE、PB 等) |
| `sync_adj_factor` | 复权因子 |
| `sync_index_member_*` | 指数成分股 |
| `sync_stk_limit` | 涨跌停价格 |
| `sync_suspend_d` | 停复牌信息 |
| `sync_stock_st` | ST 股票列表 |
| `factor_values` | 因子值结果 (三维分区优化) |

**factor_values 分区策略**:
- 三维组合分区：HASH(factor_id, 20) + RANGE(trade_date, 季度) + HASH(ts_code, 10)
- 总分区数：20 × 120 × 10 = 24,000 个分区

### 维度表 (元数据/配置)

| 表名 | 用途 |
|------|------|
| `stock_basic` | 股票基础信息 |
| `trade_cal` | 交易日历 |
| `sync_log` | 同步日志 |
| `sync_log_history` | 同步日志历史 |
| `sync_task_config` | 同步任务配置 (17个任务) |
| `etl_task_config` | ETL 任务配置 (3个任务) |
| `factor_metadata` | 因子元数据 |
| `factor_data_config` | 因子数据配置 |
| `factor_analysis` | 因子分析结果 |
| `production_task_run` | 生产任务运行记录 |
| `dag_run_log` | DAG 运行日志 |
| `dag_task_log` | DAG 任务日志 |

---

## 关键数据流

### 因子计算数据流

```
1. 接收请求 (factor_id, mode, date_range)
         ↓
2. 解析日期 (full/incremental, data_start = start - lookback_days)
         ↓
3. 加载数据 (按 depends_on 从 DolphinDB 加载)
         ↓
4. 复权调整 (forward/backward, 调整 OHLC)
         ↓
5. 股票状态过滤 (ST、新股、涨跌停标记)
         ↓
6. 执行因子计算 (Polars 向量化)
         ↓
7. 停牌处理 (停牌期间 factor_value 置空)
         ↓
8. 质量标记 (null 率、极端值检查)
         ↓
9. 保存结果 (upsert 到 factor_values 表)
```

### 数据同步数据流

```
1. 从 sync_task_config 读取任务定义
         ↓
2. 检查 last_sync_date (增量同步)
         ↓
3. 调用 Tushare/AkShare API
         ↓
4. 数据清洗和类型转换
         ↓
5. 自动建表 (如需要)
         ↓
6. Upsert 到 DolphinDB
         ↓
7. 更新 sync_log
```

---

## API 路由结构

所有路由都在 `/api/v1/` 下:

| 路由 | 模块 | 用途 |
|------|------|------|
| `/data/*` | `app/api/v1/data/` | 数据查询和同步 |
| `/production/*` | `app/api/v1/production/` | 生产因子管理 |
| `/factor/*` | `app/api/v1/factor.py` | 技术指标计算 |
| `/strategy/*` | `app/api/v1/strategy.py` | 回测执行 |
| `/ml/*` | `app/api/v1/ml.py` | AutoML 模型训练 |
| `/flows/*` | `app/api/v1/flows.py` | Prefect 工作流 |
| `/tasks/*` | `app/api/v1/tasks.py` | 任务管理 |

**注意**:
- `/production/*` 和 `/factor/*` 功能不同，不是重复：
  - `/factor/*` = 快速、轻量级技术指标计算（单只股票，无状态）
  - `/production/*` = 完整生产因子框架（全市场，有状态，保存到 factor_values）

---

## 项目文件结构

```
quant_research_system/
├── backend/
│   ├── app/                    # FastAPI 应用层
│   │   ├── api/v1/            # API 路由
│   │   ├── core/              # 配置、日志、异常
│   │   ├── services/          # 业务逻辑服务层
│   │   └── models/            # Pydantic 模型
│   ├── engine/                # 计算引擎
│   │   ├── production/        # 生产环境引擎
│   │   ├── factors/           # 因子库
│   │   ├── analysis/          # 因子分析
│   │   ├── backtester/        # 回测引擎
│   │   └── parser/            # 策略解析器
│   ├── data_manager/          # 数据管理层
│   ├── store/                 # 数据存储层 (兼容层)
│   ├── infrastructure/        # 基础设施层
│   │   ├── database/          # 数据库模块
│   │   ├── seed/              # 种子数据加载器
│   │   └── monitoring/        # 监控
│   ├── flows/                 # Prefect 工作流
│   ├── tests/                 # 测试代码
│   ├── scripts/               # 运维脚本
│   └── config/                # 配置文件
├── frontend/                  # React 前端
├── docs/                      # 项目文档
│   ├── ARCHITECTURE.md       # 本文档
│   ├── DEVELOPER_GUIDE.md    # 开发者指南
│   ├── API_REFERENCE.md      # API 参考
│   ├── DEPLOYMENT.md         # 部署指南
│   └── TROUBLESHOOTING.md    # 故障排查
├── config/
│   └── seed_data/            # 种子数据配置
│       ├── sync_tasks.json
│       ├── etl_tasks.json
│       ├── factor_data_config.json
│       └── factor_metadata.json
└── README.md                  # 快速开始
```
