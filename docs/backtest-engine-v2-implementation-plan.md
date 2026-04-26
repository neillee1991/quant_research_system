# Quant Research System - Backtest Engine v2.0 实施方案

## 概述

创建 Quant Research System 的 Backtest Engine v2.0，实现 VectorBT + RQAlpha 双模回测架构，提供高性能的策略回测和分析功能。

## 项目目标

1. **双模回测引擎**：同时支持 VectorBT 向量化计算和 RQAlpha 事件驱动回测
2. **单一事实来源**：因子数据与回测引擎共享，避免重复计算
3. **A股规则适配**：涨跌停、T+1、停牌等规则的精确模拟
4. **完整可视化**：回测中心前端页面，支持配置、执行、结果展示
5. **与现有架构集成**：无缝接入现有的因子系统、调度系统

## 技术架构

### 后端架构

```
backend/engine/backtest/
├── core/                      # 核心抽象层
│   ├── base_strategy.py       # 统一策略基类
│   ├── context.py             # 标准化交易上下文
│   └── interfaces.py          # Provider 抽象协议
├── data_pipeline/             # 数据管道层
│   ├── query_builder.py       # 动态查询构建器
│   └── prefetcher.py          # 数据预取与缓存
├── vectorbt_engine/           # VectorBT 引擎
│   ├── engine.py              # 主引擎
│   └── portfolio.py           # 投资组合管理
├── rqalpha_engine/            # RQAlpha 引擎
│   ├── rq_env.py              # 运行时环境
│   ├── memory_source.py       # 内存数据源
│   └── custom_mod/            # A股定制模块
│       ├── matchers.py        # 涨跌停匹配器
│       └── validators.py      # T+1验证器
└── analysis/                  # 报告生成
    └── report_generator.py    # 绩效指标计算

backend/app/
├── api/v1/backtest/          # 回测 API 路由
├── services/backtest_service.py  # 回测服务
└── models/backtest.py        # 回测相关模型
```

### 前端架构

```
frontend/src/pages/BacktestCenter/
├── index.tsx                 # 主页面
├── BacktestModal.tsx         # 配置弹窗
├── BacktestResult.tsx        # 结果展示
└── components/
    ├── PerformanceMetrics.tsx  # 绩效指标卡片
    ├── EquityCurve.tsx         # 净值曲线
    └── TradeList.tsx           # 交易列表
```

## 实施计划

### 阶段一：核心抽象层（Day 1）

**目标**：建立回测引擎的基础架构和抽象

- [x] 创建目录结构
- [x] 实现 `BaseStrategy` 策略基类
- [x] 实现 `BacktestContext` 回测上下文
- [x] 定义 Provider 接口（`ISuspensionProvider`、`ILimitPriceProvider`）

### 阶段二：数据管道层（Day 2）

**目标**：实现高效的数据预取和缓存机制

- [x] 实现 `QueryBuilder` - 从 DolphinDB 查询行情和因子数据
- [x] 实现 `DataPrefetcher` - 数据预取、缓存、验证
- [x] 实现因子预计算检查 - 确保回测前因子已就绪

### 阶段三：VectorBT 引擎（Day 3-4）

**目标**：实现向量化回测引擎

- [x] 实现 `VectorBTEngine` - 主引擎类
- [x] 数据格式转换 - Polars → Pandas MultiIndex
- [x] 实现 `VectorBTPortfolio` - 投资组合管理
- [x] 信号生成与执行 - 与策略基类集成

### 阶段四：RQAlpha 引擎（Day 5-6）

**目标**：实现事件驱动回测引擎

- [x] 实现 `RQAlphaMemoryDataSource` - 内存数据源
- [x] 实现 `RQAlphaEngine` - 运行时环境
- [x] 定制 A股规则 - 涨跌停、T+1 验证器
- [x] 策略适配器 - 适配统一策略基类

### 阶段五：报告生成器（Day 7）

**目标**：计算绩效指标并生成回测报告

- [x] 实现 `ReportGenerator` - 主报告生成器
- [x] 绩效指标计算 - 收益率、夏普比率、最大回撤等
- [x] 风险指标计算 - 波动率、VaR 等
- [x] 交易分析 - 胜率、盈亏比、交易列表

### 阶段六：后端集成（Day 8）

**目标**：将回测引擎集成到现有系统

- [x] 更新 `app/core/config.py` - 添加回测配置
- [x] 创建 `app/models/backtest.py` - Pydantic 模型
- [x] 创建 `app/services/backtest_service.py` - 回测服务
- [x] 创建 `app/api/v1/backtest/` - API 路由
- [x] 更新 `app/main.py` - 注册路由
- [x] 更新 `requirements.txt` - 添加依赖（vectorbt, rqalpha）

### 阶段七：前端回测中心（Day 9-10）

**目标**：创建完整的回测中心前端页面

- [x] 创建 `BacktestCenter` 主页面
- [x] 创建 `BacktestModal` 配置弹窗
- [x] 创建 `BacktestResult` 结果展示
- [x] 集成 ECharts - 净值曲线可视化
- [x] 更新 `App.tsx` 和侧边栏 - 新增回测中心入口

### 阶段八：测试与优化（Day 11）

**目标**：确保功能完整性和性能

- [x] 单元测试 - 核心模块测试
- [x] 集成测试 - 端到端回测测试
- [x] 性能优化 - 大数据量回测测试
- [x] 文档完善 - API 文档、使用指南

## 关键技术点

### 1. 数据预取与缓存

**原则**：禁止在回测循环中发起数据库请求

```python
# 正确做法：事前预取，内存注入
async def prefetch_data(self, start_date, end_date, factors, stocks):
    cache_key = self._get_cache_key(start_date, end_date, factors, stocks)
    if cache_key in self._cache:
        return self._cache[cache_key]
    
    # 从 DolphinDB 一次性加载所有数据
    df = await self._load_from_dolphindb(start_date, end_date, factors, stocks)
    self._cache[cache_key] = df
    return df
```

### 2. VectorBT 数据格式

**关键**：正确的 MultiIndex 格式

```python
# 价格数据结构
MultiIndex([('open',  '000001.SZ'),
            ('high',  '000001.SZ'),
            ('low',   '000001.SZ'),
            ('close', '000001.SZ'),
            ...],
           names=['field', 'ts_code'])
```

### 3. RQAlpha 内存数据源

**关键**：初始化时转换为 Numpy structured array

```python
class RQAlphaMemoryDataSource(AbstractDataSource):
    def __init__(self, wide_table):
        self._data_cache = {}
        # 一次性转换为 Numpy 格式
        for ts_code in wide_table['ts_code'].unique():
            stock_data = wide_table.filter(pl.col('ts_code') == ts_code)
            self._data_cache[ts_code] = stock_data.to_numpy().view(np.recarray)
```

### 4. A股规则实现

**涨跌停**：直接从数据源读取，不回测中计算

**T+1**：使用 Validator + Position.closable

```python
class T1Validator:
    def check_order(self, order, context, bar_dict):
        pos = context.portfolio.positions.get(order.order_book_id)
        if pos and pos.closable == 0 and order.side == SIDE.SELL:
            return (False, "T+1 限制：当日买入无法卖出")
        return (True, None)
```

## 依赖清单

### 后端新增依赖

```txt
# requirements.txt 新增
vectorbt>=0.28.0
rqalpha>=2.3.0
```

### 前端依赖（已存在）

```json
{
  "echarts": "^5.5.0",
  "echarts-for-react": "^3.0.2",
  "dayjs": "^1.11.19"
}
```

## 配置项

新增 `BacktestConfig` 配置类：

```python
class BacktestConfig(_BaseConfig):
    """回测配置"""
    initial_capital: float = Field(default=1000000.0, env="BACKTEST_INITIAL_CAPITAL")
    fees: float = Field(default=0.0003, env="BACKTEST_FEES")
    slippage: float = Field(default=0.001, env="BACKTEST_SLIPPAGE")
    default_start_date: str = Field(default="20100101", pattern=r"^\d{8}$")
    default_end_date: str = Field(default="20240101", pattern=r"^\d{8}$")
    max_stocks_per_trade: int = Field(default=10, ge=1, le=100)
```

## API 设计

### 回测执行

```
POST /api/v1/backtest/run
{
  "engine_mode": "vectorbt",  // 或 "rqalpha"
  "strategy_code": "...",     // 策略代码
  "factors": ["factor_1", "factor_2"],
  "stocks": ["000001.SZ", ...],  // 可选，不传则全市场
  "start_date": "20200101",
  "end_date": "20231231",
  "initial_capital": 1000000,
  "fees": 0.0003,
  "slippage": 0.001
}
```

### 因子列表

```
GET /api/v1/backtest/factors
// 返回可用因子列表
```

### 基准列表

```
GET /api/v1/backtest/benchmarks
// 返回基准指数列表
```

## 验收标准

### 功能完整性

- [x] 支持 VectorBT 和 RQAlpha 两种回测模式
- [x] 支持因子策略回测
- [x] A股规则正确模拟（涨跌停、T+1）
- [x] 完整的绩效指标计算
- [x] 可视化回测结果

### 性能要求

- [x] 3年数据、100只股票回测时间 < 10秒
- [x] 5年数据、500只股票回测时间 < 30秒
- [x] 数据缓存命中后二次回测时间 < 2秒

### 代码质量

- [x] 类型注解覆盖率 ≥ 80%
- [x] 单元测试覆盖率 ≥ 80%
- [x] 遵循项目代码规范（black, isort, flake8）

## 风险与应对

| 风险 | 可能性 | 影响 | 应对措施 |
|------|--------|------|----------|
| RQAlpha 集成复杂度高 | 中 | 高 | 先做 PoC 验证可行性，准备降级方案 |
| 数据格式转换性能问题 | 中 | 中 | 提前做性能测试，必要时用 Cython 优化 |
| 因子数据未预计算 | 高 | 中 | 添加严格的预检查，友好错误提示 |

## 后续优化方向

1. **分布式回测**：支持多进程/多机并行回测
2. **实盘对接**：回测结果直接对接实盘交易
3. **策略优化**：参数扫描、遗传算法优化
4. **高级分析**：归因分析、压力测试、蒙特卡洛模拟
