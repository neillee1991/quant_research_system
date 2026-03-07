# 代码示例

**版本**: v2.0
**更新日期**: 2026-03-07

本文档提供重构后架构的实用代码示例。

---

## 目录

1. [DolphinDB Client 使用](#dolphindb-client-使用)
2. [因子计算](#因子计算)
3. [数据预处理](#数据预处理)
4. [API 端点开发](#api-端点开发)
5. [数据同步](#数据同步)
6. [因子分析](#因子分析)
7. [Polars 数据处理](#polars-数据处理)

---

## DolphinDB Client 使用

### 基础查询

```python
from store.dolphindb import DolphinDBClient

# 创建客户端实例
client = DolphinDBClient()

# 简单查询
df = client.query(
    "SELECT * FROM sync_daily_data WHERE ts_code = %s LIMIT 10",
    ("000001.SZ",)
)

print(df)
```

### 参数化查询

```python
# 日期范围查询
df = client.query(
    """
    SELECT ts_code, trade_date, close, volume
    FROM sync_daily_data
    WHERE ts_code = %s
      AND trade_date BETWEEN %s AND %s
    ORDER BY trade_date
    """,
    ("000001.SZ", "20240101", "20240131")
)

# 多条件查询
df = client.query(
    """
    SELECT *
    FROM sync_daily_data
    WHERE ts_code IN (%s, %s, %s)
      AND trade_date >= %s
      AND volume > %s
    """,
    ("000001.SZ", "000002.SZ", "600000.SH", "20240101", 1000000)
)
```

### 表管理

```python
# 检查表是否存在
if client.table_exists("my_table"):
    print("Table exists")
else:
    # 创建表
    client.create_table(
        table_name="my_table",
        schema={
            "ts_code": {"type": "SYMBOL", "nullable": False, "comment": "股票代码"},
            "trade_date": {"type": "DATE", "nullable": False, "comment": "交易日期"},
            "value": {"type": "DOUBLE", "nullable": True, "comment": "数值"},
        },
        primary_keys=["ts_code", "trade_date"]
    )
```

### 批量操作

```python
import polars as pl

# 批量插入
data = pl.DataFrame({
    "ts_code": ["000001.SZ", "000002.SZ", "600000.SH"],
    "trade_date": ["20240101", "20240101", "20240101"],
    "close": [10.5, 20.3, 15.8],
    "volume": [1000000, 2000000, 1500000]
})

client.batch_insert("sync_daily_data", data)

# 批量更新
client.execute(
    """
    UPDATE sync_daily_data
    SET close = close * %s
    WHERE trade_date = %s
    """,
    (1.1, "20240101")
)
```

---

## 因子计算

### 使用 ProductionEngine

```python
from engine.production.engine import ProductionEngine
from engine.production.registry import get_factor

# 获取因子定义
definition = get_factor("ma20")
print(f"Factor: {definition.factor_name}")
print(f"Depends on: {definition.depends_on}")
print(f"Params: {definition.params}")

# 创建引擎实例
engine = ProductionEngine()

# 运行因子计算
result = await engine.run_task(
    factor_id="ma20",
    start_date="20240101",
    end_date="20240131",
    mode="incremental",  # 或 "full"
    ts_codes=["000001.SZ", "000002.SZ"],  # 可选，不指定则计算所有股票
    preprocess_options={
        "adjust_price": "forward",   # 前复权
        "filter_st": True,           # 过滤 ST
        "filter_new_stock": True,    # 过滤新股
        "handle_suspension": True,   # 停牌处理
        "mark_limit": True           # 标记涨跌停
    }
)

print(f"Status: {result['status']}")
print(f"Records: {result['records_computed']}")
print(f"Duration: {result['duration_seconds']}s")
```

### 创建自定义因子

```python
# 1. 在 engine/factors/technical.py 添加计算逻辑
import polars as pl

class TechnicalFactors:
    @staticmethod
    def momentum(series: pl.Series, window: int) -> pl.Series:
        """动量因子：当前价格 / N日前价格 - 1"""
        return series / series.shift(window) - 1

# 2. 创建因子定义
from engine.production.registry import factor

@factor(
    factor_id="momentum_20",
    factor_name="20日动量",
    depends_on=["close"],
    params={"window": 20},
    mode="incremental"
)
def compute_momentum_20(df: pl.DataFrame, params: dict) -> pl.Series:
    """计算20日动量因子"""
    return TechnicalFactors.momentum(df["close"], params["window"])

# 3. 通过 API 注册到数据库
import requests

response = requests.post(
    "http://localhost:8000/api/v1/production/factors",
    json={
        "factor_id": "momentum_20",
        "factor_name": "20日动量",
        "category": "technical",
        "description": "当前价格相对20日前的涨跌幅",
        "depends_on": ["close"],
        "params": {"window": 20},
        "enabled": True
    }
)

print(response.json())
```

### 批量计算因子

```python
import requests

# 批量计算多个因子
response = requests.post(
    "http://localhost:8000/api/v1/production/batch-run",
    json={
        "factor_ids": ["ma20", "rsi", "momentum_20"],
        "start_date": "20240101",
        "end_date": "20240131",
        "mode": "incremental",
        "preprocess_options": {
            "adjust_price": "forward",
            "filter_st": True
        }
    }
)

results = response.json()
for result in results["data"]:
    print(f"{result['factor_id']}: {result['status']}")
```

---

## 数据预处理

### 使用 DataProcessor

```python
from data_manager.processor import DataProcessor
import polars as pl

# 创建处理器
processor = DataProcessor()

# 加载原始数据
raw_df = client.query(
    "SELECT * FROM sync_daily_data WHERE trade_date BETWEEN %s AND %s",
    ("20240101", "20240131")
)

# 应用预处理
processed_df = processor.preprocess(
    df=raw_df,
    options={
        "adjust_price": "forward",   # 前复权
        "filter_st": True,           # 过滤 ST
        "filter_new_stock": True,    # 过滤新股 (<60天)
        "handle_suspension": True,   # 停牌处理
        "mark_limit": True           # 标记涨跌停
    }
)

print(f"原始数据: {len(raw_df)} 行")
print(f"处理后: {len(processed_df)} 行")
```

### 自定义预处理器

```python
from data_manager.processor import DataProcessor
import polars as pl

class CustomProcessor(DataProcessor):
    """自定义数据处理器"""

    def filter_by_liquidity(self, df: pl.DataFrame, min_volume: int) -> pl.DataFrame:
        """按流动性过滤"""
        return df.filter(pl.col("volume") >= min_volume)

    def add_technical_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """添加技术指标"""
        return df.with_columns([
            pl.col("close").rolling_mean(window_size=5).alias("ma5"),
            pl.col("close").rolling_mean(window_size=20).alias("ma20"),
            (pl.col("close") / pl.col("open") - 1).alias("return")
        ])

    def preprocess(self, df: pl.DataFrame, options: dict) -> pl.DataFrame:
        """扩展预处理流程"""
        # 先执行基础预处理
        df = super().preprocess(df, options)

        # 执行自定义处理
        if options.get("filter_liquidity"):
            df = self.filter_by_liquidity(df, options.get("min_volume", 1000000))

        if options.get("add_indicators"):
            df = self.add_technical_indicators(df)

        return df

# 使用自定义处理器
processor = CustomProcessor()
df_processed = processor.preprocess(
    df=raw_df,
    options={
        "adjust_price": "forward",
        "filter_st": True,
        "filter_liquidity": True,
        "min_volume": 2000000,
        "add_indicators": True
    }
)
```

---

## API 端点开发

### 创建新的查询端点

```python
# 在 app/api/v1/data/query_api.py 添加

from fastapi import APIRouter, Query, HTTPException
from app.core.response import success_response, error_response
from app.core.logger import logger
from store.dolphindb import DolphinDBClient

router = APIRouter()

@router.get("/stock-info")
async def get_stock_info(
    ts_code: str = Query(..., description="股票代码", example="000001.SZ"),
    start_date: str = Query(..., pattern=r"^\d{8}$", description="开始日期"),
    end_date: str = Query(..., pattern=r"^\d{8}$", description="结束日期")
):
    """
    获取股票基本信息和行情数据

    Args:
        ts_code: 股票代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)

    Returns:
        股票信息和行情数据
    """
    try:
        client = DolphinDBClient()

        # 查询行情数据
        df = client.query(
            """
            SELECT ts_code, trade_date, open, high, low, close, volume, amount
            FROM sync_daily_data
            WHERE ts_code = %s
              AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (ts_code, start_date, end_date)
        )

        if len(df) == 0:
            return error_response(f"No data found for {ts_code}", status_code=404)

        # 转换为字典列表
        data = df.to_dicts()

        # 计算统计信息
        stats = {
            "count": len(df),
            "avg_close": float(df["close"].mean()),
            "max_close": float(df["close"].max()),
            "min_close": float(df["close"].min()),
            "total_volume": int(df["volume"].sum())
        }

        return success_response({
            "ts_code": ts_code,
            "data": data,
            "stats": stats
        })

    except Exception as e:
        logger.error(f"Error in get_stock_info: {e}", exc_info=True)
        return error_response(str(e), status_code=500)
```

### 创建新的计算端点

```python
# 在 app/api/v1/production/factor_compute.py 添加

from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Optional
from app.core.response import success_response, error_response
from engine.production.engine import ProductionEngine

router = APIRouter()

class CustomComputeRequest(BaseModel):
    """自定义计算请求"""
    ts_codes: List[str] = Field(..., description="股票代码列表")
    start_date: str = Field(..., pattern=r"^\d{8}$", description="开始日期")
    end_date: str = Field(..., pattern=r"^\d{8}$", description="结束日期")
    formula: str = Field(..., description="计算公式")
    params: Optional[dict] = Field(default_factory=dict, description="参数")

@router.post("/custom-compute")
async def custom_compute(request: CustomComputeRequest):
    """
    自定义公式计算

    支持简单的数学表达式，例如:
    - "close / open - 1" (日内收益率)
    - "close.rolling_mean(20)" (20日均线)
    """
    try:
        engine = ProductionEngine()

        # 加载数据
        df = engine._load_data(
            depends_on=["open", "close", "high", "low", "volume"],
            start_date=request.start_date,
            end_date=request.end_date,
            ts_codes=request.ts_codes
        )

        # 执行计算 (简化示例，实际需要安全的表达式求值)
        # 注意：生产环境需要使用沙箱执行
        result = eval(request.formula, {"df": df, "pl": pl})

        return success_response({
            "formula": request.formula,
            "result": result.to_list()[:100],  # 返回前100个结果
            "count": len(result)
        })

    except Exception as e:
        logger.error(f"Error in custom_compute: {e}", exc_info=True)
        return error_response(str(e), status_code=500)
```

---

## 数据同步

### 创建同步任务

```python
from store.dolphindb import DolphinDBClient
import json

client = DolphinDBClient()

# 插入任务配置
client.execute(
    """
    INSERT INTO sync_task_config (
        task_id, task_name, data_source, sync_type,
        table_name, enabled, config, created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())
    """,
    (
        "sync_index_daily",
        "指数日线数据",
        "tushare",
        "incremental",
        "sync_index_daily",
        True,
        json.dumps({
            "api_name": "index_daily",
            "fields": ["ts_code", "trade_date", "close", "open", "high", "low", "vol", "amount"],
            "date_field": "trade_date"
        })
    )
)

print("Task created successfully")
```

### 创建 Prefect Flow

```python
# 在 flows/index_sync_flow.py

from prefect import flow, task
from data_manager.refactored_sync_engine import SyncEngine
from app.core.logger import logger

@task(name="sync-index-data", retries=3, retry_delay_seconds=60)
def sync_index_data(start_date: str, end_date: str):
    """同步指数数据"""
    try:
        engine = SyncEngine()
        result = engine.run_task(
            task_id="sync_index_daily",
            start_date=start_date,
            end_date=end_date
        )
        logger.info(f"Synced {result['records']} records")
        return result
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise

@flow(name="index-daily-sync", log_prints=True)
def index_sync_flow(start_date: str, end_date: str):
    """指数日线数据同步流程"""
    print(f"Starting sync from {start_date} to {end_date}")

    result = sync_index_data(start_date, end_date)

    print(f"Sync completed: {result['status']}")
    return result

# 部署
if __name__ == "__main__":
    index_sync_flow.serve(
        name="index-daily-sync-deployment",
        cron="0 18 * * 1-5"  # 每个工作日 18:00 执行
    )
```

### 通过 API 触发同步

```python
import requests

# 触发单个任务
response = requests.post(
    "http://localhost:8000/api/v1/data/sync/task/sync_index_daily"
)

print(response.json())

# 查看同步历史
response = requests.get(
    "http://localhost:8000/api/v1/data/sync/logs",
    params={"task_id": "sync_index_daily", "limit": 10}
)

for log in response.json()["data"]:
    print(f"{log['sync_time']}: {log['status']} - {log['records_synced']} records")
```

---

## 因子分析

### IC 分析

```python
import requests

# 运行 IC 分析
response = requests.post(
    "http://localhost:8000/api/v1/production/analysis/run",
    json={
        "factor_id": "ma20",
        "start_date": "20240101",
        "end_date": "20240131",
        "forward_periods": [1, 5, 10, 20],
        "group_count": 10
    }
)

result = response.json()["data"]

print(f"IC Mean: {result['ic_mean']}")
print(f"IC Std: {result['ic_std']}")
print(f"IR: {result['ir']}")
print(f"IC > 0 比例: {result['ic_positive_ratio']}")

# 分组收益
for group in result["group_returns"]:
    print(f"Group {group['group']}: {group['mean_return']:.4f}")
```

### Alphalens 分析

```python
import requests

# 运行 Alphalens 分析
response = requests.post(
    "http://localhost:8000/api/v1/production/analysis/alphalens",
    json={
        "factor_id": "ma20",
        "start_date": "20240101",
        "end_date": "20240131",
        "periods": [1, 5, 10],
        "quantiles": 5,
        "filter_zscore": 20
    }
)

result = response.json()["data"]

# IC 分析
print("IC Analysis:")
for period, ic in result["ic_analysis"].items():
    print(f"  Period {period}: IC={ic['ic_mean']:.4f}, IR={ic['ir']:.4f}")

# 分层收益
print("\nQuantile Returns:")
for period, returns in result["quantile_returns"].items():
    print(f"  Period {period}:")
    for quantile, ret in returns.items():
        print(f"    Q{quantile}: {ret:.4f}")
```

---

## Polars 数据处理

### 基础操作

```python
import polars as pl

# 创建 DataFrame
df = pl.DataFrame({
    "ts_code": ["000001.SZ", "000002.SZ", "600000.SH"],
    "trade_date": ["20240101", "20240101", "20240101"],
    "close": [10.5, 20.3, 15.8],
    "volume": [1000000, 2000000, 1500000]
})

# 选择列
df_selected = df.select(["ts_code", "close"])

# 过滤行
df_filtered = df.filter(pl.col("volume") > 1200000)

# 添加新列
df_with_return = df.with_columns([
    (pl.col("close") / 10 - 1).alias("return")
])

# 排序
df_sorted = df.sort("volume", descending=True)

# 分组聚合
df_grouped = df.group_by("ts_code").agg([
    pl.col("close").mean().alias("avg_close"),
    pl.col("volume").sum().alias("total_volume")
])
```

### 时间序列操作

```python
# 滚动窗口
df_with_ma = df.with_columns([
    pl.col("close").rolling_mean(window_size=5).alias("ma5"),
    pl.col("close").rolling_mean(window_size=20).alias("ma20"),
    pl.col("close").rolling_std(window_size=20).alias("std20")
])

# 移位操作
df_with_lag = df.with_columns([
    pl.col("close").shift(1).alias("close_lag1"),
    pl.col("close").shift(5).alias("close_lag5")
])

# 计算收益率
df_with_returns = df.with_columns([
    (pl.col("close") / pl.col("close").shift(1) - 1).alias("return_1d"),
    (pl.col("close") / pl.col("close").shift(5) - 1).alias("return_5d")
])

# 累积操作
df_with_cumsum = df.with_columns([
    pl.col("volume").cum_sum().alias("cumulative_volume")
])
```

### 高级操作

```python
# 条件表达式
df_with_signal = df.with_columns([
    pl.when(pl.col("close") > pl.col("ma20"))
      .then(1)
      .when(pl.col("close") < pl.col("ma20"))
      .then(-1)
      .otherwise(0)
      .alias("signal")
])

# 分组内操作
df_normalized = df.with_columns([
    ((pl.col("close") - pl.col("close").mean().over("trade_date")) /
     pl.col("close").std().over("trade_date")).alias("close_zscore")
])

# 连接操作
df1 = pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "name": ["平安银行", "万科A"]})
df2 = pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "close": [10.5, 20.3]})

df_joined = df1.join(df2, on="ts_code", how="inner")

# LazyFrame 优化
lf = pl.scan_parquet("large_data.parquet")
result = (
    lf
    .filter(pl.col("trade_date") >= "20240101")
    .select(["ts_code", "close", "volume"])
    .group_by("ts_code")
    .agg([
        pl.col("close").mean().alias("avg_close"),
        pl.col("volume").sum().alias("total_volume")
    ])
    .collect()  # 只在最后执行
)
```

### 性能优化技巧

```python
# 1. 使用表达式而不是循环
# ✅ GOOD
df = df.with_columns([
    (pl.col("close") / pl.col("open") - 1).alias("return")
])

# ❌ BAD
returns = []
for i in range(len(df)):
    ret = df["close"][i] / df["open"][i] - 1
    returns.append(ret)

# 2. 使用 LazyFrame
# ✅ GOOD
lf = pl.scan_csv("data.csv")
result = lf.filter(...).select(...).collect()

# ❌ BAD
df = pl.read_csv("data.csv")  # 立即加载全部
result = df.filter(...).select(...)

# 3. 避免重复计算
# ✅ GOOD
ma20 = pl.col("close").rolling_mean(window_size=20)
df = df.with_columns([
    ma20.alias("ma20"),
    (pl.col("close") / ma20 - 1).alias("close_to_ma20")
])

# ❌ BAD
df = df.with_columns([
    pl.col("close").rolling_mean(window_size=20).alias("ma20"),
    (pl.col("close") / pl.col("close").rolling_mean(window_size=20) - 1).alias("close_to_ma20")
])
```

---

## 完整示例：端到端因子开发

```python
# 1. 定义因子计算逻辑
import polars as pl
from engine.production.registry import factor

@factor(
    factor_id="volume_price_corr",
    factor_name="量价相关性",
    depends_on=["close", "volume"],
    params={"window": 20},
    mode="incremental"
)
def compute_volume_price_corr(df: pl.DataFrame, params: dict) -> pl.Series:
    """计算量价相关性因子"""
    window = params["window"]
    return df.select([
        pl.corr("close", "volume", window_size=window).alias("factor_value")
    ])["factor_value"]

# 2. 注册到数据库
import requests

response = requests.post(
    "http://localhost:8000/api/v1/production/factors",
    json={
        "factor_id": "volume_price_corr",
        "factor_name": "量价相关性",
        "category": "technical",
        "description": "价格和成交量的滚动相关系数",
        "depends_on": ["close", "volume"],
        "params": {"window": 20},
        "enabled": True
    }
)

# 3. 运行因子计算
response = requests.post(
    "http://localhost:8000/api/v1/production/run",
    json={
        "factor_id": "volume_price_corr",
        "start_date": "20240101",
        "end_date": "20240131",
        "mode": "full",
        "preprocess_options": {
            "adjust_price": "forward",
            "filter_st": True
        }
    }
)

# 4. 运行因子分析
response = requests.post(
    "http://localhost:8000/api/v1/production/analysis/run",
    json={
        "factor_id": "volume_price_corr",
        "start_date": "20240101",
        "end_date": "20240131",
        "forward_periods": [1, 5, 10]
    }
)

# 5. 查看结果
result = response.json()["data"]
print(f"IC: {result['ic_mean']:.4f}")
print(f"IR: {result['ir']:.4f}")
```

---

## 参考资料

- [ARCHITECTURE.md](./ARCHITECTURE.md) - 系统架构
- [DEVELOPER_GUIDE.md](./DEVELOPER_GUIDE.md) - 开发指南
- [API.md](./API.md) - API 文档
- [Polars 文档](https://docs.pola-rs.com/)
- [FastAPI 文档](https://fastapi.tiangolo.com/)
