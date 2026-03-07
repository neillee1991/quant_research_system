# Infrastructure Layer - Quick Reference

快速参考指南，帮助开发者快速上手使用 Repository 模式和 QueryBuilder。

---

## 快速开始

### 1. 导入模块

```python
from infrastructure.database.query_builder import QueryBuilder
from infrastructure.repository.market_data_repository import MarketDataRepository
from infrastructure.repository.factor_data_repository import FactorDataRepository
```

### 2. 创建 Repository

```python
# 初始化 DolphinDB 客户端
from store.dolphindb_client import DolphinDBClient
db_client = DolphinDBClient()

# 创建 Repository
market_repo = MarketDataRepository(db_client)
factor_repo = FactorDataRepository(db_client)
```

---

## QueryBuilder 速查

### 基础查询
```python
query = QueryBuilder("table_name") \
    .select(["col1", "col2"]) \
    .build()
```

### WHERE 条件
```python
.where("column", "=", value)           # 等于
.where("column", ">", value)           # 大于
.where("column", "LIKE", "%pattern%")  # 模糊匹配
```

### WHERE IN
```python
.where_in("column", [val1, val2, val3])
```

### WHERE BETWEEN
```python
.where_between("column", start, end)
```

### WHERE NULL
```python
.where_null("column")       # IS NULL
.where_not_null("column")   # IS NOT NULL
```

### ORDER BY
```python
.order_by(["col1 DESC", "col2 ASC"])
```

### LIMIT
```python
.limit(100)
```

### 完整示例
```python
query = QueryBuilder("sync_daily_data") \
    .select(["ts_code", "trade_date", "close"]) \
    .where_in("ts_code", ["000001.SZ", "000002.SZ"]) \
    .where_between("trade_date", "20240101", "20240131") \
    .order_by(["trade_date DESC"]) \
    .limit(100) \
    .build()

result = db_client.execute(query.sql, query.params)
```

---

## MarketDataRepository 速查

### 按日期范围查询
```python
df = market_repo.find_by_date_range("20240101", "20240131")
```

### 按股票代码查询
```python
df = market_repo.find_by_codes(
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131"
)
```

### 带复权查询
```python
df = market_repo.get_with_adjustment(
    ts_codes=["000001.SZ"],
    start_date="20240101",
    end_date="20240131",
    adjust_type="forward"  # forward/backward/none
)
```

### 带股票状态查询
```python
df = market_repo.get_with_status(
    start_date="20240101",
    end_date="20240131",
    filter_st=True,           # 过滤ST
    filter_new_stock=True,    # 过滤新股
    new_stock_days=60,        # 新股天数
    mark_limit=True           # 标记涨跌停
)
```

### 获取最新日期
```python
latest = market_repo.get_latest_date()              # 所有股票
latest = market_repo.get_latest_date("000001.SZ")   # 指定股票
```

### 获取股票列表
```python
codes = market_repo.get_codes_by_date("20240101")
```

### 保存数据
```python
count = market_repo.save(df)
```

---

## FactorDataRepository 速查

### 查询因子值
```python
df = factor_repo.get_factor_values(
    factor_id="momentum_20",
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131"
)
```

### 保存因子结果
```python
count = factor_repo.save_factor_results(
    factor_id="momentum_20",
    data=result_df,
    run_id=123
)
```

### 获取最新日期
```python
latest = factor_repo.get_latest_date("momentum_20")
```

### 获取日期范围
```python
min_date, max_date = factor_repo.get_date_range("momentum_20")
```

### 质量统计
```python
stats = factor_repo.get_quality_stats(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131"
)
# 返回: total_count, null_count, null_rate, valid_count, mean, std, min, max
```

### 覆盖率统计
```python
coverage = factor_repo.get_factor_coverage(
    factor_id="momentum_20",
    trade_date="20240101"
)
# 返回: total_stocks, factor_stocks, coverage_rate
```

### 删除因子值
```python
count = factor_repo.delete_factor_values(
    factor_id="momentum_20",
    start_date="20240101",
    end_date="20240131",
    ts_codes=["000001.SZ"]
)
```

### 宽表查询
```python
df = factor_repo.get_factors_by_date(
    trade_date="20240101",
    ts_codes=["000001.SZ", "000002.SZ"]
)
# 返回: ts_code | momentum_20 | rsi_14 | macd | ...
```

---

## BaseRepository 通用方法

所有 Repository 都继承这些方法：

### 查询
```python
df = repo.find_by_date_range(start_date, end_date, columns=None)
df = repo.find_by_codes(ts_codes, start_date, end_date, columns=None)
```

### 保存
```python
count = repo.save(df)  # upsert
```

### 删除
```python
count = repo.delete({"column": "value"})
count = repo.delete({"column": ["val1", "val2"]})  # IN
```

### 统计
```python
count = repo.count()                          # 总行数
count = repo.count({"column": "value"})       # 条件统计
exists = repo.exists({"column": "value"})     # 是否存在
```

---

## 常见模式

### 模式 1: 查询 + 计算 + 保存
```python
# 1. 查询数据
df = market_repo.get_with_adjustment(
    start_date="20240101",
    end_date="20240131",
    adjust_type="forward"
)

# 2. 计算因子
result = calculate_factor(df)

# 3. 保存结果
count = factor_repo.save_factor_results(
    factor_id="my_factor",
    data=result
)
```

### 模式 2: 条件查询
```python
query = QueryBuilder("sync_daily_data") \
    .select(["ts_code", "close"]) \
    .where("close", ">", 100) \
    .where("vol", ">", 1000000) \
    .where_between("trade_date", "20240101", "20240131") \
    .build()

df = db_client.execute(query.sql, query.params)
```

### 模式 3: 批量处理
```python
# 分批查询
batch_size = 100
for i in range(0, len(all_codes), batch_size):
    batch_codes = all_codes[i:i+batch_size]
    df = market_repo.find_by_codes(
        ts_codes=batch_codes,
        start_date="20240101",
        end_date="20240131"
    )
    # 处理数据...
```

---

## 最佳实践

### ✅ DO
- 使用 Repository 进行数据访问
- 使用 QueryBuilder 构建查询
- 使用参数化查询
- 指定需要的列（避免 SELECT *）
- 使用批量操作

### ❌ DON'T
- 不要手写 SQL 字符串拼接
- 不要直接使用 f-string 构建 SQL
- 不要忽略空值检查
- 不要在循环中执行单条查询
- 不要忽略异常处理

---

## 性能优化

### 1. 只查询需要的列
```python
# 好
df = repo.find_by_date_range("20240101", "20240131", columns=["ts_code", "close"])

# 差
df = repo.find_by_date_range("20240101", "20240131")  # SELECT *
```

### 2. 使用批量操作
```python
# 好
repo.save(large_df)  # 一次保存

# 差
for row in large_df.iter_rows():
    repo.save(pl.DataFrame([row]))  # 多次保存
```

### 3. 合理使用索引
```python
# 好 - 使用索引列
.where_in("ts_code", codes)  # ts_code 有索引
.where_between("trade_date", start, end)  # trade_date 有索引

# 差 - 不使用索引列
.where("name", "LIKE", "%test%")  # name 可能没有索引
```

---

## 错误处理

### 捕获异常
```python
try:
    df = market_repo.find_by_date_range("20240101", "20240131")
except Exception as e:
    logger.error(f"Failed to load data: {e}")
    # 处理错误...
```

### 检查空结果
```python
df = market_repo.find_by_codes(codes, start_date, end_date)
if df.is_empty():
    logger.warning("No data found")
    return
```

### 验证参数
```python
if not ts_codes:
    raise ValueError("ts_codes cannot be empty")

if start_date > end_date:
    raise ValueError("start_date must be <= end_date")
```

---

## 测试

### 运行测试
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 运行所有测试
.venv/bin/python -m pytest tests/infrastructure_test_*.py -v

# 运行特定测试
.venv/bin/python -m pytest tests/infrastructure_test_query_builder.py -v
```

### Mock Repository
```python
from unittest.mock import Mock
import polars as pl

# 创建 mock
mock_repo = Mock()
mock_repo.find_by_date_range.return_value = pl.DataFrame({
    "ts_code": ["000001.SZ"],
    "close": [10.0]
})

# 使用 mock
df = mock_repo.find_by_date_range("20240101", "20240131")
```

---

## 更多信息

- 完整文档: `infrastructure/README.md`
- 使用示例: `infrastructure/USAGE_EXAMPLES.py`
- 交付物清单: `infrastructure/DELIVERABLES.md`
- 单元测试: `tests/infrastructure_test_*.py`

---

## 快速问题排查

### 问题: 查询返回空结果
- 检查日期范围是否正确
- 检查股票代码是否存在
- 检查数据是否已同步

### 问题: SQL 语法错误
- 使用 QueryBuilder 而不是手写 SQL
- 检查表名和列名是否正确
- 查看日志中的完整 SQL 语句

### 问题: 性能慢
- 只查询需要的列
- 使用批量操作
- 检查是否使用了索引列
- 减小日期范围

### 问题: 测试失败
- 检查是否安装了 pytest
- 检查虚拟环境是否激活
- 查看详细错误信息

---

**最后更新**: 2026-03-07
