# Infrastructure Layer - Repository Pattern & QueryBuilder

基础设施层核心抽象实现，提供统一的数据访问接口。

## 目录结构

```
infrastructure/
├── __init__.py
├── database/
│   ├── __init__.py
│   └── query_builder.py          # SQL 查询构建器
├── repository/
│   ├── __init__.py
│   ├── base.py                    # Repository 基类和接口
│   ├── market_data_repository.py  # 市场数据 Repository
│   └── factor_data_repository.py  # 因子数据 Repository
├── processor/
│   └── __init__.py
└── USAGE_EXAMPLES.py              # 使用示例文档
```

## 核心组件

### 1. QueryBuilder

参数化 SQL 查询构建器，防止 SQL 注入。

**功能特性：**
- SELECT 子句（指定列、所有列）
- WHERE 子句（=, >, <, >=, <=, !=, LIKE）
- WHERE IN 子句
- WHERE BETWEEN 子句
- WHERE NULL/NOT NULL 子句
- ORDER BY 子句
- LIMIT 子句
- 链式调用
- 参数化查询

**使用示例：**
```python
from infrastructure.database.query_builder import QueryBuilder

query = QueryBuilder("sync_daily_data") \
    .select(["ts_code", "trade_date", "close"]) \
    .where_in("ts_code", ["000001.SZ", "000002.SZ"]) \
    .where_between("trade_date", "20240101", "20240131") \
    .order_by(["trade_date DESC"]) \
    .limit(100) \
    .build()

result = db_client.execute(query.sql, query.params)
```

### 2. Repository Pattern

数据访问抽象层，封装数据库操作。

**IRepository 接口：**
- `find_by_date_range(start_date, end_date, columns)` - 按日期范围查询
- `find_by_codes(ts_codes, start_date, end_date, columns)` - 按股票代码查询
- `save(data)` - 保存数据（upsert）
- `delete(conditions)` - 删除数据

**BaseRepository 基类：**
- 实现 IRepository 接口
- 提供通用的 CRUD 操作
- 使用 QueryBuilder 构建查询
- 额外方法：`count()`, `exists()`

### 3. MarketDataRepository

市场行情数据访问层，封装 `sync_daily_data` 表。

**核心功能：**
- 基础查询（继承自 BaseRepository）
- 带复权查询：`get_with_adjustment(adjust_type="forward/backward/none")`
- 带股票状态查询：`get_with_status(filter_st, filter_new_stock, mark_limit)`
- 获取最新日期：`get_latest_date(ts_code)`
- 获取股票列表：`get_codes_by_date(trade_date)`

**使用示例：**
```python
from infrastructure.repository.market_data_repository import MarketDataRepository

repo = MarketDataRepository(db_client)

# 查询带前复权的数据
df = repo.get_with_adjustment(
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131",
    adjust_type="forward"
)

# 查询并过滤 ST、新股
df = repo.get_with_status(
    start_date="20240101",
    end_date="20240131",
    filter_st=True,
    filter_new_stock=True,
    new_stock_days=60
)
```

### 4. FactorDataRepository

因子数据访问层，封装 `factor_values` 表。

**核心功能：**
- 查询因子值：`get_factor_values(factor_id, ts_codes, start_date, end_date)`
- 保存因子结果：`save_factor_results(factor_id, data, run_id)`
- 获取最新日期：`get_latest_date(factor_id, ts_code)`
- 获取日期范围：`get_date_range(factor_id)`
- 质量统计：`get_quality_stats(factor_id, start_date, end_date)`
- 覆盖率统计：`get_factor_coverage(factor_id, trade_date)`
- 删除因子值：`delete_factor_values(factor_id, start_date, end_date, ts_codes)`
- 宽表查询：`get_factors_by_date(trade_date, ts_codes)`

**使用示例：**
```python
from infrastructure.repository.factor_data_repository import FactorDataRepository

repo = FactorDataRepository(db_client)

# 查询因子值
df = repo.get_factor_values(
    factor_id="momentum_20",
    ts_codes=["000001.SZ", "000002.SZ"],
    start_date="20240101",
    end_date="20240131"
)

# 保存因子结果
count = repo.save_factor_results(
    factor_id="momentum_20",
    data=result_df,
    run_id=123
)

# 获取质量统计
stats = repo.get_quality_stats("momentum_20", "20240101", "20240131")
print(f"空值率: {stats['null_rate']:.2%}")
print(f"均值: {stats['mean']:.4f}")
```

## 单元测试

测试文件位于 `tests/` 目录：

- `infrastructure_test_query_builder.py` - QueryBuilder 单元测试
- `infrastructure_test_repository.py` - Repository 基类单元测试

**运行测试：**
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 运行所有测试
pytest tests/infrastructure_test_*.py -v

# 运行 QueryBuilder 测试
pytest tests/infrastructure_test_query_builder.py -v

# 运行 Repository 测试
pytest tests/infrastructure_test_repository.py -v

# 查看测试覆盖率
pytest tests/infrastructure_test_*.py --cov=infrastructure --cov-report=html
```

**测试覆盖：**
- QueryBuilder: 25+ 测试用例，覆盖所有 SQL 子句和边界情况
- BaseRepository: 15+ 测试用例，覆盖 CRUD 操作和异常处理
- 测试覆盖率目标: > 80%

## 核心优势

### 1. 统一的数据访问接口
- 所有数据访问通过 Repository 进行
- 业务逻辑与数据存储解耦
- 易于维护和扩展

### 2. 防止 SQL 注入
- 所有查询使用参数化
- QueryBuilder 自动处理参数转义
- 安全可靠

### 3. 易于测试
- Repository 可用 Mock 替换
- 单元测试无需真实数据库
- 提高测试速度和可靠性

### 4. 易于切换数据源
- Repository 接口保持不变
- 可切换到不同的数据库或 API
- 业务逻辑无需修改

### 5. 集中管理数据访问逻辑
- 数据加载逻辑不再分散在 4 处
- 统一的复权、过滤、聚合逻辑
- 减少代码重复

## 迁移指南

### 替换现有的数据加载代码

**之前（直接使用 DolphinDB 客户端）：**
```python
# 手写 SQL 字符串拼接
sql = f"SELECT * FROM sync_daily_data WHERE ts_code IN ('{code1}', '{code2}')"
df = db_client.execute(sql)
```

**之后（使用 Repository）：**
```python
# 使用 Repository
market_repo = MarketDataRepository(db_client)
df = market_repo.find_by_codes(
    ts_codes=[code1, code2],
    start_date="20240101",
    end_date="20240131"
)
```

### 在 ProductionEngine 中使用

```python
from infrastructure.repository.market_data_repository import MarketDataRepository
from infrastructure.repository.factor_data_repository import FactorDataRepository

class ProductionEngine:
    def __init__(self, db_client):
        self.db = db_client
        self.market_repo = MarketDataRepository(db_client)
        self.factor_repo = FactorDataRepository(db_client)

    def _load_data(self, definition, start_date, end_date, adjust_price):
        # 使用 Repository 加载数据
        return self.market_repo.get_with_adjustment(
            start_date=start_date,
            end_date=end_date,
            adjust_type=adjust_price
        )

    def _save_results(self, factor_id, result, run_id):
        # 使用 Repository 保存结果
        return self.factor_repo.save_factor_results(
            factor_id=factor_id,
            data=result,
            run_id=run_id
        )
```

## 扩展指南

### 创建自定义 Repository

```python
from infrastructure.repository.base import BaseRepository
from infrastructure.database.query_builder import QueryBuilder

class CustomRepository(BaseRepository):
    def __init__(self, db_client):
        super().__init__(db_client, "custom_table")

    def find_by_custom_condition(self, param1: str, param2: int):
        """自定义查询方法"""
        query = QueryBuilder(self.table_name) \
            .select_all() \
            .where("field1", "=", param1) \
            .where("field2", ">", param2) \
            .build()

        return self.db.execute(query.sql, query.params)
```

## 性能考虑

1. **查询优化**：使用 `columns` 参数只查询需要的列
2. **批量操作**：使用 `save()` 批量保存数据
3. **索引利用**：WHERE 条件优先使用索引列（ts_code, trade_date）
4. **连接池**：DolphinDB 客户端使用单例模式，避免重复连接

## 注意事项

1. **参数化查询**：始终使用 QueryBuilder 或 Repository，避免手写 SQL
2. **空值处理**：查询前检查参数是否为空（如 `ts_codes` 列表）
3. **日期格式**：统一使用 YYYYMMDD 格式（字符串）
4. **错误处理**：Repository 方法会记录日志，调用方需处理异常
5. **事务支持**：当前版本不支持事务，需要时可扩展

## 相关文档

- `USAGE_EXAMPLES.py` - 详细使用示例
- `tests/infrastructure_test_*.py` - 单元测试示例
- `engine/production/engine.py` - ProductionEngine 集成示例

## 技术栈

- **Python 3.11+**
- **Polars** - 数据框架
- **DolphinDB** - 时间序列数据库
- **pytest** - 单元测试框架

## 贡献者

Infrastructure Layer 实现于 2026-03-07

## License

内部项目，仅供 QuantSystem 使用
