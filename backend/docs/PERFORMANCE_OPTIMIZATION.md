# Performance Optimization Guide

## Overview
This document provides optimization recommendations based on performance analysis of the QuantSystem infrastructure.

## Identified Performance Bottlenecks

### 1. Database Query Performance

#### Issue
- Multiple round-trips to DolphinDB for related data
- Inefficient query construction for large date ranges
- Missing query result caching

#### Recommendations

**Priority: HIGH**

1. **Implement Query Batching**
   ```python
   # Before: Multiple queries
   for stock in stocks:
       data = repo.find_by_stock(stock)

   # After: Single batch query
   data = repo.find_by_stocks(stocks)
   ```

2. **Add Query Result Caching**
   ```python
   from functools import lru_cache

   @lru_cache(maxsize=128)
   def get_stock_data(stock_code, start_date, end_date):
       return repo.find_by_date_range(...)
   ```

3. **Optimize Date Range Queries**
   - Use DolphinDB's partition pruning
   - Add indexes on (ts_code, trade_date)
   - Use columnar selection (only needed columns)

**Expected Impact**: 30-50% reduction in query time

---

### 2. Memory Usage in Data Pipeline

#### Issue
- Large DataFrames loaded entirely into memory
- Unnecessary data copies during transformations
- No streaming for large datasets

#### Recommendations

**Priority: HIGH**

1. **Implement Chunked Processing**
   ```python
   def process_large_dataset(stock_codes, chunk_size=1000):
       for i in range(0, len(stock_codes), chunk_size):
           chunk = stock_codes[i:i + chunk_size]
           data = load_data(chunk)
           process_data(data)
           yield result
   ```

2. **Use Polars Lazy Evaluation**
   ```python
   # Use lazy API for better memory efficiency
   df = pl.scan_parquet("data.parquet")
   result = df.filter(...).select(...).collect()
   ```

3. **Avoid Unnecessary Copies**
   ```python
   # Before: Creates copy
   df_new = df.with_columns([...])

   # After: In-place when possible
   df = df.with_columns([...])
   ```

**Expected Impact**: 40-60% reduction in memory usage

---

### 3. Polars Operations Optimization

#### Issue
- Inefficient use of rolling windows
- Multiple passes over data
- Non-vectorized operations

#### Recommendations

**Priority: MEDIUM**

1. **Combine Multiple Operations**
   ```python
   # Before: Multiple passes
   df = df.with_columns([pl.col("close").rolling_mean(5).alias("ma5")])
   df = df.with_columns([pl.col("close").rolling_mean(10).alias("ma10")])

   # After: Single pass
   df = df.with_columns([
       pl.col("close").rolling_mean(5).alias("ma5"),
       pl.col("close").rolling_mean(10).alias("ma10"),
   ])
   ```

2. **Use Efficient Grouping**
   ```python
   # Use over() for grouped operations
   df = df.with_columns([
       pl.col("close").mean().over("ts_code").alias("avg_close")
   ])
   ```

3. **Vectorize Custom Functions**
   ```python
   # Use Polars expressions instead of apply()
   # Avoid: df.apply(lambda x: custom_func(x))
   # Use: pl.when(...).then(...).otherwise(...)
   ```

**Expected Impact**: 20-30% faster computation

---

### 4. Connection Pool Management

#### Issue
- Connection pool exhaustion under high load
- No connection retry logic
- Inefficient connection reuse

#### Recommendations

**Priority: MEDIUM**

1. **Increase Pool Size**
   ```python
   # In DolphinDBClient
   self.pool_size = 20  # Increase from default
   ```

2. **Add Connection Retry Logic**
   ```python
   def execute_with_retry(query, max_retries=3):
       for attempt in range(max_retries):
           try:
               return db_client.run_script(query)
           except ConnectionError:
               if attempt == max_retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff
   ```

3. **Implement Connection Health Checks**
   ```python
   def check_connection_health():
       try:
           db_client.run_script("select 1")
           return True
       except:
           return False
   ```

**Expected Impact**: 90%+ success rate under high concurrency

---

### 5. Query Builder Optimization

#### Issue
- String concatenation for query building
- No query plan caching
- Redundant query validation

#### Recommendations

**Priority: LOW**

1. **Use String Builder Pattern**
   ```python
   # Use list and join instead of string concatenation
   parts = []
   parts.append("select")
   parts.append(", ".join(columns))
   query = " ".join(parts)
   ```

2. **Cache Query Templates**
   ```python
   @lru_cache(maxsize=256)
   def get_query_template(table, columns_hash):
       return build_query_template(table, columns)
   ```

3. **Lazy Query Validation**
   ```python
   # Validate only when executing, not when building
   class QueryBuilder:
       def build(self, validate=False):
           query = self._build_query()
           if validate:
               self._validate(query)
           return query
   ```

**Expected Impact**: 10-15% faster query building

---

## Implementation Priority

### Phase 1: Critical Optimizations (Week 1)
1. Implement query batching
2. Add chunked processing for large datasets
3. Optimize Polars operations

**Expected Overall Impact**: 40-50% performance improvement

### Phase 2: Important Optimizations (Week 2)
1. Add query result caching
2. Improve connection pool management
3. Implement connection retry logic

**Expected Overall Impact**: Additional 20-30% improvement

### Phase 3: Nice-to-Have Optimizations (Week 3)
1. Optimize query builder
2. Add query plan caching
3. Fine-tune memory management

**Expected Overall Impact**: Additional 10-15% improvement

---

## Monitoring and Validation

### Before Optimization
1. Run full benchmark suite
2. Record baseline metrics
3. Identify top 3 bottlenecks

### After Each Optimization
1. Run targeted benchmarks
2. Compare with baseline
3. Verify no regressions
4. Update metrics

### Continuous Monitoring
1. Use performance_monitor decorator on critical paths
2. Review metrics weekly
3. Set up alerts for performance degradation

---

## Performance Targets

### Query Performance
- Simple queries: < 100ms (P95)
- Complex queries: < 500ms (P95)
- Aggregations: < 1s (P95)

### Memory Usage
- Small dataset (10 stocks, 30 days): < 50MB
- Medium dataset (100 stocks, 180 days): < 200MB
- Large dataset (1000 stocks, 365 days): < 1GB

### Throughput
- Query throughput: > 100 QPS
- Data processing: > 10,000 rows/sec
- Factor computation: > 1,000 factors/sec

### Concurrency
- Support 50+ concurrent queries
- Connection pool utilization: < 80%
- Error rate under load: < 1%

---

## Code Examples

### Example 1: Optimized Data Loading
```python
from infrastructure.monitoring import performance_monitor

@performance_monitor()
def load_data_optimized(stock_codes, start_date, end_date):
    """Optimized data loading with batching and caching."""
    # Batch stocks into chunks
    chunk_size = 100
    results = []

    for i in range(0, len(stock_codes), chunk_size):
        chunk = stock_codes[i:i + chunk_size]

        # Load with only needed columns
        data = repo.find_by_date_range(
            stock_codes=chunk,
            start_date=start_date,
            end_date=end_date,
            columns=["ts_code", "trade_date", "close", "volume"]
        )

        results.append(data)

    # Combine results efficiently
    return pl.concat(results)
```

### Example 2: Optimized Factor Computation
```python
@performance_monitor()
def compute_factors_optimized(df):
    """Compute multiple factors in single pass."""
    # Use lazy evaluation
    return df.lazy().with_columns([
        # All computations in one pass
        pl.col("close").rolling_mean(5).over("ts_code").alias("ma5"),
        pl.col("close").rolling_mean(20).over("ts_code").alias("ma20"),
        pl.col("returns").rolling_std(20).over("ts_code").alias("volatility"),
        (pl.col("close") / pl.col("close").shift(20).over("ts_code") - 1).alias("momentum"),
    ]).collect()
```

### Example 3: Connection Pool with Retry
```python
class ResilientDBClient:
    """DB client with retry logic."""

    def __init__(self, max_retries=3):
        self.client = DolphinDBClient()
        self.max_retries = max_retries

    @performance_monitor()
    def execute_query(self, query):
        """Execute query with retry logic."""
        for attempt in range(self.max_retries):
            try:
                return self.client.run_script(query)
            except ConnectionError as e:
                if attempt == self.max_retries - 1:
                    raise
                wait_time = 2 ** attempt
                time.sleep(wait_time)
```

---

## Testing Strategy

### Performance Regression Tests
```python
def test_performance_regression():
    """Ensure optimizations don't regress."""
    baseline = load_baseline_metrics()
    current = run_benchmark_suite()

    assert current.duration <= baseline.duration * 1.1  # Allow 10% variance
    assert current.memory <= baseline.memory * 1.1
```

### Load Testing
```python
def test_under_load():
    """Test system under realistic load."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(query_data) for _ in range(100)]
        results = [f.result() for f in futures]

    success_rate = sum(1 for r in results if r is not None) / len(results)
    assert success_rate > 0.99  # 99% success rate
```

---

## Next Steps

1. Review and prioritize optimizations
2. Implement Phase 1 optimizations
3. Run benchmarks and validate improvements
4. Document results and update baselines
5. Proceed to Phase 2

---

## References

- Polars Performance Guide: https://pola-rs.github.io/polars/user-guide/performance/
- DolphinDB Best Practices: https://www.dolphindb.com/
- Python Profiling: https://docs.python.org/3/library/profile.html
