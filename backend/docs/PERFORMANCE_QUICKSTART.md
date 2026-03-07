# Performance Monitoring System - Quick Start Guide

## Installation

The performance monitoring system is already integrated into the infrastructure. No additional installation required.

## Basic Usage

### 1. Monitor a Function

```python
from infrastructure.monitoring import performance_monitor

@performance_monitor()
def compute_factor(data):
    # Your code here
    return result
```

### 2. View Metrics

```python
from infrastructure.monitoring import get_metrics_collector

collector = get_metrics_collector()
stats = collector.get_statistics()
print(f"Average duration: {stats['duration']['mean']:.3f}s")
```

### 3. Run Benchmarks

```bash
# Quick test
python scripts/run_benchmarks.py --suite quick

# Full test
python scripts/run_benchmarks.py --suite full --output results.json
```

### 4. View Dashboard

```bash
# Overall statistics
python scripts/performance_dashboard.py

# Specific function
python scripts/performance_dashboard.py --function "my_function"

# Export metrics
python scripts/performance_dashboard.py --export metrics.json
```

## Advanced Usage

### Monitor with Context Manager

```python
from infrastructure.monitoring import PerformanceContext

with PerformanceContext("my_operation") as metrics:
    # Your code here
    result = do_something()

    # Add custom metadata
    metrics.metadata["custom_field"] = "value"
    metrics.data_rows = len(result)
```

### Async Functions

```python
from infrastructure.monitoring import async_performance_monitor

@async_performance_monitor()
async def fetch_data():
    return await db.query()
```

### Compare Implementations

```python
from scripts.profile_code import compare_implementations

compare_implementations(
    old_implementation,
    new_implementation,
    arg1, arg2
)
```

## Benchmark Suites

| Suite | Tests | Duration | Use Case |
|-------|-------|----------|----------|
| quick | Query builder only | ~1 min | Quick validation |
| standard | DB, Repo, QB, Pipeline | ~5 min | Regular testing |
| full | All except stress | ~10 min | Pre-release |
| stress | Stress tests only | ~15 min | Stability testing |
| all | Everything | ~25 min | Complete validation |

## Metrics Explained

- **Duration**: How long the function took to execute
- **Memory Delta**: Memory increase during execution
- **Memory Peak**: Maximum memory used
- **CPU %**: CPU utilization during execution
- **P50/P95/P99**: Percentile values (50th, 95th, 99th)

## Performance Targets

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Query latency (P95) | < 500ms | > 1s |
| Memory usage | < 1GB | > 2GB |
| Error rate | < 1% | > 5% |
| QPS | > 100 | < 50 |

## Troubleshooting

### High Memory Usage
1. Check for data leaks
2. Use chunked processing
3. Enable lazy evaluation

### Slow Queries
1. Check query complexity
2. Add indexes
3. Use batching

### Connection Errors
1. Check pool size
2. Add retry logic
3. Verify network

## Next Steps

1. Read full documentation: `docs/PERFORMANCE_REPORT_V2.md`
2. Review optimization guide: `docs/PERFORMANCE_OPTIMIZATION.md`
3. Run your first benchmark: `python scripts/run_benchmarks.py --suite quick`
4. Monitor your code: Add `@performance_monitor()` to critical functions

## Support

For issues or questions:
1. Check documentation in `docs/`
2. Review examples in `infrastructure/USAGE_EXAMPLES.py`
3. Run tests to validate setup: `pytest tests/benchmark/ -v`
