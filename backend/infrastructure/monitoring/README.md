# Performance Monitoring System - README

## Overview

Complete performance monitoring and benchmarking system for QuantSystem infrastructure.

## Components

### 1. Performance Monitoring (`infrastructure/monitoring/`)
- **performance_monitor.py**: Decorators and context managers for tracking
- **metrics_collector.py**: Centralized metrics storage and analysis

### 2. Benchmark Suite (`tests/benchmark/`)
- **test_db_client_benchmark.py**: Database client performance
- **test_repository_benchmark.py**: Repository layer performance
- **test_query_builder_benchmark.py**: Query construction performance
- **test_pipeline_benchmark.py**: Data pipeline performance
- **test_e2e_benchmark.py**: End-to-end workflows
- **test_stress_test.py**: Stress and stability testing

### 3. Tools (`scripts/`)
- **performance_dashboard.py**: Real-time metrics visualization
- **run_benchmarks.py**: Benchmark suite runner
- **profile_code.py**: Detailed profiling tool

### 4. Documentation (`docs/`)
- **PERFORMANCE_REPORT_V2.md**: Complete performance report
- **PERFORMANCE_OPTIMIZATION.md**: Optimization recommendations
- **PERFORMANCE_QUICKSTART.md**: Quick start guide

## Quick Start

### 1. Monitor Your Code

```python
from infrastructure.monitoring import performance_monitor

@performance_monitor()
def my_function():
    # Your code here
    pass
```

### 2. Run Benchmarks

```bash
# Quick test
python scripts/run_benchmarks.py --suite quick

# Full suite
python scripts/run_benchmarks.py --suite full --output results.json
```

### 3. View Metrics

```bash
# Dashboard
python scripts/performance_dashboard.py

# Specific function
python scripts/performance_dashboard.py --function "my_function"
```

## Benchmark Suites

| Suite | Duration | Description |
|-------|----------|-------------|
| quick | ~1 min | Query builder only |
| standard | ~5 min | Core components |
| full | ~10 min | All except stress |
| stress | ~15 min | Stability tests |
| all | ~25 min | Complete suite |

## Performance Targets

| Metric | Target | Alert |
|--------|--------|-------|
| Query latency (P95) | < 500ms | > 1s |
| Memory usage | < 1GB | > 2GB |
| Error rate | < 1% | > 5% |
| QPS | > 100 | < 50 |

## Key Features

✅ Automatic performance tracking with decorators
✅ Statistical analysis (P50, P95, P99)
✅ Memory and CPU monitoring
✅ Real-time dashboard
✅ Comprehensive benchmark suite
✅ Stress testing framework
✅ Profiling tools
✅ Export capabilities

## Usage Examples

See `examples/performance_monitoring_examples.py` for detailed examples.

## Documentation

- **Quick Start**: `docs/PERFORMANCE_QUICKSTART.md`
- **Full Report**: `docs/PERFORMANCE_REPORT_V2.md`
- **Optimization Guide**: `docs/PERFORMANCE_OPTIMIZATION.md`

## Next Steps

1. Read the quick start guide
2. Run your first benchmark
3. Add monitoring to your code
4. Review optimization recommendations

## Support

For issues or questions, refer to the documentation in `docs/`.
