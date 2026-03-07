"""
Benchmark tests for QueryBuilder.

Tests query construction, optimization, and execution planning.
"""

import pytest
from datetime import datetime, timedelta
import sys
from pathlib import Path

backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.database import QueryBuilder
from infrastructure.monitoring import performance_monitor, get_metrics_collector
from tests.benchmark import BENCHMARK_CONFIGS


class TestQueryBuilderBenchmark:
    """Benchmark tests for QueryBuilder."""

    @pytest.fixture(scope="function")
    def clear_metrics(self):
        """Clear metrics before each test."""
        collector = get_metrics_collector()
        collector.clear_metrics()
        yield

    @pytest.mark.benchmark
    def test_simple_query_build(self, clear_metrics):
        """Test simple query building performance."""

        @performance_monitor()
        def build_simple_query():
            queries = []
            for i in range(1000):
                qb = QueryBuilder("sync_daily_data")
                qb.select(["ts_code", "trade_date", "close"])
                qb.where("ts_code", "=", f"60000{i % 10}.SH")
                queries.append(qb.build())
            return queries

        queries = build_simple_query()
        assert len(queries) == 1000

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nSimple Query Build (1000 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 1000 * 1000:.3f}ms")

    @pytest.mark.benchmark
    def test_complex_query_build(self, clear_metrics):
        """Test complex query building performance."""

        @performance_monitor()
        def build_complex_query():
            queries = []
            for i in range(100):
                qb = QueryBuilder("sync_daily_data")
                qb.select(["ts_code", "trade_date", "open", "high", "low", "close", "volume"])
                qb.where("ts_code", "in", [f"60000{j}.SH" for j in range(10)])
                qb.where("trade_date", ">=", "2024.01.01")
                qb.where("trade_date", "<=", "2024.12.31")
                qb.where("volume", ">", 1000000)
                qb.order_by("trade_date", "desc")
                qb.limit(1000)
                queries.append(qb.build())
            return queries

        queries = build_complex_query()
        assert len(queries) == 100

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nComplex Query Build (100 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 100 * 1000:.3f}ms")

    @pytest.mark.benchmark
    def test_join_query_build(self, clear_metrics):
        """Test join query building performance."""

        @performance_monitor()
        def build_join_query():
            queries = []
            for i in range(100):
                qb = QueryBuilder("sync_daily_data", alias="d")
                qb.select([
                    "d.ts_code",
                    "d.trade_date",
                    "d.close",
                    "a.adj_factor"
                ])
                qb.join(
                    "sync_adj_factor",
                    alias="a",
                    on="d.ts_code = a.ts_code and d.trade_date = a.trade_date",
                    join_type="left"
                )
                qb.where("d.trade_date", ">=", "2024.01.01")
                qb.limit(10000)
                queries.append(qb.build())
            return queries

        queries = build_join_query()
        assert len(queries) == 100

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nJoin Query Build (100 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 100 * 1000:.3f}ms")

    @pytest.mark.benchmark
    def test_aggregation_query_build(self, clear_metrics):
        """Test aggregation query building performance."""

        @performance_monitor()
        def build_aggregation_query():
            queries = []
            for i in range(100):
                qb = QueryBuilder("sync_daily_data")
                qb.select([
                    "ts_code",
                    "avg(close) as avg_close",
                    "std(close) as std_close",
                    "max(high) as max_high",
                    "min(low) as min_low",
                    "sum(volume) as total_volume"
                ])
                qb.where("trade_date", ">=", "2024.01.01")
                qb.group_by(["ts_code"])
                qb.having("avg(close)", ">", 10.0)
                queries.append(qb.build())
            return queries

        queries = build_aggregation_query()
        assert len(queries) == 100

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nAggregation Query Build (100 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 100 * 1000:.3f}ms")

    @pytest.mark.benchmark
    def test_subquery_build(self, clear_metrics):
        """Test subquery building performance."""

        @performance_monitor()
        def build_subquery():
            queries = []
            for i in range(50):
                # Build subquery
                subquery = QueryBuilder("sync_daily_data")
                subquery.select(["ts_code", "avg(close) as avg_close"])
                subquery.where("trade_date", ">=", "2024.01.01")
                subquery.group_by(["ts_code"])

                # Build main query
                qb = QueryBuilder("sync_daily_data", alias="d")
                qb.select(["d.ts_code", "d.trade_date", "d.close"])
                qb.where("d.ts_code", "in", f"({subquery.build()})")
                queries.append(qb.build())
            return queries

        queries = build_subquery()
        assert len(queries) == 50

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nSubquery Build (50 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 50 * 1000:.3f}ms")

    @pytest.mark.benchmark
    def test_dynamic_query_build(self, clear_metrics):
        """Test dynamic query building with varying conditions."""

        @performance_monitor()
        def build_dynamic_queries():
            queries = []
            conditions = [
                {"volume": (">", 1000000)},
                {"close": (">", 10.0), "volume": (">", 500000)},
                {"high": ("<", 100.0), "low": (">", 5.0)},
                {"ts_code": ("in", ["600000.SH", "600001.SH"])},
            ]

            for i in range(250):
                qb = QueryBuilder("sync_daily_data")
                qb.select(["*"])

                # Add dynamic conditions
                cond = conditions[i % len(conditions)]
                for field, (op, value) in cond.items():
                    qb.where(field, op, value)

                qb.limit(1000)
                queries.append(qb.build())
            return queries

        queries = build_dynamic_queries()
        assert len(queries) == 250

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nDynamic Query Build (250 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 250 * 1000:.3f}ms")

    @pytest.mark.benchmark
    def test_query_reuse(self, clear_metrics):
        """Test query builder reuse performance."""

        @performance_monitor()
        def test_reuse():
            # Create base query
            base_qb = QueryBuilder("sync_daily_data")
            base_qb.select(["ts_code", "trade_date", "close"])
            base_qb.where("trade_date", ">=", "2024.01.01")

            queries = []
            for i in range(100):
                # Clone and modify
                qb = QueryBuilder("sync_daily_data")
                qb.select(["ts_code", "trade_date", "close"])
                qb.where("trade_date", ">=", "2024.01.01")
                qb.where("ts_code", "=", f"60000{i % 10}.SH")
                queries.append(qb.build())
            return queries

        queries = test_reuse()
        assert len(queries) == 100

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nQuery Reuse (100 queries):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / 100 * 1000:.3f}ms")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "benchmark", "-s"])
