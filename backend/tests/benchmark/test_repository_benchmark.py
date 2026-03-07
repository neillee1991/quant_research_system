"""
Benchmark tests for Repository layer.

Tests data access patterns, query building, and result processing.
"""

import pytest
from datetime import datetime, timedelta
from typing import List
import sys
from pathlib import Path

backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.repository import (
    StockDailyRepository,
    FactorValueRepository,
    FactorMetadataRepository,
)
from infrastructure.monitoring import performance_monitor, get_metrics_collector
from tests.benchmark import BENCHMARK_CONFIGS


class TestRepositoryBenchmark:
    """Benchmark tests for repository layer."""

    @pytest.fixture(scope="class")
    def stock_repo(self):
        """Get stock daily repository."""
        return StockDailyRepository()

    @pytest.fixture(scope="class")
    def factor_value_repo(self):
        """Get factor value repository."""
        return FactorValueRepository()

    @pytest.fixture(scope="class")
    def factor_meta_repo(self):
        """Get factor metadata repository."""
        return FactorMetadataRepository()

    @pytest.fixture(scope="function")
    def clear_metrics(self):
        """Clear metrics before each test."""
        collector = get_metrics_collector()
        collector.clear_metrics()
        yield

    def _generate_stock_codes(self, count: int) -> List[str]:
        """Generate stock codes."""
        codes = []
        for i in range(count):
            if i < count // 2:
                codes.append(f"{600000 + i:06d}.SH")
            else:
                codes.append(f"{000001 + i:06d}.SZ")
        return codes

    @pytest.mark.benchmark
    @pytest.mark.parametrize("config_name", ["small", "medium", "large"])
    def test_find_by_date_range(self, stock_repo, clear_metrics, config_name):
        """Test find by date range performance."""
        config = BENCHMARK_CONFIGS[config_name]
        stock_codes = self._generate_stock_codes(config["stocks"])
        end_date = datetime.now()
        start_date = end_date - timedelta(days=config["days"])

        @performance_monitor()
        def query_data():
            return stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

        result = query_data()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\n{config['description']} - Find by Date Range:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")
        print(f"  Rows: {len(result) if result is not None else 0}")

    @pytest.mark.benchmark
    def test_find_latest_performance(self, stock_repo, clear_metrics):
        """Test find latest data performance."""
        stock_codes = self._generate_stock_codes(100)

        @performance_monitor()
        def query_latest():
            return stock_repo.find_latest(stock_codes=stock_codes, limit=1)

        result = query_latest()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nFind Latest (100 stocks):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_find_with_filters(self, stock_repo, clear_metrics):
        """Test find with multiple filters."""
        stock_codes = self._generate_stock_codes(50)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        @performance_monitor()
        def query_with_filters():
            return stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                filters={
                    "volume": (">", 1000000),
                    "close": (">", 10.0)
                }
            )

        result = query_with_filters()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nFind with Filters (50 stocks, 90 days):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_factor_value_query(self, factor_value_repo, clear_metrics):
        """Test factor value query performance."""
        stock_codes = self._generate_stock_codes(100)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)

        @performance_monitor()
        def query_factor_values():
            return factor_value_repo.find_by_date_range(
                factor_ids=[1, 2, 3],
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

        result = query_factor_values()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nFactor Value Query (100 stocks, 180 days, 3 factors):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_metadata_query(self, factor_meta_repo, clear_metrics):
        """Test metadata query performance."""

        @performance_monitor()
        def query_metadata():
            return factor_meta_repo.find_all()

        result = query_metadata()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nMetadata Query:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_batch_operations(self, stock_repo, clear_metrics):
        """Test batch query operations."""
        iterations = 10
        stock_codes = self._generate_stock_codes(20)

        @performance_monitor()
        def run_batch_queries():
            results = []
            for i in range(iterations):
                end_date = datetime.now() - timedelta(days=i * 30)
                start_date = end_date - timedelta(days=30)
                result = stock_repo.find_by_date_range(
                    stock_codes=stock_codes,
                    start_date=start_date,
                    end_date=end_date
                )
                results.append(result)
            return results

        results = run_batch_queries()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nBatch Operations ({iterations} queries):")
        print(f"  Total Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / iterations:.3f}s")

    @pytest.mark.benchmark
    def test_aggregation_query(self, stock_repo, clear_metrics):
        """Test aggregation through repository."""
        stock_codes = self._generate_stock_codes(100)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        @performance_monitor()
        def query_with_aggregation():
            # Get data and perform aggregation
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )
            if data is not None and len(data) > 0:
                # Simulate aggregation
                return data.groupby('ts_code').agg({
                    'close': ['mean', 'std', 'min', 'max'],
                    'volume': 'sum'
                })
            return None

        result = query_with_aggregation()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nAggregation Query (100 stocks, 1 year):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "benchmark", "-s"])
