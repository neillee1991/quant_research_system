"""
Benchmark tests for Data Pipeline.

Tests end-to-end data processing performance including loading, transformation, and computation.
"""

import pytest
from datetime import datetime, timedelta
import polars as pl
import sys
from pathlib import Path

backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.processor import DataPipeline
from infrastructure.repository import StockDailyRepository
from infrastructure.monitoring import performance_monitor, get_metrics_collector
from tests.benchmark import BENCHMARK_CONFIGS


class TestPipelineBenchmark:
    """Benchmark tests for data pipeline."""

    @pytest.fixture(scope="class")
    def pipeline(self):
        """Get data pipeline instance."""
        return DataPipeline()

    @pytest.fixture(scope="class")
    def stock_repo(self):
        """Get stock repository."""
        return StockDailyRepository()

    @pytest.fixture(scope="function")
    def clear_metrics(self):
        """Clear metrics before each test."""
        collector = get_metrics_collector()
        collector.clear_metrics()
        yield

    def _generate_stock_codes(self, count: int):
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
    def test_load_and_transform(self, pipeline, stock_repo, clear_metrics, config_name):
        """Test data loading and transformation performance."""
        config = BENCHMARK_CONFIGS[config_name]
        stock_codes = self._generate_stock_codes(config["stocks"])
        end_date = datetime.now()
        start_date = end_date - timedelta(days=config["days"])

        @performance_monitor()
        def load_and_transform():
            # Load data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            # Transform to Polars
            df = pl.DataFrame(data)

            # Apply transformations
            df = pipeline.add_returns(df)
            df = pipeline.add_log_returns(df)
            df = pipeline.normalize_columns(df, ["close", "volume"])

            return df

        result = load_and_transform()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\n{config['description']} - Load & Transform:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")
        print(f"  Rows: {len(result) if result is not None else 0}")

    @pytest.mark.benchmark
    def test_forward_fill_performance(self, pipeline, clear_metrics):
        """Test forward fill performance on large dataset."""
        # Create test data with missing values
        n_rows = 100000
        df = pl.DataFrame({
            "ts_code": [f"60000{i % 100}.SH" for i in range(n_rows)],
            "trade_date": [datetime.now() - timedelta(days=i % 365) for i in range(n_rows)],
            "close": [10.0 if i % 10 != 0 else None for i in range(n_rows)],
            "volume": [1000000 if i % 15 != 0 else None for i in range(n_rows)],
        })

        @performance_monitor()
        def apply_forward_fill():
            return pipeline.forward_fill(df, ["close", "volume"])

        result = apply_forward_fill()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nForward Fill ({n_rows} rows):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_rolling_window_performance(self, pipeline, clear_metrics):
        """Test rolling window calculations."""
        n_rows = 50000
        df = pl.DataFrame({
            "ts_code": [f"60000{i % 50}.SH" for i in range(n_rows)],
            "trade_date": [datetime.now() - timedelta(days=i % 365) for i in range(n_rows)],
            "close": [10.0 + (i % 100) * 0.1 for i in range(n_rows)],
        })

        @performance_monitor()
        def apply_rolling_windows():
            # Calculate multiple rolling windows
            result = df.with_columns([
                pl.col("close").rolling_mean(window_size=5).over("ts_code").alias("ma5"),
                pl.col("close").rolling_mean(window_size=10).over("ts_code").alias("ma10"),
                pl.col("close").rolling_mean(window_size=20).over("ts_code").alias("ma20"),
                pl.col("close").rolling_std(window_size=20).over("ts_code").alias("std20"),
            ])
            return result

        result = apply_rolling_windows()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nRolling Windows ({n_rows} rows, 4 windows):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_group_operations(self, pipeline, clear_metrics):
        """Test group-by operations performance."""
        n_rows = 100000
        df = pl.DataFrame({
            "ts_code": [f"60000{i % 100}.SH" for i in range(n_rows)],
            "trade_date": [datetime.now() - timedelta(days=i % 365) for i in range(n_rows)],
            "close": [10.0 + (i % 100) * 0.1 for i in range(n_rows)],
            "volume": [1000000 + i * 1000 for i in range(n_rows)],
        })

        @performance_monitor()
        def apply_group_operations():
            result = df.group_by("ts_code").agg([
                pl.col("close").mean().alias("avg_close"),
                pl.col("close").std().alias("std_close"),
                pl.col("close").min().alias("min_close"),
                pl.col("close").max().alias("max_close"),
                pl.col("volume").sum().alias("total_volume"),
                pl.col("volume").mean().alias("avg_volume"),
            ])
            return result

        result = apply_group_operations()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nGroup Operations ({n_rows} rows, 100 groups):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_join_operations(self, pipeline, clear_metrics):
        """Test join operations performance."""
        n_rows = 50000

        df1 = pl.DataFrame({
            "ts_code": [f"60000{i % 100}.SH" for i in range(n_rows)],
            "trade_date": [datetime.now() - timedelta(days=i % 365) for i in range(n_rows)],
            "close": [10.0 + (i % 100) * 0.1 for i in range(n_rows)],
        })

        df2 = pl.DataFrame({
            "ts_code": [f"60000{i % 100}.SH" for i in range(n_rows)],
            "trade_date": [datetime.now() - timedelta(days=i % 365) for i in range(n_rows)],
            "adj_factor": [1.0 + i * 0.001 for i in range(n_rows)],
        })

        @performance_monitor()
        def apply_join():
            result = df1.join(
                df2,
                on=["ts_code", "trade_date"],
                how="left"
            )
            return result

        result = apply_join()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nJoin Operations ({n_rows} rows each):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_complex_pipeline(self, pipeline, stock_repo, clear_metrics):
        """Test complex multi-step pipeline."""
        stock_codes = self._generate_stock_codes(100)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)

        @performance_monitor()
        def run_complex_pipeline():
            # Step 1: Load data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            # Step 2: Convert to Polars
            df = pl.DataFrame(data)

            # Step 3: Add returns
            df = pipeline.add_returns(df)

            # Step 4: Add rolling features
            df = df.with_columns([
                pl.col("close").rolling_mean(window_size=5).over("ts_code").alias("ma5"),
                pl.col("close").rolling_mean(window_size=20).over("ts_code").alias("ma20"),
                pl.col("volume").rolling_mean(window_size=5).over("ts_code").alias("vol_ma5"),
            ])

            # Step 5: Calculate cross-sectional ranks
            df = df.with_columns([
                pl.col("close").rank().over("trade_date").alias("close_rank"),
                pl.col("volume").rank().over("trade_date").alias("volume_rank"),
            ])

            # Step 6: Filter and aggregate
            result = df.filter(pl.col("close") > 10.0).group_by("ts_code").agg([
                pl.col("returns").mean().alias("avg_return"),
                pl.col("returns").std().alias("std_return"),
            ])

            return result

        result = run_complex_pipeline()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nComplex Pipeline (100 stocks, 180 days):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_memory_efficiency(self, pipeline, clear_metrics):
        """Test memory efficiency with large dataset."""
        n_rows = 200000

        @performance_monitor()
        def process_large_dataset():
            # Create large dataset
            df = pl.DataFrame({
                "ts_code": [f"60000{i % 200}.SH" for i in range(n_rows)],
                "trade_date": [datetime.now() - timedelta(days=i % 365) for i in range(n_rows)],
                "open": [10.0 + (i % 100) * 0.1 for i in range(n_rows)],
                "high": [11.0 + (i % 100) * 0.1 for i in range(n_rows)],
                "low": [9.0 + (i % 100) * 0.1 for i in range(n_rows)],
                "close": [10.5 + (i % 100) * 0.1 for i in range(n_rows)],
                "volume": [1000000 + i * 1000 for i in range(n_rows)],
            })

            # Process in chunks to test memory efficiency
            chunk_size = 50000
            results = []

            for i in range(0, n_rows, chunk_size):
                chunk = df.slice(i, chunk_size)
                processed = pipeline.add_returns(chunk)
                results.append(processed)

            return pl.concat(results)

        result = process_large_dataset()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nMemory Efficiency Test ({n_rows} rows, chunked):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory Peak: {stats['memory_peak_mb']['mean']:.2f}MB")
        print(f"  Memory Delta: {stats['memory_delta_mb']['mean']:.2f}MB")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "benchmark", "-s"])
