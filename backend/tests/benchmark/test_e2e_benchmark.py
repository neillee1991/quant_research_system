"""
End-to-end benchmark tests.

Tests complete workflows from data loading to factor computation.
"""

import pytest
from datetime import datetime, timedelta
import sys
from pathlib import Path

backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.repository import StockDailyRepository, FactorValueRepository
from infrastructure.processor import DataPipeline
from infrastructure.monitoring import performance_monitor, get_metrics_collector
from tests.benchmark import BENCHMARK_CONFIGS


class TestE2EBenchmark:
    """End-to-end benchmark tests."""

    @pytest.fixture(scope="class")
    def stock_repo(self):
        """Get stock repository."""
        return StockDailyRepository()

    @pytest.fixture(scope="class")
    def factor_repo(self):
        """Get factor repository."""
        return FactorValueRepository()

    @pytest.fixture(scope="class")
    def pipeline(self):
        """Get data pipeline."""
        return DataPipeline()

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
    def test_complete_factor_workflow(self, stock_repo, pipeline, clear_metrics, config_name):
        """Test complete factor computation workflow."""
        config = BENCHMARK_CONFIGS[config_name]
        stock_codes = self._generate_stock_codes(config["stocks"])
        end_date = datetime.now()
        start_date = end_date - timedelta(days=config["days"])

        @performance_monitor()
        def complete_workflow():
            # Step 1: Load raw data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            # Step 2: Convert to Polars
            import polars as pl
            df = pl.DataFrame(data)

            # Step 3: Data preprocessing
            df = pipeline.add_returns(df)
            df = pipeline.forward_fill(df, ["close", "volume"])

            # Step 4: Calculate technical indicators
            df = df.with_columns([
                # Moving averages
                pl.col("close").rolling_mean(window_size=5).over("ts_code").alias("ma5"),
                pl.col("close").rolling_mean(window_size=10).over("ts_code").alias("ma10"),
                pl.col("close").rolling_mean(window_size=20).over("ts_code").alias("ma20"),

                # Volatility
                pl.col("returns").rolling_std(window_size=20).over("ts_code").alias("volatility"),

                # Volume indicators
                pl.col("volume").rolling_mean(window_size=5).over("ts_code").alias("vol_ma5"),
            ])

            # Step 5: Calculate factors
            df = df.with_columns([
                # Momentum
                (pl.col("close") / pl.col("close").shift(20).over("ts_code") - 1).alias("momentum_20d"),

                # Mean reversion
                ((pl.col("close") - pl.col("ma20")) / pl.col("ma20")).alias("mean_reversion"),

                # Volume ratio
                (pl.col("volume") / pl.col("vol_ma5")).alias("volume_ratio"),
            ])

            # Step 6: Cross-sectional processing
            df = df.with_columns([
                pl.col("momentum_20d").rank().over("trade_date").alias("momentum_rank"),
                pl.col("volume_ratio").rank().over("trade_date").alias("volume_rank"),
            ])

            # Step 7: Neutralization (simulate)
            df = pipeline.normalize_columns(df, ["momentum_20d", "mean_reversion", "volume_ratio"])

            return df

        result = complete_workflow()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\n{config['description']} - Complete Workflow:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory Peak: {stats['memory_peak_mb']['mean']:.2f}MB")
        print(f"  Memory Delta: {stats['memory_delta_mb']['mean']:.2f}MB")
        print(f"  Rows Processed: {len(result) if result is not None else 0}")

    @pytest.mark.benchmark
    def test_multi_factor_computation(self, stock_repo, pipeline, clear_metrics):
        """Test computing multiple factors simultaneously."""
        stock_codes = self._generate_stock_codes(100)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)

        @performance_monitor()
        def compute_multiple_factors():
            import polars as pl

            # Load data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            df = pl.DataFrame(data)
            df = pipeline.add_returns(df)

            # Compute 10 different factors
            df = df.with_columns([
                # Momentum factors
                (pl.col("close") / pl.col("close").shift(5).over("ts_code") - 1).alias("mom_5d"),
                (pl.col("close") / pl.col("close").shift(10).over("ts_code") - 1).alias("mom_10d"),
                (pl.col("close") / pl.col("close").shift(20).over("ts_code") - 1).alias("mom_20d"),

                # Volatility factors
                pl.col("returns").rolling_std(window_size=10).over("ts_code").alias("vol_10d"),
                pl.col("returns").rolling_std(window_size=20).over("ts_code").alias("vol_20d"),

                # Volume factors
                (pl.col("volume") / pl.col("volume").rolling_mean(window_size=5).over("ts_code")).alias("vol_ratio_5d"),
                (pl.col("volume") / pl.col("volume").rolling_mean(window_size=20).over("ts_code")).alias("vol_ratio_20d"),

                # Price factors
                ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("price_range"),
                ((pl.col("close") - pl.col("open")) / pl.col("open")).alias("intraday_return"),
                (pl.col("close") / pl.col("close").rolling_mean(window_size=20).over("ts_code")).alias("price_to_ma20"),
            ])

            return df

        result = compute_multiple_factors()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nMulti-Factor Computation (10 factors, 100 stocks, 180 days):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_factor_analysis_workflow(self, stock_repo, pipeline, clear_metrics):
        """Test factor analysis workflow including IC calculation."""
        stock_codes = self._generate_stock_codes(50)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        @performance_monitor()
        def factor_analysis():
            import polars as pl

            # Load data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            df = pl.DataFrame(data)
            df = pipeline.add_returns(df)

            # Calculate factor
            df = df.with_columns([
                (pl.col("close") / pl.col("close").shift(20).over("ts_code") - 1).alias("factor_value")
            ])

            # Calculate forward returns
            df = df.with_columns([
                pl.col("returns").shift(-1).over("ts_code").alias("forward_return_1d"),
                pl.col("returns").shift(-5).over("ts_code").alias("forward_return_5d"),
            ])

            # Calculate IC (correlation between factor and forward returns)
            ic_results = []
            for date in df["trade_date"].unique():
                date_data = df.filter(pl.col("trade_date") == date)
                if len(date_data) > 10:
                    # Simulate IC calculation
                    factor_vals = date_data["factor_value"].to_list()
                    forward_rets = date_data["forward_return_1d"].to_list()

                    # Simple correlation
                    if None not in factor_vals and None not in forward_rets:
                        ic_results.append({
                            "date": date,
                            "ic": 0.05  # Placeholder
                        })

            return ic_results

        result = factor_analysis()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nFactor Analysis Workflow (50 stocks, 90 days):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_backtest_simulation(self, stock_repo, pipeline, clear_metrics):
        """Test backtest simulation workflow."""
        stock_codes = self._generate_stock_codes(100)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)

        @performance_monitor()
        def run_backtest():
            import polars as pl

            # Load data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            df = pl.DataFrame(data)
            df = pipeline.add_returns(df)

            # Calculate signal
            df = df.with_columns([
                (pl.col("close") / pl.col("close").shift(20).over("ts_code") - 1).alias("signal")
            ])

            # Rank and select top/bottom
            df = df.with_columns([
                pl.col("signal").rank().over("trade_date").alias("signal_rank")
            ])

            # Simulate portfolio construction
            total_stocks = df.group_by("trade_date").agg(pl.count()).height
            top_n = 10

            # Calculate portfolio returns
            portfolio_returns = []
            for date in df["trade_date"].unique().sort():
                date_data = df.filter(pl.col("trade_date") == date).sort("signal_rank", descending=True)
                if len(date_data) >= top_n:
                    top_stocks = date_data.head(top_n)
                    avg_return = top_stocks["returns"].mean()
                    portfolio_returns.append({
                        "date": date,
                        "return": avg_return
                    })

            return portfolio_returns

        result = run_backtest()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nBacktest Simulation (100 stocks, 180 days):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_data_quality_check(self, stock_repo, clear_metrics):
        """Test data quality checking workflow."""
        stock_codes = self._generate_stock_codes(200)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        @performance_monitor()
        def check_data_quality():
            import polars as pl

            # Load data
            data = stock_repo.find_by_date_range(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date
            )

            if data is None or len(data) == 0:
                return None

            df = pl.DataFrame(data)

            # Check for missing values
            missing_stats = {
                col: df[col].null_count() / len(df)
                for col in df.columns
            }

            # Check for outliers
            numeric_cols = ["open", "high", "low", "close", "volume"]
            outlier_stats = {}

            for col in numeric_cols:
                if col in df.columns:
                    q1 = df[col].quantile(0.25)
                    q3 = df[col].quantile(0.75)
                    iqr = q3 - q1
                    lower = q1 - 1.5 * iqr
                    upper = q3 + 1.5 * iqr

                    outliers = df.filter(
                        (pl.col(col) < lower) | (pl.col(col) > upper)
                    )
                    outlier_stats[col] = len(outliers) / len(df)

            # Check for duplicates
            duplicates = df.group_by(["ts_code", "trade_date"]).agg(pl.count()).filter(pl.col("count") > 1)

            return {
                "missing_stats": missing_stats,
                "outlier_stats": outlier_stats,
                "duplicate_count": len(duplicates)
            }

        result = check_data_quality()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nData Quality Check (200 stocks, 90 days):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "benchmark", "-s"])
