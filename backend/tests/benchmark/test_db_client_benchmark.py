"""
Benchmark tests for DolphinDB client operations.

Tests query performance, connection pooling, and data transfer efficiency.
"""

import pytest
import time
from datetime import datetime, timedelta
from typing import List
import sys
from pathlib import Path

# Add backend to path
backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.database import DolphinDBClient
from infrastructure.monitoring import performance_monitor, get_metrics_collector
from tests.benchmark import BENCHMARK_CONFIGS


class TestDolphinDBClientBenchmark:
    """Benchmark tests for DolphinDB client."""

    @pytest.fixture(scope="class")
    def db_client(self):
        """Get DolphinDB client instance."""
        client = DolphinDBClient()
        yield client
        # Cleanup
        client.close()

    @pytest.fixture(scope="function")
    def clear_metrics(self):
        """Clear metrics before each test."""
        collector = get_metrics_collector()
        collector.clear_metrics()
        yield

    def _generate_stock_codes(self, count: int) -> List[str]:
        """Generate stock codes for testing."""
        # Generate realistic stock codes
        codes = []
        for i in range(count):
            if i < count // 2:
                codes.append(f"{600000 + i:06d}.SH")  # Shanghai
            else:
                codes.append(f"{000001 + i:06d}.SZ")  # Shenzhen
        return codes

    @pytest.mark.benchmark
    def test_simple_query_performance(self, db_client, clear_metrics):
        """Test simple query performance."""

        @performance_monitor()
        def run_simple_query():
            script = """
            select top 1000 * from sync_daily_data
            order by trade_date desc
            """
            return db_client.run_script(script)

        result = run_simple_query()
        assert result is not None

        # Check metrics
        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nSimple Query Performance:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    @pytest.mark.parametrize("config_name", ["small", "medium", "large"])
    def test_query_by_stock_count(self, db_client, clear_metrics, config_name):
        """Test query performance with different stock counts."""
        config = BENCHMARK_CONFIGS[config_name]
        stock_codes = self._generate_stock_codes(config["stocks"])
        end_date = datetime.now()
        start_date = end_date - timedelta(days=config["days"])

        @performance_monitor()
        def run_query():
            script = f"""
            select * from sync_daily_data
            where ts_code in {stock_codes}
            and trade_date between {start_date.strftime('%Y.%m.%d')} : {end_date.strftime('%Y.%m.%d')}
            """
            return db_client.run_script(script)

        result = run_query()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\n{config['description']}:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")
        print(f"  Rows: {len(result) if result is not None else 0}")

    @pytest.mark.benchmark
    def test_aggregation_performance(self, db_client, clear_metrics):
        """Test aggregation query performance."""

        @performance_monitor()
        def run_aggregation():
            script = """
            select ts_code,
                   avg(close) as avg_close,
                   std(close) as std_close,
                   max(high) as max_high,
                   min(low) as min_low,
                   sum(volume) as total_volume
            from sync_daily_data
            where trade_date >= 2024.01.01
            group by ts_code
            """
            return db_client.run_script(script)

        result = run_aggregation()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nAggregation Performance:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_join_performance(self, db_client, clear_metrics):
        """Test join query performance."""

        @performance_monitor()
        def run_join():
            script = """
            select d.ts_code, d.trade_date, d.close, a.adj_factor
            from sync_daily_data d
            left join sync_adj_factor a
            on d.ts_code = a.ts_code and d.trade_date = a.trade_date
            where d.trade_date >= 2024.01.01
            limit 10000
            """
            return db_client.run_script(script)

        result = run_join()
        assert result is not None

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nJoin Performance:")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_connection_pool_performance(self, db_client, clear_metrics):
        """Test connection pool efficiency."""
        iterations = 10

        @performance_monitor()
        def run_multiple_queries():
            for i in range(iterations):
                script = f"""
                select top 100 * from sync_daily_data
                where ts_code = '600000.SH'
                limit {i * 100}, 100
                """
                db_client.run_script(script)

        run_multiple_queries()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nConnection Pool Performance ({iterations} queries):")
        print(f"  Total Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Avg per Query: {stats['duration']['mean'] / iterations:.3f}s")

    @pytest.mark.benchmark
    def test_batch_insert_performance(self, db_client, clear_metrics):
        """Test batch insert performance."""
        # Note: This is a read-only test, actual insert would require write permissions

        @performance_monitor()
        def simulate_batch_insert():
            # Simulate preparing data for batch insert
            stock_codes = self._generate_stock_codes(100)
            dates = [(datetime.now() - timedelta(days=i)).strftime('%Y.%m.%d')
                    for i in range(30)]

            # Simulate data preparation time
            data_size = len(stock_codes) * len(dates)
            return data_size

        data_size = simulate_batch_insert()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nBatch Insert Simulation ({data_size} rows):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.benchmark
    def test_concurrent_queries(self, db_client, clear_metrics):
        """Test concurrent query handling."""
        import concurrent.futures

        @performance_monitor()
        def run_concurrent_queries():
            def single_query(query_id):
                script = f"""
                select * from sync_daily_data
                where ts_code = '60000{query_id}.SH'
                limit 1000
                """
                return db_client.run_script(script)

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(single_query, i) for i in range(10)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
                return len(results)

        count = run_concurrent_queries()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nConcurrent Queries (10 queries, 5 workers):")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Completed: {count}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "benchmark", "-s"])
