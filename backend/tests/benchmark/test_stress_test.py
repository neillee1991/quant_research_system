"""
Stress test suite for performance and stability testing.

Tests system behavior under extreme conditions.
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
import sys
from pathlib import Path

backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.database import DolphinDBClient
from infrastructure.repository import StockDailyRepository
from infrastructure.monitoring import performance_monitor, get_metrics_collector


class TestStressTest:
    """Stress tests for system stability."""

    @pytest.fixture(scope="class")
    def db_client(self):
        """Get DB client."""
        client = DolphinDBClient()
        yield client
        client.close()

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

    @pytest.mark.stress
    def test_concurrent_queries_stress(self, stock_repo, clear_metrics):
        """Test system under concurrent query load."""
        import concurrent.futures

        num_workers = 50
        queries_per_worker = 20
        stock_codes = self._generate_stock_codes(10)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        @performance_monitor()
        def stress_test():
            def worker_task(worker_id):
                results = []
                for i in range(queries_per_worker):
                    try:
                        result = stock_repo.find_by_date_range(
                            stock_codes=stock_codes,
                            start_date=start_date,
                            end_date=end_date
                        )
                        results.append(result)
                    except Exception as e:
                        print(f"Worker {worker_id} query {i} failed: {e}")
                return len(results)

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(worker_task, i) for i in range(num_workers)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
                return sum(results)

        total_queries = stress_test()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nConcurrent Stress Test:")
        print(f"  Workers: {num_workers}")
        print(f"  Queries per Worker: {queries_per_worker}")
        print(f"  Total Successful: {total_queries}")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  QPS: {total_queries / stats['duration']['mean']:.2f}")

    @pytest.mark.stress
    def test_memory_stress(self, stock_repo, clear_metrics):
        """Test memory usage under large data loads."""
        stock_codes = self._generate_stock_codes(1000)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        @performance_monitor()
        def memory_stress():
            # Load large dataset multiple times
            datasets = []
            for i in range(5):
                data = stock_repo.find_by_date_range(
                    stock_codes=stock_codes,
                    start_date=start_date,
                    end_date=end_date
                )
                if data is not None:
                    datasets.append(data)

            return len(datasets)

        count = memory_stress()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nMemory Stress Test:")
        print(f"  Datasets Loaded: {count}")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  Memory Peak: {stats['memory_peak_mb']['mean']:.2f}MB")
        print(f"  Memory Delta: {stats['memory_delta_mb']['mean']:.2f}MB")

    @pytest.mark.stress
    def test_long_running_stability(self, stock_repo, clear_metrics):
        """Test system stability over extended period."""
        duration_minutes = 5  # Run for 5 minutes
        stock_codes = self._generate_stock_codes(50)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        @performance_monitor()
        def long_running_test():
            start_time = time.time()
            end_time = start_time + (duration_minutes * 60)
            iteration = 0
            errors = 0

            while time.time() < end_time:
                try:
                    result = stock_repo.find_by_date_range(
                        stock_codes=stock_codes,
                        start_date=start_date,
                        end_date=end_date
                    )
                    iteration += 1

                    # Sleep briefly to avoid overwhelming the system
                    time.sleep(0.1)
                except Exception as e:
                    errors += 1
                    print(f"Error in iteration {iteration}: {e}")

            return {
                "iterations": iteration,
                "errors": errors,
                "duration": time.time() - start_time
            }

        result = long_running_test()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nLong Running Stability Test ({duration_minutes} minutes):")
        print(f"  Iterations: {result['iterations']}")
        print(f"  Errors: {result['errors']}")
        print(f"  Error Rate: {result['errors'] / result['iterations'] * 100:.2f}%")
        print(f"  Avg Duration per Query: {result['duration'] / result['iterations']:.3f}s")

    @pytest.mark.stress
    def test_connection_pool_exhaustion(self, db_client, clear_metrics):
        """Test behavior when connection pool is exhausted."""
        import concurrent.futures

        num_connections = 100  # Try to create more connections than pool size

        @performance_monitor()
        def exhaust_pool():
            def query_task(task_id):
                try:
                    script = f"""
                    select top 100 * from sync_daily_data
                    where ts_code = '600000.SH'
                    """
                    result = db_client.run_script(script)
                    return 1 if result is not None else 0
                except Exception as e:
                    print(f"Task {task_id} failed: {e}")
                    return 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_connections) as executor:
                futures = [executor.submit(query_task, i) for i in range(num_connections)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
                return sum(results)

        successful = exhaust_pool()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nConnection Pool Exhaustion Test:")
        print(f"  Attempted Connections: {num_connections}")
        print(f"  Successful: {successful}")
        print(f"  Success Rate: {successful / num_connections * 100:.2f}%")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")

    @pytest.mark.stress
    def test_rapid_fire_queries(self, stock_repo, clear_metrics):
        """Test system with rapid consecutive queries."""
        num_queries = 1000
        stock_codes = self._generate_stock_codes(10)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        @performance_monitor()
        def rapid_fire():
            successful = 0
            failed = 0

            for i in range(num_queries):
                try:
                    result = stock_repo.find_by_date_range(
                        stock_codes=stock_codes,
                        start_date=start_date,
                        end_date=end_date
                    )
                    if result is not None:
                        successful += 1
                except Exception as e:
                    failed += 1
                    if failed <= 5:  # Only print first 5 errors
                        print(f"Query {i} failed: {e}")

            return {"successful": successful, "failed": failed}

        result = rapid_fire()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nRapid Fire Test ({num_queries} queries):")
        print(f"  Successful: {result['successful']}")
        print(f"  Failed: {result['failed']}")
        print(f"  Success Rate: {result['successful'] / num_queries * 100:.2f}%")
        print(f"  Duration: {stats['duration']['mean']:.3f}s")
        print(f"  QPS: {num_queries / stats['duration']['mean']:.2f}")

    @pytest.mark.stress
    def test_memory_leak_detection(self, stock_repo, clear_metrics):
        """Test for memory leaks over repeated operations."""
        iterations = 100
        stock_codes = self._generate_stock_codes(50)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        @performance_monitor()
        def leak_detection():
            import psutil
            process = psutil.Process()

            memory_samples = []

            for i in range(iterations):
                # Perform operation
                result = stock_repo.find_by_date_range(
                    stock_codes=stock_codes,
                    start_date=start_date,
                    end_date=end_date
                )

                # Sample memory every 10 iterations
                if i % 10 == 0:
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    memory_samples.append(memory_mb)

            # Check if memory is growing linearly
            if len(memory_samples) > 2:
                growth = memory_samples[-1] - memory_samples[0]
                avg_growth_per_10 = growth / (len(memory_samples) - 1)
                return {
                    "initial_memory": memory_samples[0],
                    "final_memory": memory_samples[-1],
                    "total_growth": growth,
                    "avg_growth_per_10": avg_growth_per_10,
                    "samples": memory_samples
                }

            return None

        result = leak_detection()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nMemory Leak Detection ({iterations} iterations):")
        if result:
            print(f"  Initial Memory: {result['initial_memory']:.2f}MB")
            print(f"  Final Memory: {result['final_memory']:.2f}MB")
            print(f"  Total Growth: {result['total_growth']:.2f}MB")
            print(f"  Avg Growth per 10 ops: {result['avg_growth_per_10']:.2f}MB")
            print(f"  Potential Leak: {'YES' if result['avg_growth_per_10'] > 5 else 'NO'}")

    @pytest.mark.stress
    def test_error_recovery(self, stock_repo, clear_metrics):
        """Test system recovery from errors."""
        stock_codes = self._generate_stock_codes(50)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        @performance_monitor()
        def error_recovery():
            results = []

            for i in range(50):
                try:
                    # Intentionally cause some errors
                    if i % 10 == 0:
                        # Invalid date range
                        result = stock_repo.find_by_date_range(
                            stock_codes=stock_codes,
                            start_date=end_date,
                            end_date=start_date  # Reversed dates
                        )
                    else:
                        # Normal query
                        result = stock_repo.find_by_date_range(
                            stock_codes=stock_codes,
                            start_date=start_date,
                            end_date=end_date
                        )

                    results.append({"iteration": i, "success": True})
                except Exception as e:
                    results.append({"iteration": i, "success": False, "error": str(e)})

            successful = sum(1 for r in results if r["success"])
            return {
                "total": len(results),
                "successful": successful,
                "failed": len(results) - successful
            }

        result = error_recovery()

        collector = get_metrics_collector()
        stats = collector.get_statistics()
        print(f"\nError Recovery Test:")
        print(f"  Total Operations: {result['total']}")
        print(f"  Successful: {result['successful']}")
        print(f"  Failed: {result['failed']}")
        print(f"  Recovery Rate: {result['successful'] / result['total'] * 100:.2f}%")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "stress", "-s"])
