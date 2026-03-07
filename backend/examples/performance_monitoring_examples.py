"""
Example: Using the performance monitoring system.

This file demonstrates how to use the performance monitoring and benchmarking tools.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add backend to path
backend_path = Path(__file__).parent
sys.path.insert(0, str(backend_path))

from infrastructure.monitoring import (
    performance_monitor,
    async_performance_monitor,
    get_metrics_collector,
    PerformanceContext,
)


# Example 1: Basic function monitoring
@performance_monitor()
def compute_simple_factor(data):
    """Simple factor computation with monitoring."""
    # Simulate computation
    result = [x * 2 for x in data]
    return result


# Example 2: Monitoring with custom metadata
@performance_monitor(log_args=True)
def compute_complex_factor(data, window=20):
    """Complex factor with argument logging."""
    # Simulate rolling window computation
    result = []
    for i in range(len(data)):
        if i >= window:
            window_data = data[i-window:i]
            result.append(sum(window_data) / window)
        else:
            result.append(None)
    return result


# Example 3: Using context manager for fine-grained control
def process_data_with_context(data):
    """Process data with manual performance tracking."""
    with PerformanceContext("data_processing") as metrics:
        # Step 1: Load
        loaded_data = load_data(data)
        metrics.metadata["load_step"] = "completed"

        # Step 2: Transform
        transformed = transform_data(loaded_data)
        metrics.metadata["transform_step"] = "completed"

        # Step 3: Compute
        result = compute_result(transformed)
        metrics.data_rows = len(result)

        return result


def load_data(data):
    """Simulate data loading."""
    return data


def transform_data(data):
    """Simulate data transformation."""
    return [x * 1.5 for x in data]


def compute_result(data):
    """Simulate computation."""
    return [x + 10 for x in data]


# Example 4: Async function monitoring
@async_performance_monitor()
async def fetch_data_async(stock_code):
    """Async data fetching with monitoring."""
    import asyncio
    await asyncio.sleep(0.1)  # Simulate network delay
    return [1, 2, 3, 4, 5]


# Example 5: Viewing metrics
def view_metrics_example():
    """Example of viewing collected metrics."""
    collector = get_metrics_collector()

    # Get overall statistics
    stats = collector.get_statistics()
    print("Overall Statistics:")
    print(f"  Total executions: {stats['count']}")
    print(f"  Average duration: {stats['duration']['mean']:.3f}s")
    print(f"  P95 duration: {stats['duration']['p95']:.3f}s")
    print(f"  Average memory: {stats['memory_delta_mb']['mean']:.2f}MB")

    # Get function-specific statistics
    func_stats = collector.get_statistics(function_name="compute_simple_factor")
    print(f"\nFunction-specific stats:")
    print(f"  Count: {func_stats['count']}")
    print(f"  Mean duration: {func_stats['duration']['mean']:.3f}s")

    # Get recent errors
    errors = collector.get_recent_errors(limit=5)
    print(f"\nRecent errors: {len(errors)}")
    for error in errors:
        print(f"  - {error['function_name']}: {error['error']}")

    # Export metrics
    collector.export_metrics("metrics_export.json")
    print("\nMetrics exported to metrics_export.json")


# Example 6: Running benchmarks programmatically
def run_benchmark_example():
    """Example of running benchmarks programmatically."""
    import subprocess

    # Run quick benchmark
    result = subprocess.run(
        ["python", "scripts/run_benchmarks.py", "--suite", "quick"],
        capture_output=True,
        text=True
    )

    print("Benchmark output:")
    print(result.stdout)


# Example 7: Using the dashboard
def view_dashboard_example():
    """Example of using the performance dashboard."""
    import subprocess

    # View overall statistics
    subprocess.run(["python", "scripts/performance_dashboard.py"])

    # View specific function
    subprocess.run([
        "python", "scripts/performance_dashboard.py",
        "--function", "compute_simple_factor"
    ])

    # Compare functions
    subprocess.run([
        "python", "scripts/performance_dashboard.py",
        "--compare", "func1", "func2"
    ])


# Example 8: Profiling code
def profile_example():
    """Example of profiling code for bottleneck analysis."""
    import cProfile
    import pstats
    import io

    profiler = cProfile.Profile()
    profiler.enable()

    # Run code to profile
    data = list(range(10000))
    compute_complex_factor(data, window=20)

    profiler.disable()

    # Print stats
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.strip_dirs()
    stats.sort_stats('cumulative')
    stats.print_stats(10)

    print("Profiling results:")
    print(s.getvalue())


# Example 9: Batch processing with monitoring
@performance_monitor()
def batch_process_stocks(stock_codes, batch_size=100):
    """Process stocks in batches with monitoring."""
    results = []

    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]

        with PerformanceContext(f"batch_{i}") as metrics:
            batch_result = process_batch(batch)
            metrics.data_rows = len(batch_result)
            results.extend(batch_result)

    return results


def process_batch(batch):
    """Process a batch of stocks."""
    return [f"processed_{code}" for code in batch]


# Example 10: Comparing implementations
def compare_implementations_example():
    """Example of comparing two implementations."""
    from scripts.profile_code import compare_implementations

    def old_implementation(data):
        """Old implementation using loops."""
        result = []
        for x in data:
            result.append(x * 2)
        return result

    def new_implementation(data):
        """New implementation using list comprehension."""
        return [x * 2 for x in data]

    # Compare
    test_data = list(range(10000))
    compare_implementations(
        old_implementation,
        new_implementation,
        test_data
    )


# Main execution
if __name__ == "__main__":
    print("Performance Monitoring Examples")
    print("=" * 80)

    # Run examples
    print("\n1. Basic monitoring:")
    data = list(range(1000))
    result = compute_simple_factor(data)
    print(f"   Processed {len(result)} items")

    print("\n2. Complex monitoring:")
    result = compute_complex_factor(data, window=20)
    print(f"   Computed {len([x for x in result if x is not None])} values")

    print("\n3. Context manager:")
    result = process_data_with_context(data)
    print(f"   Processed {len(result)} items")

    print("\n4. Batch processing:")
    stocks = [f"60000{i}.SH" for i in range(100)]
    result = batch_process_stocks(stocks, batch_size=20)
    print(f"   Processed {len(result)} stocks")

    print("\n5. View metrics:")
    view_metrics_example()

    print("\n" + "=" * 80)
    print("Examples completed!")
    print("\nNext steps:")
    print("  - Run benchmarks: python scripts/run_benchmarks.py --suite quick")
    print("  - View dashboard: python scripts/performance_dashboard.py")
    print("  - Profile code: python scripts/profile_code.py --help")
