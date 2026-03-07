"""
Script to run comprehensive benchmark suite and generate report.

Usage:
    python run_benchmarks.py --suite all
    python run_benchmarks.py --suite quick
    python run_benchmarks.py --suite stress
    python run_benchmarks.py --output report.json
"""

import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import json

backend_path = Path(__file__).parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.monitoring import get_metrics_collector


BENCHMARK_SUITES = {
    "quick": [
        "tests/benchmark/test_query_builder_benchmark.py",
    ],
    "standard": [
        "tests/benchmark/test_db_client_benchmark.py",
        "tests/benchmark/test_repository_benchmark.py",
        "tests/benchmark/test_query_builder_benchmark.py",
        "tests/benchmark/test_pipeline_benchmark.py",
    ],
    "full": [
        "tests/benchmark/test_db_client_benchmark.py",
        "tests/benchmark/test_repository_benchmark.py",
        "tests/benchmark/test_query_builder_benchmark.py",
        "tests/benchmark/test_pipeline_benchmark.py",
        "tests/benchmark/test_e2e_benchmark.py",
    ],
    "stress": [
        "tests/benchmark/test_stress_test.py",
    ],
    "all": [
        "tests/benchmark/test_db_client_benchmark.py",
        "tests/benchmark/test_repository_benchmark.py",
        "tests/benchmark/test_query_builder_benchmark.py",
        "tests/benchmark/test_pipeline_benchmark.py",
        "tests/benchmark/test_e2e_benchmark.py",
        "tests/benchmark/test_stress_test.py",
    ],
}


def run_benchmark_suite(suite_name: str, output_file: str = None):
    """Run a benchmark suite."""
    if suite_name not in BENCHMARK_SUITES:
        print(f"Error: Unknown suite '{suite_name}'")
        print(f"Available suites: {', '.join(BENCHMARK_SUITES.keys())}")
        return False

    test_files = BENCHMARK_SUITES[suite_name]

    print("=" * 80)
    print(f"Running Benchmark Suite: {suite_name.upper()}")
    print("=" * 80)
    print(f"Test files: {len(test_files)}")
    print(f"Started at: {datetime.now().isoformat()}")
    print()

    # Clear metrics before starting
    collector = get_metrics_collector()
    collector.clear_metrics()

    # Run each test file
    results = []
    for test_file in test_files:
        print(f"\n{'=' * 80}")
        print(f"Running: {test_file}")
        print('=' * 80)

        # Determine marker based on suite
        marker = "stress" if "stress" in test_file else "benchmark"

        cmd = [
            "pytest",
            test_file,
            "-v",
            "-s",
            "-m", marker,
            "--tb=short",
        ]

        try:
            result = subprocess.run(
                cmd,
                cwd=backend_path,
                capture_output=False,
                text=True
            )

            results.append({
                "file": test_file,
                "success": result.returncode == 0,
                "returncode": result.returncode
            })

        except Exception as e:
            print(f"Error running {test_file}: {e}")
            results.append({
                "file": test_file,
                "success": False,
                "error": str(e)
            })

    # Generate summary
    print("\n" + "=" * 80)
    print("BENCHMARK SUITE SUMMARY")
    print("=" * 80)

    successful = sum(1 for r in results if r["success"])
    total = len(results)

    print(f"Total test files: {total}")
    print(f"Successful: {successful}")
    print(f"Failed: {total - successful}")
    print(f"Success rate: {successful / total * 100:.1f}%")

    # Get metrics summary
    print("\n" + "=" * 80)
    print("PERFORMANCE METRICS SUMMARY")
    print("=" * 80)

    summary = collector.get_function_summary()
    if summary:
        print(f"Total functions measured: {len(summary)}")

        # Overall statistics
        overall_stats = collector.get_statistics()
        if "error" not in overall_stats:
            print(f"\nOverall Statistics:")
            print(f"  Total executions: {overall_stats['count']}")
            print(f"  Avg duration: {overall_stats['duration']['mean']:.3f}s")
            print(f"  P95 duration: {overall_stats['duration']['p95']:.3f}s")
            print(f"  Avg memory delta: {overall_stats['memory_delta_mb']['mean']:.2f}MB")
            print(f"  Max memory peak: {overall_stats['memory_peak_mb']['max']:.2f}MB")

        # Resource usage
        resource_usage = collector.get_resource_usage_summary()
        if "error" not in resource_usage:
            print(f"\nResource Usage:")
            print(f"  Total duration: {resource_usage['total_duration_seconds']:.2f}s")
            print(f"  Total memory delta: {resource_usage['total_memory_delta_mb']:.2f}MB")
            print(f"  Max memory peak: {resource_usage['max_memory_peak_mb']:.2f}MB")

    # Export results if requested
    if output_file:
        export_path = backend_path / output_file
        collector.export_metrics(str(export_path))
        print(f"\nMetrics exported to: {export_path}")

    print(f"\nCompleted at: {datetime.now().isoformat()}")
    print("=" * 80)

    return successful == total


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run benchmark suite")
    parser.add_argument(
        "--suite",
        choices=list(BENCHMARK_SUITES.keys()),
        default="standard",
        help="Benchmark suite to run (default: standard)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for metrics (JSON format)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available benchmark suites"
    )

    args = parser.parse_args()

    if args.list:
        print("Available benchmark suites:")
        for suite_name, test_files in BENCHMARK_SUITES.items():
            print(f"\n{suite_name}:")
            for test_file in test_files:
                print(f"  - {test_file}")
        return

    success = run_benchmark_suite(args.suite, args.output)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
