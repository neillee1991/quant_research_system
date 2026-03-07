"""
Performance dashboard for real-time monitoring and visualization.

Displays performance metrics, trends, and comparisons.
"""

import sys
import argparse
from datetime import timedelta
from pathlib import Path
from typing import Optional
import json

# Add backend to path
backend_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_path))

from infrastructure.monitoring import get_metrics_collector


def print_separator(char="=", length=80):
    """Print a separator line."""
    print(char * length)


def print_section_header(title: str):
    """Print a section header."""
    print_separator()
    print(f"  {title}")
    print_separator()


def format_number(value: float, decimals: int = 2) -> str:
    """Format number with specified decimals."""
    return f"{value:.{decimals}f}"


def display_statistics(stats: dict, indent: str = ""):
    """Display statistics in a formatted way."""
    if "error" in stats:
        print(f"{indent}Error: {stats['error']}")
        return

    print(f"{indent}Count: {stats['count']}")

    if "time_range" in stats:
        print(f"{indent}Time Range: {stats['time_range']['start']} to {stats['time_range']['end']}")

    if "duration" in stats:
        dur = stats["duration"]
        print(f"{indent}Duration (seconds):")
        print(f"{indent}  Min: {format_number(dur['min'], 3)}")
        print(f"{indent}  Mean: {format_number(dur['mean'], 3)}")
        print(f"{indent}  Median: {format_number(dur['median'], 3)}")
        print(f"{indent}  P95: {format_number(dur['p95'], 3)}")
        print(f"{indent}  P99: {format_number(dur['p99'], 3)}")
        print(f"{indent}  Max: {format_number(dur['max'], 3)}")

    if "memory_delta_mb" in stats:
        mem = stats["memory_delta_mb"]
        print(f"{indent}Memory Delta (MB):")
        print(f"{indent}  Mean: {format_number(mem['mean'], 2)}")
        print(f"{indent}  P95: {format_number(mem['p95'], 2)}")
        print(f"{indent}  Max: {format_number(mem['max'], 2)}")

    if "memory_peak_mb" in stats:
        peak = stats["memory_peak_mb"]
        print(f"{indent}Memory Peak (MB):")
        print(f"{indent}  Mean: {format_number(peak['mean'], 2)}")
        print(f"{indent}  P95: {format_number(peak['p95'], 2)}")
        print(f"{indent}  Max: {format_number(peak['max'], 2)}")

    if "cpu_percent" in stats:
        cpu = stats["cpu_percent"]
        print(f"{indent}CPU Usage (%):")
        print(f"{indent}  Mean: {format_number(cpu['mean'], 1)}")
        print(f"{indent}  P95: {format_number(cpu['p95'], 1)}")
        print(f"{indent}  Max: {format_number(cpu['max'], 1)}")

    if "db_queries" in stats:
        db = stats["db_queries"]
        print(f"{indent}DB Queries:")
        print(f"{indent}  Mean: {format_number(db['mean'], 1)}")
        print(f"{indent}  P95: {format_number(db['p95'], 1)}")
        print(f"{indent}  Max: {format_number(db['max'], 1)}")

    if "data_rows" in stats:
        rows = stats["data_rows"]
        print(f"{indent}Data Rows:")
        print(f"{indent}  Mean: {format_number(rows['mean'], 0)}")
        print(f"{indent}  P95: {format_number(rows['p95'], 0)}")
        print(f"{indent}  Max: {format_number(rows['max'], 0)}")

    if "throughput_rows_per_sec" in stats:
        print(f"{indent}Throughput: {format_number(stats['throughput_rows_per_sec'], 0)} rows/sec")

    if "qps" in stats:
        print(f"{indent}QPS: {format_number(stats['qps'], 2)}")

    if "error_rate" in stats:
        print(f"{indent}Error Rate: {format_number(stats['error_rate'] * 100, 2)}% ({stats['error_count']} errors)")


def show_overall_statistics(time_window: Optional[timedelta] = None):
    """Show overall performance statistics."""
    print_section_header("Overall Performance Statistics")

    collector = get_metrics_collector()
    stats = collector.get_statistics(time_window=time_window)

    display_statistics(stats)
    print()


def show_function_summary():
    """Show summary for each function."""
    print_section_header("Performance by Function")

    collector = get_metrics_collector()
    summary = collector.get_function_summary()

    if not summary:
        print("No metrics available")
        return

    for function_name, stats in summary.items():
        print(f"\n{function_name}:")
        display_statistics(stats, indent="  ")

    print()


def show_resource_usage():
    """Show resource usage summary."""
    print_section_header("Resource Usage Summary")

    collector = get_metrics_collector()
    usage = collector.get_resource_usage_summary()

    if "error" in usage:
        print(f"Error: {usage['error']}")
        return

    print(f"Total Executions: {usage['total_executions']}")
    print(f"Total Duration: {format_number(usage['total_duration_seconds'], 2)} seconds")
    print(f"Total Memory Delta: {format_number(usage['total_memory_delta_mb'], 2)} MB")
    print(f"Max Memory Peak: {format_number(usage['max_memory_peak_mb'], 2)} MB")
    print(f"Total DB Queries: {usage['total_db_queries']}")
    print(f"Total Data Rows: {usage['total_data_rows']}")
    print(f"Average CPU: {format_number(usage['avg_cpu_percent'], 1)}%")
    print()


def show_recent_errors(limit: int = 10):
    """Show recent errors."""
    print_section_header(f"Recent Errors (Last {limit})")

    collector = get_metrics_collector()
    errors = collector.get_recent_errors(limit=limit)

    if not errors:
        print("No errors recorded")
        return

    for i, error in enumerate(errors, 1):
        print(f"\n{i}. {error['function_name']}")
        print(f"   Time: {error['timestamp']}")
        print(f"   Duration: {format_number(error['duration'], 3)}s")
        print(f"   Error: {error['error']}")

    print()


def show_comparison(function_names: list):
    """Show comparison between functions."""
    print_section_header("Function Comparison")

    collector = get_metrics_collector()

    comparison_data = []
    for func_name in function_names:
        stats = collector.get_statistics(function_name=func_name)
        if "error" not in stats:
            comparison_data.append((func_name, stats))

    if not comparison_data:
        print("No data available for comparison")
        return

    # Print comparison table
    print(f"{'Function':<50} {'Count':>8} {'Mean(s)':>10} {'P95(s)':>10} {'Mem(MB)':>10}")
    print("-" * 90)

    for func_name, stats in comparison_data:
        short_name = func_name.split(".")[-1][:48]
        count = stats['count']
        mean_dur = stats['duration']['mean']
        p95_dur = stats['duration']['p95']
        mean_mem = stats['memory_delta_mb']['mean']

        print(f"{short_name:<50} {count:>8} {mean_dur:>10.3f} {p95_dur:>10.3f} {mean_mem:>10.2f}")

    print()


def export_dashboard(output_file: str):
    """Export dashboard data to file."""
    print_section_header("Exporting Metrics")

    collector = get_metrics_collector()
    collector.export_metrics(output_file)

    print(f"Metrics exported to: {output_file}")
    print()


def main():
    """Main dashboard function."""
    parser = argparse.ArgumentParser(description="Performance Dashboard")
    parser.add_argument(
        "--time-window",
        type=int,
        help="Time window in minutes (default: all time)",
    )
    parser.add_argument(
        "--function",
        type=str,
        help="Show statistics for specific function",
    )
    parser.add_argument(
        "--compare",
        nargs="+",
        help="Compare multiple functions",
    )
    parser.add_argument(
        "--errors",
        type=int,
        default=10,
        help="Number of recent errors to show (default: 10)",
    )
    parser.add_argument(
        "--export",
        type=str,
        help="Export metrics to JSON file",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show summary only",
    )

    args = parser.parse_args()

    time_window = timedelta(minutes=args.time_window) if args.time_window else None

    print("\n")
    print_separator("=")
    print("  QUANTSYSTEM PERFORMANCE DASHBOARD")
    print_separator("=")
    print()

    if args.export:
        export_dashboard(args.export)
        return

    if args.compare:
        show_comparison(args.compare)
        return

    if args.function:
        print_section_header(f"Statistics for {args.function}")
        collector = get_metrics_collector()
        stats = collector.get_statistics(function_name=args.function, time_window=time_window)
        display_statistics(stats)
        print()
        return

    # Default: show all sections
    show_overall_statistics(time_window=time_window)

    if not args.summary:
        show_function_summary()
        show_resource_usage()
        show_recent_errors(limit=args.errors)


if __name__ == "__main__":
    main()
