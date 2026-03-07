"""
Performance profiler for detailed analysis of bottlenecks.

Usage:
    python profile_code.py --function compute_factor --iterations 10
    python profile_code.py --module engine.production.engine --output profile.txt
"""

import sys
import argparse
import cProfile
import pstats
import io
from pathlib import Path
from typing import Callable, Any

backend_path = Path(__file__).parent.parent
sys.path.insert(0, str(backend_path))


def profile_function(func: Callable, *args, **kwargs) -> Any:
    """
    Profile a function and return results.

    Args:
        func: Function to profile
        *args: Function arguments
        **kwargs: Function keyword arguments

    Returns:
        Function result and profiling stats
    """
    profiler = cProfile.Profile()
    profiler.enable()

    try:
        result = func(*args, **kwargs)
    finally:
        profiler.disable()

    return result, profiler


def print_profile_stats(profiler: cProfile.Profile, sort_by: str = "cumulative", limit: int = 30):
    """
    Print profiling statistics.

    Args:
        profiler: cProfile.Profile instance
        sort_by: Sort key (cumulative, time, calls, etc.)
        limit: Number of lines to show
    """
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.strip_dirs()
    stats.sort_stats(sort_by)
    stats.print_stats(limit)

    print(s.getvalue())


def profile_module_function(module_path: str, function_name: str, iterations: int = 1):
    """
    Profile a function from a module.

    Args:
        module_path: Module path (e.g., 'engine.production.engine')
        function_name: Function name to profile
        iterations: Number of times to run the function
    """
    # Import module
    parts = module_path.split(".")
    module = __import__(module_path, fromlist=[parts[-1]])

    # Get function
    if not hasattr(module, function_name):
        print(f"Error: Function '{function_name}' not found in module '{module_path}'")
        return

    func = getattr(module, function_name)

    print(f"Profiling: {module_path}.{function_name}")
    print(f"Iterations: {iterations}")
    print("=" * 80)

    # Profile function
    profiler = cProfile.Profile()
    profiler.enable()

    for i in range(iterations):
        try:
            # Note: This is a simplified version
            # In practice, you'd need to provide proper arguments
            func()
        except Exception as e:
            print(f"Error in iteration {i}: {e}")

    profiler.disable()

    # Print stats
    print("\nTop functions by cumulative time:")
    print_profile_stats(profiler, sort_by="cumulative", limit=30)

    print("\nTop functions by internal time:")
    print_profile_stats(profiler, sort_by="time", limit=20)

    print("\nMost called functions:")
    print_profile_stats(profiler, sort_by="calls", limit=20)


def profile_code_snippet(code: str, globals_dict: dict = None, locals_dict: dict = None):
    """
    Profile a code snippet.

    Args:
        code: Code string to profile
        globals_dict: Global variables
        locals_dict: Local variables
    """
    if globals_dict is None:
        globals_dict = {}

    if locals_dict is None:
        locals_dict = {}

    print("Profiling code snippet:")
    print("-" * 80)
    print(code)
    print("-" * 80)

    profiler = cProfile.Profile()
    profiler.enable()

    try:
        exec(code, globals_dict, locals_dict)
    finally:
        profiler.disable()

    print("\nProfile results:")
    print_profile_stats(profiler, sort_by="cumulative", limit=30)


def compare_implementations(impl1: Callable, impl2: Callable, *args, **kwargs):
    """
    Compare performance of two implementations.

    Args:
        impl1: First implementation
        impl2: Second implementation
        *args: Arguments to pass to both functions
        **kwargs: Keyword arguments to pass to both functions
    """
    print("Comparing implementations:")
    print("=" * 80)

    # Profile first implementation
    print("\nImplementation 1:")
    result1, profiler1 = profile_function(impl1, *args, **kwargs)
    stats1 = pstats.Stats(profiler1)
    total_time1 = sum(stat[2] for stat in stats1.stats.values())

    print(f"Total time: {total_time1:.4f}s")
    print_profile_stats(profiler1, sort_by="cumulative", limit=10)

    # Profile second implementation
    print("\nImplementation 2:")
    result2, profiler2 = profile_function(impl2, *args, **kwargs)
    stats2 = pstats.Stats(profiler2)
    total_time2 = sum(stat[2] for stat in stats2.stats.values())

    print(f"Total time: {total_time2:.4f}s")
    print_profile_stats(profiler2, sort_by="cumulative", limit=10)

    # Comparison
    print("\n" + "=" * 80)
    print("COMPARISON:")
    print(f"Implementation 1: {total_time1:.4f}s")
    print(f"Implementation 2: {total_time2:.4f}s")

    if total_time1 < total_time2:
        speedup = total_time2 / total_time1
        print(f"Implementation 1 is {speedup:.2f}x faster")
    else:
        speedup = total_time1 / total_time2
        print(f"Implementation 2 is {speedup:.2f}x faster")


def analyze_hotspots(profiler: cProfile.Profile, threshold: float = 0.01):
    """
    Analyze performance hotspots.

    Args:
        profiler: cProfile.Profile instance
        threshold: Minimum percentage of total time to be considered a hotspot
    """
    stats = pstats.Stats(profiler)
    total_time = sum(stat[2] for stat in stats.stats.values())

    print(f"\nPerformance Hotspots (>{threshold * 100}% of total time):")
    print("=" * 80)

    hotspots = []
    for func, (cc, nc, tt, ct, callers) in stats.stats.items():
        if ct / total_time > threshold:
            hotspots.append({
                "function": func,
                "cumulative_time": ct,
                "percentage": ct / total_time * 100,
                "calls": nc
            })

    hotspots.sort(key=lambda x: x["cumulative_time"], reverse=True)

    for i, hotspot in enumerate(hotspots[:20], 1):
        func = hotspot["function"]
        print(f"\n{i}. {func[0]}:{func[1]} {func[2]}")
        print(f"   Cumulative time: {hotspot['cumulative_time']:.4f}s ({hotspot['percentage']:.1f}%)")
        print(f"   Calls: {hotspot['calls']}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Profile Python code")
    parser.add_argument(
        "--module",
        type=str,
        help="Module path (e.g., engine.production.engine)"
    )
    parser.add_argument(
        "--function",
        type=str,
        help="Function name to profile"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of iterations (default: 1)"
    )
    parser.add_argument(
        "--sort",
        choices=["cumulative", "time", "calls"],
        default="cumulative",
        help="Sort key for results (default: cumulative)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Number of lines to show (default: 30)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for profile results"
    )

    args = parser.parse_args()

    if args.module and args.function:
        profile_module_function(args.module, args.function, args.iterations)
    else:
        print("Error: Please provide --module and --function")
        parser.print_help()


if __name__ == "__main__":
    main()
