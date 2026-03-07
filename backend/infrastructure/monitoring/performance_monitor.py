"""
Performance monitoring decorators and utilities.

Tracks execution time, memory usage, and other performance metrics.
"""

import time
import functools
import psutil
import tracemalloc
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""

    function_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    memory_start_mb: float = 0.0
    memory_end_mb: float = 0.0
    memory_peak_mb: float = 0.0
    memory_delta_mb: float = 0.0
    cpu_percent: float = 0.0
    db_queries: int = 0
    data_rows: int = 0
    data_size_mb: float = 0.0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "function_name": self.function_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "memory_start_mb": self.memory_start_mb,
            "memory_end_mb": self.memory_end_mb,
            "memory_peak_mb": self.memory_peak_mb,
            "memory_delta_mb": self.memory_delta_mb,
            "cpu_percent": self.cpu_percent,
            "db_queries": self.db_queries,
            "data_rows": self.data_rows,
            "data_size_mb": self.data_size_mb,
            "error": self.error,
            "metadata": self.metadata,
        }


class PerformanceContext:
    """Context manager for performance monitoring."""

    def __init__(self, function_name: str, track_memory: bool = True):
        self.function_name = function_name
        self.track_memory = track_memory
        self.metrics = PerformanceMetrics(
            function_name=function_name,
            start_time=datetime.now()
        )
        self.process = psutil.Process()

    def __enter__(self) -> PerformanceMetrics:
        """Start monitoring."""
        # Start memory tracking
        if self.track_memory:
            tracemalloc.start()

        # Record initial state
        self.metrics.memory_start_mb = self.process.memory_info().rss / 1024 / 1024
        self.start_cpu_times = self.process.cpu_times()
        self.start_time = time.perf_counter()

        return self.metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop monitoring and calculate metrics."""
        # Calculate duration
        end_time = time.perf_counter()
        self.metrics.duration_seconds = end_time - self.start_time
        self.metrics.end_time = datetime.now()

        # Calculate memory usage
        self.metrics.memory_end_mb = self.process.memory_info().rss / 1024 / 1024
        self.metrics.memory_delta_mb = self.metrics.memory_end_mb - self.metrics.memory_start_mb

        if self.track_memory:
            current, peak = tracemalloc.get_traced_memory()
            self.metrics.memory_peak_mb = peak / 1024 / 1024
            tracemalloc.stop()

        # Calculate CPU usage
        end_cpu_times = self.process.cpu_times()
        cpu_time = (end_cpu_times.user - self.start_cpu_times.user +
                   end_cpu_times.system - self.start_cpu_times.system)
        self.metrics.cpu_percent = (cpu_time / self.metrics.duration_seconds * 100) if self.metrics.duration_seconds > 0 else 0

        # Record error if any
        if exc_type is not None:
            self.metrics.error = f"{exc_type.__name__}: {exc_val}"

        # Log metrics
        self._log_metrics()

        # Import here to avoid circular dependency
        from .metrics_collector import get_metrics_collector
        collector = get_metrics_collector()
        collector.record_metrics(self.metrics)

        return False  # Don't suppress exceptions

    def _log_metrics(self):
        """Log performance metrics."""
        log_msg = (
            f"Performance [{self.metrics.function_name}]: "
            f"duration={self.metrics.duration_seconds:.3f}s, "
            f"memory_delta={self.metrics.memory_delta_mb:.2f}MB, "
            f"memory_peak={self.metrics.memory_peak_mb:.2f}MB, "
            f"cpu={self.metrics.cpu_percent:.1f}%"
        )

        if self.metrics.db_queries > 0:
            log_msg += f", db_queries={self.metrics.db_queries}"

        if self.metrics.data_rows > 0:
            log_msg += f", data_rows={self.metrics.data_rows}"

        if self.metrics.error:
            log_msg += f", error={self.metrics.error}"
            logger.error(log_msg)
        else:
            logger.info(log_msg)


def performance_monitor(
    track_memory: bool = True,
    log_args: bool = False,
) -> Callable:
    """
    Decorator to monitor function performance.

    Args:
        track_memory: Whether to track detailed memory usage
        log_args: Whether to log function arguments

    Example:
        @performance_monitor
        def compute_factor(data):
            return data * 2
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            function_name = f"{func.__module__}.{func.__qualname__}"

            with PerformanceContext(function_name, track_memory) as metrics:
                # Log arguments if requested
                if log_args:
                    metrics.metadata["args"] = str(args)[:100]  # Truncate long args
                    metrics.metadata["kwargs"] = str(kwargs)[:100]

                # Execute function
                result = func(*args, **kwargs)

                # Try to extract data metrics from result
                if hasattr(result, "__len__"):
                    try:
                        metrics.data_rows = len(result)
                    except:
                        pass

                if hasattr(result, "memory_usage"):
                    try:
                        metrics.data_size_mb = result.memory_usage(deep=True).sum() / 1024 / 1024
                    except:
                        pass

                return result

        return wrapper
    return decorator


def async_performance_monitor(
    track_memory: bool = True,
    log_args: bool = False,
) -> Callable:
    """
    Async version of performance_monitor decorator.

    Example:
        @async_performance_monitor
        async def fetch_data():
            return await db.query()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            function_name = f"{func.__module__}.{func.__qualname__}"

            with PerformanceContext(function_name, track_memory) as metrics:
                if log_args:
                    metrics.metadata["args"] = str(args)[:100]
                    metrics.metadata["kwargs"] = str(kwargs)[:100]

                result = await func(*args, **kwargs)

                if hasattr(result, "__len__"):
                    try:
                        metrics.data_rows = len(result)
                    except:
                        pass

                if hasattr(result, "memory_usage"):
                    try:
                        metrics.data_size_mb = result.memory_usage(deep=True).sum() / 1024 / 1024
                    except:
                        pass

                return result

        return wrapper
    return decorator


class QueryCounter:
    """Context manager to count database queries."""

    def __init__(self, metrics: PerformanceMetrics):
        self.metrics = metrics
        self.query_count = 0

    def __enter__(self):
        self.query_count = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.metrics.db_queries = self.query_count
        return False

    def increment(self):
        """Increment query counter."""
        self.query_count += 1
