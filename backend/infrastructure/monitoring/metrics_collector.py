"""
Metrics collector for aggregating and analyzing performance data.

Collects metrics from performance monitors and provides statistical analysis.
"""

import threading
from typing import List, Dict, Any, Optional
from collections import defaultdict
from datetime import datetime, timedelta
import statistics
import json
import logging

from .performance_monitor import PerformanceMetrics

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Thread-safe collector for performance metrics.

    Provides statistical analysis including percentiles, throughput, and error rates.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._metrics: List[PerformanceMetrics] = []
            self._metrics_by_function: Dict[str, List[PerformanceMetrics]] = defaultdict(list)
            self._lock = threading.Lock()
            self._initialized = True

    def record_metrics(self, metrics: PerformanceMetrics):
        """Record performance metrics."""
        with self._lock:
            self._metrics.append(metrics)
            self._metrics_by_function[metrics.function_name].append(metrics)

    def get_all_metrics(self) -> List[PerformanceMetrics]:
        """Get all recorded metrics."""
        with self._lock:
            return self._metrics.copy()

    def get_metrics_by_function(self, function_name: str) -> List[PerformanceMetrics]:
        """Get metrics for a specific function."""
        with self._lock:
            return self._metrics_by_function[function_name].copy()

    def get_statistics(
        self,
        function_name: Optional[str] = None,
        time_window: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """
        Get statistical analysis of metrics.

        Args:
            function_name: Filter by function name (None for all)
            time_window: Only include metrics within this time window

        Returns:
            Dictionary with statistical metrics
        """
        with self._lock:
            # Filter metrics
            if function_name:
                metrics = self._metrics_by_function[function_name].copy()
            else:
                metrics = self._metrics.copy()

            if time_window:
                cutoff_time = datetime.now() - time_window
                metrics = [m for m in metrics if m.start_time >= cutoff_time]

            if not metrics:
                return {
                    "count": 0,
                    "error": "No metrics available"
                }

            # Calculate statistics
            durations = [m.duration_seconds for m in metrics]
            memory_deltas = [m.memory_delta_mb for m in metrics]
            memory_peaks = [m.memory_peak_mb for m in metrics]
            cpu_percents = [m.cpu_percent for m in metrics]
            db_queries = [m.db_queries for m in metrics if m.db_queries > 0]
            data_rows = [m.data_rows for m in metrics if m.data_rows > 0]
            errors = [m for m in metrics if m.error is not None]

            stats = {
                "count": len(metrics),
                "time_range": {
                    "start": min(m.start_time for m in metrics).isoformat(),
                    "end": max(m.end_time for m in metrics if m.end_time).isoformat(),
                },
                "duration": self._calculate_percentiles(durations),
                "memory_delta_mb": self._calculate_percentiles(memory_deltas),
                "memory_peak_mb": self._calculate_percentiles(memory_peaks),
                "cpu_percent": self._calculate_percentiles(cpu_percents),
                "error_rate": len(errors) / len(metrics) if metrics else 0,
                "error_count": len(errors),
            }

            if db_queries:
                stats["db_queries"] = self._calculate_percentiles(db_queries)

            if data_rows:
                stats["data_rows"] = self._calculate_percentiles(data_rows)
                # Calculate throughput (rows per second)
                total_rows = sum(data_rows)
                total_time = sum(durations)
                stats["throughput_rows_per_sec"] = total_rows / total_time if total_time > 0 else 0

            # Calculate QPS (queries per second)
            if len(metrics) > 1:
                time_span = (max(m.end_time for m in metrics if m.end_time) -
                           min(m.start_time for m in metrics)).total_seconds()
                stats["qps"] = len(metrics) / time_span if time_span > 0 else 0

            return stats

    def _calculate_percentiles(self, values: List[float]) -> Dict[str, float]:
        """Calculate statistical percentiles."""
        if not values:
            return {
                "min": 0,
                "max": 0,
                "mean": 0,
                "median": 0,
                "p50": 0,
                "p95": 0,
                "p99": 0,
            }

        sorted_values = sorted(values)
        return {
            "min": min(values),
            "max": max(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "p50": self._percentile(sorted_values, 50),
            "p95": self._percentile(sorted_values, 95),
            "p99": self._percentile(sorted_values, 99),
        }

    def _percentile(self, sorted_values: List[float], percentile: float) -> float:
        """Calculate percentile from sorted values."""
        if not sorted_values:
            return 0.0
        k = (len(sorted_values) - 1) * percentile / 100
        f = int(k)
        c = f + 1
        if c >= len(sorted_values):
            return sorted_values[-1]
        d0 = sorted_values[f] * (c - k)
        d1 = sorted_values[c] * (k - f)
        return d0 + d1

    def get_function_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary statistics for all functions."""
        with self._lock:
            summary = {}
            for function_name in self._metrics_by_function.keys():
                summary[function_name] = self.get_statistics(function_name=function_name)
            return summary

    def get_recent_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent errors."""
        with self._lock:
            errors = [m for m in self._metrics if m.error is not None]
            errors.sort(key=lambda m: m.start_time, reverse=True)
            return [
                {
                    "function_name": m.function_name,
                    "timestamp": m.start_time.isoformat(),
                    "error": m.error,
                    "duration": m.duration_seconds,
                }
                for m in errors[:limit]
            ]

    def export_metrics(self, filepath: str):
        """Export all metrics to JSON file."""
        with self._lock:
            data = {
                "export_time": datetime.now().isoformat(),
                "total_metrics": len(self._metrics),
                "metrics": [m.to_dict() for m in self._metrics],
                "summary": self.get_function_summary(),
            }

            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)

            logger.info(f"Exported {len(self._metrics)} metrics to {filepath}")

    def clear_metrics(self, older_than: Optional[timedelta] = None):
        """
        Clear metrics from memory.

        Args:
            older_than: Only clear metrics older than this timedelta (None clears all)
        """
        with self._lock:
            if older_than is None:
                count = len(self._metrics)
                self._metrics.clear()
                self._metrics_by_function.clear()
                logger.info(f"Cleared all {count} metrics")
            else:
                cutoff_time = datetime.now() - older_than
                self._metrics = [m for m in self._metrics if m.start_time >= cutoff_time]

                # Rebuild function index
                self._metrics_by_function.clear()
                for m in self._metrics:
                    self._metrics_by_function[m.function_name].append(m)

                logger.info(f"Cleared metrics older than {older_than}")

    def get_resource_usage_summary(self) -> Dict[str, Any]:
        """Get summary of resource usage across all metrics."""
        with self._lock:
            if not self._metrics:
                return {"error": "No metrics available"}

            return {
                "total_executions": len(self._metrics),
                "total_duration_seconds": sum(m.duration_seconds for m in self._metrics),
                "total_memory_delta_mb": sum(m.memory_delta_mb for m in self._metrics),
                "max_memory_peak_mb": max(m.memory_peak_mb for m in self._metrics),
                "total_db_queries": sum(m.db_queries for m in self._metrics),
                "total_data_rows": sum(m.data_rows for m in self._metrics),
                "avg_cpu_percent": statistics.mean(m.cpu_percent for m in self._metrics),
            }


# Global singleton instance
_collector_instance: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _collector_instance
    if _collector_instance is None:
        _collector_instance = MetricsCollector()
    return _collector_instance
