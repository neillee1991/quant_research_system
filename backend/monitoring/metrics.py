"""Prometheus 指标定义，供全局使用"""
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, REGISTRY

# HTTP 请求
http_requests_total = Counter(
    "http_requests_total",
    "HTTP 请求总数",
    ["method", "path", "status"],
)
http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP 请求耗时",
    ["method", "path"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
)

# 任务执行
task_run_total = Counter(
    "task_run_total",
    "任务执行次数",
    ["task_type", "status"],
)
task_run_duration_seconds = Histogram(
    "task_run_duration_seconds",
    "任务执行耗时",
    ["task_type"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
)

# 因子计算
factor_calculation_total = Counter(
    "factor_calculation_total",
    "因子计算次数",
    ["factor_id", "status"],
)
factor_calculation_duration_seconds = Histogram(
    "factor_calculation_duration_seconds",
    "因子计算耗时",
    ["factor_id"],
    buckets=[1, 5, 10, 30, 60, 120, 300],
)
factor_null_ratio = Gauge(
    "factor_null_ratio",
    "因子值空值率",
    ["factor_id"],
)

# 数据库连接
dolphindb_connection_status = Gauge(
    "dolphindb_connection_status",
    "DolphinDB 连接状态 (1=正常, 0=断开)",
)
postgres_connection_status = Gauge(
    "postgres_connection_status",
    "PostgreSQL 连接状态 (1=正常, 0=断开)",
)
