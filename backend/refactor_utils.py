#!/usr/bin/env python3
"""
Phase 2: Refactor Duplicate Code
Extract common patterns into reusable utility functions
"""
from typing import Optional


def build_date_range_query(
    base_conditions: list[str],
    base_params: list,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_column: str = "trade_date"
) -> tuple[str, list]:
    """
    Build SQL WHERE clause with date range filtering.

    Args:
        base_conditions: Initial WHERE conditions (e.g., ["ts_code = %s"])
        base_params: Parameters for base conditions
        start_date: Start date in YYYYMMDD format (inclusive)
        end_date: End date in YYYYMMDD format (inclusive)
        date_column: Name of the date column (default: "trade_date")

    Returns:
        Tuple of (where_clause, params_list)

    Example:
        >>> where, params = build_date_range_query(
        ...     ["ts_code = %s"], ["000001.SZ"],
        ...     start_date="20240101", end_date="20241231"
        ... )
        >>> where
        'ts_code = %s AND trade_date >= %s AND trade_date <= %s'
        >>> params
        ['000001.SZ', '20240101', '20241231']
    """
    conditions = base_conditions.copy()
    params = base_params.copy()

    if start_date:
        conditions.append(f"{date_column} >= %s")
        params.append(start_date)
    if end_date:
        conditions.append(f"{date_column} <= %s")
        params.append(end_date)

    where = " AND ".join(conditions)
    return where, params


# Example usage in app/api/v1/data_merged.py:
# BEFORE:
#     conditions = ["ts_code = %s"]
#     params = [ts_code]
#     if start_date:
#         conditions.append("trade_date >= %s")
#         params.append(start_date)
#     if end_date:
#         conditions.append("trade_date <= %s")
#         params.append(end_date)
#     where = " AND ".join(conditions)
#
# AFTER:
#     from app.core.utils import build_date_range_query
#     where, params = build_date_range_query(
#         ["ts_code = %s"], [ts_code],
#         start_date=start_date, end_date=end_date
#     )
