"""脚本执行器

将编译产出的 IR 转为 signals DataFrame，调用 VectorEngine 执行回测。
复用 FlowParser 的算子逻辑和 VectorEngine 回测引擎。
"""
import re
from typing import Any, Callable

import polars as pl

from engine.backtester.vector_engine import BacktestConfig, VectorEngine
from engine.factors.technical import CrossSectionalFactors, TechnicalFactors

# condition 表达式中的逻辑运算符映射
_LOGIC_OPS = {"and", "or", "not"}


class ExecutionError(Exception):
    """脚本执行错误"""


def execute_ir(
    ir: dict[str, Any],
    df_loader: Callable[[str, str, str], pl.DataFrame],
) -> dict[str, Any]:
    """执行 IR，返回回测结果。

    Args:
        ir: 编译器产出的 IR dict
        df_loader: 数据加载函数 (ts_code, start, end) -> DataFrame

    Returns:
        包含 metrics/equity_curve/trades_sample 的 dict
    """
    data_source = ir["data_source"]
    ts_code = data_source["ts_code"]
    start_date = data_source["start_date"]
    end_date = data_source["end_date"]

    # 1. 加载数据
    df = df_loader(ts_code, start_date, end_date)
    if df is None or df.is_empty():
        raise ExecutionError(f"无数据: {ts_code} {start_date}-{end_date}")

    # 2. 执行算子链
    signal_col = "signal"
    for step in ir["pipeline"]:
        step_type = step.get("type", "")
        if step_type == "indicator":
            df = _apply_indicator(df, step)
        elif step_type == "condition":
            signal_col = step.get("output_col", "signal")
            df = _apply_condition(df, step.get("expr", ""), signal_col)
        else:
            raise ExecutionError(f"未知算子类型: {step_type}")

    # 3. 如果没有 condition 步骤，默认全仓信号
    if signal_col not in df.columns:
        df = df.with_columns(pl.lit(1).alias("signal"))
        signal_col = "signal"

    # 4. 执行回测
    bt_config = ir.get("backtest_config", {})
    config = BacktestConfig(
        initial_capital=bt_config.get("initial_capital", 1_000_000),
        commission_rate=bt_config.get("commission_rate", 0.0003),
        slippage_rate=bt_config.get("slippage_rate", 0.0001),
    )
    engine = VectorEngine(config)
    result = engine.run(df, signal_col=signal_col)

    return {
        "metrics": result.metrics,
        "equity_curve": result.equity_curve.to_dicts(),
        "trades_sample": result.trades.head(100).to_dicts(),
    }


def _apply_indicator(df: pl.DataFrame, step: dict) -> pl.DataFrame:
    """对 DataFrame 应用单个算子，复用 OPERATOR_REGISTRY 中的算子。"""
    op = step.get("op", "")
    params = step.get("params", {})
    out_col = step.get("output_col", op)

    if op == "sma":
        return df.with_columns(
            TechnicalFactors.sma(df["close"], params.get("window", 20)).alias(out_col)
        )
    elif op == "ema":
        return df.with_columns(
            TechnicalFactors.ema(df["close"], params.get("window", 20)).alias(out_col)
        )
    elif op == "rsi":
        return df.with_columns(
            TechnicalFactors.rsi(df["close"], params.get("window", 14)).alias(out_col)
        )
    elif op == "macd":
        fast, slow, signal_w = params.get("fast", 12), params.get("slow", 26), params.get("signal", 9)
        macd_line, signal_line, hist = TechnicalFactors.macd(df["close"], fast, slow, signal_w)
        return df.with_columns([
            macd_line.alias(f"{out_col}_macd"),
            signal_line.alias(f"{out_col}_signal"),
            hist.alias(f"{out_col}_hist"),
        ])
    elif op == "kdj":
        n, m1, m2 = params.get("n", 9), params.get("m1", 3), params.get("m2", 3)
        k, d, j = TechnicalFactors.kdj(df["high"], df["low"], df["close"], n, m1, m2)
        return df.with_columns([
            k.alias(f"{out_col}_k"),
            d.alias(f"{out_col}_d"),
            j.alias(f"{out_col}_j"),
        ])
    elif op == "bollinger":
        upper, mid, lower = TechnicalFactors.bollinger_bands(
            df["close"], params.get("window", 20), params.get("num_std", 2.0),
        )
        return df.with_columns([
            upper.alias(f"{out_col}_upper"),
            mid.alias(f"{out_col}_mid"),
            lower.alias(f"{out_col}_lower"),
        ])
    elif op == "rank":
        return CrossSectionalFactors.rank(df, params.get("col", "close"))
    elif op == "zscore":
        return CrossSectionalFactors.zscore(df, params.get("col", "close"))
    else:
        raise ExecutionError(f"未知算子: {op}")


def _apply_condition(df: pl.DataFrame, expr: str, signal_col: str) -> pl.DataFrame:
    """将 condition 表达式解析为 Polars when/then/otherwise 信号列。

    支持:
    - 简单比较: close > sma20, rsi14 < 70
    - 组合条件: close > sma20 and rsi14 < 70
    - 逻辑运算: and, or, not
    """
    if not expr:
        return df.with_columns(pl.lit(1).alias(signal_col))

    try:
        polars_expr = _parse_condition_expr(expr, df)
        return df.with_columns(
            pl.when(polars_expr).then(1).otherwise(0).alias(signal_col)
        )
    except Exception as e:
        raise ExecutionError(f"条件表达式解析失败: {expr} -> {e}")


def _parse_condition_expr(expr: str, df: pl.DataFrame) -> pl.Expr:
    """解析条件表达式为 Polars Expr。

    处理 and/or 逻辑运算符，递归解析子表达式。
    """
    expr = expr.strip()

    # 处理 or（优先级低于 and）
    or_parts = _split_logical(expr, " or ")
    if len(or_parts) > 1:
        parts = [_parse_condition_expr(p, df) for p in or_parts]
        result = parts[0]
        for p in parts[1:]:
            result = result | p
        return result

    # 处理 and
    and_parts = _split_logical(expr, " and ")
    if len(and_parts) > 1:
        parts = [_parse_condition_expr(p, df) for p in and_parts]
        result = parts[0]
        for p in parts[1:]:
            result = result & p
        return result

    # 处理 not
    if expr.startswith("not "):
        inner = _parse_condition_expr(expr[4:], df)
        return ~inner

    # 单个比较表达式
    return _parse_comparison(expr, df)


def _split_logical(expr: str, sep: str) -> list[str]:
    """按逻辑运算符拆分表达式，但不拆分嵌套的括号内内容。"""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    i = 0
    while i < len(expr):
        if expr[i] == "(":
            depth += 1
            current.append(expr[i])
            i += 1
        elif expr[i] == ")":
            depth -= 1
            current.append(expr[i])
            i += 1
        elif depth == 0 and expr[i:i + len(sep)] == sep:
            parts.append("".join(current).strip())
            current = []
            i += len(sep)
        else:
            current.append(expr[i])
            i += 1
    if current:
        parts.append("".join(current).strip())
    return parts


def _parse_comparison(expr: str, df: pl.DataFrame) -> pl.Expr:
    """解析单个比较表达式，如 close > sma20 或 rsi14 < 70。"""
    expr = expr.strip()

    # 去掉外层括号
    if expr.startswith("(") and expr.endswith(")"):
        return _parse_condition_expr(expr[1:-1], df)

    # 匹配比较运算符
    pattern = r"^(.+?)\s*(>=|<=|!=|==|>|<)\s*(.+)$"
    match = re.match(pattern, expr)
    if not match:
        raise ExecutionError(f"无法解析比较表达式: {expr}")

    left_str, op, right_str = match.group(1).strip(), match.group(2), match.group(3).strip()
    left_expr = _to_polars_expr(left_str, df)
    right_expr = _to_polars_expr(right_str, df)

    if op == ">":
        return left_expr > right_expr
    elif op == "<":
        return left_expr < right_expr
    elif op == ">=":
        return left_expr >= right_expr
    elif op == "<=":
        return left_expr <= right_expr
    elif op == "==":
        return left_expr == right_expr
    elif op == "!=":
        return left_expr != right_expr
    else:
        raise ExecutionError(f"未知运算符: {op}")


def _to_polars_expr(token: str, df: pl.DataFrame) -> pl.Expr:
    """将 token 转为 Polars Expr：列名或数值常量。"""
    token = token.strip()
    if token in df.columns:
        return pl.col(token)
    try:
        return pl.lit(float(token))
    except ValueError:
        raise ExecutionError(f"无法识别的列名或数值: {token}")
