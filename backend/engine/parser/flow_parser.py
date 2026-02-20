import polars as pl
from engine.factors.technical import TechnicalFactors, CrossSectionalFactors
from engine.backtester.vector_engine import VectorEngine, BacktestConfig
from app.core.logger import logger


# Registry of available operators
OPERATOR_REGISTRY = {
    "sma": {"fn": "sma", "params": ["window"], "input": "close"},
    "ema": {"fn": "ema", "params": ["window"], "input": "close"},
    "rsi": {"fn": "rsi", "params": ["window"], "input": "close"},
    "macd": {"fn": "macd", "params": ["fast", "slow", "signal"], "input": "close"},
    "kdj": {"fn": "kdj", "params": ["n", "m1", "m2"], "input": ["high", "low", "close"]},
    "bollinger": {"fn": "bollinger_bands", "params": ["window", "num_std"], "input": "close"},
    "rank": {"fn": "rank", "params": ["col"], "cross_sectional": True},
    "zscore": {"fn": "zscore", "params": ["col"], "cross_sectional": True},
}


class FlowParser:
    """
    Parses a React Flow JSON graph into a sequential computation chain
    and executes it against market data.

    Expected JSON format:
    {
        "nodes": [
            {"id": "1", "type": "data_input", "data": {"ts_code": "000001.SZ", "start": "20230101", "end": "20241231"}},
            {"id": "2", "type": "operator", "data": {"op": "sma", "window": 20, "output_col": "sma20"}},
            {"id": "3", "type": "operator", "data": {"op": "rsi", "window": 14, "output_col": "rsi14"}},
            {"id": "4", "type": "signal", "data": {"condition": "close > sma20", "signal_col": "signal"}},
            {"id": "5", "type": "backtest_output", "data": {"config": {}}}
        ],
        "edges": [
            {"source": "1", "target": "2"},
            {"source": "2", "target": "3"},
            {"source": "3", "target": "4"},
            {"source": "4", "target": "5"}
        ]
    }
    """

    def __init__(self, df_loader):
        """df_loader: callable(ts_code, start, end) -> pl.DataFrame"""
        self.df_loader = df_loader
        self.tf = TechnicalFactors()
        self.cs = CrossSectionalFactors()

    def parse_and_run(self, graph: dict) -> dict:
        nodes = {n["id"]: n for n in graph["nodes"]}
        edges = graph["edges"]

        # Build execution order via topological sort
        order = self._topo_sort(nodes, edges)

        df: pl.DataFrame | None = None
        backtest_config = BacktestConfig()
        signal_col = "signal"

        for node_id in order:
            node = nodes[node_id]
            ntype = node["type"]
            data = node.get("data", {})

            if ntype == "data_input":
                df = self.df_loader(
                    data["ts_code"],
                    data.get("start", "20200101"),
                    data.get("end", "20241231"),
                )
                if df is None or df.is_empty():
                    raise ValueError(f"No data for {data['ts_code']}")
                logger.info(f"Loaded {len(df)} rows for {data['ts_code']}")

            elif ntype == "operator":
                if df is None:
                    raise ValueError("Operator node reached before data_input")
                df = self._apply_operator(df, data)

            elif ntype == "signal":
                if df is None:
                    raise ValueError("Signal node reached before data_input")
                signal_col = data.get("signal_col", "signal")
                df = self._apply_signal(df, data, signal_col)

            elif ntype == "backtest_output":
                cfg_data = data.get("config", {})
                backtest_config = BacktestConfig(
                    initial_capital=cfg_data.get("initial_capital", 1_000_000),
                    commission_rate=cfg_data.get("commission_rate", 0.0003),
                    slippage_rate=cfg_data.get("slippage_rate", 0.0001),
                )

        if df is None:
            raise ValueError("Graph produced no data")

        engine = VectorEngine(backtest_config)
        result = engine.run(df, signal_col=signal_col)

        return {
            "metrics": result.metrics,
            "equity_curve": result.equity_curve.to_dicts(),
            "trades_sample": result.trades.head(100).to_dicts(),
        }

    def _apply_operator(self, df: pl.DataFrame, data: dict) -> pl.DataFrame:
        op = data.get("op", "")
        out_col = data.get("output_col", op)

        if op == "sma":
            df = df.with_columns(
                TechnicalFactors.sma(pl.col("close"), data.get("window", 20)).alias(out_col)
            )
        elif op == "ema":
            df = df.with_columns(
                TechnicalFactors.ema(pl.col("close"), data.get("window", 20)).alias(out_col)
            )
        elif op == "rsi":
            df = df.with_columns(
                TechnicalFactors.rsi(pl.col("close"), data.get("window", 14)).alias(out_col)
            )
        elif op == "macd":
            macd_line, signal_line, hist = TechnicalFactors.macd(
                pl.col("close"),
                data.get("fast", 12),
                data.get("slow", 26),
                data.get("signal", 9),
            )
            df = df.with_columns([
                macd_line.alias(f"{out_col}_macd"),
                signal_line.alias(f"{out_col}_signal"),
                hist.alias(f"{out_col}_hist"),
            ])
        elif op == "bollinger":
            upper, mid, lower = TechnicalFactors.bollinger_bands(
                pl.col("close"), data.get("window", 20), data.get("num_std", 2.0)
            )
            df = df.with_columns([
                upper.alias(f"{out_col}_upper"),
                mid.alias(f"{out_col}_mid"),
                lower.alias(f"{out_col}_lower"),
            ])
        elif op == "rank":
            df = CrossSectionalFactors.rank(df, data.get("col", "close"))
        elif op == "zscore":
            df = CrossSectionalFactors.zscore(df, data.get("col", "close"))
        else:
            logger.warning(f"Unknown operator: {op}")
        return df

    def _apply_signal(self, df: pl.DataFrame, data: dict, signal_col: str) -> pl.DataFrame:
        """
        Evaluate a simple condition string to generate buy/sell signals.
        condition examples: "close > sma20", "rsi14 < 30"
        """
        condition = data.get("condition", "")
        if not condition:
            df = df.with_columns(pl.lit(1).alias(signal_col))
            return df

        # Parse simple "col op value" or "col op col" conditions
        try:
            parts = condition.split()
            if len(parts) == 3:
                left, op, right = parts
                left_expr = pl.col(left)
                try:
                    right_expr = pl.lit(float(right))
                except ValueError:
                    right_expr = pl.col(right)

                if op == ">":
                    cond_expr = left_expr > right_expr
                elif op == "<":
                    cond_expr = left_expr < right_expr
                elif op == ">=":
                    cond_expr = left_expr >= right_expr
                elif op == "<=":
                    cond_expr = left_expr <= right_expr
                elif op == "==":
                    cond_expr = left_expr == right_expr
                else:
                    cond_expr = pl.lit(True)

                df = df.with_columns(
                    pl.when(cond_expr).then(1).otherwise(0).alias(signal_col)
                )
        except Exception as e:
            logger.warning(f"Signal condition parse error: {e}, defaulting to 1")
            df = df.with_columns(pl.lit(1).alias(signal_col))

        return df

    def _topo_sort(self, nodes: dict, edges: list) -> list[str]:
        """Kahn's algorithm topological sort."""
        from collections import deque, defaultdict
        in_degree = {nid: 0 for nid in nodes}
        adj = defaultdict(list)
        for e in edges:
            adj[e["source"]].append(e["target"])
            in_degree[e["target"]] += 1

        queue = deque([n for n, d in in_degree.items() if d == 0])
        order = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        return order
