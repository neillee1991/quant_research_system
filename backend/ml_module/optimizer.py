import json
import pickle
from pathlib import Path
from typing import Any
import polars as pl
import optuna
from app.core.config import settings
from app.core.logger import logger
from engine.backtester.vector_engine import VectorEngine, BacktestConfig


class FactorOptimizer:
    """
    Uses Optuna to find optimal factor weights that maximize Sharpe ratio.
    """

    def __init__(self, df: pl.DataFrame, factor_cols: list[str], signal_col: str = "signal"):
        self.df = df
        self.factor_cols = factor_cols
        self.signal_col = signal_col
        self.best_weights: dict[str, float] = {}
        self.study: optuna.Study | None = None

    def _objective(self, trial: optuna.Trial) -> float:
        weights = {col: trial.suggest_float(col, -1.0, 1.0) for col in self.factor_cols}

        # Compute composite signal as weighted sum of factors
        df = self.df.clone()
        composite = sum(
            df[col] * w for col, w in weights.items()
        )
        df = df.with_columns(
            pl.Series(name=self.signal_col, values=(composite > 0).cast(pl.Int32) * 2 - 1)
        )

        engine = VectorEngine(BacktestConfig())
        try:
            result = engine.run(df, signal_col=self.signal_col)
            sharpe = result.metrics.get("sharpe_ratio", -999)
            return sharpe if not (sharpe != sharpe) else -999  # handle NaN
        except Exception:
            return -999

    def optimize(self, n_trials: int = 100, direction: str = "maximize") -> dict[str, float]:
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        self.study = optuna.create_study(direction=direction)
        self.study.optimize(self._objective, n_trials=n_trials, n_jobs=1)
        self.best_weights = self.study.best_params
        logger.info(f"Best weights: {self.best_weights}, Sharpe: {self.study.best_value:.4f}")
        return self.best_weights

    def save_weights(self, filename: str = "best_weights.json"):
        path = settings.models_dir / filename
        with open(path, "w") as f:
            json.dump(self.best_weights, f, indent=2)
        logger.info(f"Saved best weights to {path}")

    @staticmethod
    def load_weights(filename: str = "best_weights.json") -> dict[str, float]:
        path = settings.models_dir / filename
        if not path.exists():
            return {}
        with open(path) as f:
            return json.load(f)
