import json
from pathlib import Path
import polars as pl
from app.core.config import settings
from app.core.logger import logger
from ml_module.trainer import AutoMLTrainer
from ml_module.optimizer import FactorOptimizer
from engine.factors.technical import TechnicalFactors


class MLPipeline:
    """
    Automated ML pipeline: feature engineering -> AutoML training -> weight optimization.
    Triggered by scheduler or API call.
    """

    def __init__(self, df: pl.DataFrame):
        self.df = df

    def build_features(self) -> pl.DataFrame:
        """Compute standard factor features from OHLCV data."""
        df = self.df.sort(["ts_code", "trade_date"])
        df = df.with_columns([
            pl.col("close").rolling_mean(window_size=5, min_periods=1).over("ts_code").alias("sma5"),
            pl.col("close").rolling_mean(window_size=20, min_periods=1).over("ts_code").alias("sma20"),
            pl.col("close").rolling_std(window_size=20, min_periods=1).over("ts_code").alias("vol20"),
        ])
        # RSI requires diff/clip/ewm which are harder in pure expressions,
        # so compute per group and join back
        rsi_frames = []
        for name, group in df.group_by("ts_code"):
            rsi_series = TechnicalFactors.rsi(group["close"], 14)
            rsi_frames.append(group.select("ts_code", "trade_date").with_columns(
                rsi_series.alias("rsi14")
            ))
        rsi_df = pl.concat(rsi_frames)
        df = df.join(rsi_df, on=["ts_code", "trade_date"], how="left")
        # Forward return as target (next 5-day return)
        df = df.with_columns(
            (pl.col("close").shift(-5).over("ts_code") / pl.col("close") - 1).alias("fwd_return_5d")
        )
        # Binary target: 1 if positive return, 0 otherwise
        df = df.with_columns(
            (pl.col("fwd_return_5d") > 0).cast(pl.Int32).alias("target")
        )
        return df.drop_nulls(subset=["sma5", "sma20", "rsi14", "vol20", "target"])

    def run_automl(self, feature_cols: list[str] | None = None) -> dict:
        df = self.build_features()
        if feature_cols is None:
            feature_cols = ["sma5", "sma20", "rsi14", "vol20"]

        trainer = AutoMLTrainer(task="classification")
        trainer.train(df, feature_cols=feature_cols, target_col="target")
        trainer.save("automl_model")
        return {"status": "automl_done", "model": type(trainer.best_model).__name__}

    def run_optimization(self, feature_cols: list[str] | None = None) -> dict:
        df = self.build_features()
        if feature_cols is None:
            feature_cols = ["sma5", "sma20", "rsi14", "vol20"]

        optimizer = FactorOptimizer(df, factor_cols=feature_cols)
        best_weights = optimizer.optimize(n_trials=50)
        optimizer.save_weights()
        return {"status": "optimization_done", "best_weights": best_weights}

    def run_full(self) -> dict:
        logger.info("Starting full ML pipeline")
        results = {}
        try:
            results["automl"] = self.run_automl()
        except Exception as e:
            logger.error(f"AutoML failed: {e}")
            results["automl"] = {"status": "failed", "error": str(e)}
        try:
            results["optimization"] = self.run_optimization()
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            results["optimization"] = {"status": "failed", "error": str(e)}
        return results
