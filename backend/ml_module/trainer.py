import pickle
from pathlib import Path
import polars as pl
import pandas as pd
from app.core.config import settings
from app.core.logger import logger

try:
    from pycaret.classification import setup as cls_setup, compare_models as cls_compare, save_model as cls_save
    from pycaret.regression import setup as reg_setup, compare_models as reg_compare, save_model as reg_save
    PYCARET_AVAILABLE = True
except ImportError:
    PYCARET_AVAILABLE = False
    logger.warning("PyCaret not available, AutoML disabled")


class AutoMLTrainer:
    """
    Wraps PyCaret for automatic model comparison and selection.
    Supports both classification (signal direction) and regression (return prediction).
    """

    def __init__(self, task: str = "classification"):
        if not PYCARET_AVAILABLE:
            raise RuntimeError("PyCaret is not installed")
        self.task = task
        self.best_model = None
        self.model_path: Path | None = None

    def train(
        self,
        df: pl.DataFrame,
        feature_cols: list[str],
        target_col: str,
        n_select: int = 3,
    ):
        """
        Train AutoML on the given DataFrame.
        Returns the best model.
        """
        pdf = df.select(feature_cols + [target_col]).drop_nulls().to_pandas()
        logger.info(f"AutoML training on {len(pdf)} rows, task={self.task}")

        if self.task == "classification":
            cls_setup(data=pdf, target=target_col, verbose=False, session_id=42)
            self.best_model = cls_compare(n_select=1, sort="AUC", verbose=False)
        else:
            reg_setup(data=pdf, target=target_col, verbose=False, session_id=42)
            self.best_model = reg_compare(n_select=1, sort="R2", verbose=False)

        logger.info(f"Best model: {type(self.best_model).__name__}")
        return self.best_model

    def save(self, name: str = "automl_model"):
        if self.best_model is None:
            raise ValueError("No model trained yet")
        path = str(settings.models_dir / name)
        if self.task == "classification":
            cls_save(self.best_model, path)
        else:
            reg_save(self.best_model, path)
        self.model_path = Path(path + ".pkl")
        logger.info(f"Model saved to {self.model_path}")

    def predict(self, df: pl.DataFrame, feature_cols: list[str]) -> pl.Series:
        if self.best_model is None:
            raise ValueError("No model loaded")
        pdf = df.select(feature_cols).to_pandas()
        preds = self.best_model.predict(pdf)
        return pl.Series("prediction", preds)
