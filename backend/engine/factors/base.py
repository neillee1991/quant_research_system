from abc import ABC, abstractmethod
import polars as pl


class BaseOperator(ABC):
    """Abstract base class for all factor operators."""

    name: str = ""
    description: str = ""
    params: dict = {}

    @abstractmethod
    def compute(self, df: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """Compute the factor and return df with new column(s)."""
        ...

    def validate_params(self, **kwargs):
        for key, default in self.params.items():
            if key not in kwargs:
                kwargs[key] = default
        return kwargs
