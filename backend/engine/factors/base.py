from abc import ABC, abstractmethod
import polars as pl


class BaseOperator(ABC):
    """Abstract base class for all factor operators."""

    name: str = ""
    description: str = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Give each subclass its own params dict to avoid shared mutable state
        if 'params' not in cls.__dict__:
            cls.params = {}

    @abstractmethod
    def compute(self, df: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """Compute the factor and return df with new column(s)."""
        ...

    def validate_params(self, **kwargs):
        for key, default in self.params.items():
            if key not in kwargs:
                kwargs[key] = default
        return kwargs
