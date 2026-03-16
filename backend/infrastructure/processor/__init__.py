"""
Infrastructure Processor Package
"""
from infrastructure.processor.pipeline import (
    IProcessor,
    ProcessContext,
    DataPipeline,
)
from infrastructure.processor.processors import (
    DataLoaderProcessor,
    AdjustmentProcessor,
    StatusFilterProcessor,
    FactorComputeProcessor,
    CalendarAlignProcessor,
    DateRangeFilterProcessor,
    QualityCheckerProcessor,
    ResultWriterProcessor,
)
from infrastructure.processor.pipeline_factory import PipelineFactory

__all__ = [
    # Core
    "IProcessor",
    "ProcessContext",
    "DataPipeline",
    # Processors
    "DataLoaderProcessor",
    "AdjustmentProcessor",
    "StatusFilterProcessor",
    "FactorComputeProcessor",
    "CalendarAlignProcessor",
    "DateRangeFilterProcessor",
    "QualityCheckerProcessor",
    "ResultWriterProcessor",
    # Factory
    "PipelineFactory",
]
