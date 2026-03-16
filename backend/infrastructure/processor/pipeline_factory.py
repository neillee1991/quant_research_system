"""
PipelineFactory - 根据因子定义和预处理配置动态构建 Pipeline
"""
from typing import Dict, Any, Optional

from app.core.logger import logger
from infrastructure.processor.pipeline import DataPipeline
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


class PipelineFactory:
    """Pipeline 工厂 - 根据配置构建数据处理管道"""

    def __init__(self, db_client, data_config, trading_cal):
        self.db = db_client
        self.data_config = data_config
        self.trading_cal = trading_cal

    def create_factor_pipeline(
        self,
        factor_id: str,
        preprocess_options: Dict[str, Any],
        save_results: bool = True
    ) -> DataPipeline:
        """创建因子计算管道

        Args:
            factor_id: 因子ID
            preprocess_options: 预处理选项
            save_results: 是否保存结果到数据库

        Returns:
            配置好的 DataPipeline
        """
        pipeline = DataPipeline(name=f"FactorPipeline-{factor_id}")

        # 1. 数据加载
        pipeline.add_stage(
            DataLoaderProcessor(self.db, self.data_config)
        )

        # 2. 复权处理（可选）
        if preprocess_options.get("adjust_price") in ("forward", "backward"):
            pipeline.add_stage(
                AdjustmentProcessor(self.db, self.data_config)
            )

        # 3. 状态过滤（ST、新股、涨跌停）
        if any([
            preprocess_options.get("filter_st"),
            preprocess_options.get("filter_new_stock"),
            preprocess_options.get("mark_limit")
        ]):
            pipeline.add_stage(
                StatusFilterProcessor(self.db, self.data_config, self.trading_cal)
            )

        # 4. 因子计算
        pipeline.add_stage(
            FactorComputeProcessor()
        )

        # 5. 交易日历对齐（align_calendar=True 时生效，由处理器 should_run 判断）
        pipeline.add_stage(
            CalendarAlignProcessor(self.db, self.trading_cal)
        )

        # 6. 日期范围过滤
        pipeline.add_stage(
            DateRangeFilterProcessor()
        )

        # 6. 质量检查
        pipeline.add_stage(
            QualityCheckerProcessor()
        )

        # 7. 结果写入（可选）
        if save_results:
            pipeline.add_stage(
                ResultWriterProcessor(self.db)
            )

        logger.info(f"Created pipeline with {len(pipeline.get_stages())} stages")
        return pipeline

    def create_custom_pipeline(
        self,
        stages: list,
        name: str = "CustomPipeline"
    ) -> DataPipeline:
        """创建自定义管道

        Args:
            stages: 处理器列表
            name: 管道名称

        Returns:
            配置好的 DataPipeline
        """
        pipeline = DataPipeline(name=name)
        for stage in stages:
            pipeline.add_stage(stage)
        return pipeline
