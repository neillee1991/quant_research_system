"""
DataPipeline - 可组合的数据处理管道

核心设计：
1. IProcessor 接口：定义处理器契约
2. ProcessContext：传递上下文信息
3. DataPipeline：编排处理器执行
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import polars as pl
from app.core.logger import logger


@dataclass
class ProcessContext:
    """处理上下文 - 在 Pipeline 各阶段间传递信息"""

    # 因子定义
    factor_id: str
    factor_definition: Any  # FactorDefinition

    # 日期范围
    calc_start: str  # 计算结果起始日期
    calc_end: str    # 计算结果结束日期
    data_start: str  # 数据加载起始日期（含 lookback）

    # 计算模式
    compute_mode: str = "incremental"  # "full" 或 "incremental"

    # 预处理选项
    preprocess_options: Dict[str, Any] = field(default_factory=dict)

    # 运行时信息
    run_id: Optional[str] = None

    # 共享状态（处理器间传递数据）
    shared_state: Dict[str, Any] = field(default_factory=dict)

    # 数据引用（避免重复传递大对象）
    dataframe: Optional[pl.DataFrame] = None

    def get_option(self, key: str, default: Any = None) -> Any:
        """获取预处理选项"""
        return self.preprocess_options.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """设置共享状态"""
        self.shared_state[key] = value

    def get_state(self, key: str, default: Any = None) -> Any:
        """获取共享状态"""
        return self.shared_state.get(key, default)


class IProcessor(ABC):
    """处理器接口"""

    @property
    @abstractmethod
    def name(self) -> str:
        """处理器名称（用于日志）"""
        pass

    def should_run(self, context: ProcessContext) -> bool:
        """判断是否需要执行（默认总是执行）"""
        return True

    @abstractmethod
    def process(self, df: pl.DataFrame, context: ProcessContext) -> pl.DataFrame:
        """执行处理逻辑

        Args:
            df: 输入数据
            context: 处理上下文

        Returns:
            处理后的数据
        """
        pass

    def on_error(self, error: Exception, context: ProcessContext) -> None:
        """错误处理钩子（默认记录日志）"""
        logger.error(f"Processor {self.name} failed: {error}")


class DataPipeline:
    """数据处理管道 - 编排多个处理器按顺序执行"""

    def __init__(self, name: str = "DataPipeline"):
        self.name = name
        self._stages: List[IProcessor] = []

    def add_stage(self, processor: IProcessor) -> 'DataPipeline':
        """添加处理阶段（支持链式调用）"""
        self._stages.append(processor)
        return self

    def execute(self, context: ProcessContext) -> pl.DataFrame:
        """执行完整管道

        Args:
            context: 处理上下文（包含初始数据）

        Returns:
            最终处理结果
        """
        logger.info(f"Pipeline {self.name} started with {len(self._stages)} stages")

        df = context.dataframe
        if df is None:
            raise ValueError("ProcessContext.dataframe is None")

        # 确保 df 是 Polars DataFrame
        df = self._ensure_polars_df(df)

        for idx, processor in enumerate(self._stages, 1):
            try:
                # 检查是否需要执行
                if not processor.should_run(context):
                    logger.debug(f"Stage {idx}/{len(self._stages)}: {processor.name} skipped")
                    continue

                logger.info(f"Stage {idx}/{len(self._stages)}: {processor.name} processing {len(df)} rows")

                # 执行处理
                df = processor.process(df, context)

                # 确保 df 是 Polars DataFrame
                df = self._ensure_polars_df(df)

                # 更新上下文中的数据引用
                context.dataframe = df

                if df is None or df.is_empty():
                    logger.warning(f"Stage {idx}/{len(self._stages)}: {processor.name} returned empty data")
                    break

                logger.info(f"Stage {idx}/{len(self._stages)}: {processor.name} completed, {len(df)} rows remaining")

            except Exception as e:
                processor.on_error(e, context)
                raise RuntimeError(f"Pipeline failed at stage {idx} ({processor.name}): {e}") from e

        logger.info(f"Pipeline {self.name} completed, final output: {len(df) if df is not None else 0} rows")
        return df

    def _ensure_polars_df(self, df: Any) -> pl.DataFrame:
        """确保返回的是 Polars DataFrame

        Args:
            df: 可能的 DataFrame 类型

        Returns:
            Polars DataFrame
        """
        if df is None:
            return pl.DataFrame()
        if isinstance(df, pl.DataFrame):
            return df
        if isinstance(df, dict):
            try:
                return pl.DataFrame(df)
            except Exception as e:
                logger.warning(f"Failed to convert dict to Polars DataFrame: {e}")
                return pl.DataFrame()
        if isinstance(df, list):
            try:
                return pl.DataFrame(df)
            except Exception as e:
                logger.warning(f"Failed to convert list to Polars DataFrame: {e}")
                return pl.DataFrame()
        if hasattr(df, 'to_pandas'):
            try:
                return pl.from_pandas(df.to_pandas())
            except Exception as e:
                logger.warning(f"Failed to convert from pandas-like object to Polars DataFrame: {e}")
                return pl.DataFrame()

        logger.warning(f"Unknown type {type(df)}, returning empty Polars DataFrame")
        return pl.DataFrame()

    def get_stages(self) -> List[IProcessor]:
        """获取所有处理阶段（用于调试）"""
        return self._stages.copy()
