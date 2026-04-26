"""
回测服务 - 集成 VectorBT 和 RQAlpha 引擎
"""
from typing import Dict, Any
from app.core.logger import logger
from backend.engine.backtest.vectorbt_engine.engine import VectorBTEngine
from backend.engine.backtest.rqalpha_engine.rq_env import RQAlphaEngine
from backend.engine.backtest.analysis.report_generator import ReportGenerator


class BacktestService:
    """回测服务"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.vectorbt_engine = VectorBTEngine(config)
        self.rqalpha_engine = RQAlphaEngine(config)

    async def run_backtest(self, strategy, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行回测"""
        engine_mode = config.get("engine_mode", "vectorbt")

        if engine_mode == "vectorbt":
            return await self._run_vectorbt(strategy, config)
        elif engine_mode == "rqalpha":
            return await self._run_rqalpha(strategy, config)
        else:
            raise ValueError(f"未知的引擎模式: {engine_mode}")

    async def _run_vectorbt(self, strategy, config: Dict[str, Any]) -> Dict[str, Any]:
        """运行 VectorBT 回测"""
        result = await self.vectorbt_engine.run(
            strategy,
            config["start_date"],
            config["end_date"],
            config["factors"],
            config["stocks"]
        )

        return ReportGenerator.generate_report(result)

    async def _run_rqalpha(self, strategy, config: Dict[str, Any]) -> Dict[str, Any]:
        """运行 RQAlpha 回测"""
        result = await self.rqalpha_engine.run(
            strategy,
            config["start_date"],
            config["end_date"],
            config["factors"],
            config["stocks"]
        )

        return ReportGenerator.generate_report(result)
