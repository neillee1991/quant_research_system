"""
回测 API - 执行策略回测
"""
from fastapi import APIRouter, Depends, HTTPException
from app.services.backtest_service import BacktestService
from app.models.backtest_config import BacktestConfig

router = APIRouter(prefix="/api/v1/backtest")


@router.post("/run")
async def run_backtest(request: BacktestConfig):
    """
    执行回测

    注意：当前为同步阻塞实现，大规模数据回测会导致 504 超时
    未来优化方向：使用调度器压入任务，返回 task_id 后轮询状态
    """
    if len(request.stocks) > 100:
        raise HTTPException(status_code=400, detail="MVP阶段单次回测最多选择100只股票")

    if not request.strategy_code or not request.strategy_code.strip():
        raise HTTPException(status_code=400, detail="策略代码不能为空")

    service = BacktestService(request.dict())

    strategy = await _load_strategy(request.strategy_code)

    result = await service.run_backtest(strategy, request.dict())

    return result


@router.get("/factors")
async def list_available_factors():
    """列出可用因子"""
    from app.services.factor_service import factor_service
    factors = await factor_service.list_factors()
    return [factor["factor_id"] for factor in factors]


@router.get("/benchmarks")
async def list_benchmarks():
    """列出基准指数"""
    return [
        {"code": "000300.SH", "name": "沪深300"},
        {"code": "000001.SH", "name": "上证指数"},
        {"code": "399001.SZ", "name": "深圳成指"},
    ]


async def _load_strategy(strategy_code: str):
    """加载策略"""
    from backend.engine.backtest.core.strategy_loader import StrategyLoader

    if strategy_code:
        loader = StrategyLoader()
        return loader.load_strategy_from_code(strategy_code, "MyStrategy", None)
    else:
        from backend.engine.backtest.core.base_strategy import BaseStrategy

        class SimpleStrategy(BaseStrategy):
            def initialize(self, context):
                pass

            def generate_signals(self, prices, factors):
                return []

            def on_bar(self, data):
                pass

            def on_tick(self, data):
                pass

            def on_order_book_update(self, data):
                pass

            def on_trade(self, data):
                pass

            def on_position_change(self, data):
                pass

            def on_order_change(self, data):
                pass

            def terminate(self):
                pass

        return SimpleStrategy({})
