from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
import polars as pl
from store.postgres_client import db_client
from engine.parser.flow_parser import FlowParser
from engine.backtester.vector_engine import VectorEngine, BacktestConfig
from app.core.logger import logger

router = APIRouter()


class BacktestRequest(BaseModel):
    graph: dict[str, Any]


class SimpleBacktestRequest(BaseModel):
    ts_code: str
    start_date: str = "20200101"
    end_date: str = "20241231"
    signal_col: str = "signal"
    commission_rate: float = 0.0003
    slippage_rate: float = 0.0001
    initial_capital: float = 1_000_000.0


def _load_data(ts_code: str, start: str, end: str) -> pl.DataFrame:
    return db_client.query(
        "SELECT * FROM daily_data WHERE ts_code=? AND trade_date>=? AND trade_date<=? ORDER BY trade_date",
        [ts_code, start, end],
    )


@router.post("/strategy/backtest")
def run_backtest(req: BacktestRequest):
    """Run a backtest from a React Flow graph JSON."""
    try:
        parser = FlowParser(df_loader=_load_data)
        result = parser.parse_and_run(req.graph)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy/operators")
def list_operators():
    """Return available operator definitions for the frontend node palette."""
    from engine.parser.flow_parser import OPERATOR_REGISTRY
    return {"operators": OPERATOR_REGISTRY}
