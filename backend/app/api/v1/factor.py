from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import polars as pl
from store.dolphindb_client import db_client
from engine.factors.technical import TechnicalFactors, CrossSectionalFactors
from engine.factors.financial import FactorAnalyzer
from app.core.logger import logger

router = APIRouter()


class FactorRequest(BaseModel):
    ts_code: str
    start_date: Optional[str] = "20200101"
    end_date: Optional[str] = None
    factors: list[str] = ["sma20", "rsi14"]


def _load_stock_data(ts_code: str, start: str, end: str) -> pl.DataFrame:
    conditions = ["ts_code = %s"]
    params = [ts_code]
    if start:
        conditions.append("trade_date >= %s")
        params.append(start)
    if end:
        conditions.append("trade_date <= %s")
        params.append(end)
    where = " AND ".join(conditions)
    return db_client.query(
        f"SELECT * FROM sync_daily_data WHERE {where} ORDER BY trade_date", params
    )


@router.post("/factor/compute")
def compute_factors(req: FactorRequest):
    """Compute requested factors for a stock."""
    try:
        end = req.end_date or "99991231"
        df = _load_stock_data(req.ts_code, req.start_date, end)
        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"No data for {req.ts_code}")

        for factor in req.factors:
            if factor.startswith("sma"):
                w = int(factor[3:]) if factor[3:].isdigit() else 20
                df = df.with_columns(TechnicalFactors.sma(df["close"], w).alias(factor))
            elif factor.startswith("ema"):
                w = int(factor[3:]) if factor[3:].isdigit() else 20
                df = df.with_columns(TechnicalFactors.ema(df["close"], w).alias(factor))
            elif factor.startswith("rsi"):
                w = int(factor[3:]) if factor[3:].isdigit() else 14
                df = df.with_columns(TechnicalFactors.rsi(df["close"], w).alias(factor))
            elif factor == "macd":
                macd, sig, hist = TechnicalFactors.macd(df["close"])
                df = df.with_columns([
                    macd.alias("macd"), sig.alias("macd_signal"), hist.alias("macd_hist")
                ])

        return {"data": df.to_dicts(), "count": len(df)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor/ic")
def compute_ic(req: FactorRequest):
    """Compute IC/Rank IC for a factor."""
    try:
        end = req.end_date or "99991231"
        df = _load_stock_data(req.ts_code, req.start_date, end)
        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"No data for {req.ts_code}")

        factor_col = req.factors[0] if req.factors else "close"
        df = df.with_columns(TechnicalFactors.sma(df["close"], 20).alias(factor_col))
        df = df.with_columns(
            (pl.col("close").shift(-1) / pl.col("close") - 1).alias("fwd_return")
        ).drop_nulls(subset=["fwd_return"])

        ic = FactorAnalyzer.ic(df[factor_col], df["fwd_return"])
        rank_ic = FactorAnalyzer.rank_ic(df[factor_col], df["fwd_return"])
        return {"ic": ic, "rank_ic": rank_ic}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
