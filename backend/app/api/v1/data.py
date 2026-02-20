from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import polars as pl
from store.postgres_client import db_client
from data_manager.sync_task import sync_task
from app.core.logger import logger

router = APIRouter()


@router.get("/data/stocks")
def list_stocks():
    """List all stocks available in the database."""
    try:
        df = db_client.query("SELECT DISTINCT ts_code FROM daily_data ORDER BY ts_code")
        return {"stocks": df["ts_code"].to_list()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/daily")
def get_daily(
    ts_code: str = Query(..., description="Stock code e.g. 000001.SZ"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(500, le=5000),
):
    """Query daily OHLCV data for a stock."""
    try:
        conditions = ["ts_code = ?"]
        params = [ts_code]
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)
        where = " AND ".join(conditions)
        sql = f"SELECT * FROM daily_data WHERE {where} ORDER BY trade_date DESC LIMIT {limit}"
        df = db_client.query(sql, params)
        return {"data": df.to_dicts(), "count": len(df)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/sync")
def trigger_sync(ts_code: Optional[str] = None, source: str = "tushare"):
    """Trigger data sync. If ts_code is None, sync all stocks."""
    try:
        if ts_code:
            sync_task.sync_single_stock(ts_code, source=source)
            return {"status": "ok", "message": f"Synced {ts_code}"}
        else:
            import threading
            t = threading.Thread(target=sync_task.sync_all_stocks, args=(source,), daemon=True)
            t.start()
            return {"status": "ok", "message": "Full sync started in background"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/sync/status")
def sync_status():
    """Get last sync dates from sync_log."""
    try:
        df = db_client.query("SELECT * FROM sync_log ORDER BY updated_at DESC LIMIT 100")
        return {"logs": df.to_dicts()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
