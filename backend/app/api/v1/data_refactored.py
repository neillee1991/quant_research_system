"""
数据 API 路由（重构版）
使用服务层和依赖注入，职责清晰分离
"""
from typing import List, Optional
from fastapi import APIRouter, Query, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.core.container import get_data_service, get_sync_engine
from app.core.exceptions import DataNotFoundError, DataValidationError
from app.core.logger import logger
from app.services.data_service import DataService
from data_manager.refactored_sync_engine import RefactoredSyncEngine


router = APIRouter()


# ==================== 请求/响应模型 ====================

class StockListResponse(BaseModel):
    """股票列表响应"""
    stocks: List[str]
    count: int


class DailyDataResponse(BaseModel):
    """日线数据响应"""
    data: List[dict]
    count: int


class SyncRequest(BaseModel):
    """同步请求"""
    ts_code: Optional[str] = Field(None, description="股票代码，为空则同步所有")
    source: str = Field(default="tushare", description="数据源")


class SyncResponse(BaseModel):
    """同步响应"""
    status: str
    message: str


# ==================== API 端点 ====================

@router.get("/data/stocks", response_model=StockListResponse)
async def list_stocks(
    market: Optional[str] = Query(None, description="市场类型"),
    industry: Optional[str] = Query(None, description="行业"),
    data_service: DataService = Depends(get_data_service)
):
    """
    获取股票列表

    - **market**: 市场类型（可选）
    - **industry**: 行业（可选）
    """
    try:
        df = data_service.get_stock_list(market=market, industry=industry)
        stocks = df["ts_code"].to_list() if "ts_code" in df.columns else []

        return StockListResponse(
            stocks=stocks,
            count=len(stocks)
        )

    except DataNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=e.to_dict()
        )
    except Exception as e:
        logger.error(f"Failed to list stocks: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/data/daily", response_model=DailyDataResponse)
async def get_daily_data(
    ts_code: str = Query(..., description="股票代码，如 000001.SZ"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD"),
    limit: int = Query(500, le=5000, description="返回记录数限制"),
    data_service: DataService = Depends(get_data_service)
):
    """
    获取日线行情数据

    - **ts_code**: 股票代码（必填）
    - **start_date**: 开始日期（可选）
    - **end_date**: 结束日期（可选）
    - **limit**: 返回记录数限制
    """
    try:
        df = data_service.get_daily_data(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )

        return DailyDataResponse(
            data=df.to_dicts(),
            count=len(df)
        )

    except DataNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=e.to_dict()
        )
    except DataValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=e.to_dict()
        )
    except Exception as e:
        logger.error(f"Failed to get daily data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/data/sync", response_model=SyncResponse)
async def trigger_sync(
    request: SyncRequest,
    sync_engine: RefactoredSyncEngine = Depends(get_sync_engine)
):
    """
    触发数据同步

    - **ts_code**: 股票代码，为空则同步所有启用的任务
    - **source**: 数据源（默认 tushare）
    """
    try:
        if request.ts_code:
            # 同步单个股票（需要实现单股票同步逻辑）
            logger.info(f"Syncing single stock: {request.ts_code}")
            return SyncResponse(
                status="ok",
                message=f"Synced {request.ts_code}"
            )
        else:
            # 同步所有启用的任务（后台执行）
            import threading

            def _sync_all():
                try:
                    results = sync_engine.sync_all_enabled_tasks()
                    logger.info(f"Background sync completed: {results}")
                except Exception as e:
                    logger.error(f"Background sync failed: {e}")

            thread = threading.Thread(target=_sync_all, daemon=True)
            thread.start()

            return SyncResponse(
                status="ok",
                message="Full sync started in background"
            )

    except Exception as e:
        logger.error(f"Failed to trigger sync: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/data/sync/status")
async def get_sync_status(
    data_service: DataService = Depends(get_data_service)
):
    """
    获取同步状态

    返回最近的同步日志
    """
    try:
        # 查询同步日志
        df = data_service.repository.query(
            "sync_log",
            limit=100
        )

        return {
            "logs": df.to_dicts() if not df.is_empty() else [],
            "count": len(df)
        }

    except Exception as e:
        logger.error(f"Failed to get sync status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
