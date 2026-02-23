from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.logger import logger
from app.core.exceptions import QuantException, quant_exception_handler, general_exception_handler
from app.api.v1 import data_merged as data, factor, strategy, ml, production
from data_manager.scheduler import sync_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    logger.info("Starting application...")
    try:
        sync_scheduler.start()
        sync_scheduler.load_schedules_from_config()
        logger.info("Sync scheduler started and loaded schedules")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")

    yield

    # 关闭时执行
    logger.info("Shutting down application...")
    try:
        sync_scheduler.shutdown()
        logger.info("Sync scheduler stopped")
    except Exception as e:
        logger.error(f"Failed to stop scheduler: {e}")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 异常处理器
    app.add_exception_handler(QuantException, quant_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)

    # 路由注册（使用合并后的 data 路由）
    app.include_router(data.router, prefix=settings.api_v1_prefix, tags=["data"])
    app.include_router(factor.router, prefix=settings.api_v1_prefix, tags=["factor"])
    app.include_router(strategy.router, prefix=settings.api_v1_prefix, tags=["strategy"])
    app.include_router(ml.router, prefix=settings.api_v1_prefix, tags=["ml"])
    app.include_router(production.router, prefix=settings.api_v1_prefix, tags=["production"])

    return app


app = create_app()
