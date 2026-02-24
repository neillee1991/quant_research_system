from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import settings
from app.core.logger import logger
from app.core.exceptions import QuantException, quant_exception_handler, general_exception_handler
from app.api.v1 import data_merged as data, factor, strategy, ml, production


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("Starting application...")
    yield
    logger.info("Shutting down application...")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )

    # GZip 压缩中间件
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # CORS 中间件
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

    # 路由注册
    app.include_router(data.router, prefix=settings.api_v1_prefix, tags=["data"])
    app.include_router(factor.router, prefix=settings.api_v1_prefix, tags=["factor"])
    app.include_router(strategy.router, prefix=settings.api_v1_prefix, tags=["strategy"])
    app.include_router(ml.router, prefix=settings.api_v1_prefix, tags=["ml"])
    app.include_router(production.router, prefix=settings.api_v1_prefix, tags=["production"])

    return app


app = create_app()
