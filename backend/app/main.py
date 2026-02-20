from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.exceptions import QuantException, quant_exception_handler, general_exception_handler
from app.api.v1 import data_merged as data, factor, strategy, ml


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
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
    app.include_router(data.router, prefix=settings.API_V1_PREFIX, tags=["data"])
    app.include_router(factor.router, prefix=settings.API_V1_PREFIX, tags=["factor"])
    app.include_router(strategy.router, prefix=settings.API_V1_PREFIX, tags=["strategy"])
    app.include_router(ml.router, prefix=settings.API_V1_PREFIX, tags=["ml"])

    return app


app = create_app()
