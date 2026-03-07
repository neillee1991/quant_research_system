from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import settings
from app.core.logger import logger
from app.core.exceptions import QuantException, quant_exception_handler, general_exception_handler
from app.api.v1 import factor, strategy, ml, flows, versions
from app.api.v1 import production  # 使用拆分后的 production 模块
from app.api.v1 import data  # 使用拆分后的 data 模块
from app.api.v1.generic_task import create_task_router
from app.services import sync_service, etl_service, factor_service
from store.dolphindb_client import db_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("Starting application...")
    # 动态创建缺失的维度表（使用全局单例，避免重复连接）
    try:
        db_client.ensure_meta_tables()
    except Exception as e:
        logger.error(f"创建维度表失败: {e}")
        raise  # 表结构创建失败应该终止启动

    # Seed 数据（失败不影响启动）
    for seed_func, name in [
        (db_client.seed_sync_task_config, "同步任务配置"),
        (db_client.seed_etl_task_config, "ETL任务配置"),
        (db_client.seed_factor_data_config, "因子数据配置"),
        (db_client.seed_factor_metadata, "因子元数据"),
    ]:
        try:
            seed_func()
        except Exception as e:
            logger.error(f"Seed {name} 失败: {e}", exc_info=True)

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
    origins = settings.cors_origins
    if origins == "*":
        allow_origins = ["*"]
        allow_credentials = False  # wildcard origin is incompatible with credentials
    else:
        allow_origins = [o.strip() for o in origins.split(",") if o.strip()]
        allow_credentials = True
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
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
    app.include_router(flows.router, prefix=settings.api_v1_prefix, tags=["flows"])
    app.include_router(versions.router, prefix=settings.api_v1_prefix, tags=["versions"])

    # 新的通用任务管理路由
    app.include_router(
        create_task_router(sync_service, f"{settings.api_v1_prefix}/sync", ["Sync Tasks"]),
    )
    app.include_router(
        create_task_router(etl_service, f"{settings.api_v1_prefix}/etl", ["ETL Tasks"]),
    )
    app.include_router(
        create_task_router(factor_service, f"{settings.api_v1_prefix}/factors", ["Factor Tasks"]),
    )

    return app


app = create_app()
