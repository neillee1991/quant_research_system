from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.core.config import settings
from app.core.logger import logger
from app.core.exceptions import QuantException, quant_exception_handler, general_exception_handler
from app.api.v1 import strategy, ml, flows, tasks, schema_tools
from app.api.v1 import factor  # 使用拆分后的 factor 模块
from app.api.v1 import data  # 使用拆分后的 data 模块
from app.api.v1 import config_api  # 配置管理 API
from store.dolphindb_client import db_client
from infrastructure.seed import SeedDataLoader, SeedDataManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("Starting application...")

    # 初始化 PostgreSQL 连接池（必须在调度器和 seed 之前）
    from scheduler.db import init_db, close_db
    await init_db()
    logger.info("PostgreSQL 连接池已初始化")

    # 启动调度器
    from scheduler.core import get_scheduler
    scheduler = get_scheduler()
    await scheduler.start()
    logger.info("调度器已启动")

    # 动态创建缺失的 DolphinDB 维度表（只保留时序数据表）
    try:
        db_client.ensure_meta_tables()
    except Exception as e:
        logger.error(f"创建维度表失败: {e}")
        raise  # 表结构创建失败应该终止启动

    # Seed 数据到 PostgreSQL（失败不影响启动）
    try:
        seed_manager = SeedDataManager(loader=SeedDataLoader())
        await seed_manager.seed_all()
    except Exception as e:
        logger.error(f"Seed 数据失败: {e}", exc_info=True)

    # 预加载因子字段映射缓存
    try:
        from engine.factor.data_config import DataConfigLoader
        _data_config = DataConfigLoader(db_client)
        await _data_config.refresh()
    except Exception as e:
        logger.warning(f"预加载因子字段映射缓存失败: {e}")

    # 清理僵尸任务：重启后所有 running 记录必然已中断
    try:
        from app.services.task_runner import TaskRunner
        cleaned = await TaskRunner.cleanup_stale(reason="server restart")
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} stale running tasks on startup")
    except Exception as e:
        logger.warning(f"Failed to cleanup stale tasks: {e}")

    yield

    logger.info("Shutting down application...")
    await scheduler.stop()
    logger.info("调度器已停止")
    await close_db()
    logger.info("PostgreSQL 连接池已关闭")


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
    app.include_router(strategy.router, prefix=settings.api_v1_prefix, tags=["strategy"])
    app.include_router(ml.router, prefix=settings.api_v1_prefix, tags=["ml"])
    app.include_router(factor.router, prefix=settings.api_v1_prefix, tags=["factor"])
    app.include_router(flows.router, prefix=settings.api_v1_prefix, tags=["flows"])
    app.include_router(schema_tools.router, prefix=settings.api_v1_prefix, tags=["schema-tools"])
    app.include_router(config_api.router, prefix=settings.api_v1_prefix, tags=["config"])

    # 统一任务管理路由
    app.include_router(tasks.router, prefix=settings.api_v1_prefix, tags=["tasks"])

    return app


app = create_app()
