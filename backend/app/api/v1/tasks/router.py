"""
任务管理路由汇总
"""
from fastapi import APIRouter

from .monitor import router as monitor_router
from .etl import router as etl_router
from .execute import router as execute_router
from .crud import router as crud_router

router = APIRouter()

# 注意顺序：固定路径优先于参数路径
router.include_router(monitor_router)
router.include_router(etl_router)
router.include_router(execute_router)
router.include_router(crud_router)
