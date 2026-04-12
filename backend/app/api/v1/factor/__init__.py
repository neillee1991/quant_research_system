"""因子系统 API 路由聚合

将原 production.py 拆分为功能模块：
- factor_analysis.py: 因子分析端点
- factor_compute.py: 因子计算执行端点
- factor_registry.py: 因子注册和元数据管理端点
- factor_config.py: 配置和指数池管理端点
"""
from fastapi import APIRouter

from .factor_analysis import router as analysis_router
from .factor_compute import router as compute_router
from .factor_registry import router as registry_router

# 创建主路由
router = APIRouter()

# 聚合所有子路由
router.include_router(analysis_router, tags=["因子分析"])
router.include_router(compute_router, tags=["因子计算"])
router.include_router(registry_router, tags=["因子注册"])
