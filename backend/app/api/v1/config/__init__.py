"""配置中心路由聚合"""
from fastapi import APIRouter

from .field_mappings import router as field_mappings_router
from .index_subscribe import router as index_subscribe_router
from .import_export import router as import_export_router

router = APIRouter()

router.include_router(field_mappings_router, tags=["字段映射配置"])
router.include_router(index_subscribe_router, tags=["指数订阅配置"])
router.include_router(import_export_router, tags=["配置导入导出"])
