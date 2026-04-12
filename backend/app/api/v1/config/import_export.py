from fastapi import APIRouter, HTTPException
from typing import Dict, List

from app.models.config_import_export import (
    ConfigType,
    ImportMode,
    ExportRequest,
    ExportResponse,
    ImportVerifyRequest,
    ImportVerifyResponse,
    ImportApplyRequest,
    ImportApplyResponse,
)
from app.services.config_export_service import ConfigExportService
from app.services.config_import_service import ConfigImportService
from app.core.logger import logger

router = APIRouter()
export_service = ConfigExportService()
import_service = ConfigImportService()


@router.post("/config/export", response_model=ExportResponse)
def export_configs(request: ExportRequest) -> ExportResponse:
    """
    导出配置

    Args:
        request: 导出请求

    Returns:
        导出响应

    Raises:
        HTTPException: 导出失败时
    """
    try:
        logger.info(f"导出配置: {request.config_types}")
        result = export_service.export_configs(request.config_types)
        return ExportResponse(**result)
    except RuntimeError as e:
        logger.error(f"导出失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="导出配置失败")
    except Exception as e:
        logger.error(f"导出过程发生错误: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="导出过程发生内部错误")


@router.post("/config/import/verify", response_model=ImportVerifyResponse)
def verify_import(request: ImportVerifyRequest) -> ImportVerifyResponse:
    """
    验证导入文件并预览差异

    Args:
        request: 验证请求

    Returns:
        验证响应

    Raises:
        HTTPException: 验证失败时
    """
    try:
        logger.info(f"验证导入文件，模式: {request.mode}")
        valid, errors, diffs, _ = import_service.verify_import(request.content, request.mode)
        return ImportVerifyResponse(
            valid=valid,
            errors=errors,
            diffs=diffs if request.mode == ImportMode.SAFE else None
        )
    except Exception as e:
        logger.error(f"验证失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="验证过程发生内部错误")


@router.post("/config/import/apply", response_model=ImportApplyResponse)
def apply_import(request: ImportApplyRequest) -> ImportApplyResponse:
    """
    执行导入

    Args:
        request: 导入请求

    Returns:
        导入响应

    Raises:
        HTTPException: 导入失败时
    """
    try:
        logger.info(f"执行导入，模式: {request.mode}")
        success, summary, errors = import_service.apply_import(
            request.content,
            request.mode,
            request.selections
        )
        return ImportApplyResponse(
            success=success,
            summary=summary,
            errors=errors
        )
    except Exception as e:
        logger.error(f"导入失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="导入过程发生内部错误")


@router.get("/config/types", response_model=List[Dict[str, str]])
def get_config_types() -> List[Dict[str, str]]:
    """
    获取可用的配置类型列表（新旧值都包含，向后兼容）

    Returns:
        配置类型列表
    """
    return [
        {"value": ConfigType.SYNC_TASKS.value, "label": "同步任务配置"},
        {"value": ConfigType.ETL_TASKS.value, "label": "ETL任务配置"},
        {"value": ConfigType.FACTOR_CONFIGS.value, "label": "因子配置"},
        {"value": ConfigType.FACTOR_FIELD_MAPPINGS.value, "label": "因子字段映射配置"},
        {"value": ConfigType.FLOW_CONFIGS.value, "label": "调度Flow配置"},
    ]
