"""Schema 工具 API

提供 Schema 相关工具端点：
- 从 Tushare API 生成 schema
- 验证 schema 定义
- 比较 schema 差异
- 验证 ETL 脚本
- 预览 ETL 输出
"""
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from data_manager.schema_generator import generate_schema_from_api
from app.validators.schema_validator import SchemaValidator
from app.core.config import settings
from app.core.logger import logger
from store.dolphindb_client import db_client
import polars as pl


router = APIRouter()


# ==================== 请求/响应模型 ====================

class GenerateSchemaRequest(BaseModel):
    """生成 Schema 请求"""
    api_name: str = Field(..., description="Tushare API 名称，如 'daily', 'daily_basic'")
    sample_params: Optional[Dict[str, Any]] = Field(default=None, description="API 调用参数")


class GenerateSchemaResponse(BaseModel):
    """生成 Schema 响应"""
    status: str
    schema: Dict[str, Dict]
    message: Optional[str] = None


class ValidateSchemaRequest(BaseModel):
    """验证 Schema 请求"""
    schema: Dict[str, Dict] = Field(..., description="Schema 定义")
    primary_keys: Optional[List[str]] = Field(default=None, description="主键列表")


class ValidateSchemaResponse(BaseModel):
    """验证 Schema 响应"""
    is_valid: bool
    errors: List[str]
    message: str


class CompareSchemaRequest(BaseModel):
    """比较 Schema 请求"""
    old_schema: Dict[str, Dict] = Field(..., description="旧 Schema")
    new_schema: Dict[str, Dict] = Field(..., description="新 Schema")


class CompareSchemaResponse(BaseModel):
    """比较 Schema 响应"""
    is_compatible: bool
    errors: List[str]
    changes: Dict[str, List]
    message: str


class ValidateETLScriptRequest(BaseModel):
    """验证 ETL 脚本请求"""
    script: str = Field(..., description="ETL 脚本代码")


class ValidateETLScriptResponse(BaseModel):
    """验证 ETL 脚本响应"""
    is_valid: bool
    errors: List[str]
    message: str


class PreviewETLOutputRequest(BaseModel):
    """预览 ETL 输出请求"""
    script: str = Field(..., description="ETL 脚本代码")
    limit: int = Field(default=10, ge=1, le=100, description="返回行数限制")


class PreviewETLOutputResponse(BaseModel):
    """预览 ETL 输出响应"""
    status: str
    preview_data: List[Dict]
    inferred_schema: Dict[str, Dict]
    row_count: int
    message: Optional[str] = None


# ==================== Schema 生成端点 ====================

@router.post("/schema/generate", response_model=GenerateSchemaResponse)
def generate_schema(request: GenerateSchemaRequest):
    """
    从 Tushare API 生成 Schema

    通过调用 Tushare API 获取样本数据，自动推断字段类型并生成 DolphinDB schema 定义
    """
    try:
        token = settings.data_collector.tushare_token
        if not token:
            raise HTTPException(
                status_code=400,
                detail="Tushare token not configured. Please set TUSHARE_TOKEN in .env"
            )

        logger.info(f"Generating schema for API: {request.api_name}")

        schema = generate_schema_from_api(
            api_name=request.api_name,
            token=token,
            sample_params=request.sample_params or {}
        )

        return GenerateSchemaResponse(
            status="success",
            schema=schema,
            message=f"Successfully generated schema for {request.api_name} with {len(schema)} fields"
        )

    except ValueError as e:
        logger.error(f"Failed to generate schema: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error generating schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Schema 验证端点 ====================

@router.post("/schema/validate", response_model=ValidateSchemaResponse)
def validate_schema(request: ValidateSchemaRequest):
    """
    验证 Schema 定义

    检查 schema 格式是否正确，字段类型是否有效，主键是否存在等
    """
    try:
        is_valid, errors = SchemaValidator.validate_schema(
            schema=request.schema,
            primary_keys=request.primary_keys
        )

        message = "Schema is valid" if is_valid else f"Schema validation failed with {len(errors)} error(s)"

        return ValidateSchemaResponse(
            is_valid=is_valid,
            errors=errors,
            message=message
        )

    except Exception as e:
        logger.error(f"Error validating schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Schema 比较端点 ====================

@router.post("/schema/compare", response_model=CompareSchemaResponse)
def compare_schemas(request: CompareSchemaRequest):
    """
    比较两个 Schema

    检查 schema 演化是否兼容（允许新增字段，禁止删除字段或修改类型）
    """
    try:
        is_compatible, errors, changes = SchemaValidator.compare_schemas(
            old_schema=request.old_schema,
            new_schema=request.new_schema
        )

        change_summary = []
        if changes["added"]:
            change_summary.append(f"{len(changes['added'])} field(s) added")
        if changes["removed"]:
            change_summary.append(f"{len(changes['removed'])} field(s) removed")
        if changes["type_changed"]:
            change_summary.append(f"{len(changes['type_changed'])} type(s) changed")
        if changes["nullable_changed"]:
            change_summary.append(f"{len(changes['nullable_changed'])} nullable(s) changed")

        message = (
            f"Schemas are compatible. {', '.join(change_summary) if change_summary else 'No changes detected'}"
            if is_compatible
            else f"Schemas are incompatible. {', '.join(change_summary)}"
        )

        return CompareSchemaResponse(
            is_compatible=is_compatible,
            errors=errors,
            changes=changes,
            message=message
        )

    except Exception as e:
        logger.error(f"Error comparing schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ETL 脚本验证端点 ====================

@router.post("/etl/validate-script", response_model=ValidateETLScriptResponse)
def validate_etl_script(request: ValidateETLScriptRequest):
    """
    验证 ETL 脚本语法

    尝试编译 Python 脚本，检查语法错误
    """
    errors = []

    try:
        compile(request.script, "<etl_script>", "exec")

        return ValidateETLScriptResponse(
            is_valid=True,
            errors=[],
            message="ETL script syntax is valid"
        )

    except SyntaxError as e:
        errors.append(f"Syntax error at line {e.lineno}: {e.msg}")
        return ValidateETLScriptResponse(
            is_valid=False,
            errors=errors,
            message="ETL script has syntax errors"
        )

    except Exception as e:
        errors.append(f"Compilation error: {str(e)}")
        return ValidateETLScriptResponse(
            is_valid=False,
            errors=errors,
            message="ETL script compilation failed"
        )


# ==================== ETL 输出预览端点 ====================

@router.post("/etl/preview-output", response_model=PreviewETLOutputResponse)
def preview_etl_output(request: PreviewETLOutputRequest):
    """
    预览 ETL 输出

    执行 ETL 脚本并返回前 N 行数据，同时推断输出 schema
    """
    try:
        namespace = {
            "pl": pl,
            "db_client": db_client,
        }

        exec(request.script, namespace)

        if "result" not in namespace:
            raise ValueError(
                "ETL script must assign output to 'result' variable. "
                "Example: result = df.select([...])"
            )

        result_df = namespace["result"]

        if not isinstance(result_df, pl.DataFrame):
            raise ValueError(
                f"ETL script 'result' must be a Polars DataFrame, got {type(result_df).__name__}"
            )

        preview_df = result_df.head(request.limit)
        preview_data = preview_df.to_dicts()

        inferred_schema = {}
        for col_name in result_df.columns:
            dtype = str(result_df[col_name].dtype)

            dtype_map = {
                "Int64": "LONG",
                "Int32": "INT",
                "Float64": "DOUBLE",
                "Float32": "FLOAT",
                "Boolean": "BOOL",
                "Utf8": "SYMBOL",
                "Date": "DATE",
                "Datetime": "TIMESTAMP",
            }

            ddb_type = dtype_map.get(dtype, "STRING")
            has_null = result_df[col_name].null_count() > 0

            inferred_schema[col_name] = {
                "type": ddb_type,
                "nullable": has_null,
                "comment": col_name
            }

        return PreviewETLOutputResponse(
            status="success",
            preview_data=preview_data,
            inferred_schema=inferred_schema,
            row_count=len(result_df),
            message=f"Preview showing {len(preview_data)} of {len(result_df)} rows"
        )

    except ValueError as e:
        logger.error(f"ETL preview validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.error(f"ETL preview execution error: {e}")
        raise HTTPException(status_code=500, detail=f"ETL execution failed: {str(e)}")
