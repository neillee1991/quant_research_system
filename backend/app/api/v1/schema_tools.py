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

