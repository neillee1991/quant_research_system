from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class ConfigType(str, Enum):
    SYNC_TASKS = "sync_tasks"
    ETL_TASKS = "etl_tasks"
    FACTOR_CONFIGS = "factor_configs"
    DATA_FIELD_MAPPINGS = "data_field_mappings"
    FLOW_CONFIGS = "flow_configs"


class ImportMode(str, Enum):
    FAST = "fast"
    SAFE = "safe"


class ExportRequest(BaseModel):
    config_types: List[ConfigType] = Field(..., description="要导出的配置类型列表")


class ExportResponse(BaseModel):
    filename: str = Field(..., description="导出文件名")
    content: str = Field(..., description="Base64 编码的 JSON 内容")


class ConfigItemDiff(BaseModel):
    item_id: str = Field(..., description="配置项 ID")
    status: str = Field(..., description="状态: new/modified/unchanged/deleted")
    current: Optional[Dict[str, Any]] = Field(None, description="当前配置（如存在）")
    imported: Optional[Dict[str, Any]] = Field(None, description="导入配置（如存在）")


class ConfigTypeDiff(BaseModel):
    config_type: ConfigType = Field(..., description="配置类型")
    items: List[ConfigItemDiff] = Field(default_factory=list, description="配置项差异列表")
    summary: Dict[str, int] = Field(default_factory=dict, description="统计: new/modified/unchanged/deleted")


class ImportVerifyRequest(BaseModel):
    content: str = Field(..., description="Base64 编码的 JSON 内容")
    mode: ImportMode = Field(..., description="导入模式")


class ImportVerifyResponse(BaseModel):
    valid: bool = Field(..., description="是否验证通过")
    errors: List[str] = Field(default_factory=list, description="错误列表")
    diffs: Optional[List[ConfigTypeDiff]] = Field(None, description="差异预览（安全模式）")


class ImportApplyRequest(BaseModel):
    content: str = Field(..., description="Base64 编码的 JSON 内容")
    mode: ImportMode = Field(..., description="导入模式")
    selections: Optional[Dict[ConfigType, List[str]]] = Field(None, description="选中的配置项 ID 列表")


class ImportResultSummary(BaseModel):
    created: int = Field(0, description="新创建数量")
    updated: int = Field(0, description="更新数量")
    skipped: int = Field(0, description="跳过数量")


class ImportApplyResponse(BaseModel):
    success: bool = Field(..., description="是否成功")
    summary: Dict[ConfigType, ImportResultSummary] = Field(default_factory=dict, description="各类型导入结果统计")
    errors: List[str] = Field(default_factory=list, description="错误列表")


class BackupFile(BaseModel):
    version: str = Field("1.0", description="备份文件格式版本")
    exported_at: datetime = Field(..., description="导出时间")
    system_version: str = Field("1.0.0", description="系统版本")
    configs: Dict[ConfigType, List[Dict[str, Any]]] = Field(default_factory=dict, description="配置数据")
