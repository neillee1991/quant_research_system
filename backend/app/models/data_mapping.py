"""
数据映射模型
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional


class DataMapping(BaseModel):
    """数据字段映射配置"""
    id: Optional[str] = Field(None, description="映射ID")
    name: str = Field(..., description="映射名称")
    price_table: str = Field(default="daily_data", description="价格表名")
    factor_table: str = Field(default="factor_values", description="因子表名")
    field_mappings: Dict[str, str] = Field(..., description="字段映射关系：{引擎字段名 -> 数据表字段名}")

    class Config:
        extra = "allow"
