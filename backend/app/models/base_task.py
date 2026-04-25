"""
任务配置基础模型
"""
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field


class BaseTaskConfig(BaseModel):
    """任务配置基类 - 包含通用字段"""

    description: str = Field(default="", description="描述")
    enabled: bool = Field(default=True, description="是否启用")
    created_at: Optional[datetime] = Field(default=None, description="创建时间")
    updated_at: Optional[datetime] = Field(default=None, description="更新时间")

    class Config:
        extra = "forbid"

    @classmethod
    def from_row(cls, row: dict) -> "BaseTaskConfig":
        """从数据库行构造模型，过滤掉模型未定义的列"""
        fields = set(cls.model_fields.keys())
        return cls(**{k: v for k, v in row.items() if k in fields})

    def to_dict_with_parsed_json(self) -> dict:
        """返回字典（JSONB 迁移后字段已是原生类型，保留此方法兼容调用方）"""
        return self.model_dump()


class SyncTaskConfig(BaseTaskConfig):
    """同步任务配置"""

    task_id: str = Field(..., description="任务ID")
    api_name: str = Field(..., description="API名称")
    api_limit: int = Field(default=5000, description="API限制")
    sync_type: str = Field(default="incremental", description="同步类型: full/incremental")
    params_json: dict = Field(default_factory=dict, description="API参数")
    date_field: str = Field(default="", description="日期字段名")
    primary_keys_json: list = Field(default_factory=list, description="主键列表")
    table_name: str = Field(..., description="目标表名")
    schema_json: dict = Field(default_factory=dict, description="表结构")
    column_mapping_json: Optional[dict] = Field(default=None, description="列名映射，如 {\"con_code\": \"ts_code\"}")
    source: Optional[str] = Field(default=None, description="数据来源，如 tushare/akshare")


class ETLTaskConfig(BaseTaskConfig):
    """ETL任务配置"""

    task_id: str = Field(..., description="任务ID")
    script: str = Field(..., description="ETL脚本")
    sync_type: str = Field(default="full", description="同步类型: full/incremental")
    date_field: Optional[str] = Field(default=None, description="日期字段名")
    primary_keys_json: list = Field(default_factory=list, description="主键列表")
    table_name: str = Field(..., description="目标表名")

    # 以下三个字段已废弃，保留仅为兼容旧数据，下个迭代执行迁移删除
    source_table: Optional[str] = Field(default=None, description="[已废弃] 源表名")
    target_table: Optional[str] = Field(default=None, description="[已废弃] 目标表名")
    schedule: Optional[str] = Field(default=None, description="[已废弃] 调度表达式")


class FactorConfig(BaseTaskConfig):
    """因子配置"""

    factor_id: str = Field(..., description="因子ID")
    code: str = Field(..., description="因子代码")
    depends_on: str = Field(default="", description="依赖数据源")
    params: dict = Field(default_factory=dict, description="参数")
    lookback_days: int = Field(default=250, description="回溯天数")
