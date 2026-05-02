"""
任务配置基础模型
"""
import json
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict


class BaseTaskConfig(BaseModel):
    """任务配置基类 - 包含通用字段"""

    model_config = ConfigDict(extra="forbid")

    description: str = Field(default="", description="描述")
    enabled: bool = Field(default=True, description="是否启用")
    created_at: Optional[datetime] = Field(default=None, description="创建时间")
    updated_at: Optional[datetime] = Field(default=None, description="更新时间")

    @classmethod
    def from_row(cls, row: dict) -> "BaseTaskConfig":
        """从数据库行构造模型，过滤掉模型未定义的列"""
        fields = set(cls.model_fields.keys())
        return cls(**{k: v for k, v in row.items() if k in fields})

    def to_dict_with_parsed_json(self) -> dict:
        """序列化为字典，将 JSON 字符串字段自动解析为对象"""
        result = {}
        for key, value in self.model_dump().items():
            if isinstance(value, str):
                try:
                    result[key] = json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    result[key] = value
            else:
                result[key] = value
        return result


class SyncTaskConfig(BaseTaskConfig):
    """同步任务配置"""

    task_id: str = Field(..., description="任务ID")
    api_name: str = Field(..., description="API名称")
    api_limit: int = Field(default=5000, description="API限制")
    sync_type: str = Field(default="incremental", description="同步类型: full/incremental")
    params: dict = Field(default_factory=dict, description="API参数")
    date_field: str = Field(default="", description="日期字段名")
    primary_keys: list = Field(default_factory=list, description="主键列表")
    table_name: str = Field(..., description="目标表名")
    schema: dict = Field(default_factory=dict, description="表结构")
    column_mapping: Optional[dict] = Field(default=None, description="列名映射，如 {\"con_code\": \"ts_code\"}")
    source: Optional[str] = Field(default=None, description="数据来源，如 tushare/akshare")


class ETLTaskConfig(BaseTaskConfig):
    """ETL任务配置"""

    task_id: str = Field(..., description="任务ID")
    script: str = Field(..., description="ETL脚本")
    sync_type: str = Field(default="full", description="同步类型: full/incremental")
    date_field: Optional[str] = Field(default=None, description="日期字段名")
    primary_keys: list = Field(default_factory=list, description="主键列表")
    table_name: str = Field(..., description="目标表名")
    schema: dict = Field(default_factory=dict, description="表结构")
    params: dict = Field(default_factory=dict, description="参数")

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
