"""
配置管理相关的常量
"""
from typing import Dict, List
from app.models.config_import_export import ConfigType

# 配置类型到表名的映射（迁移到 PostgreSQL 后的新表名）
CONFIG_TYPE_TABLE_MAP: Dict[ConfigType, str] = {
    ConfigType.SYNC_TASKS: "sync_task_configs",
    ConfigType.ETL_TASKS: "etl_task_configs",
    ConfigType.FACTOR_CONFIGS: "factor_configs",
    ConfigType.DATA_FIELD_MAPPINGS: "data_field_mappings",
    ConfigType.FLOW_CONFIGS: "flow_configs",
}

# 配置类型到ID字段的映射
CONFIG_TYPE_ID_FIELD_MAP: Dict[ConfigType, str] = {
    ConfigType.SYNC_TASKS: "task_id",
    ConfigType.ETL_TASKS: "task_id",
    ConfigType.FACTOR_CONFIGS: "factor_id",
    ConfigType.DATA_FIELD_MAPPINGS: "field_key",
    ConfigType.FLOW_CONFIGS: "name",
}

# 允许的表名白名单
ALLOWED_TABLES: List[str] = list(CONFIG_TYPE_TABLE_MAP.values())

# 允许的ID字段白名单
ALLOWED_ID_FIELDS: List[str] = list(CONFIG_TYPE_ID_FIELD_MAP.values())


def get_validated_table_name(config_type: ConfigType) -> str:
    """
    获取验证过的表名

    Args:
        config_type: 配置类型

    Returns:
        验证过的表名

    Raises:
        ValueError: 如果配置类型无效
    """
    table_name = CONFIG_TYPE_TABLE_MAP.get(config_type)
    if not table_name:
        raise ValueError(f"Invalid config type: {config_type}")
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def get_validated_id_field(config_type: ConfigType) -> str:
    """
    获取验证过的ID字段名

    Args:
        config_type: 配置类型

    Returns:
        验证过的ID字段名

    Raises:
        ValueError: 如果配置类型无效
    """
    id_field = CONFIG_TYPE_ID_FIELD_MAP.get(config_type)
    if not id_field:
        raise ValueError(f"Invalid config type: {config_type}")
    if id_field not in ALLOWED_ID_FIELDS:
        raise ValueError(f"Invalid ID field: {id_field}")
    return id_field
