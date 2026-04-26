"""
数据字段映射服务
"""
from typing import Dict, Any, List
from app.models.data_mapping import DataMapping


class DataMappingService:
    """数据字段映射服务"""

    def __init__(self):
        # 内存存储，实际应使用数据库
        self._mappings: Dict[str, DataMapping] = {}

    def list_mappings(self) -> List[DataMapping]:
        """列出所有数据映射"""
        return list(self._mappings.values())

    def get_mapping(self, mapping_id: str) -> DataMapping:
        """获取数据映射"""
        if mapping_id not in self._mappings:
            raise ValueError(f"Mapping {mapping_id} not found")
        return self._mappings[mapping_id]

    def create_mapping(self, mapping: DataMapping) -> DataMapping:
        """创建数据映射"""
        self._mappings[mapping.id] = mapping
        return mapping

    def delete_mapping(self, mapping_id: str) -> None:
        """删除数据映射"""
        if mapping_id not in self._mappings:
            raise ValueError(f"Mapping {mapping_id} not found")
        del self._mappings[mapping_id]

    def apply_mapping(self, config: Dict[str, Any], mapping_id: str) -> Dict[str, Any]:
        """应用数据字段映射到查询配置"""
        mapping = self.get_mapping(mapping_id)
        new_config = config.copy()

        # 应用价格字段映射
        price_fields = ["open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down"]
        for field in price_fields:
            if field in mapping.field_mappings:
                new_config[f"{field}_field"] = mapping.field_mappings[field]

        return new_config
