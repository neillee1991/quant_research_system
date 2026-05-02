from typing import Dict, List, Any

from app.models.config_import_export import (
    ConfigType,
    ConfigItemDiff,
    ConfigTypeDiff,
)
from app.core.config_constants import get_validated_id_field


class ConfigDiffService:
    def compute_diff(
        self,
        current_configs: Dict[ConfigType, List[Dict[str, Any]]],
        imported_configs: Dict[ConfigType, List[Dict[str, Any]]]
    ) -> List[ConfigTypeDiff]:
        """
        计算当前配置和导入配置之间的差异

        Args:
            current_configs: 当前配置
            imported_configs: 导入配置

        Returns:
            差异列表
        """
        diffs = []

        all_config_types = set(current_configs.keys()) | set(imported_configs.keys())

        for config_type in all_config_types:
            type_diff = self._compute_type_diff(
                config_type,
                current_configs.get(config_type, []),
                imported_configs.get(config_type, [])
            )
            diffs.append(type_diff)

        return diffs

    def _compute_type_diff(
        self,
        config_type: ConfigType,
        current_items: List[Dict[str, Any]],
        imported_items: List[Dict[str, Any]]
    ) -> ConfigTypeDiff:
        """
        计算特定类型配置的差异

        Args:
            config_type: 配置类型
            current_items: 当前配置项
            imported_items: 导入配置项

        Returns:
            配置类型差异
        """
        id_field = get_validated_id_field(config_type)

        current_map = {item[id_field]: item for item in current_items if id_field in item}
        imported_map = {item[id_field]: item for item in imported_items if id_field in item}

        items = []
        summary = {"new": 0, "modified": 0, "unchanged": 0, "deleted": 0}

        # 检查新增和修改的项
        for item_id, imported_item in imported_map.items():
            current_item = current_map.get(item_id)
            if current_item is None:
                items.append(ConfigItemDiff(
                    item_id=item_id,
                    status="new",
                    current=None,
                    imported=imported_item
                ))
                summary["new"] += 1
            else:
                if self._items_equal(current_item, imported_item):
                    items.append(ConfigItemDiff(
                        item_id=item_id,
                        status="unchanged",
                        current=current_item,
                        imported=imported_item
                    ))
                    summary["unchanged"] += 1
                else:
                    items.append(ConfigItemDiff(
                        item_id=item_id,
                        status="modified",
                        current=current_item,
                        imported=imported_item
                    ))
                    summary["modified"] += 1

        # 检查删除的项（只在 current 中存在）
        for item_id, current_item in current_map.items():
            if item_id not in imported_map:
                items.append(ConfigItemDiff(
                    item_id=item_id,
                    status="deleted",
                    current=current_item,
                    imported=None
                ))
                summary["deleted"] += 1

        return ConfigTypeDiff(
            config_type=config_type,
            items=items,
            summary=summary
        )

    def _get_id_field(self, config_type: ConfigType) -> str:
        """
        获取配置类型的ID字段（保留向后兼容）

        Args:
            config_type: 配置类型

        Returns:
            ID字段名
        """
        return get_validated_id_field(config_type)

    def _items_equal(self, item1: Dict[str, Any], item2: Dict[str, Any]) -> bool:
        """
        比较两个配置项是否相等，忽略顺序

        Args:
            item1: 配置项1
            item2: 配置项2

        Returns:
            是否相等
        """
        def normalize(item):
            if isinstance(item, dict):
                return sorted((k, normalize(v)) for k, v in item.items() if k not in ['created_at', 'updated_at'])
            elif isinstance(item, list):
                return sorted(normalize(x) for x in item)
            else:
                return item

        return normalize(item1) == normalize(item2)
