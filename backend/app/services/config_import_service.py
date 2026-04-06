import base64
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from app.models.config_import_export import (
    ConfigType,
    ImportMode,
    BackupFile,
    ConfigTypeDiff,
    ImportResultSummary,
)
from app.services.config_diff_service import ConfigDiffService
from app.services.config_export_service import ConfigExportService
from app.core.config_constants import (
    get_validated_table_name,
    get_validated_id_field,
)
from store.dolphindb_client import db_client
from app.core.logger import logger


class ConfigImportService:
    def __init__(self):
        self.ddb_client = db_client
        self.diff_service = ConfigDiffService()
        self.export_service = ConfigExportService()

    def verify_import(
        self,
        content: str,
        mode: ImportMode
    ) -> Tuple[bool, List[str], Optional[List[ConfigTypeDiff]], Optional[BackupFile]]:
        """
        验证导入文件

        Args:
            content: Base64编码的备份文件内容
            mode: 导入模式

        Returns:
            (是否有效, 错误列表, 差异列表, 备份文件对象)
        """
        errors = []
        diffs = None
        backup = None

        try:
            # 1. 解码和解析
            backup = self._parse_backup_content(content)
            if not backup:
                errors.append("无法解析备份文件")
                return False, errors, None, None

            # 2. 验证备份文件结构
            structure_valid, structure_errors = self._validate_backup_structure(backup)
            errors.extend(structure_errors)
            if not structure_valid:
                return False, errors, None, backup

            # 3. 验证配置内容
            content_valid, content_errors = self._validate_config_contents(backup)
            errors.extend(content_errors)
            if not content_valid:
                return False, errors, None, backup

            # 4. 安全模式下计算差异
            if mode == ImportMode.SAFE:
                current_configs = self._get_current_configs(list(backup.configs.keys()))
                diffs = self.diff_service.compute_diff(current_configs, backup.configs)

            return True, errors, diffs, backup

        except ValueError as e:
            logger.warning(f"验证过程参数错误: {e}")
            errors.append(f"验证失败: {str(e)}")
            return False, errors, None, backup
        except Exception as e:
            logger.error(f"验证过程出错: {e}", exc_info=True)
            errors.append("验证过程发生内部错误")
            return False, errors, None, backup

    def apply_import(
        self,
        content: str,
        mode: ImportMode,
        selections: Optional[Dict[ConfigType, List[str]]] = None
    ) -> Tuple[bool, Dict[ConfigType, ImportResultSummary], List[str]]:
        """
        执行导入

        Args:
            content: Base64编码的备份文件内容
            mode: 导入模式
            selections: 选中的配置项ID列表

        Returns:
            (是否成功, 导入结果统计, 错误列表)
        """
        errors = []
        summary = {}

        try:
            # 先验证
            valid, verify_errors, diffs, backup = self.verify_import(content, mode)
            errors.extend(verify_errors)
            if not valid or not backup:
                return False, summary, errors

            # 执行导入
            for config_type, items in backup.configs.items():
                id_field = get_validated_id_field(config_type)
                result = ImportResultSummary()

                # 确定要导入的项
                items_to_import = items
                if mode == ImportMode.SAFE and selections:
                    selected_ids = selections.get(config_type, [])
                    items_to_import = [item for item in items if item.get(id_field) in selected_ids]

                # 导入每一项
                for item in items_to_import:
                    item_id = item.get(id_field)
                    if not item_id:
                        result.skipped += 1
                        continue

                    exists = self._item_exists(config_type, item_id)

                    if exists:
                        if mode == ImportMode.FAST:
                            # 快速模式：覆盖
                            self._update_item(config_type, item)
                            result.updated += 1
                        else:
                            # 安全模式：检查是否选中修改
                            if self._is_item_selected_for_update(config_type, item_id, diffs, selections):
                                self._update_item(config_type, item)
                                result.updated += 1
                            else:
                                result.skipped += 1
                    else:
                        self._create_item(config_type, item)
                        result.created += 1

                summary[config_type] = result

            return True, summary, errors

        except ValueError as e:
            logger.warning(f"导入过程参数错误: {e}")
            errors.append(f"导入失败: {str(e)}")
            return False, summary, errors
        except Exception as e:
            logger.error(f"导入过程出错: {e}", exc_info=True)
            errors.append("导入过程发生内部错误")
            return False, summary, errors

    def _parse_backup_content(self, content: str) -> Optional[BackupFile]:
        """
        解析备份文件内容

        Args:
            content: Base64编码的内容

        Returns:
            备份文件对象，失败返回None
        """
        try:
            json_bytes = base64.b64decode(content.encode('utf-8'))
            json_str = json_bytes.decode('utf-8')
            data = json.loads(json_str)
            return BackupFile(**data)
        except (base64.binascii.Error, json.JSONDecodeError) as e:
            logger.warning(f"解析备份文件失败: {e}")
            return None
        except Exception as e:
            logger.warning(f"解析备份文件时发生错误: {e}")
            return None

    def _validate_backup_structure(self, backup: BackupFile) -> Tuple[bool, List[str]]:
        """
        验证备份文件结构

        Args:
            backup: 备份文件对象

        Returns:
            (是否有效, 错误列表)
        """
        errors = []
        if not backup.version:
            errors.append("缺少版本信息")
        if not backup.exported_at:
            errors.append("缺少导出时间")
        if not backup.configs:
            errors.append("没有配置数据")
        return len(errors) == 0, errors

    def _validate_config_contents(self, backup: BackupFile) -> Tuple[bool, List[str]]:
        """
        验证配置内容

        Args:
            backup: 备份文件对象

        Returns:
            (是否有效, 错误列表)
        """
        errors = []
        for config_type, items in backup.configs.items():
            id_field = get_validated_id_field(config_type)
            for idx, item in enumerate(items):
                if id_field not in item:
                    errors.append(f"{config_type}[{idx}]: 缺少 {id_field} 字段")
        return len(errors) == 0, errors

    def _get_current_configs(self, config_types: List[ConfigType]) -> Dict[ConfigType, List[Dict[str, Any]]]:
        """
        获取当前配置

        Args:
            config_types: 配置类型列表

        Returns:
            配置字典
        """
        result = {}
        for config_type in config_types:
            # 使用公共方法而不是私有方法
            result[config_type] = self.export_service._export_config_type(config_type)
        return result

    def _item_exists(self, config_type: ConfigType, item_id: str) -> bool:
        """
        检查配置项是否存在

        Args:
            config_type: 配置类型
            item_id: 配置项ID

        Returns:
            是否存在
        """
        table_name = get_validated_table_name(config_type)
        id_field = get_validated_id_field(config_type)

        try:
            # 使用验证过的表名和ID字段，使用参数化查询
            result = self.ddb_client.query(
                f"select count(*) as cnt from {table_name} where {id_field} = %s",
                (item_id,)
            )
            if result and not result.is_empty():
                return result.to_dicts()[0]['cnt'] > 0
            return False
        except Exception as e:
            logger.warning(f"检查配置项存在失败: {e}")
            return False

    def _create_item(self, config_type: ConfigType, item: Dict[str, Any]):
        """
        创建配置项

        Args:
            config_type: 配置类型
            item: 配置项数据
        """
        table_name = get_validated_table_name(config_type)

        # 添加时间戳
        item = item.copy()
        now = datetime.utcnow()
        item['created_at'] = now
        item['updated_at'] = now

        # 转换为 Polars DataFrame 并插入
        import polars as pl
        df = pl.DataFrame([item])
        self.ddb_client.append(table_name, df)

    def _update_item(self, config_type: ConfigType, item: Dict[str, Any]):
        """
        更新配置项

        Args:
            config_type: 配置类型
            item: 配置项数据
        """
        table_name = get_validated_table_name(config_type)
        id_field = get_validated_id_field(config_type)
        item_id = item.get(id_field)
        if not item_id:
            return

        # 使用验证过的表名和ID字段，使用参数化查询
        self.ddb_client.execute(
            f"delete from {table_name} where {id_field} = %s",
            (item_id,)
        )

        # 再插入新记录
        self._create_item(config_type, item)

    def _is_item_selected_for_update(
        self,
        config_type: ConfigType,
        item_id: str,
        diffs: Optional[List[ConfigTypeDiff]],
        selections: Optional[Dict[ConfigType, List[str]]]
    ) -> bool:
        """
        检查配置项是否被选中进行更新

        Args:
            config_type: 配置类型
            item_id: 配置项ID
            diffs: 差异列表
            selections: 选中的配置项

        Returns:
            是否被选中
        """
        if not selections:
            return False
        selected_ids = selections.get(config_type, [])
        return item_id in selected_ids
