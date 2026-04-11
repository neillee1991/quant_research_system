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
from app.core.logger import logger


def _pg_query(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """用 psycopg2 同步查询 PostgreSQL，返回字典列表"""
    import psycopg2
    import psycopg2.extras
    from app.core.config import settings

    conn = psycopg2.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        dbname=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return list(cur.fetchall())
    finally:
        conn.close()


def _pg_execute(sql: str, params: tuple = ()) -> None:
    """用 psycopg2 同步执行 PostgreSQL SQL"""
    import psycopg2
    from app.core.config import settings

    conn = psycopg2.connect(
        host=settings.postgresql.postgres_host,
        port=settings.postgresql.postgres_port,
        dbname=settings.postgresql.postgres_db,
        user=settings.postgresql.postgres_user,
        password=settings.postgresql.postgres_password,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
    finally:
        conn.close()


class ConfigImportService:
    def __init__(self):
        self.diff_service = ConfigDiffService()
        self.export_service = ConfigExportService()

    def verify_import(
        self,
        content: str,
        mode: ImportMode
    ) -> Tuple[bool, List[str], Optional[List[ConfigTypeDiff]], Optional[BackupFile]]:
        """验证导入文件"""
        errors = []
        diffs = None
        backup = None

        try:
            backup = self._parse_backup_content(content)
            if not backup:
                errors.append("无法解析备份文件")
                return False, errors, None, None

            structure_valid, structure_errors = self._validate_backup_structure(backup)
            errors.extend(structure_errors)
            if not structure_valid:
                return False, errors, None, backup

            content_valid, content_errors = self._validate_config_contents(backup)
            errors.extend(content_errors)
            if not content_valid:
                return False, errors, None, backup

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
        """执行导入"""
        errors = []
        summary = {}

        try:
            valid, verify_errors, diffs, backup = self.verify_import(content, mode)
            errors.extend(verify_errors)
            if not valid or not backup:
                return False, summary, errors

            for config_type, items in backup.configs.items():
                id_field = get_validated_id_field(config_type)
                result = ImportResultSummary()

                items_to_import = items
                if mode == ImportMode.SAFE and selections:
                    selected_ids = selections.get(config_type, [])
                    items_to_import = [item for item in items if item.get(id_field) in selected_ids]

                for item in items_to_import:
                    item_id = item.get(id_field)
                    if not item_id:
                        result.skipped += 1
                        continue

                    exists = self._item_exists(config_type, item_id)

                    if exists:
                        if mode == ImportMode.FAST:
                            self._update_item(config_type, item)
                            result.updated += 1
                        else:
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
        """解析备份文件内容"""
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
        """验证备份文件结构"""
        errors = []
        if not backup.version:
            errors.append("缺少版本信息")
        if not backup.exported_at:
            errors.append("缺少导出时间")
        if not backup.configs:
            errors.append("没有配置数据")
        return len(errors) == 0, errors

    def _validate_config_contents(self, backup: BackupFile) -> Tuple[bool, List[str]]:
        """验证配置内容"""
        errors = []
        for config_type, items in backup.configs.items():
            id_field = get_validated_id_field(config_type)
            for idx, item in enumerate(items):
                if id_field not in item:
                    errors.append(f"{config_type}[{idx}]: 缺少 {id_field} 字段")
        return len(errors) == 0, errors

    def _get_current_configs(self, config_types: List[ConfigType]) -> Dict[ConfigType, List[Dict[str, Any]]]:
        """获取当前配置"""
        result = {}
        for config_type in config_types:
            result[config_type] = self.export_service._export_config_type(config_type)
        return result

    def _item_exists(self, config_type: ConfigType, item_id: str) -> bool:
        """检查配置项是否存在"""
        table_name = get_validated_table_name(config_type)
        id_field = get_validated_id_field(config_type)

        try:
            rows = _pg_query(
                f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE {id_field} = %s",
                (item_id,)
            )
            return rows and rows[0]['cnt'] > 0
        except Exception as e:
            logger.warning(f"检查配置项存在失败: {e}")
            return False

    def _create_item(self, config_type: ConfigType, item: Dict[str, Any]):
        """创建配置项"""
        table_name = get_validated_table_name(config_type)
        id_field = get_validated_id_field(config_type)

        item = item.copy()
        now = datetime.utcnow()
        item['created_at'] = now
        item['updated_at'] = now

        keys = list(item.keys())
        values = list(item.values())
        placeholders = ", ".join(["%s"] * len(keys))
        cols = ", ".join(keys)

        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT ({id_field}) DO NOTHING"
        _pg_execute(sql, tuple(values))

    def _update_item(self, config_type: ConfigType, item: Dict[str, Any]):
        """更新配置项"""
        table_name = get_validated_table_name(config_type)
        id_field = get_validated_id_field(config_type)
        item_id = item.get(id_field)
        if not item_id:
            return

        item = item.copy()
        item['updated_at'] = datetime.utcnow()
        if 'created_at' in item:
            del item['created_at']

        update_pairs = []
        values = []
        for key, value in item.items():
            if key != id_field:
                update_pairs.append(f"{key} = %s")
                values.append(value)
        values.append(item_id)

        sql = f"UPDATE {table_name} SET {', '.join(update_pairs)} WHERE {id_field} = %s"
        _pg_execute(sql, tuple(values))

    def _is_item_selected_for_update(
        self,
        config_type: ConfigType,
        item_id: str,
        diffs: Optional[List[ConfigTypeDiff]],
        selections: Optional[Dict[ConfigType, List[str]]]
    ) -> bool:
        """检查配置项是否被选中进行更新"""
        if not selections:
            return False
        selected_ids = selections.get(config_type, [])
        return item_id in selected_ids
