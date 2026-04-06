import base64
import json
from datetime import datetime
from typing import Dict, List, Any

from app.models.config_import_export import ConfigType, BackupFile
from app.core.config_constants import get_validated_table_name
from store.dolphindb_client import db_client
from app.core.logger import logger


class ConfigExportService:
    def __init__(self):
        self.ddb_client = db_client

    def export_configs(self, config_types: List[ConfigType]) -> Dict[str, Any]:
        """
        导出配置

        Args:
            config_types: 要导出的配置类型列表

        Returns:
            包含文件名和Base64编码内容的字典
        """
        backup = BackupFile(
            exported_at=datetime.utcnow(),
            configs={}
        )

        for config_type in config_types:
            backup.configs[config_type] = self._export_config_type(config_type)

        # 转换为 JSON 字符串并 Base64 编码
        json_str = json.dumps(backup.model_dump(), default=str, ensure_ascii=False)
        base64_content = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

        filename = f"config_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

        return {
            "filename": filename,
            "content": base64_content
        }

    def _export_config_type(self, config_type: ConfigType) -> List[Dict[str, Any]]:
        """
        导出指定类型的配置

        Args:
            config_type: 配置类型

        Returns:
            配置项列表
        """
        table_name = get_validated_table_name(config_type)
        return self._read_table_as_dicts(table_name)

    def _read_table_as_dicts(self, table_name: str) -> List[Dict[str, Any]]:
        """
        读取表数据并转换为字典列表

        Args:
            table_name: 表名（已验证）

        Returns:
            字典列表
        """
        try:
            # 使用验证过的表名，安全查询
            df = self.ddb_client.query(f"SELECT * FROM {table_name}")
            if df is None or df.is_empty():
                return []

            # 转换为字典列表
            result = []
            for row in df.to_dicts():
                # 清理不需要的字段（如 created_at, updated_at 等由系统自动管理的字段）
                cleaned_row = {}
                for key, value in row.items():
                    if key not in ['created_at', 'updated_at']:
                        cleaned_row[key] = value
                result.append(cleaned_row)
            return result
        except Exception as e:
            logger.error(f"读取表 {table_name} 失败: {e}", exc_info=True)
            # 重新抛出异常而不是静默失败
            raise RuntimeError(f"Failed to read table {table_name}") from e
