import base64
import json
from datetime import datetime
from typing import Dict, List, Any

from app.models.config_import_export import ConfigType, BackupFile
from app.core.config_constants import get_validated_table_name, get_validated_id_field
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


class ConfigExportService:
    def __init__(self):
        pass

    def export_configs(self, config_types: List[ConfigType]) -> Dict[str, Any]:
        """
        导出配置

        Args:
            config_types: 要导出的配置类型列表

        Returns:
            包含文件名和Base64编码内容的字典

        Raises:
            HTTPException: 导出失败时
        """
        backup = BackupFile(
            exported_at=datetime.utcnow(),
            configs={}
        )

        for config_type in config_types:
            backup.configs[config_type] = self._export_config_type(config_type)

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
        读取表数据并转换为字典列表（PostgreSQL 版本）

        Args:
            table_name: 表名（已验证）

        Returns:
            字典列表
        """
        try:
            rows = _pg_query(f"SELECT * FROM {table_name}")

            result = []
            for row in rows:
                cleaned_row = {}
                for key, value in row.items():
                    if key not in ['created_at', 'updated_at']:
                        if isinstance(value, datetime):
                            cleaned_row[key] = value.isoformat() if value else None
                        else:
                            cleaned_row[key] = value
                result.append(cleaned_row)
            return result
        except Exception as e:
            logger.error(f"读取表 {table_name} 失败: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read table {table_name}") from e
