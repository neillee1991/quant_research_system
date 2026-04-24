"""
共享表验证器
用于检查多个任务共享同一张表的情况
"""
from typing import List, Optional, Dict, Any
import json

from app.core.logger import logger


def _pg_query(sql: str, params: tuple) -> list:
    """用 psycopg2 同步查询 PostgreSQL，返回 RealDictRow 列表"""
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
                return cur.fetchall()
    finally:
        conn.close()


class SharedTableValidator:
    """共享表验证器，检查多个任务共享同一张表的情况"""

    def __init__(self):
        self.config_tables = ["sync_task_configs", "etl_task_configs"]

    def check_shared_table(
        self,
        table_name: str,
        exclude_task_id: Optional[str] = None,
        config_table: Optional[str] = None
    ) -> bool:
        """检查表是否被其他任务使用"""
        from infrastructure.database.dolphindb_client import db_client
        if not db_client.table_exists(table_name):
            return False
        sharing_tasks = self.get_sharing_tasks(table_name, exclude_task_id, config_table)
        return len(sharing_tasks) > 0

    def validate_shared_schema(
        self,
        table_name: str,
        schema: Dict[str, Any],
        primary_keys: List[str],
        exclude_task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        验证共享表的 schema 和主键一致性

        Args:
            table_name: 表名
            schema: 新任务的 schema 定义
            primary_keys: 新任务的主键列表
            exclude_task_id: 要排除的任务ID

        Returns:
            验证结果字典:
            {
                "valid": bool,
                "conflicts": List[str],  # 冲突描述列表
                "sharing_tasks": List[str]  # 共享该表的任务ID列表
            }
        """
        result = {
            "valid": True,
            "conflicts": [],
            "sharing_tasks": []
        }

        from infrastructure.database.dolphindb_client import db_client
        if not db_client.table_exists(table_name):
            return result

        sharing_tasks = self.get_sharing_tasks(table_name, exclude_task_id)
        result["sharing_tasks"] = sharing_tasks

        if not sharing_tasks:
            return result

        # 获取现有任务的 schema 和主键配置（仅 sync_task_configs 有 schema_json）
        conflicts = []
        for config_table in self.config_tables:
            try:
                if config_table == "etl_task_configs":
                    rows = _pg_query(
                        f"SELECT task_id, primary_keys_json FROM {config_table} WHERE table_name = %s",
                        (table_name,)
                    )
                else:
                    rows = _pg_query(
                        f"SELECT task_id, schema_json, primary_keys_json FROM {config_table} WHERE table_name = %s",
                        (table_name,)
                    )

                for row in rows:
                    task_id = row["task_id"]
                    if exclude_task_id and task_id == exclude_task_id:
                        continue

                    existing_schema_raw = row.get("schema_json")
                    existing_primary_keys_raw = row.get("primary_keys_json")

                    existing_schema = json.loads(existing_schema_raw) if isinstance(existing_schema_raw, str) else existing_schema_raw
                    existing_primary_keys = json.loads(existing_primary_keys_raw) if isinstance(existing_primary_keys_raw, str) else existing_primary_keys_raw

                    if existing_schema and schema:
                        schema_diff = self._compare_schemas(existing_schema, schema)
                        if schema_diff:
                            conflicts.append(f"任务 {task_id} 的 schema 不一致: {schema_diff}")

                    if existing_primary_keys and primary_keys:
                        if set(existing_primary_keys) != set(primary_keys):
                            conflicts.append(
                                f"任务 {task_id} 的主键不一致: 现有={existing_primary_keys}, 新={primary_keys}"
                            )

            except Exception as e:
                logger.error(f"验证配置表 {config_table} 时出错: {e}")
                conflicts.append(f"无法验证配置表 {config_table}: {str(e)}")

        if conflicts:
            result["valid"] = False
            result["conflicts"] = conflicts

        return result

    def get_sharing_tasks(
        self,
        table_name: str,
        exclude_task_id: Optional[str] = None,
        exclude_config_table: Optional[str] = None
    ) -> List[str]:
        """
        获取所有使用该表的任务列表

        Args:
            table_name: 表名
            exclude_task_id: 要排除的任务ID
            exclude_config_table: 要排除的配置表（该表中会排除 exclude_task_id）

        Returns:
            使用该表的任务ID列表
        """
        sharing_tasks = []

        for config_table in self.config_tables:
            try:
                if config_table == exclude_config_table and exclude_task_id:
                    rows = _pg_query(
                        f"SELECT task_id FROM {config_table} WHERE table_name = %s AND task_id != %s AND enabled = true",
                        (table_name, exclude_task_id)
                    )
                else:
                    rows = _pg_query(
                        f"SELECT task_id FROM {config_table} WHERE table_name = %s AND enabled = true",
                        (table_name,)
                    )
                sharing_tasks.extend(row["task_id"] for row in rows)
            except Exception as e:
                logger.error(f"查询配置表 {config_table} 时出错: {e}")

        return sharing_tasks

    def can_delete_table(
        self,
        table_name: str,
        task_id: str,
        config_table: str
    ) -> Dict[str, Any]:
        """
        判断是否可以安全删除表

        Args:
            table_name: 表名
            task_id: 当前任务ID
            config_table: 当前任务所在的配置表名

        Returns:
            判断结果字典:
            {
                "can_delete": bool,
                "reason": str,  # 不能删除的原因
                "sharing_tasks": List[str]  # 共享该表的其他任务
            }
        """
        result = {
            "can_delete": False,
            "reason": "",
            "sharing_tasks": []
        }

        from infrastructure.database.dolphindb_client import db_client
        if not db_client.table_exists(table_name):
            result["can_delete"] = True
            result["reason"] = "表不存在"
            return result

        sharing_tasks = self.get_sharing_tasks(table_name, task_id, config_table)
        result["sharing_tasks"] = sharing_tasks

        if sharing_tasks:
            result["can_delete"] = False
            result["reason"] = f"表被其他任务共用: {', '.join(sharing_tasks)}"
        else:
            result["can_delete"] = True
            result["reason"] = "表未被其他任务使用"

        return result

    def _compare_schemas(
        self,
        schema1: Dict[str, Any],
        schema2: Dict[str, Any]
    ) -> Optional[str]:
        """
        比较两个 schema 定义

        Args:
            schema1: 第一个 schema
            schema2: 第二个 schema

        Returns:
            差异描述，如果一致则返回 None
        """
        # 检查字段是否一致
        fields1 = set(schema1.keys())
        fields2 = set(schema2.keys())

        if fields1 != fields2:
            missing_in_2 = fields1 - fields2
            missing_in_1 = fields2 - fields1
            diff_parts = []
            if missing_in_2:
                diff_parts.append(f"缺少字段 {missing_in_2}")
            if missing_in_1:
                diff_parts.append(f"多余字段 {missing_in_1}")
            return ", ".join(diff_parts)

        # 检查字段类型是否一致
        type_diffs = []
        for field in fields1:
            type1 = schema1[field].get("type") if isinstance(schema1[field], dict) else schema1[field]
            type2 = schema2[field].get("type") if isinstance(schema2[field], dict) else schema2[field]
            if type1 != type2:
                type_diffs.append(f"{field}: {type1} vs {type2}")

        if type_diffs:
            return f"字段类型不一致: {', '.join(type_diffs)}"

        return None


# 单例实例
shared_table_validator = SharedTableValidator()


__all__ = ["SharedTableValidator", "shared_table_validator"]
