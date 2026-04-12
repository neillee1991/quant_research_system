"""
Schema Validator

验证任务配置中的 schema 定义，确保：
1. Schema 格式正确（dict，每个字段有 type/nullable/comment）
2. 字段类型为有效的 DolphinDB 类型
3. 主键字段都在 schema 中
4. Schema 演化规则（只允许新增字段，不允许删除或修改类型）
"""
from typing import Dict, List, Set, Optional, Tuple


class SchemaValidator:
    """Schema 验证器"""

    VALID_DOLPHINDB_TYPES = {
        "BOOL",
        "CHAR",
        "SHORT",
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "SYMBOL",
        "DATE",
        "TIMESTAMP",
        "TIME",
    }

    REQUIRED_FIELD_KEYS = {"type"}

    @classmethod
    def validate_schema(
        cls,
        schema: Dict,
        primary_keys: Optional[List[str]] = None
    ) -> Tuple[bool, List[str]]:
        """
        验证 schema 定义

        Args:
            schema: schema 字典
            primary_keys: 主键列表（可选）

        Returns:
            (is_valid, errors): 验证结果和错误列表
        """
        errors = []

        if not isinstance(schema, dict):
            errors.append("Schema must be a dict")
            return False, errors

        if not schema:
            errors.append("Schema cannot be empty")
            return False, errors

        for field_name, field_def in schema.items():
            field_errors = cls._validate_field(field_name, field_def)
            errors.extend(field_errors)

        if primary_keys:
            pk_errors = cls._validate_primary_keys(schema, primary_keys)
            errors.extend(pk_errors)

        return len(errors) == 0, errors

    @classmethod
    def _validate_field(cls, field_name: str, field_def: Dict) -> List[str]:
        """验证单个字段定义"""
        errors = []

        if not isinstance(field_def, dict):
            errors.append(f"Field '{field_name}' definition must be a dict")
            return errors

        missing_keys = cls.REQUIRED_FIELD_KEYS - set(field_def.keys())
        if missing_keys:
            errors.append(
                f"Field '{field_name}' missing required keys: {missing_keys}"
            )

        if "type" in field_def:
            field_type = field_def["type"]
            if field_type not in cls.VALID_DOLPHINDB_TYPES:
                errors.append(
                    f"Field '{field_name}' has invalid type '{field_type}'. "
                    f"Valid types: {cls.VALID_DOLPHINDB_TYPES}"
                )

        if "nullable" in field_def:
            nullable = field_def["nullable"]
            if not isinstance(nullable, bool):
                errors.append(
                    f"Field '{field_name}' nullable must be bool, got {type(nullable).__name__}"
                )

        if "comment" in field_def:
            comment = field_def["comment"]
            if not isinstance(comment, str):
                errors.append(
                    f"Field '{field_name}' comment must be str, got {type(comment).__name__}"
                )

        return errors

    @classmethod
    def _validate_primary_keys(
        cls,
        schema: Dict,
        primary_keys: List[str]
    ) -> List[str]:
        """验证主键字段是否都在 schema 中"""
        errors = []
        schema_fields = set(schema.keys())

        for pk in primary_keys:
            if pk not in schema_fields:
                errors.append(f"Primary key '{pk}' not found in schema")

        return errors

    @classmethod
    def compare_schemas(
        cls,
        old_schema: Dict,
        new_schema: Dict
    ) -> Tuple[bool, List[str], Dict]:
        """
        比较新旧 schema，验证演化规则

        演化规则：
        - 允许：新增字段
        - 禁止：删除字段、修改字段类型

        Args:
            old_schema: 旧 schema
            new_schema: 新 schema

        Returns:
            (is_compatible, errors, changes): 兼容性、错误列表、变更详情
        """
        errors = []
        changes = {
            "added": [],
            "removed": [],
            "type_changed": [],
            "nullable_changed": [],
        }

        old_fields = set(old_schema.keys())
        new_fields = set(new_schema.keys())

        removed = old_fields - new_fields
        if removed:
            changes["removed"] = list(removed)
            errors.append(f"Cannot remove fields: {removed}")

        added = new_fields - old_fields
        if added:
            changes["added"] = list(added)

        common_fields = old_fields & new_fields
        for field in common_fields:
            old_def = old_schema[field]
            new_def = new_schema[field]

            if old_def.get("type") != new_def.get("type"):
                changes["type_changed"].append({
                    "field": field,
                    "old_type": old_def.get("type"),
                    "new_type": new_def.get("type"),
                })
                errors.append(
                    f"Cannot change type of field '{field}' "
                    f"from {old_def.get('type')} to {new_def.get('type')}"
                )

            if old_def.get("nullable") != new_def.get("nullable"):
                changes["nullable_changed"].append({
                    "field": field,
                    "old_nullable": old_def.get("nullable"),
                    "new_nullable": new_def.get("nullable"),
                })

        is_compatible = len(errors) == 0
        return is_compatible, errors, changes

    @classmethod
    def validate_schema_evolution(
        cls,
        old_schema: Dict,
        new_schema: Dict,
        primary_keys: Optional[List[str]] = None
    ) -> Tuple[bool, List[str]]:
        """
        验证 schema 演化（组合验证）

        Args:
            old_schema: 旧 schema
            new_schema: 新 schema
            primary_keys: 主键列表（可选）

        Returns:
            (is_valid, errors): 验证结果和错误列表
        """
        all_errors = []

        is_valid, errors = cls.validate_schema(new_schema, primary_keys)
        all_errors.extend(errors)

        is_compatible, evo_errors, _ = cls.compare_schemas(old_schema, new_schema)
        all_errors.extend(evo_errors)

        return len(all_errors) == 0, all_errors
