"""
输入验证工具 — 防止 DolphinDB SQL 注入和路径遍历
"""
import re
from pathlib import Path

_SAFE_IDENTIFIER_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')


def validate_table_name(table_name: str) -> str:
    """验证 DolphinDB 表名，防止 SQL 注入。返回原值或抛出 ValueError。"""
    if not table_name or not _SAFE_IDENTIFIER_RE.match(table_name):
        raise ValueError(f"Invalid table_name: '{table_name}'")
    return table_name


def validate_factor_id(factor_id: str) -> str:
    """验证因子 ID，防止 SQL 注入和路径遍历。返回原值或抛出 ValueError。"""
    if not factor_id or not _SAFE_IDENTIFIER_RE.match(factor_id):
        raise ValueError(f"Invalid factor_id: '{factor_id}'")
    return factor_id


def validate_path_within(base_dir: Path, target_path: Path) -> Path:
    """验证 target_path 在 base_dir 内，防止路径遍历。返回 resolved 路径或抛出 ValueError。"""
    resolved_base = base_dir.resolve()
    resolved_target = target_path.resolve()
    if not str(resolved_target).startswith(str(resolved_base)):
        raise ValueError(f"Path traversal detected: '{target_path}' is outside '{base_dir}'")
    return resolved_target
