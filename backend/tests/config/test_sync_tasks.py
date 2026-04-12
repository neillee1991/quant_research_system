# tests/config/test_sync_tasks.py
import json
import pytest
from pathlib import Path


def test_sync_tasks_file_exists():
    """测试 sync_tasks.json 文件存在"""
    config_path = Path(__file__).parent.parent.parent / "config/seed_data/sync_tasks.json"
    assert config_path.exists(), f"配置文件不存在: {config_path}"


def test_sync_tasks_file_is_valid_json():
    """测试 sync_tasks.json 是有效的 JSON"""
    config_path = Path(__file__).parent.parent.parent / "config/seed_data/sync_tasks.json"
    with open(config_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    assert isinstance(tasks, list), "配置应该是一个列表"
    assert len(tasks) > 0, "配置列表不应该为空"


def test_sync_index_basic_task_exists():
    """测试 sync_index_basic 任务存在"""
    config_path = Path(__file__).parent.parent.parent / "config/seed_data/sync_tasks.json"
    with open(config_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    task_ids = [t["task_id"] for t in tasks]
    assert "sync_index_basic" in task_ids, "sync_index_basic 任务应该存在"


def test_task_structure():
    """测试任务配置有正确的结构"""
    config_path = Path(__file__).parent.parent.parent / "config/seed_data/sync_tasks.json"
    with open(config_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    for task in tasks:
        # 检查必需字段
        assert "task_id" in task, f"任务缺少 task_id: {task}"
        assert "api_name" in task, f"任务缺少 api_name: {task}"
        assert "description" in task, f"任务缺少 description: {task}"
        assert "sync_type" in task, f"任务缺少 sync_type: {task}"
        assert "table_name" in task, f"任务缺少 table_name: {task}"
        assert "params" in task, f"任务缺少 params: {task}"
        assert "primary_keys" in task, f"任务缺少 primary_keys: {task}"
        assert "schema" in task, f"任务缺少 schema: {task}"


def test_index_basic_task_has_correct_schema():
    """测试 sync_index_basic 任务有正确的 schema"""
    config_path = Path(__file__).parent.parent.parent / "config/seed_data/sync_tasks.json"
    with open(config_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    index_basic_task = next((t for t in tasks if t["task_id"] == "sync_index_basic"), None)
    assert index_basic_task is not None, "找不到 sync_index_basic 任务"

    # 检查 schema 字段
    schema = index_basic_task["schema"]
    expected_fields = ["ts_code", "name", "market", "publisher", "list_date", "weight_rule", "desc", "exp_date", "updated_at"]
    for field in expected_fields:
        assert field in schema, f"schema 缺少字段: {field}"

