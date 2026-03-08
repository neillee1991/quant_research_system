"""
SharedTableValidator 单元测试
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl

from app.validators.shared_table_validator import SharedTableValidator, shared_table_validator


@pytest.fixture
def validator():
    """创建验证器实例"""
    return SharedTableValidator()


@pytest.fixture
def mock_db_client():
    """Mock DolphinDB 客户端"""
    # Patch the _get_db_client function to avoid actual connection
    with patch("store.dolphindb_client._get_db_client") as mock_get:
        mock_client = Mock()
        mock_get.return_value = mock_client
        yield mock_client


class TestCheckSharedTable:
    """测试 check_shared_table 方法"""

    def test_table_not_exists(self, validator, mock_db_client):
        """测试表不存在的情况"""
        mock_db_client.table_exists.return_value = False

        result = validator.check_shared_table("test_table")

        assert result is False
        mock_db_client.table_exists.assert_called_once_with("test_table")

    def test_no_sharing_tasks(self, validator, mock_db_client):
        """测试没有其他任务使用该表"""
        mock_db_client.table_exists.return_value = True
        mock_db_client.query.return_value = pl.DataFrame()

        result = validator.check_shared_table("test_table", "task1", "sync_task_config")

        assert result is False

    def test_has_sharing_tasks(self, validator, mock_db_client):
        """测试有其他任务使用该表"""
        mock_db_client.table_exists.return_value = True
        mock_db_client.query.side_effect = [
            pl.DataFrame(),  # sync_task_config 中没有其他任务
            pl.DataFrame({"task_id": ["task2", "task3"]})  # etl_task_config 中有任务
        ]

        result = validator.check_shared_table("test_table", "task1", "sync_task_config")

        assert result is True

    def test_exclude_task_id(self, validator, mock_db_client):
        """测试排除指定任务ID"""
        mock_db_client.table_exists.return_value = True
        mock_db_client.query.side_effect = [
            pl.DataFrame({"task_id": ["task2"]}),  # sync_task_config 中有其他任务
            pl.DataFrame()  # etl_task_config 中没有任务
        ]

        result = validator.check_shared_table("test_table", "task1", "sync_task_config")

        assert result is True
        # 验证查询参数
        calls = mock_db_client.query.call_args_list
        assert len(calls) == 2
        # 第一个查询应该排除 task1
        assert "task_id != %s" in calls[0][0][0]


class TestGetSharingTasks:
    """测试 get_sharing_tasks 方法"""

    def test_no_sharing_tasks(self, validator, mock_db_client):
        """测试没有共享任务"""
        mock_db_client.query.return_value = pl.DataFrame()

        result = validator.get_sharing_tasks("test_table")

        assert result == []
        assert mock_db_client.query.call_count == 2  # 查询两个配置表

    def test_sharing_tasks_in_sync_config(self, validator, mock_db_client):
        """测试在 sync_task_config 中有共享任务"""
        mock_db_client.query.side_effect = [
            pl.DataFrame({"task_id": ["task1", "task2"]}),
            pl.DataFrame()
        ]

        result = validator.get_sharing_tasks("test_table")

        assert result == ["task1", "task2"]

    def test_sharing_tasks_in_both_configs(self, validator, mock_db_client):
        """测试在两个配置表中都有共享任务"""
        mock_db_client.query.side_effect = [
            pl.DataFrame({"task_id": ["task1", "task2"]}),
            pl.DataFrame({"task_id": ["task3"]})
        ]

        result = validator.get_sharing_tasks("test_table")

        assert result == ["task1", "task2", "task3"]

    def test_exclude_task_from_config_table(self, validator, mock_db_client):
        """测试从指定配置表中排除任务"""
        mock_db_client.query.side_effect = [
            pl.DataFrame({"task_id": ["task2"]}),  # sync_task_config 排除了 task1
            pl.DataFrame({"task_id": ["task1", "task3"]})  # etl_task_config 包含所有任务
        ]

        result = validator.get_sharing_tasks("test_table", "task1", "sync_task_config")

        assert set(result) == {"task1", "task2", "task3"}

    def test_query_error_handling(self, validator, mock_db_client):
        """测试查询错误处理"""
        mock_db_client.query.side_effect = [
            Exception("Database error"),
            pl.DataFrame({"task_id": ["task1"]})
        ]

        result = validator.get_sharing_tasks("test_table")

        # 即使第一个查询失败，也应该继续查询第二个表
        assert result == ["task1"]


class TestCanDeleteTable:
    """测试 can_delete_table 方法"""

    def test_table_not_exists(self, validator, mock_db_client):
        """测试表不存在"""
        mock_db_client.table_exists.return_value = False

        result = validator.can_delete_table("test_table", "task1", "sync_task_config")

        assert result["can_delete"] is True
        assert result["reason"] == "表不存在"
        assert result["sharing_tasks"] == []

    def test_can_delete_no_sharing(self, validator, mock_db_client):
        """测试可以删除（没有其他任务使用）"""
        mock_db_client.table_exists.return_value = True
        mock_db_client.query.return_value = pl.DataFrame()

        result = validator.can_delete_table("test_table", "task1", "sync_task_config")

        assert result["can_delete"] is True
        assert result["reason"] == "表未被其他任务使用"
        assert result["sharing_tasks"] == []

    def test_cannot_delete_has_sharing(self, validator, mock_db_client):
        """测试不能删除（有其他任务使用）"""
        mock_db_client.table_exists.return_value = True
        mock_db_client.query.side_effect = [
            pl.DataFrame(),
            pl.DataFrame({"task_id": ["task2", "task3"]})
        ]

        result = validator.can_delete_table("test_table", "task1", "sync_task_config")

        assert result["can_delete"] is False
        assert "task2" in result["reason"]
        assert "task3" in result["reason"]
        assert result["sharing_tasks"] == ["task2", "task3"]


class TestValidateSharedSchema:
    """测试 validate_shared_schema 方法"""

    def test_table_not_exists(self, validator, mock_db_client):
        """测试表不存在"""
        mock_db_client.table_exists.return_value = False

        schema = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        primary_keys = ["col1"]

        result = validator.validate_shared_schema("test_table", schema, primary_keys)

        assert result["valid"] is True
        assert result["conflicts"] == []
        assert result["sharing_tasks"] == []

    def test_no_sharing_tasks(self, validator, mock_db_client):
        """测试没有共享任务"""
        mock_db_client.table_exists.return_value = True
        mock_db_client.query.return_value = pl.DataFrame()

        schema = {"col1": {"type": "STRING"}}
        primary_keys = ["col1"]

        result = validator.validate_shared_schema("test_table", schema, primary_keys)

        assert result["valid"] is True
        assert result["conflicts"] == []

    def test_schema_matches(self, validator, mock_db_client):
        """测试 schema 匹配"""
        mock_db_client.table_exists.return_value = True

        existing_schema = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        new_schema = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}

        # Mock get_sharing_tasks to return task list
        with patch.object(validator, 'get_sharing_tasks', return_value=["task1"]):
            # Create a mock DataFrame that supports iter_rows
            mock_df = Mock()
            mock_df.is_empty.return_value = False
            mock_df.iter_rows.return_value = [
                {
                    "task_id": "task1",
                    "schema": existing_schema,
                    "primary_keys": ["col1"]
                }
            ]

            mock_db_client.query.return_value = mock_df
            result = validator.validate_shared_schema("test_table", new_schema, ["col1"])

        assert result["valid"] is True
        assert result["conflicts"] == []
        assert result["sharing_tasks"] == ["task1"]

    def test_schema_field_mismatch(self, validator, mock_db_client):
        """测试 schema 字段不匹配"""
        mock_db_client.table_exists.return_value = True

        existing_schema = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        new_schema = {"col1": {"type": "STRING"}, "col3": {"type": "DOUBLE"}}

        with patch.object(validator, 'get_sharing_tasks', return_value=["task1"]):
            mock_df = Mock()
            mock_df.is_empty.return_value = False
            mock_df.iter_rows.return_value = [
                {
                    "task_id": "task1",
                    "schema": existing_schema,
                    "primary_keys": ["col1"]
                }
            ]

            mock_db_client.query.return_value = mock_df
            result = validator.validate_shared_schema("test_table", new_schema, ["col1"])

        assert result["valid"] is False
        assert len(result["conflicts"]) > 0
        assert "schema 不一致" in result["conflicts"][0]

    def test_schema_type_mismatch(self, validator, mock_db_client):
        """测试 schema 类型不匹配"""
        mock_db_client.table_exists.return_value = True

        existing_schema = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        new_schema = {"col1": {"type": "STRING"}, "col2": {"type": "DOUBLE"}}

        with patch.object(validator, 'get_sharing_tasks', return_value=["task1"]):
            mock_df = Mock()
            mock_df.is_empty.return_value = False
            mock_df.iter_rows.return_value = [
                {
                    "task_id": "task1",
                    "schema": existing_schema,
                    "primary_keys": ["col1"]
                }
            ]

            mock_db_client.query.return_value = mock_df
            result = validator.validate_shared_schema("test_table", new_schema, ["col1"])

        assert result["valid"] is False
        assert any("类型不一致" in conflict for conflict in result["conflicts"])

    def test_primary_key_mismatch(self, validator, mock_db_client):
        """测试主键不匹配"""
        mock_db_client.table_exists.return_value = True

        schema = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}

        with patch.object(validator, 'get_sharing_tasks', return_value=["task1"]):
            mock_df = Mock()
            mock_df.is_empty.return_value = False
            mock_df.iter_rows.return_value = [
                {
                    "task_id": "task1",
                    "schema": schema,
                    "primary_keys": ["col1"]
                }
            ]

            mock_db_client.query.return_value = mock_df
            result = validator.validate_shared_schema("test_table", schema, ["col1", "col2"])

        assert result["valid"] is False
        assert any("主键不一致" in conflict for conflict in result["conflicts"])

    def test_exclude_task_id(self, validator, mock_db_client):
        """测试排除指定任务ID"""
        mock_db_client.table_exists.return_value = True

        schema = {"col1": {"type": "STRING"}}

        with patch.object(validator, 'get_sharing_tasks', return_value=["task1", "task2"]):
            mock_df = Mock()
            mock_df.is_empty.return_value = False
            # task1 should be excluded, only task2 should be checked
            mock_df.iter_rows.return_value = [
                {
                    "task_id": "task2",
                    "schema": schema,
                    "primary_keys": ["col1"]
                }
            ]

            mock_db_client.query.return_value = mock_df
            result = validator.validate_shared_schema("test_table", schema, ["col1"], "task1")

        # task1 should be excluded, task2 matches, so valid
        assert result["valid"] is True
        assert result["sharing_tasks"] == ["task1", "task2"]


class TestCompareSchemas:
    """测试 _compare_schemas 方法"""

    def test_identical_schemas(self, validator):
        """测试相同的 schema"""
        schema1 = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        schema2 = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}

        result = validator._compare_schemas(schema1, schema2)

        assert result is None

    def test_missing_fields(self, validator):
        """测试缺少字段"""
        schema1 = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        schema2 = {"col1": {"type": "STRING"}}

        result = validator._compare_schemas(schema1, schema2)

        assert result is not None
        assert "缺少字段" in result

    def test_extra_fields(self, validator):
        """测试多余字段"""
        schema1 = {"col1": {"type": "STRING"}}
        schema2 = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}

        result = validator._compare_schemas(schema1, schema2)

        assert result is not None
        assert "多余字段" in result

    def test_type_mismatch(self, validator):
        """测试类型不匹配"""
        schema1 = {"col1": {"type": "STRING"}, "col2": {"type": "INT"}}
        schema2 = {"col1": {"type": "STRING"}, "col2": {"type": "DOUBLE"}}

        result = validator._compare_schemas(schema1, schema2)

        assert result is not None
        assert "类型不一致" in result
        assert "col2" in result

    def test_simple_type_format(self, validator):
        """测试简单类型格式（字符串而非字典）"""
        schema1 = {"col1": "STRING", "col2": "INT"}
        schema2 = {"col1": "STRING", "col2": "INT"}

        result = validator._compare_schemas(schema1, schema2)

        assert result is None


class TestSingletonInstance:
    """测试单例实例"""

    def test_singleton_exists(self):
        """测试单例实例存在"""
        assert shared_table_validator is not None
        assert isinstance(shared_table_validator, SharedTableValidator)

    def test_singleton_config_tables(self):
        """测试单例配置表列表"""
        assert shared_table_validator.config_tables == ["sync_task_config", "etl_task_config"]
