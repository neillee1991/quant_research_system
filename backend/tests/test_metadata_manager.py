"""
MetadataManager 单元测试
测试元数据管理、版本控制、种子数据等功能
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
import polars as pl

from infrastructure.database.metadata_manager import MetadataManager


class TestMetadataManagerBasics:
    """元数据管理器基础功能测试"""

    @pytest.fixture
    def mock_data_ops(self):
        """创建 mock DataOperations"""
        data_ops = Mock()
        data_ops.query = Mock(return_value=pl.DataFrame())
        data_ops.execute = Mock()
        data_ops.upsert = Mock()
        return data_ops

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        """创建 MetadataManager 实例"""
        return MetadataManager(mock_data_ops)

    def test_initialization(self, metadata_manager, mock_data_ops):
        """测试初始化"""
        assert metadata_manager._data_ops is mock_data_ops
        assert hasattr(metadata_manager, '_META_TABLE_SCHEMAS')

    def test_meta_table_schemas_defined(self, metadata_manager):
        """测试元数据表结构定义"""
        schemas = metadata_manager._META_TABLE_SCHEMAS

        # 验证关键表存在
        assert "factor_metadata" in schemas
        assert "factor_metadata" in schemas
        assert "sync_task_config" in schemas
        assert "etl_task_config" in schemas

        # 验证结构格式
        for table_name, (schema_expr, primary_keys) in schemas.items():
            assert isinstance(schema_expr, str)
            assert isinstance(primary_keys, list)
            assert len(primary_keys) > 0


class TestTableCreation:
    """表创建功能测试"""

    @pytest.fixture
    def mock_data_ops(self):
        data_ops = Mock()
        data_ops.query = Mock(return_value=pl.DataFrame())
        data_ops.execute = Mock()
        return data_ops

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        return MetadataManager(mock_data_ops)

    def test_create_meta_table_success(self, metadata_manager, mock_data_ops):
        """测试创建元数据表成功"""
        # Mock 表不存在
        mock_data_ops.query.return_value = pl.DataFrame()

        metadata_manager.create_meta_table("factor_metadata")

        # 验证执行了创建表的 SQL
        assert mock_data_ops.execute.called

    def test_create_meta_table_already_exists(self, metadata_manager, mock_data_ops):
        """测试表已存在时跳过创建"""
        # Mock 表已存在
        mock_data_ops.query.return_value = pl.DataFrame({"name": ["factor_metadata"]})

        metadata_manager.create_meta_table("factor_metadata")

        # 不应该执行创建表的 SQL
        assert not mock_data_ops.execute.called

    def test_create_meta_table_invalid_table(self, metadata_manager):
        """测试创建不存在的表定义"""
        with pytest.raises(ValueError, match="Unknown meta table"):
            metadata_manager.create_meta_table("invalid_table")

    def test_create_all_meta_tables(self, metadata_manager, mock_data_ops):
        """测试创建所有元数据表"""
        mock_data_ops.query.return_value = pl.DataFrame()

        metadata_manager.create_all_meta_tables()

        # 应该为每个表调用 execute
        assert mock_data_ops.execute.call_count >= len(metadata_manager._META_TABLE_SCHEMAS)


class TestVersionManagement:
    """版本管理功能测试"""

    @pytest.fixture
    def mock_data_ops(self):
        data_ops = Mock()
        data_ops.query = Mock()
        data_ops.execute = Mock()
        data_ops.upsert = Mock()
        return data_ops

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        return MetadataManager(mock_data_ops)

    def test_create_task_version_first_version(self, metadata_manager, mock_data_ops):
        """测试创建第一个版本"""
        # Mock 没有现有版本
        mock_data_ops.query.return_value = pl.DataFrame()

        config = {
            "task_id": "test_task",
            "description": "Test task",
            "api_name": "test_api"
        }

        version = metadata_manager.create_task_version("sync", "test_task", config)

        assert version == 1
        assert mock_data_ops.upsert.called

    def test_create_task_version_increment(self, metadata_manager, mock_data_ops):
        """测试版本号自增"""
        # Mock 已有版本 3
        mock_data_ops.query.return_value = pl.DataFrame({
            "version_number": [3, 2, 1]
        })

        config = {"task_id": "test_task"}

        version = metadata_manager.create_task_version("sync", "test_task", config)

        assert version == 4

    def test_create_task_version_marks_old_as_not_current(self, metadata_manager, mock_data_ops):
        """测试创建新版本时标记旧版本为非当前"""
        mock_data_ops.query.return_value = pl.DataFrame({
            "version_number": [2]
        })

        config = {"task_id": "test_task"}

        metadata_manager.create_task_version("sync", "test_task", config)

        # 应该调用 execute 更新旧版本
        assert mock_data_ops.execute.called

    def test_create_task_version_invalid_type(self, metadata_manager):
        """测试无效的任务类型"""
        with pytest.raises(ValueError, match="Invalid task_type"):
            metadata_manager.create_task_version("invalid", "test_task", {})

    def test_get_task_versions(self, metadata_manager, mock_data_ops):
        """测试获取任务所有版本"""
        mock_data_ops.query.return_value = pl.DataFrame({
            "task_id": ["test_task", "test_task"],
            "version_number": [2, 1],
            "is_current": [True, False]
        })

        versions = metadata_manager.get_task_versions("sync", "test_task")

        assert len(versions) == 2
        assert versions[0]["version_number"] == 2

    def test_get_task_versions_empty(self, metadata_manager, mock_data_ops):
        """测试获取不存在的任务版本"""
        mock_data_ops.query.return_value = pl.DataFrame()

        versions = metadata_manager.get_task_versions("sync", "nonexistent")

        assert versions == []

    def test_get_task_version_specific(self, metadata_manager, mock_data_ops):
        """测试获取特定版本"""
        mock_data_ops.query.return_value = pl.DataFrame({
            "task_id": ["test_task"],
            "version_number": [2],
            "description": ["Version 2"]
        })

        version = metadata_manager.get_task_version("sync", "test_task", 2)

        assert version is not None
        assert version["version_number"] == 2

    def test_get_task_version_not_found(self, metadata_manager, mock_data_ops):
        """测试获取不存在的版本"""
        mock_data_ops.query.return_value = pl.DataFrame()

        version = metadata_manager.get_task_version("sync", "test_task", 999)

        assert version is None


class TestVersionRollback:
    """版本回滚功能测试"""

    @pytest.fixture
    def mock_data_ops(self):
        data_ops = Mock()
        data_ops.query = Mock()
        data_ops.execute = Mock()
        data_ops.upsert = Mock()
        return data_ops

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        return MetadataManager(mock_data_ops)

    def test_rollback_task_version_success(self, metadata_manager, mock_data_ops):
        """测试回滚到指定版本"""
        # Mock 目标版本存在
        mock_data_ops.query.return_value = pl.DataFrame({
            "task_id": ["test_task"],
            "version_number": [2]
        })

        result = metadata_manager.rollback_task_version("sync", "test_task", 2)

        assert result is True
        # 应该更新 is_current 标志
        assert mock_data_ops.execute.call_count >= 2

    def test_rollback_task_version_not_found(self, metadata_manager, mock_data_ops):
        """测试回滚到不存在的版本"""
        mock_data_ops.query.return_value = pl.DataFrame()

        result = metadata_manager.rollback_task_version("sync", "test_task", 999)

        assert result is False

    def test_rollback_task_version_invalid_type(self, metadata_manager):
        """测试无效的任务类型"""
        with pytest.raises(ValueError, match="Invalid task_type"):
            metadata_manager.rollback_task_version("invalid", "test_task", 1)


class TestCurrentVersion:
    """当前版本查询测试"""

    @pytest.fixture
    def mock_data_ops(self):
        data_ops = Mock()
        data_ops.query = Mock()
        return data_ops

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        return MetadataManager(mock_data_ops)

    def test_get_current_task_version_exists(self, metadata_manager, mock_data_ops):
        """测试获取当前版本"""
        mock_data_ops.query.return_value = pl.DataFrame({
            "task_id": ["test_task"],
            "version_number": [3],
            "is_current": [True],
            "description": ["Current version"]
        })

        current = metadata_manager.get_current_task_version("sync", "test_task")

        assert current is not None
        assert current["version_number"] == 3
        assert current["is_current"] is True

    def test_get_current_task_version_not_found(self, metadata_manager, mock_data_ops):
        """测试获取不存在的当前版本"""
        mock_data_ops.query.return_value = pl.DataFrame()

        current = metadata_manager.get_current_task_version("sync", "nonexistent")

        assert current is None

    def test_get_current_task_version_invalid_type(self, metadata_manager):
        """测试无效的任务类型"""
        with pytest.raises(ValueError, match="Invalid task_type"):
            metadata_manager.get_current_task_version("invalid", "test_task")

    def test_get_current_task_version_all_types(self, metadata_manager, mock_data_ops):
        """测试所有任务类型"""
        mock_data_ops.query.return_value = pl.DataFrame({
            "task_id": ["test"],
            "version_number": [1],
            "is_current": [True]
        })

        # 测试 sync
        result = metadata_manager.get_current_task_version("sync", "test")
        assert result is not None

        # 测试 etl
        result = metadata_manager.get_current_task_version("etl", "test")
        assert result is not None

        # 测试 factor
        mock_data_ops.query.return_value = pl.DataFrame({
            "factor_id": ["test"],
            "version_number": [1],
            "is_current": [True]
        })
        result = metadata_manager.get_current_task_version("factor", "test")
        assert result is not None


class TestErrorHandling:
    """错误处理测试"""

    @pytest.fixture
    def mock_data_ops(self):
        data_ops = Mock()
        data_ops.query = Mock()
        data_ops.execute = Mock()
        data_ops.upsert = Mock()
        return data_ops

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        return MetadataManager(mock_data_ops)

    def test_create_table_database_error(self, metadata_manager, mock_data_ops):
        """测试数据库错误处理"""
        mock_data_ops.query.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            metadata_manager.create_meta_table("factor_metadata")

    def test_create_version_database_error(self, metadata_manager, mock_data_ops):
        """测试创建版本时的数据库错误"""
        mock_data_ops.query.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            metadata_manager.create_task_version("sync", "test", {})

    def test_rollback_database_error(self, metadata_manager, mock_data_ops):
        """测试回滚时的数据库错误"""
        mock_data_ops.query.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            metadata_manager.rollback_task_version("sync", "test", 1)


class TestTableMapping:
    """表映射测试"""

    @pytest.fixture
    def mock_data_ops(self):
        return Mock()

    @pytest.fixture
    def metadata_manager(self, mock_data_ops):
        return MetadataManager(mock_data_ops)

    def test_task_type_to_table_mapping(self, metadata_manager, mock_data_ops):
        """测试任务类型到表名的映射"""
        mock_data_ops.query.return_value = pl.DataFrame()

        # 验证映射关系通过调用方法
        try:
            metadata_manager.get_current_task_version("sync", "test")
            metadata_manager.get_current_task_version("etl", "test")
            metadata_manager.get_current_task_version("factor", "test")
        except Exception as e:
            # 只要不是 ValueError 就说明映射正确
            assert "Invalid task_type" not in str(e)
