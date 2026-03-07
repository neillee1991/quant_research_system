"""
Unit tests for Repository base classes

测试覆盖：
- BaseRepository 基础功能
- find_by_date_range
- find_by_codes
- save (upsert)
- delete
- count
- exists
"""
import pytest
from unittest.mock import Mock, MagicMock
import polars as pl

from infrastructure.repository.base import BaseRepository, IRepository


class TestBaseRepository:
    """BaseRepository 单元测试"""

    @pytest.fixture
    def mock_db_client(self):
        """创建 mock DolphinDB 客户端"""
        mock_client = Mock()
        mock_client.execute = Mock(return_value=pl.DataFrame())
        mock_client.upsert = Mock(return_value=0)
        mock_client.execute_delete = Mock(return_value=0)
        return mock_client

    @pytest.fixture
    def repository(self, mock_db_client):
        """创建测试用 Repository"""
        return BaseRepository(mock_db_client, "test_table")

    def test_init(self, mock_db_client):
        """测试初始化"""
        repo = BaseRepository(mock_db_client, "test_table")
        assert repo.db == mock_db_client
        assert repo.table_name == "test_table"

    def test_find_by_date_range(self, repository, mock_db_client):
        """测试按日期范围查询"""
        # 准备测试数据
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "close": [10.0, 20.0]
        })
        mock_db_client.execute.return_value = test_data

        # 执行查询
        result = repository.find_by_date_range("20240101", "20240131")

        # 验证
        assert not result.is_empty()
        assert len(result) == 2
        mock_db_client.execute.assert_called_once()

        # 验证 SQL 语句
        call_args = mock_db_client.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "SELECT * FROM test_table" in sql
        assert "trade_date >= %s AND trade_date <= %s" in sql
        assert params == ("20240101", "20240131")

    def test_find_by_date_range_with_columns(self, repository, mock_db_client):
        """测试按日期范围查询（指定列）"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "close": [10.0]
        })
        mock_db_client.execute.return_value = test_data

        result = repository.find_by_date_range(
            "20240101", "20240131",
            columns=["ts_code", "close"]
        )

        call_args = mock_db_client.execute.call_args
        sql = call_args[0][0]
        assert "SELECT ts_code, close FROM test_table" in sql

    def test_find_by_codes(self, repository, mock_db_client):
        """测试按股票代码查询"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ", "000002.SZ"],
            "trade_date": ["20240101", "20240101"],
            "close": [10.0, 20.0]
        })
        mock_db_client.execute.return_value = test_data

        result = repository.find_by_codes(
            ["000001.SZ", "000002.SZ"],
            "20240101",
            "20240131"
        )

        assert not result.is_empty()
        assert len(result) == 2

        call_args = mock_db_client.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "ts_code IN (%s, %s)" in sql
        assert "trade_date >= %s AND trade_date <= %s" in sql
        assert params == ("000001.SZ", "000002.SZ", "20240101", "20240131")

    def test_find_by_codes_empty_list(self, repository, mock_db_client):
        """测试空股票代码列表"""
        result = repository.find_by_codes([], "20240101", "20240131")

        assert result.is_empty()
        mock_db_client.execute.assert_not_called()

    def test_save(self, repository, mock_db_client):
        """测试保存数据"""
        test_data = pl.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "close": [10.0]
        })
        mock_db_client.upsert.return_value = 1

        count = repository.save(test_data)

        assert count == 1
        mock_db_client.upsert.assert_called_once_with("test_table", test_data)

    def test_save_empty_dataframe(self, repository, mock_db_client):
        """测试保存空 DataFrame"""
        empty_df = pl.DataFrame()

        count = repository.save(empty_df)

        assert count == 0
        mock_db_client.upsert.assert_not_called()

    def test_delete(self, repository, mock_db_client):
        """测试删除数据"""
        mock_db_client.execute_delete.return_value = 5

        count = repository.delete({"ts_code": "000001.SZ"})

        assert count == 5
        mock_db_client.execute_delete.assert_called_once()

    def test_delete_empty_conditions(self, repository, mock_db_client):
        """测试空删除条件（应该抛出异常）"""
        with pytest.raises(ValueError, match="Delete conditions cannot be empty"):
            repository.delete({})

    def test_delete_with_list_condition(self, repository, mock_db_client):
        """测试使用列表条件删除"""
        mock_db_client.execute_delete.return_value = 10

        count = repository.delete({"ts_code": ["000001.SZ", "000002.SZ"]})

        assert count == 10

        call_args = mock_db_client.execute_delete.call_args
        sql = call_args[0][0]
        assert "DELETE FROM test_table" in sql
        assert "ts_code IN (%s, %s)" in sql

    def test_count(self, repository, mock_db_client):
        """测试统计行数"""
        mock_db_client.execute.return_value = pl.DataFrame({"cnt": [100]})

        count = repository.count()

        assert count == 100

        call_args = mock_db_client.execute.call_args
        sql = call_args[0][0]
        assert "SELECT COUNT(*) as cnt FROM test_table" in sql

    def test_count_with_conditions(self, repository, mock_db_client):
        """测试带条件统计"""
        mock_db_client.execute.return_value = pl.DataFrame({"cnt": [50]})

        count = repository.count({"status": "active"})

        assert count == 50

        call_args = mock_db_client.execute.call_args
        sql = call_args[0][0]
        assert "WHERE status = %s" in sql

    def test_count_empty_result(self, repository, mock_db_client):
        """测试空结果统计"""
        mock_db_client.execute.return_value = pl.DataFrame()

        count = repository.count()

        assert count == 0

    def test_exists(self, repository, mock_db_client):
        """测试数据是否存在"""
        mock_db_client.execute.return_value = pl.DataFrame({"cnt": [1]})

        exists = repository.exists({"ts_code": "000001.SZ"})

        assert exists is True

    def test_not_exists(self, repository, mock_db_client):
        """测试数据不存在"""
        mock_db_client.execute.return_value = pl.DataFrame({"cnt": [0]})

        exists = repository.exists({"ts_code": "999999.SZ"})

        assert exists is False


class TestIRepository:
    """IRepository 接口测试"""

    def test_interface_methods(self):
        """测试接口定义了所有必需方法"""
        required_methods = [
            "find_by_date_range",
            "find_by_codes",
            "save",
            "delete"
        ]

        for method in required_methods:
            assert hasattr(IRepository, method)

    def test_cannot_instantiate_interface(self):
        """测试不能直接实例化接口"""
        with pytest.raises(TypeError):
            IRepository()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
