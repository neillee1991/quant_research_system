"""
DataOperations 单元测试
测试数据查询、执行、upsert、bulk_copy 等功能
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import polars as pl
import pandas as pd
from datetime import datetime

from infrastructure.database.data_operations import DataOperations


class TestDataOperationsBasics:
    """数据操作基础功能测试"""

    @pytest.fixture
    def mock_connection(self):
        """创建 mock 连接"""
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        return conn

    @pytest.fixture
    def mock_sql_adapter(self):
        """创建 mock SQL 适配器"""
        adapter = Mock()
        adapter.build_sql = Mock(side_effect=lambda sql, params: sql)
        return adapter

    @pytest.fixture
    def mock_table_manager(self):
        """创建 mock 表管理器"""
        manager = Mock()
        manager._resolve_db_path = Mock(return_value="dfs://quant_ts")
        manager._META_TABLES = ["factor_metadata", "sync_task_config"]
        return manager

    @pytest.fixture
    def data_ops(self, mock_connection, mock_sql_adapter, mock_table_manager):
        """创建 DataOperations 实例"""
        return DataOperations(mock_connection, mock_sql_adapter, mock_table_manager)

    def test_initialization(self, data_ops, mock_connection, mock_sql_adapter, mock_table_manager):
        """测试初始化"""
        assert data_ops._conn is mock_connection
        assert data_ops._sql_adapter is mock_sql_adapter
        assert data_ops._table_manager is mock_table_manager

    def test_extra_date_columns_defined(self, data_ops):
        """测试额外日期列配置"""
        assert "factor_values" in data_ops._EXTRA_DATE_COLUMNS
        assert "trade_date" in data_ops._EXTRA_DATE_COLUMNS["factor_values"]


class TestQueryOperations:
    """查询操作测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        return conn

    @pytest.fixture
    def mock_sql_adapter(self):
        adapter = Mock()
        adapter.build_sql = Mock(side_effect=lambda sql, params: sql)
        return adapter

    @pytest.fixture
    def mock_table_manager(self):
        manager = Mock()
        manager._resolve_db_path = Mock(return_value="dfs://quant_ts")
        manager._META_TABLES = []
        return manager

    @pytest.fixture
    def data_ops(self, mock_connection, mock_sql_adapter, mock_table_manager):
        return DataOperations(mock_connection, mock_sql_adapter, mock_table_manager)

    def test_query_returns_polars_dataframe(self, data_ops, mock_connection):
        """测试查询返回 Polars DataFrame"""
        # Mock 返回 pandas DataFrame
        mock_connection.session.run.return_value = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"]
        })

        result = data_ops.query("SELECT * FROM test", return_type="polars")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_query_returns_pandas_dataframe(self, data_ops, mock_connection):
        """测试查询返回 Pandas DataFrame"""
        mock_df = pd.DataFrame({"id": [1, 2, 3]})
        mock_connection.session.run.return_value = mock_df

        result = data_ops.query("SELECT * FROM test", return_type="pandas")

        assert isinstance(result, pd.DataFrame)

    def test_query_with_params(self, data_ops, mock_connection, mock_sql_adapter):
        """测试带参数的查询"""
        mock_connection.session.run.return_value = pd.DataFrame()

        data_ops.query("SELECT * FROM test WHERE id = %s", params=(123,))

        mock_sql_adapter.build_sql.assert_called_once()

    def test_query_error_handling(self, data_ops, mock_connection):
        """测试查询错误处理"""
        mock_connection.session.run.side_effect = Exception("Query failed")

        with pytest.raises(Exception, match="Query failed"):
            data_ops.query("SELECT * FROM test")

    def test_query_empty_result(self, data_ops, mock_connection):
        """测试空结果查询"""
        mock_connection.session.run.return_value = pd.DataFrame()

        result = data_ops.query("SELECT * FROM test")

        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()


class TestToPolarsConversion:
    """Polars 转换测试"""

    @pytest.fixture
    def data_ops(self):
        conn = Mock()
        adapter = Mock()
        manager = Mock()
        return DataOperations(conn, adapter, manager)

    def test_to_polars_from_pandas(self, data_ops):
        """测试从 Pandas 转换"""
        pdf = pd.DataFrame({"a": [1, 2, 3]})
        result = data_ops._to_polars(pdf)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_to_polars_from_empty_pandas(self, data_ops):
        """测试从空 Pandas 转换"""
        pdf = pd.DataFrame()
        result = data_ops._to_polars(pdf)

        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()

    def test_to_polars_from_dict(self, data_ops):
        """测试从字典转换"""
        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        result = data_ops._to_polars(data)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_to_polars_from_none(self, data_ops):
        """测试 None 值"""
        result = data_ops._to_polars(None)

        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()

    def test_to_polars_unknown_type(self, data_ops):
        """测试未知类型"""
        result = data_ops._to_polars("unknown")

        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()


class TestExecuteOperations:
    """执行操作测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        return conn

    @pytest.fixture
    def mock_sql_adapter(self):
        adapter = Mock()
        adapter.build_sql = Mock(side_effect=lambda sql, params: sql)
        return adapter

    @pytest.fixture
    def mock_table_manager(self):
        return Mock()

    @pytest.fixture
    def data_ops(self, mock_connection, mock_sql_adapter, mock_table_manager):
        return DataOperations(mock_connection, mock_sql_adapter, mock_table_manager)

    def test_execute_success(self, data_ops, mock_connection):
        """测试执行成功"""
        data_ops.execute("INSERT INTO test VALUES (1, 'a')")

        mock_connection.session.run.assert_called_once()

    def test_execute_with_params(self, data_ops, mock_sql_adapter):
        """测试带参数的执行"""
        data_ops.execute("INSERT INTO test VALUES (%s, %s)", params=(1, "a"))

        mock_sql_adapter.build_sql.assert_called_once()

    def test_execute_error_handling(self, data_ops, mock_connection):
        """测试执行错误处理"""
        mock_connection.session.run.side_effect = Exception("Execute failed")

        with pytest.raises(Exception, match="Execute failed"):
            data_ops.execute("INSERT INTO test VALUES (1)")


class TestUpsertOperations:
    """Upsert 操作测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        return conn

    @pytest.fixture
    def mock_sql_adapter(self):
        return Mock()

    @pytest.fixture
    def mock_table_manager(self):
        manager = Mock()
        manager._resolve_db_path = Mock(return_value="dfs://quant_ts")
        manager._META_TABLES = ["factor_metadata"]
        return manager

    @pytest.fixture
    def data_ops(self, mock_connection, mock_sql_adapter, mock_table_manager):
        return DataOperations(mock_connection, mock_sql_adapter, mock_table_manager)

    def test_upsert_empty_dataframe(self, data_ops):
        """测试空 DataFrame"""
        df = pl.DataFrame()

        data_ops.upsert("test_table", df, ["id"])

        # 不应该执行任何操作
        assert not data_ops._conn.session.run.called

    def test_upsert_tsdb_table(self, data_ops, mock_connection, mock_table_manager):
        """测试 TSDB 表 upsert"""
        mock_table_manager._META_TABLES = []
        df = pl.DataFrame({
            "id": [1, 2],
            "value": [10, 20]
        })

        with patch.object(data_ops, '_prepare_upload_df', return_value=(["id", "value"], "tmp_var")):
            data_ops.upsert("daily_data", df, ["id"])

            # 应该调用 tableInsert
            assert mock_connection.session.run.called

    def test_upsert_meta_table(self, data_ops, mock_connection, mock_table_manager):
        """测试维度表 upsert (delete + insert)"""
        mock_table_manager._META_TABLES = ["factor_metadata"]
        df = pl.DataFrame({
            "factor_id": ["momentum"],
            "description": ["test"]
        })

        with patch.object(data_ops, '_prepare_upload_df', return_value=(["factor_id", "description"], "tmp_var")):
            data_ops.upsert("factor_metadata", df, ["factor_id"])

            # 应该调用 delete + insert
            assert mock_connection.session.run.called


class TestBulkCopyOperations:
    """Bulk Copy 操作测试"""

    @pytest.fixture
    def mock_connection(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        return conn

    @pytest.fixture
    def data_ops(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        adapter = Mock()
        manager = Mock()
        manager._resolve_db_path = Mock(return_value="dfs://quant_ts")
        return DataOperations(conn, adapter, manager)

    def test_bulk_copy_empty_dataframe(self, data_ops):
        """测试空 DataFrame"""
        df = pl.DataFrame()

        data_ops.bulk_copy("test_table", df)

        # 不应该执行任何操作
        assert not data_ops._conn.session.run.called

    def test_bulk_copy_success(self, data_ops):
        """测试批量复制成功"""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })

        with patch.object(data_ops, '_prepare_upload_df', return_value=(["id", "value"], "tmp_var")):
            data_ops.bulk_copy("test_table", df)

            assert data_ops._conn.session.run.called


class TestPrepareUploadDF:
    """准备上传 DataFrame 测试"""

    @pytest.fixture
    def data_ops(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        adapter = Mock()
        manager = Mock()
        manager._resolve_db_path = Mock(return_value="dfs://quant_ts")
        manager._META_TABLES = []
        return DataOperations(conn, adapter, manager)

    def test_prepare_upload_df_with_known_columns(self, data_ops):
        """测试使用已知列"""
        df = pl.DataFrame({
            "id": [1, 2],
            "value": [10, 20]
        })

        ordered_cols, tmp_var = data_ops._prepare_upload_df(
            "test_table", df, "dfs://quant_ts", ["id", "value"], "upsert"
        )

        assert ordered_cols == ["id", "value"]
        assert "test_table" in tmp_var

    def test_prepare_upload_df_date_conversion(self, data_ops):
        """测试日期列转换"""
        df = pl.DataFrame({
            "trade_date": ["20240101", "20240102"],
            "value": [10, 20]
        })

        with patch.object(data_ops._conn.session, 'upload'):
            ordered_cols, tmp_var = data_ops._prepare_upload_df(
                "factor_values", df, "dfs://quant_ts", None, "upsert"
            )

            # 验证日期列被处理
            assert "trade_date" in ordered_cols


class TestErrorHandling:
    """错误处理测试"""

    @pytest.fixture
    def data_ops(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        adapter = Mock()
        adapter.build_sql = Mock(side_effect=lambda sql, params: sql)
        manager = Mock()
        return DataOperations(conn, adapter, manager)

    def test_query_exception_logging(self, data_ops):
        """测试查询异常日志"""
        data_ops._conn.session.run.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            data_ops.query("SELECT * FROM test")

    def test_execute_exception_logging(self, data_ops):
        """测试执行异常日志"""
        data_ops._conn.session.run.side_effect = Exception("Execute error")

        with pytest.raises(Exception):
            data_ops.execute("INSERT INTO test VALUES (1)")

    def test_upsert_exception_handling(self, data_ops):
        """测试 upsert 异常处理"""
        df = pl.DataFrame({"id": [1]})
        data_ops._conn.session.run.side_effect = Exception("Upsert error")

        with pytest.raises(Exception):
            with patch.object(data_ops, '_prepare_upload_df', return_value=(["id"], "tmp")):
                data_ops.upsert("test", df, ["id"])


class TestThreadSafety:
    """线程安全测试"""

    @pytest.fixture
    def data_ops(self):
        conn = Mock()
        conn.session = MagicMock()
        conn.lock = MagicMock()
        conn._ensure_connected = Mock()
        adapter = Mock()
        adapter.build_sql = Mock(side_effect=lambda sql, params: sql)
        manager = Mock()
        return DataOperations(conn, adapter, manager)

    def test_query_uses_lock(self, data_ops):
        """测试查询使用锁"""
        data_ops._conn.session.run.return_value = pd.DataFrame()

        data_ops.query("SELECT * FROM test")

        # 验证使用了锁
        data_ops._conn.lock.__enter__.assert_called()

    def test_execute_uses_lock(self, data_ops):
        """测试执行使用锁"""
        data_ops.execute("INSERT INTO test VALUES (1)")

        # 验证使用了锁
        data_ops._conn.lock.__enter__.assert_called()
