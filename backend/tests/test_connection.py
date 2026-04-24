"""
DolphinDBConnection 单元测试
测试连接管理和基本功能
"""
import pytest
from unittest.mock import Mock, patch, MagicMock

from infrastructure.database.connection import DolphinDBConnection


class TestConnectionBasics:
    """连接基础功能测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_connection_initialization(self, mock_ddb):
        """测试连接初始化"""
        mock_session = MagicMock()
        mock_session.connect.return_value = True
        mock_ddb.Session.return_value = mock_session

        with patch('infrastructure.database.connection.settings') as mock_settings:
            mock_settings.database.dolphindb_host = 'localhost'
            mock_settings.database.dolphindb_port = 8848
            mock_settings.database.dolphindb_user = 'admin'
            mock_settings.database.dolphindb_password = '123456'
            mock_settings.database.db_path = 'dfs://quant'

            conn = DolphinDBConnection()

            assert conn is not None
            mock_ddb.Session.assert_called_once()
            mock_session.connect.assert_called_once()

    @patch('infrastructure.database.connection.ddb')
    def test_close_connection(self, mock_ddb):
        """测试关闭连接"""
        mock_session = MagicMock()
        mock_session.connect.return_value = True
        mock_ddb.Session.return_value = mock_session

        with patch('infrastructure.database.connection.settings') as mock_settings:
            mock_settings.database.dolphindb_host = 'localhost'
            mock_settings.database.dolphindb_port = 8848
            mock_settings.database.dolphindb_user = 'admin'
            mock_settings.database.dolphindb_password = '123456'
            mock_settings.database.db_path = 'dfs://quant'

            conn = DolphinDBConnection()
            conn.close()

            mock_session.close.assert_called_once()

    @patch('infrastructure.database.connection.ddb')
    def test_close_already_closed_connection(self, mock_ddb):
        """测试关闭已关闭的连接"""
        mock_session = MagicMock()
        mock_session.connect.return_value = True
        mock_ddb.Session.return_value = mock_session

        with patch('infrastructure.database.connection.settings') as mock_settings:
            mock_settings.database.dolphindb_host = 'localhost'
            mock_settings.database.dolphindb_port = 8848
            mock_settings.database.dolphindb_user = 'admin'
            mock_settings.database.dolphindb_password = '123456'
            mock_settings.database.db_path = 'dfs://quant'

            conn = DolphinDBConnection()
            conn.close()
            conn.close()  # 第二次关闭不应该报错

            # 应该只调用一次 close
            assert mock_session.close.call_count == 1

    @patch('infrastructure.database.connection.ddb')
    def test_connection_properties(self, mock_ddb):
        """测试连接属性"""
        mock_session = MagicMock()
        mock_session.connect.return_value = True
        mock_ddb.Session.return_value = mock_session

        with patch('infrastructure.database.connection.settings') as mock_settings:
            mock_settings.database.dolphindb_host = 'localhost'
            mock_settings.database.dolphindb_port = 8848
            mock_settings.database.dolphindb_user = 'admin'
            mock_settings.database.dolphindb_password = '123456'
            mock_settings.database.db_path = 'dfs://quant'

            conn = DolphinDBConnection()

            assert conn.session is mock_session
            assert conn.db_path == 'dfs://quant'
            assert conn.lock is not None
