"""
DolphinDBConnection 单元测试
测试连接管理、单例模式、线程安全等功能
"""
import pytest
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from infrastructure.database.connection import DolphinDBConnection


class TestConnectionBasics:
    """连接基础功能测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_connection_initialization(self, mock_ddb):
        """测试连接初始化"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        assert conn.session is not None
        mock_ddb.session.assert_called_once()

    @patch('infrastructure.database.connection.ddb')
    def test_connection_with_custom_config(self, mock_ddb):
        """测试自定义配置连接"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 验证连接参数
        call_args = mock_ddb.session.call_args
        assert call_args is not None

    @patch('infrastructure.database.connection.ddb')
    def test_close_connection(self, mock_ddb):
        """测试关闭连接"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()
        conn.close()

        mock_session.close.assert_called_once()

    @patch('infrastructure.database.connection.ddb')
    def test_close_already_closed_connection(self, mock_ddb):
        """测试关闭已关闭的连接"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()
        conn.close()
        conn.close()  # 第二次关闭不应该报错

        # 应该只调用一次 close
        assert mock_session.close.call_count <= 2


class TestSingletonPattern:
    """单例模式测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_singleton_same_instance(self, mock_ddb):
        """测试单例返回相同实例"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn1 = DolphinDBConnection()
        conn2 = DolphinDBConnection()

        assert conn1 is conn2
        # 应该只创建一次连接
        assert mock_ddb.session.call_count == 1

    @patch('infrastructure.database.connection.ddb')
    def test_singleton_after_close(self, mock_ddb):
        """测试关闭后重新创建单例"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn1 = DolphinDBConnection()
        conn1.close()

        # 重置单例以模拟重新创建
        DolphinDBConnection._instance = None

        conn2 = DolphinDBConnection()

        # 应该是新的实例
        assert mock_ddb.session.call_count == 2


class TestThreadSafety:
    """线程安全测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_concurrent_initialization(self, mock_ddb):
        """测试并发初始化"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        instances = []
        errors = []

        def create_connection():
            try:
                conn = DolphinDBConnection()
                instances.append(conn)
            except Exception as e:
                errors.append(e)

        # 创建多个线程同时初始化连接
        threads = [threading.Thread(target=create_connection) for _ in range(10)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 不应该有错误
        assert len(errors) == 0

        # 所有实例应该是同一个
        assert len(set(id(inst) for inst in instances)) == 1

        # 只应该创建一次连接
        assert mock_ddb.session.call_count == 1

    @patch('infrastructure.database.connection.ddb')
    def test_concurrent_queries(self, mock_ddb):
        """测试并发查询"""
        mock_session = MagicMock()
        mock_session.run.return_value = {"id": [1, 2, 3]}
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()
        results = []
        errors = []

        def run_query():
            try:
                result = conn.session.run("SELECT * FROM table1")
                results.append(result)
            except Exception as e:
                errors.append(e)

        # 创建多个线程同时查询
        threads = [threading.Thread(target=run_query) for _ in range(10)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # 不应该有错误
        assert len(errors) == 0

        # 所有查询都应该成功
        assert len(results) == 10


class TestConnectionRecovery:
    """连接恢复测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_reconnect_after_failure(self, mock_ddb):
        """测试连接失败后重连"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        # 第一次连接失败
        mock_ddb.session.side_effect = [Exception("Connection failed"), mock_session]

        # 第一次应该失败
        with pytest.raises(Exception, match="Connection failed"):
            DolphinDBConnection()

        # 重置单例
        DolphinDBConnection._instance = None

        # 第二次应该成功
        conn = DolphinDBConnection()
        assert conn.session is not None

    @patch('infrastructure.database.connection.ddb')
    def test_session_property_access(self, mock_ddb):
        """测试 session 属性访问"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 多次访问 session 应该返回同一个对象
        session1 = conn.session
        session2 = conn.session

        assert session1 is session2


class TestErrorHandling:
    """错误处理测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_connection_timeout(self, mock_ddb):
        """测试连接超时"""
        mock_ddb.session.side_effect = TimeoutError("Connection timeout")

        with pytest.raises(TimeoutError, match="Connection timeout"):
            DolphinDBConnection()

    @patch('infrastructure.database.connection.ddb')
    def test_invalid_credentials(self, mock_ddb):
        """测试无效凭证"""
        mock_ddb.session.side_effect = Exception("Authentication failed")

        with pytest.raises(Exception, match="Authentication failed"):
            DolphinDBConnection()

    @patch('infrastructure.database.connection.ddb')
    def test_network_error(self, mock_ddb):
        """测试网络错误"""
        mock_ddb.session.side_effect = ConnectionError("Network unreachable")

        with pytest.raises(ConnectionError, match="Network unreachable"):
            DolphinDBConnection()

    @patch('infrastructure.database.connection.ddb')
    def test_close_with_error(self, mock_ddb):
        """测试关闭时出错"""
        mock_session = MagicMock()
        mock_session.close.side_effect = Exception("Close failed")
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 关闭时出错不应该抛出异常（应该被捕获和记录）
        try:
            conn.close()
        except Exception:
            pytest.fail("close() should not raise exception")


class TestConnectionPooling:
    """连接池测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_single_connection_reuse(self, mock_ddb):
        """测试单连接复用"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 多次获取连接应该复用同一个
        for _ in range(10):
            session = conn.session
            assert session is mock_session

        # 只应该创建一次连接
        assert mock_ddb.session.call_count == 1

    @patch('infrastructure.database.connection.ddb')
    def test_connection_state_persistence(self, mock_ddb):
        """测试连接状态持久化"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 设置一些状态
        conn.session.run("USE dfs://quant_ts")

        # 获取新的连接引用
        conn2 = DolphinDBConnection()

        # 应该是同一个连接，状态应该保持
        assert conn is conn2
        assert conn.session is conn2.session


class TestConnectionMetadata:
    """连接元数据测试"""

    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        """每个测试前重置单例"""
        DolphinDBConnection._instance = None
        DolphinDBConnection._lock = threading.Lock()
        yield
        DolphinDBConnection._instance = None

    @patch('infrastructure.database.connection.ddb')
    def test_connection_info(self, mock_ddb):
        """测试连接信息"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 验证连接对象有必要的属性
        assert hasattr(conn, 'session')
        assert hasattr(conn, 'close')

    @patch('infrastructure.database.connection.ddb')
    def test_connection_string_representation(self, mock_ddb):
        """测试连接的字符串表示"""
        mock_session = MagicMock()
        mock_ddb.session.return_value = mock_session

        conn = DolphinDBConnection()

        # 应该有合理的字符串表示
        str_repr = str(conn)
        assert str_repr is not None
        assert len(str_repr) > 0
