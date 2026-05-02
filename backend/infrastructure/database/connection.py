"""
DolphinDB 连接管理模块
负责建立、维护和管理 DolphinDB 数据库连接
"""
import threading
from typing import Optional

import dolphindb as ddb

from app.core.config import settings
from app.core.logger import logger


class DolphinDBConnection:
    """DolphinDB 连接管理器（线程安全单例）"""

    def __init__(self):
        """初始化连接管理器"""
        self._host = settings.database.dolphindb_host
        self._port = settings.database.dolphindb_port
        self._user = settings.database.dolphindb_user
        self._password = settings.database.dolphindb_password
        self._db_path = settings.database.db_path

        # 会话与线程锁
        self._session: Optional[ddb.Session] = None
        self._lock = threading.Lock()

        self._connect()
        logger.info(
            f"DolphinDB connection initialized: {self._host}:{self._port}, "
            f"db={self._db_path}"
        )

    def _connect(self):
        """建立 DolphinDB 连接"""
        try:
            self._session = ddb.Session(enableASYNC=False)
            success = self._session.connect(
                self._host, self._port, self._user, self._password
            )
            if not success:
                raise ConnectionError(
                    f"无法连接 DolphinDB {self._host}:{self._port}"
                )
            logger.info("DolphinDB 连接成功")
        except Exception as e:
            logger.error(f"DolphinDB 连接失败: {e}")
            raise

    def _ensure_connected(self):
        """
        确保连接可用，断线自动重连

        注意: 必须在持有 _lock 的情况下调用
        """
        try:
            # 简单心跳检测
            self._session.run("1+1")
        except Exception as e:
            logger.warning(f"DolphinDB 连接已断开，正在重连: {e}")
            self._connect()
            try:
                self._session.run("1+1")  # 验证重连成功
            except Exception as reconnect_error:
                raise ConnectionError(f"重连失败: {reconnect_error}") from reconnect_error

    @property
    def session(self) -> ddb.Session:
        """获取 DolphinDB 会话对象"""
        return self._session

    @property
    def lock(self) -> threading.Lock:
        """获取线程锁"""
        return self._lock

    @property
    def db_path(self) -> str:
        """获取数据库路径"""
        return self._db_path

    def close(self) -> None:
        """关闭连接"""
        with self._lock:
            if self._session:
                try:
                    self._session.close()
                    logger.info("DolphinDB 连接已关闭")
                except Exception as e:
                    logger.error(f"关闭 DolphinDB 连接时出错: {e}")
                finally:
                    self._session = None
