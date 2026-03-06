import sys
from pathlib import Path
from loguru import logger

# 从配置读取日志目录
from app.core.config import settings

# 确保日志目录存在
LOG_DIR = settings.log_dir
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)
logger.add(
    LOG_DIR / "app.log",
    rotation="10 MB",
    retention="7 days",
    level="DEBUG",
    encoding="utf-8",
)

__all__ = ["logger"]
