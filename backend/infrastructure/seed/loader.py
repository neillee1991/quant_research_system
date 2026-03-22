"""
Seed Data Loader
Loads seed configuration from JSON files
"""
import json
from pathlib import Path
from typing import Any

from app.core.logger import logger


class SeedDataLoader:
    """种子数据加载器"""

    def __init__(self, config_dir: Path | None = None) -> None:
        """
        Initialize seed data loader

        Args:
            config_dir: 配置文件目录，默认为 backend/config/seed_data/
        """
        if config_dir is None:
            # 默认路径：backend/config/seed_data/
            self.config_dir = Path(__file__).parent.parent.parent / "config" / "seed_data"
        else:
            self.config_dir = Path(config_dir)

        logger.info(f"Seed data config directory: {self.config_dir}")

    def load_sync_tasks(self) -> list[dict[str, Any]]:
        """加载同步任务配置"""
        return self._load_json("sync_tasks.json")

    def load_etl_tasks(self) -> list[dict[str, Any]]:
        """加载 ETL 任务配置"""
        return self._load_json("etl_tasks.json")

    def load_factor_metadata(self) -> list[dict[str, Any]]:
        """加载因子元数据配置"""
        return self._load_json("factor_metadata.json")

    def load_factor_data_config(self) -> list[dict[str, Any]]:
        """加载因子数据配置"""
        return self._load_json("factor_data_config.json")

    def _load_json(self, filename: str) -> list[dict[str, Any]]:
        """
        加载 JSON 配置文件

        Args:
            filename: 文件名

        Returns:
            配置数据列表
        """
        file_path = self.config_dir / filename
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} records from {filename}")
            return data
        except FileNotFoundError:
            logger.error(f"Config file not found: {file_path}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []
