"""
预处理配置加载器
"""
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from app.core.logger import logger


class PreprocessConfigLoader:
    """预处理配置加载器"""

    def __init__(self, config_path: Optional[str] = None):
        """初始化配置加载器

        Args:
            config_path: 配置文件路径，默认为 config/preprocess_config.yaml
        """
        if config_path is None:
            # 默认路径：backend/config/preprocess_config.yaml
            base_dir = Path(__file__).parent
            config_path = base_dir / "preprocess_config.yaml"

        self.config_path = Path(config_path)
        self._config_cache: Optional[Dict[str, Any]] = None

    def load(self) -> Dict[str, Any]:
        """加载配置文件

        Returns:
            完整的配置字典
        """
        if self._config_cache is not None:
            return self._config_cache

        if not self.config_path.exists():
            logger.warning(f"Preprocess config not found: {self.config_path}")
            return {"preprocess_profiles": {}}

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                self._config_cache = config
                logger.info(f"Loaded preprocess config from {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"Failed to load preprocess config: {e}")
            return {"preprocess_profiles": {}}

    def get_profile(self, profile_name: str) -> Dict[str, Any]:
        """获取指定的预处理配置

        Args:
            profile_name: 配置名称（default, conservative, aggressive, etc.）

        Returns:
            预处理选项字典
        """
        config = self.load()
        profiles = config.get("preprocess_profiles", {})

        if profile_name not in profiles:
            logger.warning(f"Profile '{profile_name}' not found, using 'default'")
            profile_name = "default"

        profile = profiles.get(profile_name, {})
        logger.info(f"Loaded preprocess profile: {profile_name}")
        return profile

    def list_profiles(self) -> list:
        """列出所有可用的配置名称

        Returns:
            配置名称列表
        """
        config = self.load()
        profiles = config.get("preprocess_profiles", {})
        return list(profiles.keys())

    def get_default_profile(self) -> Dict[str, Any]:
        """获取默认配置

        Returns:
            默认预处理选项
        """
        return self.get_profile("default")

    def merge_options(
        self,
        profile_name: Optional[str] = None,
        custom_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """合并配置和自定义选项

        Args:
            profile_name: 配置名称，None 时使用 default
            custom_options: 自定义选项，会覆盖配置中的值

        Returns:
            合并后的预处理选项
        """
        # 加载基础配置
        if profile_name:
            base_options = self.get_profile(profile_name)
        else:
            base_options = self.get_default_profile()

        # 合并自定义选项
        if custom_options:
            merged = {**base_options, **custom_options}
            logger.debug(f"Merged options: base={profile_name or 'default'}, custom={custom_options}")
            return merged

        return base_options

    def reload(self) -> None:
        """重新加载配置文件（清除缓存）"""
        self._config_cache = None
        logger.info("Preprocess config cache cleared")


# 全局单例
_loader_instance: Optional[PreprocessConfigLoader] = None


def get_preprocess_loader(config_path: Optional[str] = None) -> PreprocessConfigLoader:
    """获取预处理配置加载器单例

    Args:
        config_path: 配置文件路径（仅首次调用时有效）

    Returns:
        PreprocessConfigLoader 实例
    """
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = PreprocessConfigLoader(config_path)
    return _loader_instance
