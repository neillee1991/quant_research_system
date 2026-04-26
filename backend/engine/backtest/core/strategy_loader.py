"""
策略加载器 - 支持安全加载策略
"""
import importlib
import inspect
import logging
import sys
from typing import Any, Dict, Optional, Type

from backend.engine.backtest.core.base_strategy import BaseStrategy, StrategyConfig

logger = logging.getLogger(__name__)


class StrategyLoader:
    """
    策略加载器
    支持从多种来源安全加载策略类
    """

    @classmethod
    def load_strategy_from_module(
        cls,
        module_path: str,
        strategy_class_name: str,
        config: StrategyConfig
    ) -> Optional[BaseStrategy]:
        """
        从模块加载策略类

        Args:
            module_path: 模块路径（例如："strategies.mystrategy"）
            strategy_class_name: 策略类名
            config: 策略配置

        Returns:
            策略实例，加载失败返回None
        """
        try:
            logger.info("正在加载策略: %s from %s", strategy_class_name, module_path)

            # 导入模块
            if module_path in sys.modules:
                module = sys.modules[module_path]
            else:
                module = importlib.import_module(module_path)

            # 获取策略类
            strategy_class = cls._get_strategy_class(
                module, strategy_class_name
            )

            if strategy_class is None:
                logger.error(
                    "在模块 %s 中未找到策略类 %s",
                    module_path, strategy_class_name
                )
                return None

            # 创建策略实例
            strategy = strategy_class(config)
            logger.info("策略加载成功: %s", strategy_class_name)
            return strategy

        except ImportError as e:
            logger.error("模块导入失败: %s - %s", module_path, e)
            return None
        except Exception as e:
            logger.error(
                "策略加载失败: %s - %s", strategy_class_name, e,
                exc_info=True
            )
            return None

    @classmethod
    def load_strategy_from_file(
        cls,
        file_path: str,
        strategy_class_name: str,
        config: StrategyConfig
    ) -> Optional[BaseStrategy]:
        """
        从文件加载策略类

        Args:
            file_path: 文件路径
            strategy_class_name: 策略类名
            config: 策略配置

        Returns:
            策略实例，加载失败返回None
        """
        try:
            logger.info("正在从文件加载策略: %s from %s", strategy_class_name, file_path)

            # 动态导入模块
            module_name = cls._generate_module_name(file_path)
            spec = importlib.util.spec_from_file_location(module_name, file_path)

            if spec is None or spec.loader is None:
                logger.error("无法加载策略文件: %s", file_path)
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # 获取策略类
            strategy_class = cls._get_strategy_class(
                module, strategy_class_name
            )

            if strategy_class is None:
                logger.error(
                    "在文件 %s 中未找到策略类 %s",
                    file_path, strategy_class_name
                )
                return None

            # 创建策略实例
            strategy = strategy_class(config)
            logger.info("策略加载成功: %s", strategy_class_name)
            return strategy

        except Exception as e:
            logger.error(
                "策略加载失败: %s - %s", strategy_class_name, e,
                exc_info=True
            )
            return None

    @classmethod
    def load_strategy_from_code(
        cls,
        code: str,
        strategy_class_name: str,
        config: StrategyConfig
    ) -> Optional[BaseStrategy]:
        """
        从代码字符串加载策略类（使用沙箱安全执行）

        Args:
            code: 策略代码
            strategy_class_name: 策略类名
            config: 策略配置

        Returns:
            策略实例，加载失败返回None
        """
        try:
            logger.info("正在从代码加载策略: %s", strategy_class_name)

            # 使用沙箱安全执行代码
            from app.core.sandbox import execute_safe_code, SandboxSecurityError, SandboxTimeoutError

            # 创建临时模块
            module_name = cls._generate_temporary_module_name()
            module = importlib.import_module(module_name) if module_name in sys.modules else \
                importlib.util.module_from_spec(
                    importlib.util.spec_from_loader(module_name, loader=None)
                )

            # 使用沙箱执行代码
            result = execute_safe_code(code, local_vars=module.__dict__)

            if not result["success"]:
                logger.error("策略代码执行失败: %s", result["error"])
                return None

            sys.modules[module_name] = module

            # 获取策略类
            strategy_class = cls._get_strategy_class(
                module, strategy_class_name
            )

            if strategy_class is None:
                logger.error("在代码中未找到策略类 %s", strategy_class_name)
                return None

            # 创建策略实例
            strategy = strategy_class(config)
            logger.info("策略加载成功: %s", strategy_class_name)
            return strategy

        except SandboxSecurityError as e:
            logger.error(
                "策略代码安全检查失败: %s - %s", strategy_class_name, e,
                exc_info=True
            )
            return None
        except SandboxTimeoutError as e:
            logger.error(
                "策略代码执行超时: %s - %s", strategy_class_name, e,
                exc_info=True
            )
            return None
        except Exception as e:
            logger.error(
                "策略加载失败: %s - %s", strategy_class_name, e,
                exc_info=True
            )
            return None

    @classmethod
    def validate_strategy_class(cls, strategy_class: Type[BaseStrategy]) -> bool:
        """
        验证策略类是否符合要求

        Args:
            strategy_class: 策略类

        Returns:
            True表示验证通过
        """
        try:
            # 检查是否继承自BaseStrategy
            if not issubclass(strategy_class, BaseStrategy):
                logger.error(
                    "策略类 %s 必须继承自 BaseStrategy",
                    strategy_class.__name__
                )
                return False

            # 检查是否实现了所有抽象方法
            missing_methods = []
            for name, method in inspect.getmembers(BaseStrategy, inspect.isfunction):
                if name == "__init__":
                    continue
                if (
                    hasattr(method, "__isabstractmethod__")
                    and method.__isabstractmethod__
                    and not hasattr(strategy_class, name)
                ):
                    missing_methods.append(name)

            if missing_methods:
                logger.error(
                    "策略类 %s 缺少抽象方法: %s",
                    strategy_class.__name__, ", ".join(missing_methods)
                )
                return False

            logger.debug("策略类验证通过: %s", strategy_class.__name__)
            return True

        except Exception as e:
            logger.error(
                "策略类验证失败: %s - %s",
                strategy_class.__name__, e, exc_info=True
            )
            return False

    @classmethod
    def _get_strategy_class(cls, module: Any, class_name: str) -> Optional[Type[BaseStrategy]]:
        """
        从模块中获取策略类并验证

        Args:
            module: 模块对象
            class_name: 类名

        Returns:
            策略类，验证失败返回None
        """
        try:
            strategy_class = getattr(module, class_name, None)

            if strategy_class is None:
                return None

            # 检查是否是类
            if not inspect.isclass(strategy_class):
                logger.error("策略 %s 不是一个类", class_name)
                return None

            # 验证策略类
            if not cls.validate_strategy_class(strategy_class):
                return None

            return strategy_class

        except Exception as e:
            logger.error(
                "获取策略类失败: %s - %s", class_name, e, exc_info=True
            )
            return None

    @classmethod
    def _generate_module_name(cls, file_path: str) -> str:
        """
        从文件路径生成模块名

        Args:
            file_path: 文件路径

        Returns:
            模块名
        """
        from pathlib import Path

        file_path = Path(file_path).absolute()

        # 获取项目根目录（假设在策略根目录下）
        # 这里简化处理，实际项目可能需要更复杂的逻辑
        return f"strategy_{hash(str(file_path))}"

    @classmethod
    def _generate_temporary_module_name(cls) -> str:
        """
        生成临时模块名

        Returns:
            临时模块名
        """
        import uuid
        return f"temp_strategy_{uuid.uuid4().hex[:8]}"


class StrategyFactory:
    """
    策略工厂
    提供统一的策略创建接口
    """

    @classmethod
    def create_strategy(
        cls,
        config: StrategyConfig,
        strategy_source: str,
        strategy_info: Dict[str, str]
    ) -> Optional[BaseStrategy]:
        """
        创建策略实例

        Args:
            config: 策略配置
            strategy_source: 策略来源类型，'module'、'file'或'code'
            strategy_info: 策略信息，包含加载所需的参数

        Returns:
            策略实例
        """
        loader = StrategyLoader()

        strategy: Optional[BaseStrategy] = None

        if strategy_source == "module":
            strategy = loader.load_strategy_from_module(
                strategy_info.get("module_path", ""),
                strategy_info.get("class_name", ""),
                config
            )
        elif strategy_source == "file":
            strategy = loader.load_strategy_from_file(
                strategy_info.get("file_path", ""),
                strategy_info.get("class_name", ""),
                config
            )
        elif strategy_source == "code":
            strategy = loader.load_strategy_from_code(
                strategy_info.get("code", ""),
                strategy_info.get("class_name", ""),
                config
            )
        else:
            logger.error("不支持的策略来源类型: %s", strategy_source)

        return strategy


class StrategyMetadata:
    """
    策略元数据
    """

    def __init__(
        self,
        strategy_id: str,
        strategy_class_name: str,
        module_path: str,
        description: str = "",
        tags: Optional[Dict[str, Any]] = None
    ):
        self.strategy_id = strategy_id
        self.strategy_class_name = strategy_class_name
        self.module_path = module_path
        self.description = description
        self.tags = tags or {}

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "strategy_id": self.strategy_id,
            "strategy_class_name": self.strategy_class_name,
            "module_path": self.module_path,
            "description": self.description,
            "tags": self.tags
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StrategyMetadata":
        """从字典创建实例"""
        return cls(
            strategy_id=data.get("strategy_id", ""),
            strategy_class_name=data.get("strategy_class_name", ""),
            module_path=data.get("module_path", ""),
            description=data.get("description", ""),
            tags=data.get("tags", {})
        )
