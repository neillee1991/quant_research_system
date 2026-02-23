"""
配置管理模块
使用 Pydantic 管理所有配置项，支持环境变量和 .env 文件
"""
from typing import Literal
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
_ENV_FILE = str(BASE_DIR / ".env")


class _BaseConfig(BaseSettings):
    """所有子配置的基类，统一 .env 路径"""
    class Config:
        env_file = _ENV_FILE
        env_file_encoding = "utf-8"
        extra = "ignore"


class DataCollectorConfig(_BaseConfig):
    """数据采集配置"""
    tushare_token: str = Field(default="", env="TUSHARE_TOKEN")
    calls_per_minute: int = Field(default=120, ge=1, le=500)
    retry_times: int = Field(default=3, ge=1, le=10)
    retry_delay: int = Field(default=2, ge=1, le=60)
    timeout: int = Field(default=30, ge=5, le=300)


class DatabaseConfig(_BaseConfig):
    """数据库配置"""
    postgres_host: str = Field(default="localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, env="POSTGRES_PORT")
    postgres_db: str = Field(default="quant_research", env="POSTGRES_DB")
    postgres_user: str = Field(default="quant_user", env="POSTGRES_USER")
    postgres_password: str = Field(default="", env="POSTGRES_PASSWORD")

    # 连接池配置（优化后）
    connection_pool_min: int = Field(default=10, ge=1, le=50, env="DB_POOL_MIN")
    connection_pool_size: int = Field(default=50, ge=1, le=100, env="DB_POOL_MAX")
    query_timeout: int = Field(default=300, ge=10, le=3600)


class RedisConfig(_BaseConfig):
    """Redis缓存配置"""
    redis_host: str = Field(default="localhost", env="REDIS_HOST")
    redis_port: int = Field(default=6379, env="REDIS_PORT")
    redis_db: int = Field(default=0, env="REDIS_DB")
    redis_password: str = Field(default="", env="REDIS_PASSWORD")
    redis_max_connections: int = Field(default=50, ge=1, le=100)

    # 缓存TTL配置（秒）
    cache_ttl_stock_list: int = Field(default=3600, ge=60)  # 股票列表缓存1小时
    cache_ttl_daily_data: int = Field(default=1800, ge=60)  # 日线数据缓存30分钟
    cache_ttl_factor_metadata: int = Field(default=3600, ge=60)  # 因子元数据缓存1小时
    cache_ttl_factor_analysis: int = Field(default=7200, ge=60)  # 因子分析缓存2小时


class BacktestConfig(_BaseConfig):
    """回测配置"""
    initial_capital: float = Field(default=1_000_000.0, gt=0)
    commission_rate: float = Field(default=0.0003, ge=0, le=0.01)
    slippage_rate: float = Field(default=0.0001, ge=0, le=0.01)
    min_position_size: float = Field(default=0.01, ge=0, le=1)
    max_position_size: float = Field(default=0.2, ge=0, le=1)


class MLConfig(_BaseConfig):
    """机器学习配置"""
    n_trials: int = Field(default=100, ge=10, le=1000)
    cv_folds: int = Field(default=5, ge=2, le=10)
    test_size: float = Field(default=0.2, ge=0.1, le=0.5)
    random_state: int = Field(default=42)
    n_jobs: int = Field(default=-1)


class SyncConfig(_BaseConfig):
    """数据同步配置"""
    config_path: str = Field(
        default=str(BASE_DIR / "backend" / "data_manager" / "sync_config.json")
    )
    schedule_time: str = Field(default="18:00", pattern=r"^\d{2}:\d{2}$")
    enable_scheduler: bool = Field(default=True)
    default_start_date: str = Field(default="20100101", pattern=r"^\d{8}$")


class Settings(BaseSettings):
    """主配置类"""
    # 应用配置
    app_name: str = Field(default="Quant Research System")
    environment: Literal["development", "testing", "production"] = Field(default="development")
    debug: bool = Field(default=False)
    api_v1_prefix: str = Field(default="/api/v1")

    # 路径配置
    data_dir: Path = Field(default=BASE_DIR / "data")
    raw_data_dir: Path = Field(default=BASE_DIR / "data" / "raw")
    factors_dir: Path = Field(default=BASE_DIR / "data" / "factors")
    models_dir: Path = Field(default=BASE_DIR / "data" / "models")
    log_dir: Path = Field(default=BASE_DIR / "backend" / "logs")

    # 子配置
    collector: DataCollectorConfig = Field(default_factory=DataCollectorConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    ml: MLConfig = Field(default_factory=MLConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)

    @field_validator("data_dir", "raw_data_dir", "factors_dir", "models_dir", "log_dir")
    @classmethod
    def create_directories(cls, v: Path) -> Path:
        """自动创建目录"""
        v.mkdir(parents=True, exist_ok=True)
        return v

    class Config:
        env_file = _ENV_FILE
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        extra = "ignore"


settings = Settings()
