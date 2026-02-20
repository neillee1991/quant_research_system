"""
配置管理模块
使用 Pydantic 管理所有配置项，支持环境变量和类型验证
"""
from typing import Literal
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]


class DataCollectorConfig(BaseSettings):
    """数据采集配置"""
    tushare_token: str = Field(default="", env="TUSHARE_TOKEN")
    calls_per_minute: int = Field(default=120, ge=1, le=500)
    retry_times: int = Field(default=3, ge=1, le=10)
    retry_delay: int = Field(default=2, ge=1, le=60)
    timeout: int = Field(default=30, ge=5, le=300)


class DatabaseConfig(BaseSettings):
    """数据库配置"""
    # PostgreSQL 配置
    postgres_host: str = Field(default="localhost", env="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, env="POSTGRES_PORT")
    postgres_db: str = Field(default="quant_research", env="POSTGRES_DB")
    postgres_user: str = Field(default="quant_user", env="POSTGRES_USER")
    postgres_password: str = Field(default="quant_pass_2024", env="POSTGRES_PASSWORD")

    # 连接池配置
    connection_pool_size: int = Field(default=10, ge=1, le=50)
    query_timeout: int = Field(default=300, ge=10, le=3600)

    # 兼容旧版 DuckDB 配置（已废弃）
    duckdb_path: str = Field(default=str(BASE_DIR / "data" / "quant.duckdb"))


class BacktestConfig(BaseSettings):
    """回测配置"""
    initial_capital: float = Field(default=1_000_000.0, gt=0)
    commission_rate: float = Field(default=0.0003, ge=0, le=0.01)
    slippage_rate: float = Field(default=0.0001, ge=0, le=0.01)
    min_position_size: float = Field(default=0.01, ge=0, le=1)
    max_position_size: float = Field(default=0.2, ge=0, le=1)


class MLConfig(BaseSettings):
    """机器学习配置"""
    n_trials: int = Field(default=100, ge=10, le=1000)
    cv_folds: int = Field(default=5, ge=2, le=10)
    test_size: float = Field(default=0.2, ge=0.1, le=0.5)
    random_state: int = Field(default=42)
    n_jobs: int = Field(default=-1)


class SyncConfig(BaseSettings):
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
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    ml: MLConfig = Field(default_factory=MLConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)

    # Tushare (保持向后兼容)
    TUSHARE_TOKEN: str = Field(default="", env="TUSHARE_TOKEN")

    # DuckDB (保持向后兼容)
    DUCKDB_PATH: str = Field(default=str(BASE_DIR / "data" / "quant.duckdb"))

    # API V1 PREFIX (保持向后兼容)
    API_V1_PREFIX: str = Field(default="/api/v1")

    @field_validator("data_dir", "raw_data_dir", "factors_dir", "models_dir", "log_dir")
    @classmethod
    def create_directories(cls, v: Path) -> Path:
        """自动创建目录"""
        v.mkdir(parents=True, exist_ok=True)
        return v

    @property
    def APP_NAME(self) -> str:
        """向后兼容：APP_NAME"""
        return self.app_name

    @property
    def DEBUG(self) -> bool:
        """向后兼容：DEBUG"""
        return self.debug

    @property
    def DATA_DIR(self) -> Path:
        """向后兼容：DATA_DIR"""
        return self.data_dir

    @property
    def RAW_DATA_DIR(self) -> Path:
        """向后兼容：RAW_DATA_DIR"""
        return self.raw_data_dir

    @property
    def FACTORS_DIR(self) -> Path:
        """向后兼容：FACTORS_DIR"""
        return self.factors_dir

    @property
    def MODELS_DIR(self) -> Path:
        """向后兼容：MODELS_DIR"""
        return self.models_dir

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"  # 支持嵌套配置，如 COLLECTOR__CALLS_PER_MINUTE


settings = Settings()
