"""
RQAlpha 运行时外壳与生命周期管理
"""
import rqalpha as rqa
from app.core.logger import logger
from backend.engine.backtest.rqalpha_engine.memory_source import RQAlphaMemoryDataSource


class RQAlphaEngine:
    """RQAlpha 引擎"""

    def __init__(self, config: dict):
        self.config = config

    async def run(
        self,
        strategy,
        start_date: str,
        end_date: str,
        factors: list,
        stocks: list = None
    ) -> dict:
        """执行回测"""
        logger.info(f"开始 RQAlpha 回测: {start_date} 到 {end_date}")

        # 配置 RQAlpha
        rq_config = {
            "base": {
                "start_date": start_date,
                "end_date": end_date,
                "accounts": {
                    "stock": self.config.get('initial_capital', 1000000)
                },
                "benchmark": None,
            },
            "extra": {
                "log_level": "error",
            },
            "mod": {
                "sys_accounts": {
                    "enabled": True,
                },
                "sys_simulation": {
                    "enabled": True,
                    "slippage": self.config.get('slippage', 0.001),
                    "fee": self.config.get('fees', 0.0003)
                },
                "my_custom_orders": {
                    "enabled": True
                }
            },
            "validators": {
                "order": [
                    {
                        "class": "LimitUpValidator",
                        "enabled": True,
                    },
                    {
                        "class": "T1Validator",
                        "enabled": True,
                    }
                ]
            }
        }

        # 创建自定义数据 Feed
        from backend.engine.backtest.rqalpha_engine.memory_source import RQAlphaMemoryDataSource
        data_source = RQAlphaMemoryDataSource()

        # 运行回测
        result = rqa.run(
            config=rq_config,
            data_source=data_source,
            strategies=strategy
        )

        logger.info("RQAlpha 回测完成")
        return result
