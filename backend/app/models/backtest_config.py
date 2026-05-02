"""
回测配置模型
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from datetime import datetime


class BacktestConfig(BaseModel):
    """回测配置"""
    initial_capital: float = Field(default=1000000.0, description="初始资金")
    fees: float = Field(default=0.0003, description="手续费")
    slippage: float = Field(default=0.001, description="滑点")
    engine_mode: str = Field(default="vectorbt", description="引擎模式")
    factors: List[str] = Field(default=[], description="使用的因子")
    stocks: List[str] = Field(default=[], description="股票列表（必填，MVP限制最多100只）")
    strategy_code: str = Field(default="", description="策略代码（Python字符串）")
    start_date: str = Field(default="20100101", description="开始日期")
    end_date: str = Field(default="20240101", description="结束日期")
    benchmark: str = Field(default="000300.SH", description="基准指数")

    class Config:
        extra = "allow"
