"""
自定义撮合器 - 涨跌停限制
"""
from rqalpha.mod.rqalpha_mod_sys_simulation.simulation import LimitUpValidator


class AShareLimitUpValidator(LimitUpValidator):
    """A股涨跌停限制验证"""

    def check_order(self, order, bar_dict):
        """检查委托是否违反涨跌停限制"""
        limit_up = bar_dict[order.order_book_id].limit_up
        limit_down = bar_dict[order.order_book_id].limit_down

        if order.price >= limit_up and order.side == "buy":
            return (False, "触及涨停价，无法买入")

        if order.price <= limit_down and order.side == "sell":
            return (False, "触及跌停价，无法卖出")

        return (True, None)
