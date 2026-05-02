"""
自定义数量调整器 - T+1 限制
"""


class AShareT1Validator:
    """A股 T+1 交易限制验证"""

    def check_order(self, order, context, bar_dict):
        """检查 T+1 限制"""
        if order.order_book_id in context.portfolio.positions:
            position = context.portfolio.positions[order.order_book_id]

            if order.side == "sell" and order.quantity > position.closable:
                return (False, f"T+1 限制：可卖出 {position.closable} 股，但委托卖出 {order.quantity} 股")

        return (True, None)
