"""
查询构建器 - 根据元数据生成 DolphinDB 查询
"""
from typing import Dict, Any, List, Tuple
from app.core.logger import logger


class QueryBuilder:
    """查询构建器 - 根据 metadata 生成 DolphinDB 查询"""

    def __init__(self, config: Dict[str, Any], data_config_loader=None):
        self.config = config
        if data_config_loader is not None:
            self._loader = data_config_loader
        else:
            from engine.factor.data_config import data_config_loader as global_loader
            self._loader = global_loader

    def build_wide_table_query(
        self,
        start_date: str,
        end_date: str,
        factors: List[str],
        stocks: List[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """构建宽表查询，返回 (SQL, params) 避免 SQL 注入"""

        # 防呆校验：禁止全市场全量数据拉取
        if not stocks or len(stocks) == 0:
            raise ValueError("股票列表不能为空，禁止全市场全量数据拉取！")

        if len(stocks) > 100:
            raise ValueError(f"MVP阶段单次回测最多选择100只股票，当前选择了{len(stocks)}只")

        # 获取字段映射
        price_fields = self._get_price_fields()
        factor_fields = self._get_factor_fields(factors)

        # 构建主查询
        base_query, base_params = self._build_base_price_query(start_date, end_date, stocks)
        factor_query, factor_params = self._build_factor_query(start_date, end_date, factors, stocks)

        # 动态 Join
        wide_query = self._build_join_query(base_query, factor_query)

        # 合并参数
        params = {**base_params, **factor_params}

        return wide_query, params

    def _get_price_fields(self) -> Dict[str, str]:
        """从 DataConfigLoader 读取价格字段映射：{标准字段名 -> 实际列名}"""
        keys = ["open", "high", "low", "close", "volume", "amount", "limit_up", "limit_down"]
        return {k: self._loader.get(k).get("column_name") or k for k in keys}

    def _get_price_table(self) -> str:
        """从配置读取行情表名（以 open 字段为准）"""
        return self._loader.get("open").get("table_name") or "sync_daily_data"

    def _get_factor_fields(self, factors: List[str]) -> Dict[str, str]:
        """获取因子字段映射"""
        field_mapping = {}
        for factor_id in factors:
            field_mapping[factor_id] = f"{factor_id}_value"
        return field_mapping

    def _build_base_price_query(
        self, start_date: str, end_date: str, stocks: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """构建基础行情查询"""
        params = {"start_date": start_date, "end_date": end_date}
        price_fields = self._get_price_fields()
        table = self._get_price_table()

        # column_name AS field_key，保证下游字段名统一
        select_cols = ", ".join(f"{col} AS {fk}" for fk, col in price_fields.items())

        if stocks:
            stock_filter = "WHERE ts_code in stock_list"
            params["stock_list"] = stocks
        else:
            stock_filter = ""

        return f"""
            SELECT ts_code, trade_date, {select_cols}
            FROM {table}
            {stock_filter}
            WHERE trade_date BETWEEN {{start_date}} AND {{end_date}}
            ORDER BY ts_code, trade_date
        """, params

    def _build_factor_query(
        self, start_date: str, end_date: str, factors: List[str], stocks: List[str]
    ) -> Tuple[str, Dict[str, Any]]:
        """构建因子查询"""
        params = {"start_date": start_date, "end_date": end_date, "factor_list": factors}

        stock_filter = ""
        if stocks:
            stock_filter = "AND ts_code in stock_list"
            params["stock_list"] = stocks

        fields = ", ".join([f"factor_value AS {f}" for f in factors])

        return f"""
            SELECT ts_code, trade_date, {fields}
            FROM factor_values
            WHERE factor_id in factor_list
            AND trade_date BETWEEN {{start_date}} AND {{end_date}}
            {stock_filter}
            ORDER BY ts_code, trade_date
        """, params

    def _build_join_query(self, base_query: str, factor_query: str) -> str:
        """构建 Join 查询"""
        return f"""
            SELECT * FROM aj(
                ({base_query}),
                ({factor_query}),
                `ts_code`trade_date
            )
        """
