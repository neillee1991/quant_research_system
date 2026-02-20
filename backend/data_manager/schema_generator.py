"""
Tushare Schema 生成工具

用于快速生成 sync_config.json 中的 schema 定义
"""

import tushare as ts
import pandas as pd
from typing import Dict, Any


# Pandas dtype 到 DuckDB 类型的映射
DTYPE_MAPPING = {
    'int64': 'BIGINT',
    'int32': 'INTEGER',
    'float64': 'DOUBLE',
    'float32': 'FLOAT',
    'object': 'VARCHAR',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP',
}


def infer_duckdb_type(pandas_dtype: str) -> str:
    """推断 DuckDB 类型"""
    dtype_str = str(pandas_dtype)
    return DTYPE_MAPPING.get(dtype_str, 'VARCHAR')


def generate_schema_from_api(api_name: str, token: str, sample_params: Dict[str, Any] = None) -> Dict[str, Dict]:
    """
    从 Tushare API 调用结果生成 schema 定义

    Args:
        api_name: API 名称，如 'daily', 'daily_basic'
        token: Tushare token
        sample_params: 示例参数，用于调用 API 获取样本数据

    Returns:
        schema 字典
    """
    ts.set_token(token)
    pro = ts.pro_api()

    # 获取 API 函数
    api_func = getattr(pro, api_name, None)
    if api_func is None:
        raise ValueError(f"API {api_name} not found")

    # 调用 API 获取样本数据
    if sample_params is None:
        sample_params = {}

    try:
        df = api_func(**sample_params)
        if df is None or df.empty:
            raise ValueError(f"No data returned from {api_name}")
    except Exception as e:
        raise ValueError(f"Failed to call {api_name}: {e}")

    # 生成 schema
    schema = {}
    for col in df.columns:
        dtype = df[col].dtype
        duckdb_type = infer_duckdb_type(dtype)

        # 检查是否有空值
        has_null = df[col].isnull().any()

        schema[col] = {
            "type": duckdb_type,
            "nullable": bool(has_null),
            "comment": f"{col}"  # 可以手动补充中文注释
        }

    return schema


def print_schema_json(schema: Dict[str, Dict], indent: int = 2):
    """打印格式化的 schema JSON"""
    import json
    print(json.dumps({"schema": schema}, indent=indent, ensure_ascii=False))


# 使用示例
if __name__ == "__main__":
    # 需要设置你的 Tushare token
    TOKEN = "your_tushare_token_here"

    # 示例1: 生成 daily 接口的 schema
    print("=== daily 接口 schema ===")
    schema = generate_schema_from_api(
        api_name="daily",
        token=TOKEN,
        sample_params={"trade_date": "20240101"}
    )
    print_schema_json(schema)

    print("\n=== daily_basic 接口 schema ===")
    schema = generate_schema_from_api(
        api_name="daily_basic",
        token=TOKEN,
        sample_params={"trade_date": "20240101"}
    )
    print_schema_json(schema)

    print("\n=== stock_basic 接口 schema ===")
    schema = generate_schema_from_api(
        api_name="stock_basic",
        token=TOKEN,
        sample_params={"exchange": "", "list_status": "L"}
    )
    print_schema_json(schema)
