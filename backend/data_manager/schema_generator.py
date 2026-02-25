"""
Tushare Schema 生成工具

用于快速生成 DolphinDB sync_task_config 表中的 schema 定义。
从 Tushare API 采样数据推断列类型，输出 JSON 格式的 schema。

使用方式：
    export TUSHARE_TOKEN=your_token_here
    python -m data_manager.schema_generator
"""

import os
import tushare as ts
import pandas as pd
from typing import Dict, Any


# Pandas dtype 到 DolphinDB 类型的映射
DTYPE_MAPPING = {
    'int64': 'LONG',
    'int32': 'INT',
    'float64': 'DOUBLE',
    'float32': 'FLOAT',
    'object': 'SYMBOL',
    'bool': 'BOOL',
    'datetime64[ns]': 'TIMESTAMP',
}


def infer_dolphindb_type(pandas_dtype: str) -> str:
    """推断 DolphinDB 列类型"""
    dtype_str = str(pandas_dtype)
    return DTYPE_MAPPING.get(dtype_str, 'STRING')


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
        ddb_type = infer_dolphindb_type(dtype)

        # 检查是否有空值
        has_null = df[col].isnull().any()

        schema[col] = {
            "type": ddb_type,
            "nullable": bool(has_null),
            "comment": col
        }

    return schema


def print_schema_json(schema: Dict[str, Dict], indent: int = 2):
    """打印格式化的 schema JSON"""
    import json
    print(json.dumps({"schema": schema}, indent=indent, ensure_ascii=False))


# 使用示例
if __name__ == "__main__":
    TOKEN = os.environ.get("TUSHARE_TOKEN", "")
    if not TOKEN:
        print("请设置环境变量 TUSHARE_TOKEN 后再运行")
        exit(1)

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
