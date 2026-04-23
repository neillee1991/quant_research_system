"""
DolphinDB 类型转换模块
负责 Python 类型与 DolphinDB 类型之间的转换
"""
import re
from datetime import datetime, date
from typing import Any


class TypeConverter:
    """Python 与 DolphinDB 类型转换器"""

    @staticmethod
    def convert_date_format(value: str) -> str:
        """
        将 YYYYMMDD 格式的日期字符串转换为 DolphinDB 日期格式

        Args:
            value: 日期字符串，如 '20200101'

        Returns:
            DolphinDB 日期格式，如 '2020.01.01'
        """
        if isinstance(value, str) and re.match(r"^\d{8}$", value):
            return f"{value[:4]}.{value[4:6]}.{value[6:8]}"
        return value

    @staticmethod
    def escape_value(value: Any) -> str:
        """
        将 Python 值转换为 DolphinDB SQL 字面量
        处理字符串引号转义、日期格式、None 等

        Args:
            value: Python 值

        Returns:
            DolphinDB SQL 字面量字符串
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            return f"{value.strftime('%Y.%m.%dT%H:%M:%S')}"
        if isinstance(value, date):
            return f"{value.strftime('%Y.%m.%d')}"
        # 检查是否是 YYYYMMDD 格式的日期字符串，使用 temporalParse 转换
        if isinstance(value, str) and re.match(r"^\d{8}$", value):
            return f'temporalParse("{value}", "yyyyMMdd")'
        # 普通字符串，转义双引号
        s = str(value)
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{s}"'

    @staticmethod
    def escape_symbol(value: Any) -> str:
        """将 Python 字符串转换为 DolphinDB SYMBOL 字面量（反引号语法）"""
        if value is None:
            return "NULL"
        return f"`{value}"
