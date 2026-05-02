"""
数据映射API - 管理数据源字段映射配置
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.models.data_mapping import DataMapping
from app.services.data_mapping_service import DataMappingService

router = APIRouter(prefix="/api/v1/data_mappings")

# 模拟数据库，实际应使用 PostgreSQL
_data_mappings_db = {}


@router.get("")
async def list_mappings():
    """列出所有数据映射配置"""
    return list(_data_mappings_db.values())


@router.post("")
async def create_mapping(mapping: DataMapping):
    """创建数据映射配置"""
    if not mapping.id:
        import uuid
        mapping.id = str(uuid.uuid4())
    _data_mappings_db[mapping.id] = mapping
    return mapping


@router.get("/{mapping_id}")
async def get_mapping(mapping_id: str):
    """获取指定数据映射配置"""
    if mapping_id not in _data_mappings_db:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return _data_mappings_db[mapping_id]


@router.delete("/{mapping_id}")
async def delete_mapping(mapping_id: str):
    """删除数据映射配置"""
    if mapping_id not in _data_mappings_db:
        raise HTTPException(status_code=404, detail="Mapping not found")
    del _data_mappings_db[mapping_id]
    return {"success": True}


@router.get("/{mapping_id}/schema")
async def get_table_schema():
    """获取可用表和字段结构（从DolphinDB）"""
    # 从 DolphinDB 获取表结构
    # 简化模拟实现
    return {
        "tables": [
            {
                "name": "daily_data",
                "description": "日线行情数据",
                "fields": [
                    {"name": "ts_code", "type": "string", "description": "股票代码"},
                    {"name": "trade_date", "type": "string", "description": "交易日期"},
                    {"name": "open", "type": "double", "description": "开盘价"},
                    {"name": "high", "type": "double", "description": "最高价"},
                    {"name": "low", "type": "double", "description": "最低价"},
                    {"name": "close", "type": "double", "description": "收盘价"},
                    {"name": "vol", "type": "double", "description": "成交量"},
                    {"name": "amount", "type": "double", "description": "成交额"},
                    {"name": "limit_up", "type": "double", "description": "涨停价"},
                    {"name": "limit_down", "type": "double", "description": "跌停价"},
                ]
            },
            {
                "name": "factor_values",
                "description": "因子值数据",
                "fields": [
                    {"name": "ts_code", "type": "string", "description": "股票代码"},
                    {"name": "trade_date", "type": "string", "description": "交易日期"},
                    {"name": "factor_id", "type": "string", "description": "因子ID"},
                    {"name": "factor_value", "type": "double", "description": "因子值"},
                ]
            }
        ]
    }
