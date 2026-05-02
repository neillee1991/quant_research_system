"""统一数据字段映射配置 API"""
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List

from infrastructure.database.dolphindb_client import db_client
from app.core.logger import logger
from app.core.cache import api_cache

router = APIRouter()


class DataFieldMapping(BaseModel):
    field_key: str
    description: str = ""
    table_name: str = ""
    column_name: str = ""
    extra_config: str = "{}"
    used_by: List[str] = ["factor", "backtest"]  # 支持数组，标注使用模块


class DataConfigUpdateRequest(BaseModel):
    mappings: List[DataFieldMapping]


# 统一的预设字段配置，包含因子分析和回测需要的所有字段
DATA_PRESET_FIELDS = [
    # 因子分析专用字段
    {"field_key": "adj_factor",    "description": "复权因子",   "used_by": ["factor"]},
    {"field_key": "industry_l1",   "description": "一级行业",   "used_by": ["factor"]},
    {"field_key": "industry_l2",   "description": "二级行业",   "used_by": ["factor"]},
    {"field_key": "is_limit",      "description": "涨跌停标记", "used_by": ["factor"]},
    {"field_key": "is_st",         "description": "ST标记",     "used_by": ["factor"]},
    {"field_key": "list_date",     "description": "上市日期",   "used_by": ["factor"]},
    {"field_key": "market_cap",    "description": "市值",       "used_by": ["factor"]},
    # 因子分析 + 回测共用行情字段
    {"field_key": "open",          "description": "开盘价",     "used_by": ["factor", "backtest"]},
    {"field_key": "high",          "description": "最高价",     "used_by": ["factor", "backtest"]},
    {"field_key": "low",           "description": "最低价",     "used_by": ["factor", "backtest"]},
    {"field_key": "close",         "description": "收盘价",     "used_by": ["factor", "backtest"]},
    {"field_key": "volume",        "description": "成交量",     "used_by": ["factor", "backtest"]},
    # 回测专用字段
    {"field_key": "amount",        "description": "成交额",     "used_by": ["backtest"]},
    {"field_key": "limit_up",      "description": "涨停价",     "used_by": ["backtest"]},
    {"field_key": "limit_down",    "description": "跌停价",     "used_by": ["backtest"]},
]


@router.get("/config/data-mappings")
async def get_data_mappings():
    """获取所有数据字段映射配置（PostgreSQL data_field_mappings）"""
    from scheduler.db import DatabasePool

    cached = api_cache.get("production:data-config")
    if cached is not None:
        return cached
    try:
        rows = await DatabasePool.fetch(
            "SELECT * FROM data_field_mappings ORDER BY field_key"
        )
        data = []
        for row in rows:
            row_dict = dict(row)
            if row_dict.get("updated_at"):
                row_dict["updated_at"] = str(row_dict["updated_at"])
            # 确保 used_by 是列表格式
            if isinstance(row_dict.get("used_by"), str):
                row_dict["used_by"] = json.loads(row_dict["used_by"])
            data.append(row_dict)
        # 用数据库数据覆盖预设字段
        db_map = {row["field_key"]: row for row in data}
        merged_data = []
        for preset in DATA_PRESET_FIELDS:
            if preset["field_key"] in db_map:
                # 数据库中存在，合并
                merged_row = {**preset, **db_map[preset["field_key"]]}
                # 确保 used_by 不被覆盖为 None
                if not merged_row.get("used_by"):
                    merged_row["used_by"] = preset["used_by"]
                merged_data.append(merged_row)
            else:
                merged_data.append(preset)
        result = {"status": "success", "data": merged_data}
        api_cache.set("production:data-config", result, ttl=120)
        return result
    except Exception as e:
        logger.error(f"Failed to get data config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/config/data-mappings")
async def update_data_mappings(
    req: DataConfigUpdateRequest,
):
    """批量更新数据字段映射配置（PostgreSQL data_field_mappings）"""
    from scheduler.db import DatabasePool

    try:
        now = datetime.now()
        for m in req.mappings:
            used_by_json = json.dumps(m.used_by) if m.used_by else json.dumps(["factor", "backtest"])
            await DatabasePool.execute("""
                INSERT INTO data_field_mappings
                  (field_key, description, table_name, column_name, extra_config, used_by, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (field_key) DO UPDATE SET
                  description  = EXCLUDED.description,
                  table_name   = EXCLUDED.table_name,
                  column_name  = EXCLUDED.column_name,
                  extra_config = EXCLUDED.extra_config,
                  used_by      = EXCLUDED.used_by,
                  updated_at   = EXCLUDED.updated_at
            """,
                m.field_key, m.description, m.table_name,
                m.column_name, m.extra_config, used_by_json, now,
            )
        api_cache.invalidate("production:data-config")
        api_cache.invalidate("production:data-config:resolved")
        return {"status": "success", "message": f"已更新 {len(req.mappings)} 条配置"}
    except Exception as e:
        logger.error(f"Failed to update data config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config/data-mappings/resolved")
async def get_resolved_data_mappings():
    """返回简化的 field_key -> source_label 字典，供前端注解显示"""
    from scheduler.db import DatabasePool

    cached = api_cache.get("production:data-config:resolved")
    if cached is not None:
        return cached
    try:
        rows = await DatabasePool.fetch(
            "SELECT field_key, table_name, column_name, extra_config FROM data_field_mappings"
        )
        result = {}
        for row in rows:
            fk = row["field_key"]
            tbl = row.get("table_name", "") or ""
            col = row.get("column_name", "") or ""
            extra = row.get("extra_config", "{}") or "{}"
            if tbl and col:
                source_label = f"{tbl}.{col}"
            elif extra != "{}":
                try:
                    cfg = json.loads(extra)
                    mode = cfg.get("mode", "")
                    if mode == "infer_from_gaps":
                        source_label = "从交易日缺失推断"
                    elif mode == "compute_from_ohlcv":
                        source_label = "从OHLCV计算"
                    else:
                        source_label = "未配置"
                except Exception:
                    source_label = "未配置"
            else:
                source_label = "未配置"
            result[fk] = {"source_label": source_label, "values": None}

        response = {"status": "success", "data": result}
        api_cache.set("production:data-config:resolved", response, ttl=120)
        return response
    except Exception as e:
        logger.error(f"Failed to get resolved data config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config/available-tables")
async def get_available_tables():
    """返回 DolphinDB 中所有可用表名列表"""
    try:
        tables = db_client.list_tables()
        data = []
        for t in tables:
            if isinstance(t, dict):
                name = t.get("table_name") or t.get("name") or str(t)
            else:
                name = str(t)
            data.append({"value": name})
        return {"status": "success", "data": data}
    except Exception as e:
        logger.error(f"Failed to list available tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config/table-columns/{table_name}")
async def get_table_columns(table_name: str):
    """返回指定表的列名列表"""
    try:
        columns = db_client.get_table_columns(table_name)
        return {"status": "success", "columns": columns}
    except Exception as e:
        logger.error(f"Failed to get columns for table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
