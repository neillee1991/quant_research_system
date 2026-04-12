"""因子字段映射配置 API"""
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List

from store.dolphindb_client import db_client
from app.core.logger import logger
from app.core.cache import api_cache

router = APIRouter()


class DataFieldMapping(BaseModel):
    field_key: str
    description: str = ""
    table_name: str = ""
    column_name: str = ""
    extra_config: str = "{}"


class DataConfigUpdateRequest(BaseModel):
    mappings: List[DataFieldMapping]


@router.get("/config/field-mappings")
async def get_field_mappings():
    """获取所有字段映射配置（PostgreSQL factor_field_mappings）"""
    from scheduler.db import DatabasePool

    cached = api_cache.get("production:data-config")
    if cached is not None:
        return cached
    try:
        rows = await DatabasePool.fetch(
            "SELECT * FROM factor_field_mappings ORDER BY field_key"
        )
        data = [dict(r) for r in rows]
        for r in data:
            if r.get("updated_at"):
                r["updated_at"] = str(r["updated_at"])
        result = {"status": "success", "data": data}
        api_cache.set("production:data-config", result, ttl=120)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/config/field-mappings")
async def update_field_mappings(req: DataConfigUpdateRequest):
    """批量更新字段映射配置（PostgreSQL factor_field_mappings）"""
    from scheduler.db import DatabasePool

    try:
        now = datetime.now()
        for m in req.mappings:
            await DatabasePool.execute("""
                INSERT INTO factor_field_mappings
                  (field_key, description, table_name, column_name, extra_config, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (field_key) DO UPDATE SET
                  description  = EXCLUDED.description,
                  table_name   = EXCLUDED.table_name,
                  column_name  = EXCLUDED.column_name,
                  extra_config = EXCLUDED.extra_config,
                  updated_at   = EXCLUDED.updated_at
            """,
                m.field_key, m.description, m.table_name,
                m.column_name, m.extra_config, now,
            )
        api_cache.invalidate("production:data-config")
        api_cache.invalidate("production:data-config:resolved")
        return {"status": "success", "message": f"已更新 {len(req.mappings)} 条配置"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config/field-mappings/resolved")
async def get_resolved_field_mappings():
    """返回简化的 field_key → source_label 字典，供前端注解显示"""
    from scheduler.db import DatabasePool

    cached = api_cache.get("production:data-config:resolved")
    if cached is not None:
        return cached
    try:
        rows = await DatabasePool.fetch(
            "SELECT field_key, table_name, column_name, extra_config FROM factor_field_mappings"
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
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config/available-tables")
async def get_available_tables():
    """获取所有可用的数据表（sync/etl/factor，均从 PostgreSQL 查询）"""
    from scheduler.db import DatabasePool

    cached = api_cache.get("production:available-tables")
    if cached is not None:
        return cached
    try:
        tables = []

        try:
            rows = await DatabasePool.fetch(
                "SELECT task_id, table_name, description FROM sync_task_configs "
                "WHERE enabled = true ORDER BY task_id"
            )
            for row in rows:
                tables.append({
                    "value": row["table_name"],
                    "label": row["table_name"],
                    "description": row.get("description", "") or f"同步任务: {row['task_id']}",
                    "type": "sync",
                })
        except Exception as e:
            logger.warning(f"Failed to load sync tasks: {e}")

        try:
            rows = await DatabasePool.fetch(
                "SELECT task_id, table_name, description FROM etl_task_configs "
                "WHERE enabled = true ORDER BY task_id"
            )
            for row in rows:
                tables.append({
                    "value": row["table_name"],
                    "label": row["table_name"],
                    "description": row.get("description", "") or f"ETL任务: {row['task_id']}",
                    "type": "etl",
                })
        except Exception as e:
            logger.warning(f"Failed to load ETL tasks: {e}")

        try:
            rows = await DatabasePool.fetch(
                "SELECT DISTINCT factor_id, description FROM factor_configs ORDER BY factor_id"
            )
            for row in rows:
                tables.append({
                    "value": f"factor:{row['factor_id']}",
                    "label": row["factor_id"],
                    "description": row.get("description", "") or "因子数据",
                    "type": "factor",
                })
        except Exception as e:
            logger.warning(f"Failed to load factors: {e}")

        response = {"status": "success", "data": tables}
        api_cache.set("production:available-tables", response, ttl=300)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
