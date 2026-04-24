"""因子注册和元数据管理 API 端点"""
import json
import re
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from infrastructure.database.dolphindb_client import db_client
from engine.factor.registry import list_factors, discover_factors, unregister_factor
from app.core.logger import logger
from app.core.utils import safe_json_parse
from app.core.cache import api_cache

router = APIRouter()

_SAFE_FACTOR_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')

# ==================== Pydantic Models ====================

class FactorCreateRequest(BaseModel):
    factor_id: str
    description: str = ""
    category: str = "custom"
    compute_mode: str = "incremental"
    depends_on: List[str] = []
    storage_target: str = "factor_values"
    params: Dict[str, Any] = {}
    code: Optional[str] = None
    align_calendar: bool = False

class FactorUpdateRequest(BaseModel):
    description: Optional[str] = None
    category: Optional[str] = None
    compute_mode: Optional[str] = None
    depends_on: Optional[List[str]] = None
    storage_target: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    align_calendar: Optional[bool] = None

class FactorCodeUpdateRequest(BaseModel):
    filename: str
    code: str

# ==================== Helper Functions ====================

def _validate_factor_id(factor_id: str):
    if not _SAFE_FACTOR_ID_RE.match(factor_id):
        raise HTTPException(status_code=400, detail=f"Invalid factor_id: '{factor_id}'")

# ==================== API Endpoints ====================

@router.get("/factor/factors")
async def list_registered_factors():
    """列出所有因子（从 PostgreSQL factor_configs + DolphinDB factor_values 最新日期）"""
    from scheduler.db import DatabasePool

    cached = api_cache.get("production:factors")
    if cached is not None:
        return cached

    discover_factors(db_client=db_client)

    db_meta: Dict[str, Any] = {}
    latest_dates: Dict[str, str] = {}
    last_computed: Dict[str, str] = {}

    # factor_configs from PostgreSQL
    try:
        rows = await DatabasePool.fetch("SELECT * FROM factor_configs ORDER BY factor_id")
        for row in rows:
            db_meta[row["factor_id"]] = dict(row)
    except Exception as e:
        logger.debug(f"查询 factor_configs 失败: {e}")

    # factor_values latest dates from DolphinDB (time-series, stays)
    try:
        fv_df = db_client.query(
            "SELECT factor_id, max(trade_date) AS latest_date FROM factor_values GROUP BY factor_id"
        )
        if not fv_df.is_empty():
            for row in fv_df.to_dicts():
                date_val = row["latest_date"]
                if date_val:
                    date_str = str(date_val)
                    if len(date_str) == 8:
                        latest_dates[row["factor_id"]] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                    else:
                        latest_dates[row["factor_id"]] = date_str
    except Exception as e:
        logger.debug(f"查询因子最新日期失败: {e}")

    # last computed from PostgreSQL task_runs
    try:
        run_rows = await DatabasePool.fetch(
            "SELECT task_id, MAX(started_at) AS last_run FROM task_runs "
            "WHERE task_type = 'factor' AND status = 'success' GROUP BY task_id"
        )
        for row in run_rows:
            last_computed[row["task_id"]] = str(row["last_run"]) if row.get("last_run") else None
    except Exception as e:
        logger.debug(f"查询因子上次计算时间失败: {e}")

    merged = []
    for fid in sorted(db_meta.keys()):
        meta = db_meta[fid]
        merged.append({
            "factor_id": fid,
            "description": meta.get("description", ""),
            "category": meta.get("category", "custom"),
            "compute_mode": meta.get("compute_mode", "incremental"),
            "depends_on": safe_json_parse(meta.get("depends_on"), default=[]),
            "storage_target": meta.get("storage_target", "factor_values"),
            "params": safe_json_parse(meta.get("params")),
            "align_calendar": bool(meta.get("align_calendar", False)),
            "latest_date": latest_dates.get(fid),
            "last_computed_at": last_computed.get(fid),
            "source": "db",
        })

    result = {"status": "success", "data": merged}
    api_cache.set("production:factors", result, ttl=60)
    return result

@router.post("/factor/factors")
async def create_factor(req: FactorCreateRequest):
    """创建新因子（写入 PostgreSQL factor_configs）"""
    from scheduler.db import DatabasePool

    _validate_factor_id(req.factor_id)
    try:
        existing = await DatabasePool.fetchrow(
            "SELECT factor_id FROM factor_configs WHERE factor_id = $1", req.factor_id
        )
        if existing:
            raise HTTPException(status_code=400, detail=f"因子 {req.factor_id} 已存在")

        now = datetime.now()
        await DatabasePool.execute("""
            INSERT INTO factor_configs
              (factor_id, description, category, compute_mode, storage_target,
               depends_on, params, code, enabled, align_calendar, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        """,
            req.factor_id, req.description, req.category, req.compute_mode,
            req.storage_target, json.dumps(req.depends_on), json.dumps(req.params),
            req.code or "", True, req.align_calendar, now, now,
        )
        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": req.factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/factor/factors/{factor_id}")
async def update_factor(factor_id: str, req: FactorUpdateRequest):
    """更新因子元数据（PostgreSQL factor_configs）"""
    from scheduler.db import DatabasePool

    _validate_factor_id(factor_id)
    try:
        row = await DatabasePool.fetchrow(
            "SELECT * FROM factor_configs WHERE factor_id = $1", factor_id
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

        existing = dict(row)
        updates = req.model_dump(exclude_unset=True)
        now = datetime.now()

        await DatabasePool.execute("""
            UPDATE factor_configs SET
              description    = $2,
              category       = $3,
              compute_mode   = $4,
              storage_target = $5,
              depends_on     = $6,
              params         = $7,
              align_calendar = $8,
              updated_at     = $9
            WHERE factor_id = $1
        """,
            factor_id,
            updates.get("description", existing.get("description", "")),
            updates.get("category", existing.get("category", "custom")),
            updates.get("compute_mode", existing.get("compute_mode", "incremental")),
            updates.get("storage_target", existing.get("storage_target", "factor_values")),
            json.dumps(updates["depends_on"]) if "depends_on" in updates else existing.get("depends_on", "[]"),
            json.dumps(updates["params"]) if "params" in updates else existing.get("params", "{}"),
            updates.get("align_calendar", existing.get("align_calendar", False)),
            now,
        )
        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/factor/factors/{factor_id}")
async def delete_factor(factor_id: str, delete_data: bool = False):
    """删除因子元数据，可选删除因子值数据"""
    from scheduler.db import DatabasePool

    _validate_factor_id(factor_id)
    try:
        await DatabasePool.execute(
            "DELETE FROM factor_configs WHERE factor_id = $1", factor_id
        )
        if delete_data:
            from infrastructure.database.type_converter import TypeConverter
            db_client.execute(f"DELETE FROM factor_values WHERE factor_id = {TypeConverter.escape_symbol(factor_id)}")

        unregister_factor(factor_id)
        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": factor_id, "data_deleted": delete_data}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/factor/factors/{factor_id}/code")
async def get_factor_code(factor_id: str):
    """获取因子源代码（从 PostgreSQL factor_configs 读取）"""
    from scheduler.db import DatabasePool

    try:
        row = await DatabasePool.fetchrow(
            "SELECT code FROM factor_configs WHERE factor_id = $1", factor_id
        )
        if row and row["code"] and row["code"].strip():
            return {
                "status": "success",
                "data": {"filename": f"{factor_id}.py", "code": row["code"]}
            }
    except Exception as e:
        logger.warning(f"Failed to read code from database: {e}")

    raise HTTPException(status_code=404, detail=f"因子 {factor_id} 的源代码未找到")

@router.put("/factor/factors/{factor_id}/code")
async def update_factor_code(factor_id: str, req: FactorCodeUpdateRequest):
    """更新因子源代码（保存到 PostgreSQL factor_configs）"""
    from scheduler.db import DatabasePool

    try:
        existing = await DatabasePool.fetchrow(
            "SELECT factor_id FROM factor_configs WHERE factor_id = $1", factor_id
        )
        if not existing:
            raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

        await DatabasePool.execute(
            "UPDATE factor_configs SET code = $2, updated_at = $3 WHERE factor_id = $1",
            factor_id, req.code, datetime.now(),
        )
        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update factor code failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/factor/factors/{factor_id}/data")
async def get_factor_data(
    factor_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    limit: int = 200,
):
    """查询因子值数据（DolphinDB factor_values，时序数据）"""
    try:
        from infrastructure.database.type_converter import TypeConverter
        conditions = [f"factor_id = {TypeConverter.escape_symbol(factor_id)}"]
        params: list = []
        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)
        if ts_code:
            conditions.append(f"ts_code = {TypeConverter.escape_symbol(ts_code)}")
        where = " AND ".join(conditions)
        params.append(limit)

        df = db_client.query(
            f"SELECT ts_code, trade_date, factor_value FROM factor_values "
            f"WHERE {where} ORDER BY trade_date DESC, ts_code LIMIT %s",
            tuple(params),
        )
        data = []
        if not df.is_empty():
            for row in df.to_dicts():
                if row.get("trade_date"):
                    date_str = str(row["trade_date"])
                    if 'T' in date_str:
                        row["trade_date"] = date_str.split('T')[0]
                    elif len(date_str) == 8 and date_str.isdigit():
                        row["trade_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                    else:
                        row["trade_date"] = date_str[:10]
                data.append(row)
        return {"status": "success", "data": data, "total": len(data)}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": [], "total": 0}
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/factor/factors/{factor_id}/stats")
async def get_factor_stats(factor_id: str):
    """获取因子统计摘要（DolphinDB factor_values，时序数据）"""
    try:
        from infrastructure.database.type_converter import TypeConverter
        fid_sym = TypeConverter.escape_symbol(factor_id)
        df = db_client.query(f"""
            SELECT
                count(*) AS total_rows,
                min(trade_date) AS min_date,
                max(trade_date) AS max_date,
                avg(factor_value) AS mean_val,
                std(factor_value) AS std_val,
                min(factor_value) AS min_val,
                max(factor_value) AS max_val
            FROM factor_values WHERE factor_id = {fid_sym}
        """)

        if df.is_empty():
            return {"status": "success", "data": None}

        row = df.to_dicts()[0]

        stock_count_df = db_client.query(f"""
            SELECT count(*) AS stock_count
            FROM (SELECT DISTINCT ts_code FROM factor_values WHERE factor_id = {fid_sym})
        """)
        row["stock_count"] = stock_count_df["stock_count"][0] if not stock_count_df.is_empty() else 0

        for k, v in row.items():
            if k in ["min_date", "max_date"]:
                continue
            if v is not None and not isinstance(v, (str, int)):
                row[k] = float(v)
        for date_field in ["min_date", "max_date"]:
            if row.get(date_field):
                date_str = str(row[date_field])
                if 'T' in date_str:
                    row[date_field] = date_str.split('T')[0]
                elif len(date_str) == 8 and date_str.isdigit():
                    row[date_field] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                else:
                    row[date_field] = date_str[:10]
        return {"status": "success", "data": row}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": None}
        raise HTTPException(status_code=500, detail=str(e))

