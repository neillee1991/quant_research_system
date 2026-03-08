"""因子注册和元数据管理 API 端点"""
import json
import os
import re
import inspect
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import polars as pl

from store.dolphindb_client import db_client
from engine.production.registry import list_factors, discover_factors, unregister_factor
from app.core.logger import logger
from app.core.utils import safe_json_parse
from app.core.cache import api_cache

router = APIRouter()

FACTORS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "engine", "production", "factors")
_SAFE_FACTOR_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')


# ==================== Pydantic Models ====================

class FactorCreateRequest(BaseModel):
    """因子创建请求"""
    factor_id: str
    description: str = ""
    category: str = "custom"
    compute_mode: str = "incremental"
    depends_on: List[str] = []
    storage_target: str = "factor_values"
    params: Dict[str, Any] = {}
    code: Optional[str] = None


class FactorUpdateRequest(BaseModel):
    """因子更新请求"""
    description: Optional[str] = None
    category: Optional[str] = None
    compute_mode: Optional[str] = None
    depends_on: Optional[List[str]] = None
    storage_target: Optional[str] = None
    params: Optional[Dict[str, Any]] = None


class FactorCodeUpdateRequest(BaseModel):
    """因子代码更新请求"""
    filename: str
    code: str


# ==================== Helper Functions ====================

def _validate_factor_id(factor_id: str):
    """验证因子 ID 格式"""
    if not _SAFE_FACTOR_ID_RE.match(factor_id):
        raise HTTPException(status_code=400, detail=f"Invalid factor_id: '{factor_id}'")


# ==================== API Endpoints ====================

@router.get("/production/factors")
async def list_registered_factors():
    """列出所有因子（合并装饰器注册 + 数据库手动注册）"""
    cached = api_cache.get("production:factors")
    if cached is not None:
        return cached

    discover_factors(db_client=db_client)

    # 装饰器注册的因子
    code_factors = {f["factor_id"]: f for f in list_factors()}

    # 优化：使用单个查询获取元数据和最新日期（消除N+1查询）
    db_meta = {}
    latest_dates: Dict[str, str] = {}
    try:
        # 先查 factor_metadata
        meta_df = db_client.query("SELECT * FROM factor_metadata ORDER BY factor_id")
        if not meta_df.is_empty():
            for row in meta_df.to_dicts():
                db_meta[row["factor_id"]] = row

        # 再查 factor_values 的最新日期
        try:
            fv_df = db_client.query(
                "SELECT factor_id, max(trade_date) AS latest_date FROM factor_values GROUP BY factor_id"
            )
            if not fv_df.is_empty():
                for row in fv_df.to_dicts():
                    latest_dates[row["factor_id"]] = row["latest_date"]
        except Exception as e:
            logger.debug(f"查询因子最新日期失败: {e}")
    except Exception as e:
        logger.debug(f"查询因子元数据失败: {e}")

    # 自动种子：代码因子不在 DB 中时自动写入（包含代码）
    for fid, fdef in code_factors.items():
        if fid not in db_meta:
            try:
                now = datetime.now()

                # 提取因子函数的源代码
                code_str = ""
                if "func" in fdef and callable(fdef["func"]):
                    try:
                        code_str = inspect.getsource(fdef["func"])
                    except (OSError, TypeError):
                        pass

                seed_df = pl.DataFrame({
                    "factor_id": [fid],
                    "description": [fdef.get("description", "")],
                    "category": [fdef.get("category", "custom")],
                    "compute_mode": [fdef.get("compute_mode", "incremental")],
                    "storage_target": [fdef.get("storage_target", "factor_values")],
                    "depends_on": [json.dumps(fdef.get("depends_on", []))],
                    "params": [json.dumps(fdef.get("params", {}))],
                    "code": [code_str],
                    "created_at": [now],
                    "updated_at": [now],
                })
                db_client.upsert("factor_metadata", seed_df, ["factor_id"])
                db_meta[fid] = {"factor_id": fid, "description": fdef.get("description", ""),
                                "category": fdef.get("category", "custom")}
            except Exception as e:
                logger.warning(f"自动种子因子 {fid} 失败: {e}")

    # 合并：DB 元数据优先（用户手动修改），代码定义作为 fallback
    all_ids = set(code_factors.keys()) | set(db_meta.keys())
    merged = []
    for fid in sorted(all_ids):
        item = code_factors.get(fid, {})
        meta = db_meta.get(fid, {})
        db_params = safe_json_parse(meta.get("params"))
        db_depends_on = safe_json_parse(meta.get("depends_on"), default=[])

        merged.append({
            "factor_id": fid,
            "description": meta.get("description") or item.get("description", ""),
            "category": meta.get("category") or item.get("category", "custom"),
            "compute_mode": meta.get("compute_mode") or item.get("compute_mode", "incremental"),
            "depends_on": db_depends_on if db_depends_on else item.get("depends_on", []),
            "storage_target": meta.get("storage_target") or item.get("storage_target", "factor_values"),
            "params": db_params if db_params else item.get("params", {}),
            "latest_date": latest_dates.get(fid),
            "source": "code" if fid in code_factors else "db",
        })

    result = {"status": "success", "data": merged}
    api_cache.set("production:factors", result, ttl=60)
    return result


@router.post("/production/factors")
async def create_factor(req: FactorCreateRequest):
    """创建新因子（写入数据库）"""
    _validate_factor_id(req.factor_id)
    try:
        existing = db_client.query(
            "SELECT * FROM factor_metadata WHERE factor_id = %s", (req.factor_id,)
        )
        if not existing.is_empty():
            raise HTTPException(status_code=400, detail=f"因子 {req.factor_id} 已存在")

        now = datetime.now()
        new_df = pl.DataFrame({
            "factor_id": [req.factor_id],
            "description": [req.description],
            "category": [req.category],
            "compute_mode": [req.compute_mode],
            "storage_target": [req.storage_target],
            "depends_on": [json.dumps(req.depends_on)],
            "params": [json.dumps(req.params)],
            "code": [req.code or ""],
            "created_at": [now],
            "updated_at": [now],
        })
        db_client.upsert("factor_metadata", new_df, ["factor_id"])
        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": req.factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/production/factors/{factor_id}")
async def update_factor(factor_id: str, req: FactorUpdateRequest):
    """更新因子元数据（直接更新）"""
    _validate_factor_id(factor_id)
    try:
        existing = db_client.query(
            "SELECT * FROM factor_metadata WHERE factor_id = %s", (factor_id,)
        )
        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

        row = existing.to_dicts()[0]
        updates = req.model_dump(exclude_unset=True)
        now = datetime.now()

        # 构建更新数据
        update_df = pl.DataFrame({
            "factor_id": [factor_id],
            "description": [updates.get("description", row.get("description", ""))],
            "category": [updates.get("category", row.get("category", "custom"))],
            "compute_mode": [updates.get("compute_mode", row.get("compute_mode", "incremental"))],
            "storage_target": [updates.get("storage_target", row.get("storage_target", "factor_values"))],
            "depends_on": [json.dumps(updates["depends_on"]) if "depends_on" in updates else row.get("depends_on", "[]")],
            "params": [json.dumps(updates["params"]) if "params" in updates else row.get("params", "{}")],
            "code": [row.get("code", "")],
            "enabled": [updates.get("enabled", row.get("enabled", True))],
            "created_at": [row.get("created_at", now)],
            "updated_at": [now],
        })

        db_client.upsert("factor_metadata", update_df, ["factor_id"])

        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/production/factors/{factor_id}")
async def delete_factor(factor_id: str, delete_data: bool = False):
    """删除因子元数据、代码文件和注册表条目，可选删除因子值数据"""
    _validate_factor_id(factor_id)
    try:
        db_client.execute("DELETE FROM factor_metadata WHERE factor_id = %s", (factor_id,))
        if delete_data:
            db_client.execute("DELETE FROM factor_values WHERE factor_id = %s", (factor_id,))

        # 删除对应的代码文件（如果存在）
        factors_dir = os.path.normpath(FACTORS_DIR)
        code_file = os.path.join(factors_dir, f"{factor_id}.py")
        if os.path.isfile(code_file):
            os.remove(code_file)
            logger.info(f"Deleted factor code file: {code_file}")

        # 从内存注册表中移除
        unregister_factor(factor_id)
        api_cache.invalidate("production:factors")

        logger.debug(f"Cleared factor list cache after deleting {factor_id}")

        return {"status": "success", "data": {"factor_id": factor_id, "data_deleted": delete_data}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/factors/{factor_id}/code")
async def get_factor_code(factor_id: str):
    """获取因子源代码（优先从数据库读取，备用从文件读取）"""
    # 1. 优先从数据库读取
    try:
        df = db_client.query(
            "SELECT code FROM factor_metadata WHERE factor_id = %s", (factor_id,)
        )
        if not df.is_empty():
            code = df["code"][0]
            if code and code.strip():
                return {"status": "success", "data": {"filename": f"{factor_id}.py", "code": code}}
    except Exception as e:
        logger.warning(f"Failed to read code from database: {e}")

    # 2. 备用：从文件读取（兼容旧的因子）
    factors_dir = os.path.normpath(FACTORS_DIR)
    for fname in os.listdir(factors_dir):
        if not fname.endswith(".py") or fname.startswith("__"):
            continue
        fpath = os.path.join(factors_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
            if factor_id in content:
                return {"status": "success", "data": {"filename": fname, "code": content}}
        except Exception as e:
            logger.debug(f"读取文件 {fname} 失败: {e}")
            continue

    raise HTTPException(status_code=404, detail=f"因子 {factor_id} 的源代码未找到")


@router.put("/production/factors/{factor_id}/code")
async def update_factor_code(factor_id: str, req: FactorCodeUpdateRequest):
    """更新因子源代码（保存到数据库）"""
    try:
        # 检查因子是否存在
        existing = db_client.query(
            "SELECT * FROM factor_metadata WHERE factor_id = %s", (factor_id,)
        )

        if existing.is_empty():
            raise HTTPException(status_code=404, detail=f"因子 {factor_id} 不存在")

        # 更新代码字段
        row = existing.to_dicts()[0]
        now = datetime.now()

        update_df = pl.DataFrame({
            "factor_id": [factor_id],
            "description": [row.get("description", "")],
            "category": [row.get("category", "custom")],
            "compute_mode": [row.get("compute_mode", "incremental")],
            "storage_target": [row.get("storage_target", "factor_values")],
            "depends_on": [row.get("depends_on", "[]")],
            "params": [row.get("params", "{}")],
            "code": [req.code],
            "enabled": [row.get("enabled", True)],
            "created_at": [row.get("created_at", now)],
            "updated_at": [now],
        })

        db_client.upsert("factor_metadata", update_df, ["factor_id"])

        api_cache.invalidate("production:factors")
        return {"status": "success", "data": {"factor_id": factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update factor code failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/factors/{factor_id}/data")
async def get_factor_data(
    factor_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    limit: int = 200,
):
    """查询因子值数据（支持按日期/股票筛选）"""
    try:
        conditions = ["factor_id = %s"]
        params: list = [factor_id]
        if start_date:
            conditions.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= %s")
            params.append(end_date)
        if ts_code:
            conditions.append("ts_code = %s")
            params.append(ts_code)
        where = " AND ".join(conditions)
        params.append(limit)

        df = db_client.query(
            f"SELECT ts_code, trade_date, factor_value FROM factor_values WHERE {where} ORDER BY trade_date DESC, ts_code LIMIT %s",
            tuple(params),
        )
        data = df.to_dicts() if not df.is_empty() else []
        return {"status": "success", "data": data, "total": len(data)}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": [], "total": 0}
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/factors/{factor_id}/stats")
async def get_factor_stats(factor_id: str):
    """获取因子统计摘要"""
    try:
        df = db_client.query("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT ts_code) AS stock_count,
                MIN(trade_date) AS min_date,
                MAX(trade_date) AS max_date,
                AVG(factor_value) AS mean_val,
                SQRT(AVG(POWER(factor_value - AVG(factor_value), 2))) AS std_val,
                MIN(factor_value) AS min_val,
                MAX(factor_value) AS max_val
            FROM factor_values WHERE factor_id = %s
        """, (factor_id,))
        if df.is_empty():
            return {"status": "success", "data": None}
        row = df.to_dicts()[0]
        # 转换 Decimal 等类型
        for k, v in row.items():
            if v is not None and not isinstance(v, (str, int)):
                row[k] = float(v)
        return {"status": "success", "data": row}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": None}
        raise HTTPException(status_code=500, detail=str(e))
