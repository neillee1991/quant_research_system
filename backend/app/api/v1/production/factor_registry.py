"""因子注册和元数据管理 API 端点"""
import json
import re
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
    align_calendar: bool = False


class FactorUpdateRequest(BaseModel):
    """因子更新请求"""
    description: Optional[str] = None
    category: Optional[str] = None
    compute_mode: Optional[str] = None
    depends_on: Optional[List[str]] = None
    storage_target: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    align_calendar: Optional[bool] = None


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

    # 从数据库加载因子（不再从代码文件自动种子）
    code_factors = {}  # 空字典，因为不再从代码加载

    # 优化：使用单个查询获取元数据和最新日期（消除N+1查询）
    db_meta = {}
    latest_dates: Dict[str, str] = {}
    last_computed: Dict[str, str] = {}
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
                    date_val = row["latest_date"]
                    if date_val:
                        date_str = str(date_val)
                        # 格式化日期：YYYYMMDD -> YYYY-MM-DD
                        if len(date_str) == 8:
                            latest_dates[row["factor_id"]] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                        else:
                            latest_dates[row["factor_id"]] = date_str
        except Exception as e:
            logger.debug(f"查询因子最新日期失败: {e}")

        # 查询上次计算时间
        try:
            run_df = db_client.query(
                "SELECT factor_id, max(created_at) AS last_run FROM factor_run_log WHERE status = 'success' GROUP BY factor_id"
            )
            if not run_df.is_empty():
                for row in run_df.to_dicts():
                    last_computed[row["factor_id"]] = str(row["last_run"]) if row.get("last_run") else None
        except Exception as e:
            logger.debug(f"查询因子上次计算时间失败: {e}")
    except Exception as e:
        logger.debug(f"查询因子元数据失败: {e}")

    # 合并：只使用数据库中的因子
    all_ids = set(db_meta.keys())
    merged = []
    for fid in sorted(all_ids):
        meta = db_meta.get(fid, {})
        db_params = safe_json_parse(meta.get("params"))
        db_depends_on = safe_json_parse(meta.get("depends_on"), default=[])

        merged.append({
            "factor_id": fid,
            "description": meta.get("description", ""),
            "category": meta.get("category", "custom"),
            "compute_mode": meta.get("compute_mode", "incremental"),
            "depends_on": db_depends_on,
            "storage_target": meta.get("storage_target", "factor_values"),
            "params": db_params,
            "align_calendar": bool(meta.get("align_calendar", False)),
            "latest_date": latest_dates.get(fid),
            "last_computed_at": last_computed.get(fid),
            "source": "db",
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
            "enabled": [True],
            "align_calendar": [req.align_calendar],
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
            "align_calendar": [updates.get("align_calendar", row.get("align_calendar", False))],
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
    """删除因子元数据和注册表条目，可选删除因子值数据"""
    _validate_factor_id(factor_id)
    try:
        db_client.execute("DELETE FROM factor_metadata WHERE factor_id = %s", (factor_id,))
        if delete_data:
            db_client.execute("DELETE FROM factor_values WHERE factor_id = %s", (factor_id,))

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


@router.get("/production/factors/{factor_id}/logs")
async def get_factor_logs(factor_id: str, limit: int = 20):
    """获取因子运行日志"""
    try:
        logs_df = db_client.query(
            f"SELECT * FROM factor_run_log WHERE factor_id = '{factor_id}' ORDER BY created_at DESC LIMIT {limit}"
        )
        if logs_df.is_empty():
            return {"status": "success", "data": []}

        logs = logs_df.to_dicts()
        logger.debug(f"Raw logs from DB: {logs}")

        # 统一字段名（映射数据库列名到API字段名）
        # 支持两种列名，兼容不同版本的表结构
        # 先尝试新列名，如果没有则尝试旧列名
        for log in logs:
            log["rows"] = log.get("rows") or log.get("rows_affected")
            log["elapsed_seconds"] = log.get("elapsed_seconds") or log.get("duration_seconds")
            log["error_message"] = log.get("error_message") or log.get("message")
            # 确保时间戳字段存在
            for ts_field in ["created_at", "finished_at"]:
                if ts_field not in log:
                    log[ts_field] = None

        logger.debug(f"Processed logs: {logs}")
        return {"status": "success", "data": logs}
    except Exception as e:
        logger.error(f"Failed to get factor logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/factors/{factor_id}/code")
async def get_factor_code(factor_id: str):
    """获取因子源代码（从数据库读取）"""
    try:
        df = db_client.query(
            "SELECT code FROM factor_metadata WHERE factor_id = %s", (factor_id,)
        )
        if not df.is_empty():
            code = df["code"][0]
            if code and code.strip():
                return {
                    "status": "success",
                    "data": {
                        "filename": f"{factor_id}.py",
                        "code": code
                    }
                }
    except Exception as e:
        logger.warning(f"Failed to read code from database: {e}")

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
        data = []
        if not df.is_empty():
            for row in df.to_dicts():
                # 格式化日期字段：处理多种日期格式
                if row.get("trade_date"):
                    date_val = row["trade_date"]
                    # 如果是 datetime 对象，转换为字符串
                    date_str = str(date_val)
                    # 如果是 ISO 格式 (YYYY-MM-DDTHH:MM:SS)，只取日期部分
                    if 'T' in date_str:
                        row["trade_date"] = date_str.split('T')[0]
                    # 如果是 YYYYMMDD 格式，转换为 YYYY-MM-DD
                    elif len(date_str) == 8 and date_str.isdigit():
                        row["trade_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                    # 如果已经是 YYYY-MM-DD 格式，保持不变
                    elif len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
                        row["trade_date"] = date_str
                    # 其他格式，只取前10个字符
                    else:
                        row["trade_date"] = date_str[:10]
                data.append(row)
        return {"status": "success", "data": data, "total": len(data)}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": [], "total": 0}
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/factors/{factor_id}/stats")
async def get_factor_stats(factor_id: str):
    """获取因子统计摘要"""
    try:
        # 基本统计
        df = db_client.query("""
            SELECT
                count(*) AS total_rows,
                min(trade_date) AS min_date,
                max(trade_date) AS max_date,
                avg(factor_value) AS mean_val,
                std(factor_value) AS std_val,
                min(factor_value) AS min_val,
                max(factor_value) AS max_val
            FROM factor_values WHERE factor_id = %s
        """, (factor_id,))

        if df.is_empty():
            return {"status": "success", "data": None}

        row = df.to_dicts()[0]

        # 单独查询股票数（DolphinDB 不支持在聚合中嵌套 count(distinct)）
        stock_count_df = db_client.query("""
            SELECT count(*) AS stock_count
            FROM (SELECT DISTINCT ts_code FROM factor_values WHERE factor_id = %s)
        """, (factor_id,))

        if not stock_count_df.is_empty():
            row["stock_count"] = stock_count_df["stock_count"][0]
        else:
            row["stock_count"] = 0
        # 转换 Decimal 等类型（跳过日期字段）
        for k, v in row.items():
            if k in ["min_date", "max_date"]:
                continue  # 跳过日期字段
            if v is not None and not isinstance(v, (str, int)):
                row[k] = float(v)
        # 格式化日期字段：处理多种日期格式
        for date_field in ["min_date", "max_date"]:
            if row.get(date_field):
                date_val = row[date_field]
                date_str = str(date_val)
                # 如果是 ISO 格式 (YYYY-MM-DDTHH:MM:SS)，只取日期部分
                if 'T' in date_str:
                    row[date_field] = date_str.split('T')[0]
                # 如果是 YYYYMMDD 格式，转换为 YYYY-MM-DD
                elif len(date_str) == 8 and date_str.isdigit():
                    row[date_field] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                # 其他格式，只取前10个字符
                else:
                    row[date_field] = date_str[:10]
        return {"status": "success", "data": row}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": None}
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/factors/{factor_id}/missing-dates")
async def get_factor_missing_dates(factor_id: str):
    """检查因子数据缺失的交易日

    逻辑：获取因子数据的最早和最晚日期，然后检查该范围内是否有缺失的交易日
    """
    try:
        # 获取因子数据中实际存在的所有日期
        actual_dates_df = db_client.query("""
            SELECT DISTINCT trade_date FROM factor_values
            WHERE factor_id = %s
            ORDER BY trade_date
        """, (factor_id,))

        if actual_dates_df.is_empty():
            return {"status": "success", "data": {"missing_dates": [], "total_missing": 0, "has_data": False}}

        # 格式化实际日期为 YYYYMMDD 格式
        actual_dates = []
        for d in actual_dates_df["trade_date"].to_list():
            date_str = str(d)
            # 处理 ISO 格式 (YYYY-MM-DDTHH:MM:SS)
            if 'T' in date_str:
                date_str = date_str.split('T')[0].replace('-', '')
            # 处理 YYYY-MM-DD 格式
            elif '-' in date_str and len(date_str) >= 10:
                date_str = date_str[:10].replace('-', '')
            # 处理带空格的格式 (YYYYMMDD HH:MM:SS)
            elif ' ' in date_str:
                date_str = date_str.split(' ')[0].replace('-', '')
            actual_dates.append(date_str)

        actual_dates = sorted(set(actual_dates))

        if len(actual_dates) == 0:
            return {"status": "success", "data": {"missing_dates": [], "total_missing": 0, "has_data": False}}

        min_date = actual_dates[0]
        max_date = actual_dates[-1]

        # 从交易日历获取该范围内的所有交易日
        trade_cal_df = db_client.query("""
            SELECT cal_date FROM sync_trade_cal
            WHERE is_open = 1 AND cal_date >= %s AND cal_date <= %s
            ORDER BY cal_date
        """, (min_date, max_date))

        # 如果交易日历表为空，返回警告信息
        if trade_cal_df.is_empty():
            # 格式化日期
            min_date_formatted = f"{min_date[:4]}-{min_date[4:6]}-{min_date[6:]}" if len(min_date) == 8 else min_date
            max_date_formatted = f"{max_date[:4]}-{max_date[4:6]}-{max_date[6:]}" if len(max_date) == 8 else max_date

            return {
                "status": "success",
                "data": {
                    "missing_dates": [],
                    "total_missing": 0,
                    "has_data": True,
                    "warning": "交易日历表为空，无法检查完整性",
                    "actual_days": len(actual_dates),
                    "date_range": {
                        "min": min_date_formatted,
                        "max": max_date_formatted
                    }
                }
            }

        # 格式化交易日历日期为 YYYYMMDD
        all_trade_dates = set()
        for d in trade_cal_df["cal_date"].to_list():
            date_str = str(d)
            # 处理 datetime 对象
            if 'T' in date_str:
                date_str = date_str.split('T')[0].replace('-', '')
            elif '-' in date_str and len(date_str) >= 10:
                date_str = date_str[:10].replace('-', '')
            elif ' ' in date_str:
                date_str = date_str.split(' ')[0].replace('-', '')
            all_trade_dates.add(date_str)
        actual_dates_set = set(actual_dates)

        # 计算缺失的日期
        missing = sorted(all_trade_dates - actual_dates_set)

        # 格式化日期：YYYYMMDD -> YYYY-MM-DD
        formatted_missing = []
        for date_str in missing:
            if len(date_str) == 8 and date_str.isdigit():
                formatted_missing.append(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")
            else:
                formatted_missing.append(date_str)

        return {
            "status": "success",
            "data": {
                "missing_dates": formatted_missing,
                "total_missing": len(formatted_missing),
                "expected_days": len(all_trade_dates),
                "actual_days": len(actual_dates),
                "has_data": True,
                "date_range": {
                    "min": f"{min_date[:4]}-{min_date[4:6]}-{min_date[6:]}",
                    "max": f"{max_date[:4]}-{max_date[4:6]}-{max_date[6:]}"
                }
            }
        }
    except Exception as e:
        logger.error(f"Failed to check missing dates: {e}")
        if "does not exist" in str(e):
            return {"status": "success", "data": {"missing_dates": [], "total_missing": 0, "has_data": False}}
        raise HTTPException(status_code=500, detail=str(e))
