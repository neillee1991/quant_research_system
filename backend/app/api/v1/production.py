"""生产系统 API：因子分析、生产任务"""
import json
import os
import re
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from store.dolphindb_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from engine.production.registry import list_factors, get_registry, discover_factors, unregister_factor
from engine.production.engine import ProductionEngine
import polars as pl
from data_manager.refactored_sync_engine import sync_engine as _sync_engine
from app.core.config import settings
from app.core.logger import logger
from app.core.utils import DateUtils, safe_json_parse
from app.core.cache import api_cache

router = APIRouter()
analyzer = FactorAnalyzer(db_client)
prod_engine = ProductionEngine(db_client)

FACTORS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "engine", "production", "factors")

_SAFE_FACTOR_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]+$')


def _validate_factor_id(factor_id: str):
    if not _SAFE_FACTOR_ID_RE.match(factor_id):
        raise HTTPException(status_code=400, detail=f"Invalid factor_id: '{factor_id}'")


# ==================== 因子分析 ====================

class AnalyzeRequest(BaseModel):
    factor_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5


def _format_analysis_summary(summary: dict) -> dict:
    """将 analyzer.analyze() 返回的 summary 转换为前端期望的格式"""
    ic_summary = []
    layer_returns = []

    for period_str, pdata in summary.get("periods", {}).items():
        period = int(period_str)
        ic_summary.append({
            "period": period,
            "ic_mean": pdata.get("ic_mean", 0),
            "ic_std": pdata.get("ic_std", 0),
            "icir": pdata.get("ic_ir", 0),
            "ic_positive_ratio": pdata.get("ic_positive_ratio", 0),
            "long_short_return": pdata.get("long_short_return", 0),
        })
        for qr in pdata.get("quantile_returns", []):
            layer_returns.append({
                "period": period,
                "quantile": qr.get("quantile", ""),
                "mean_return": qr.get("avg_return", 0),
            })

    ic_summary.sort(key=lambda x: x["period"])
    return {
        "factor_id": summary.get("factor_id"),
        "ic_summary": ic_summary,
        "layer_returns": layer_returns,
        "turnover": summary.get("turnover"),
        "ic_mean": summary.get("ic_mean", 0),
        "ic_std": summary.get("ic_std", 0),
        "ic_ir": summary.get("ic_ir", 0),
    }


def _format_db_analysis(row: dict) -> dict:
    """将 DB 行记录转换为前端期望的格式"""
    ic_summary = []
    layer_returns = []

    periods = safe_json_parse(row.get("periods"), default=[])
    quantile_returns = safe_json_parse(row.get("quantile_returns"))

    for p in periods:
        ic_summary.append({
            "period": p,
            "ic_mean": row.get("ic_mean", 0),
            "ic_std": row.get("ic_std", 0),
            "icir": row.get("ic_ir", 0),
            "ic_positive_ratio": 0,
        })

    if quantile_returns:
        for qr in quantile_returns:
            layer_returns.append({
                "period": periods[0] if periods else 1,
                "quantile": qr.get("quantile", ""),
                "mean_return": qr.get("avg_return", 0),
            })

    return {
        "factor_id": row.get("factor_id"),
        "ic_summary": ic_summary,
        "layer_returns": layer_returns,
        "turnover_mean": row.get("turnover_mean", 0),
        "ic_mean": row.get("ic_mean", 0),
        "ic_std": row.get("ic_std", 0),
        "ic_ir": row.get("ic_ir", 0),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
        "analysis_date": str(row.get("analysis_date", "")),
    }


@router.post("/analysis/run")
async def run_analysis(req: AnalyzeRequest):
    """运行因子分析"""
    try:
        result = analyzer.analyze(
            factor_id=req.factor_id,
            start_date=req.start_date,
            end_date=req.end_date,
            periods=req.periods,
            quantiles=req.quantiles,
        )
        if result is None:
            raise HTTPException(status_code=404, detail=f"因子 {req.factor_id} 无数据或分析失败")
        return {"status": "success", "data": _format_analysis_summary(result)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/{factor_id}")
async def get_analysis(factor_id: str):
    """获取最新分析结果"""
    result = analyzer.get_latest_analysis(factor_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"因子 {factor_id} 无分析记录")
    return {"status": "success", "data": _format_db_analysis(result)}


@router.get("/analysis/{factor_id}/history")
async def get_analysis_history(factor_id: str, limit: int = 10):
    """获取分析历史"""
    records = analyzer.get_analysis_history(factor_id, limit)
    return {"status": "success", "data": records}


# ==================== 因子管理 CRUD ====================

class FactorCreateRequest(BaseModel):
    factor_id: str
    description: str = ""
    category: str = "custom"
    compute_mode: str = "incremental"
    depends_on: List[str] = []
    storage_target: str = "factor_values"
    params: Dict[str, Any] = {}
    code: Optional[str] = None  # 因子计算代码


class FactorUpdateRequest(BaseModel):
    description: Optional[str] = None
    category: Optional[str] = None
    compute_mode: Optional[str] = None
    depends_on: Optional[List[str]] = None
    storage_target: Optional[str] = None
    params: Optional[Dict[str, Any]] = None


class PreprocessOptions(BaseModel):
    """因子计算预处理选项"""
    adjust_price: str = "forward"       # 复权方式: "none"=不复权, "forward"=前复权, "backward"=后复权
    filter_st: bool = True              # 过滤 ST/*ST 股票
    filter_new_stock: bool = True       # 过滤新股（上市不足 N 天）
    new_stock_days: int = 60            # 新股排除天数
    handle_suspension: bool = True      # 停牌复牌处理（复牌后 window 天因子置空）
    mark_limit: bool = True             # 标记一字涨跌停


class ProductionRunRequest(BaseModel):
    factor_id: str
    mode: str = "incremental"
    target_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    preprocess: Optional[PreprocessOptions] = None


class BatchRunRequest(BaseModel):
    factor_ids: List[str]
    mode: str = "incremental"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    preprocess: Optional[PreprocessOptions] = None


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
        except Exception:
            pass
    except Exception:
        pass

    # 自动种子：代码因子不在 DB 中时自动写入（包含代码）
    import inspect
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
                        pass  # 无法获取源代码时保持为空

                seed_df = pl.DataFrame({
                    "factor_id": [fid],
                    "description": [fdef.get("description", "")],
                    "category": [fdef.get("category", "custom")],
                    "compute_mode": [fdef.get("compute_mode", "incremental")],
                    "storage_target": [fdef.get("storage_target", "factor_values")],
                    "depends_on": [json.dumps(fdef.get("depends_on", []))],
                    "params": [json.dumps(fdef.get("params", {}))],
                    "code": [code_str],  # 保存因子函数源代码
                    "last_computed_date": [""],
                    "last_computed_at": pl.Series([None], dtype=pl.Datetime),
                    "created_at": [now],
                    "updated_at": [now],
                })
                db_client.upsert("factor_metadata", seed_df, ["factor_id"])
                db_meta[fid] = {"factor_id": fid, "description": fdef.get("description", ""),
                                "category": fdef.get("category", "custom")}
            except Exception:
                pass

    # 合并：DB 元数据优先（用户手动修改），代码定义作为 fallback
    all_ids = set(code_factors.keys()) | set(db_meta.keys())
    merged = []
    for fid in sorted(all_ids):
        item = code_factors.get(fid, {})
        meta = db_meta.get(fid, {})
        db_params = safe_json_parse(meta.get("params"))

        # 解析 depends_on (JSON 数组)
        db_depends_on = safe_json_parse(meta.get("depends_on"), default=[])

        merged.append({
            "factor_id": fid,
            "description": meta.get("description") or item.get("description", ""),
            "category": meta.get("category") or item.get("category", "custom"),
            "compute_mode": meta.get("compute_mode") or item.get("compute_mode", "incremental"),
            "depends_on": db_depends_on if db_depends_on else item.get("depends_on", []),
            "storage_target": meta.get("storage_target") or item.get("storage_target", "factor_values"),
            "params": db_params if db_params else item.get("params", {}),
            "last_computed_date": meta.get("last_computed_date"),
            "last_computed_at": str(meta["last_computed_at"]) if meta.get("last_computed_at") else None,
            "latest_data_date": latest_dates.get(fid),
        })

    result = {"status": "success", "data": merged}
    api_cache.set("production:factors", result, ttl=60)
    return result


@router.post("/production/factors")
async def create_factor(req: FactorCreateRequest):
    """手动注册因子（写入 factor_metadata），代码保存到数据库"""
    try:
        existing = db_client.query(
            "SELECT factor_id FROM factor_metadata WHERE factor_id = %s", (req.factor_id,)
        )
        if not existing.is_empty():
            raise HTTPException(status_code=409, detail=f"因子 {req.factor_id} 已存在")

        now = datetime.now()
        new_df = pl.DataFrame({
            "factor_id": [req.factor_id],
            "description": [req.description],
            "category": [req.category],
            "compute_mode": [req.compute_mode],
            "storage_target": [req.storage_target],
            "depends_on": [json.dumps(req.depends_on if hasattr(req, 'depends_on') else [])],
            "params": [json.dumps(req.params)],
            "code": [req.code if req.code else ""],
            "last_computed_date": [""],
            "last_computed_at": pl.Series([None], dtype=pl.Datetime),
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
    """修改因子元数据"""
    try:
        # 先读取现有记录
        existing = db_client.query(
            "SELECT * FROM factor_metadata WHERE factor_id = %s", (factor_id,)
        )
        if existing.is_empty():
            # 不存在则创建默认记录
            now = datetime.now()
            row = {
                "factor_id": factor_id, "description": "", "category": "custom",
                "compute_mode": "incremental", "storage_target": "factor_values",
                "depends_on": "[]", "params": "{}", "code": "",
                "last_computed_date": "", "last_computed_at": None,
                "created_at": now, "updated_at": now,
            }
        else:
            row = existing.to_dicts()[0]

        # 应用更新
        if req.description is not None:
            row["description"] = req.description
        if req.category is not None:
            row["category"] = req.category
        if req.compute_mode is not None:
            row["compute_mode"] = req.compute_mode
        if req.storage_target is not None:
            row["storage_target"] = req.storage_target
        if req.params is not None:
            row["params"] = json.dumps(req.params)
        row["updated_at"] = datetime.now()

        # 使用显式 schema 确保 TIMESTAMP 列类型正确
        update_df = pl.DataFrame([row], schema={
            "factor_id": pl.Utf8,
            "description": pl.Utf8,
            "category": pl.Utf8,
            "compute_mode": pl.Utf8,
            "storage_target": pl.Utf8,
            "depends_on": pl.Utf8,
            "params": pl.Utf8,
            "code": pl.Utf8,
            "last_computed_date": pl.Utf8,
            "last_computed_at": pl.Datetime,
            "created_at": pl.Datetime,
            "updated_at": pl.Datetime,
        })
        db_client.upsert("factor_metadata", update_df, ["factor_id"])
        api_cache.invalidate("production:factors")

        # 清除因子列表缓存
        logger.debug(f"Cleared factor list cache after updating {factor_id}")

        return {"status": "success", "data": {"factor_id": factor_id}}
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

        # 清除因子列表缓存
        logger.debug(f"Cleared factor list cache after deleting {factor_id}")

        return {"status": "success", "data": {"factor_id": factor_id, "data_deleted": delete_data}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/production/run")
async def run_production(req: ProductionRunRequest):
    """运行生产任务"""
    try:
        preprocess = req.preprocess.model_dump() if req.preprocess else None
        success = prod_engine.run_task(
            factor_id=req.factor_id,
            mode=req.mode,
            target_date=req.target_date,
            start_date=req.start_date,
            end_date=req.end_date,
            preprocess=preprocess,
        )
        return {"status": "success", "data": {"completed": success}}
    except Exception as e:
        logger.error(f"Production run failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/production/batch-run")
async def batch_run_production(req: BatchRunRequest):
    """批量计算因子"""
    preprocess = req.preprocess.model_dump() if req.preprocess else None
    results = []
    for fid in req.factor_ids:
        try:
            success = prod_engine.run_task(
                factor_id=fid,
                mode=req.mode,
                start_date=req.start_date,
                end_date=req.end_date,
                preprocess=preprocess,
            )
            results.append({"factor_id": fid, "success": success})
        except Exception as e:
            results.append({"factor_id": fid, "success": False, "error": str(e)})
    return {"status": "success", "data": results}


@router.get("/production/history")
async def get_production_history(factor_id: Optional[str] = None, limit: int = 20):
    """获取生产运行历史"""
    try:
        conditions = []
        params = []
        if factor_id:
            conditions.append("factor_id = %s")
            params.append(factor_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        df = db_client.query(f"""
            SELECT * FROM factor_task_run
            {where}
            ORDER BY created_at DESC LIMIT %s
        """, tuple(params))
        data = []
        if not df.is_empty():
            for row in df.to_dicts():
                row["created_at"] = str(row["created_at"]) if row.get("created_at") else None
                data.append(row)
        return {"status": "success", "data": data}
    except Exception as e:
        # 表不存在时返回空列表
        if "does not exist" in str(e):
            return {"status": "success", "data": []}
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 因子代码查看/编辑 ====================

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
        except Exception:
            continue

    raise HTTPException(status_code=404, detail=f"因子 {factor_id} 的源代码未找到")


class FactorCodeUpdateRequest(BaseModel):
    filename: str
    code: str


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
        row["code"] = req.code
        row["updated_at"] = datetime.now()

        update_df = pl.DataFrame([row], schema={
            "factor_id": pl.Utf8,
            "description": pl.Utf8,
            "category": pl.Utf8,
            "compute_mode": pl.Utf8,
            "storage_target": pl.Utf8,
            "depends_on": pl.Utf8,
            "params": pl.Utf8,
            "code": pl.Utf8,
            "last_computed_date": pl.Utf8,
            "last_computed_at": pl.Datetime,
            "created_at": pl.Datetime,
            "updated_at": pl.Datetime,
        })
        db_client.upsert("factor_metadata", update_df, ["factor_id"])

        return {"status": "success", "data": {"factor_id": factor_id}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update factor code failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 因子代码测试 ====================

class FactorTestRequest(BaseModel):
    """因子代码测试请求"""
    code: str                          # 因子计算代码
    start_date: str                    # 测试数据起始日期 YYYYMMDD
    end_date: str                      # 测试数据结束日期 YYYYMMDD
    depends_on: List[str] = ["sync_daily_data"]  # 依赖数据源
    params: Dict[str, Any] = {}        # 因子参数
    preprocess: Optional[Dict[str, Any]] = None  # 预处理选项


@router.post("/production/factors/test")
async def test_factor_code(req: FactorTestRequest):
    """编译并测试因子代码，返回计算结果预览

    流程：
    1. 编译代码，提取 compute 函数（通过 @factor 装饰器或直接定义）
    2. 加载指定日期范围的真实数据
    3. 执行因子计算
    4. 返回结果预览（含统计信息）
    """
    import traceback
    import types
    import io
    import time

    logs: list = []  # 结构化日志 [{phase, level, message}]
    stdout_capture = io.StringIO()

    def log(phase: str, msg: str, level: str = "info"):
        logs.append({"phase": phase, "level": level, "message": msg})

    def make_error(phase: str, error: str):
        log(phase, error, "error")
        return {
            "status": "error",
            "phase": phase,
            "error": error,
            "logs": logs,
            "stdout": stdout_capture.getvalue(),
        }

    # 1. 编译代码
    log("compile", f"编译代码 ({len(req.code)} 字符)...")
    t0 = time.time()
    try:
        compiled = compile(req.code, "<factor_test>", "exec")
    except SyntaxError as e:
        return make_error("compile", f"语法错误 (第{e.lineno}行, 第{e.offset}列): {e.msg}")
    log("compile", f"编译成功 ({(time.time()-t0)*1000:.0f}ms)")

    # 2. 在隔离命名空间中执行代码，捕获 @factor 注册的函数
    captured_definitions: list = []

    def mock_factor_decorator(*args, **kwargs):
        """拦截 @factor 装饰器，捕获函数定义"""
        def decorator(func):
            fid = args[0] if args else kwargs.get("factor_id", "unknown")
            deps = kwargs.get("depends_on", ["sync_daily_data"])
            params = kwargs.get("params", {})
            captured_definitions.append({
                "factor_id": fid, "func": func,
                "depends_on": deps, "params": params,
            })
            log("exec", f"注册因子: {fid} (depends_on={deps}, params={params})")
            return func
        if len(args) == 1 and callable(args[0]) and not kwargs:
            captured_definitions.append({"factor_id": "unknown", "func": args[0], "depends_on": ["sync_daily_data"], "params": {}})
            return args[0]
        return decorator

    mock_registry = types.ModuleType("engine.production.registry")
    mock_registry.factor = mock_factor_decorator

    namespace = {
        "__builtins__": {
            # Allow only safe builtins needed for factor computation
            "__import__": __import__,
            "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
            "enumerate": enumerate, "filter": filter, "float": float,
            "int": int, "isinstance": isinstance, "len": len, "list": list,
            "map": map, "max": max, "min": min, "range": range, "round": round,
            "set": set, "sorted": sorted, "str": str, "sum": sum, "tuple": tuple,
            "type": type, "zip": zip, "None": None, "True": True, "False": False,
        },
        "pl": __import__("polars"),
        "polars": __import__("polars"),
        "print": lambda *a, **kw: stdout_capture.write(" ".join(str(x) for x in a) + kw.get("end", "\n")),
    }

    import sys
    original_module = sys.modules.get("engine.production.registry")
    sys.modules["engine.production.registry"] = mock_registry

    log("exec", "执行代码...")
    t0 = time.time()
    try:
        exec(compiled, namespace)
    except Exception:
        return make_error("exec", f"代码执行错误:\n{traceback.format_exc()}")
    finally:
        if original_module is not None:
            sys.modules["engine.production.registry"] = original_module
        else:
            sys.modules.pop("engine.production.registry", None)

    exec_stdout = stdout_capture.getvalue()
    if exec_stdout.strip():
        log("exec", f"[stdout]\n{exec_stdout.strip()}")
    log("exec", f"代码执行完成 ({(time.time()-t0)*1000:.0f}ms)")

    # 确定要调用的函数
    compute_func = None
    func_params = req.params
    depends_on = req.depends_on  # 默认使用请求参数

    if captured_definitions:
        defn = captured_definitions[0]
        compute_func = defn["func"]
        func_params = {**defn["params"], **req.params}
        depends_on = defn["depends_on"]  # 使用装饰器中的 depends_on
        log("resolve", f"使用 @factor 注册的函数: {defn['factor_id']} (depends_on={depends_on})")
    else:
        for name, obj in namespace.items():
            if callable(obj) and name.startswith("compute"):
                compute_func = obj
                log("resolve", f"使用命名空间中的函数: {name}")
                break

    if compute_func is None:
        return make_error("resolve", "未找到因子计算函数。请使用 @factor 装饰器注册，或定义 compute_xxx 函数。")

    # 3. 加载真实数据
    log("data", f"加载数据 {req.start_date}~{req.end_date} (depends_on={depends_on})...")
    t0 = time.time()

    # 提取预处理选项
    from engine.production.engine import ProductionEngine
    # 优先级：请求中的 preprocess > 因子 params 中的 preprocess > 默认值
    preprocess_opts = func_params.get("preprocess", {})
    if req.preprocess:
        preprocess_opts = {**preprocess_opts, **req.preprocess}
    opts = {**ProductionEngine.DEFAULT_PREPROCESS, **preprocess_opts}
    log("data", f"预处理配置: adjust_price={opts['adjust_price']}, filter_st={opts['filter_st']}, filter_new_stock={opts['filter_new_stock']}, handle_suspension={opts['handle_suspension']}, mark_limit={opts['mark_limit']}")

    try:
        from engine.production.registry import FactorDefinition, StorageConfig
        mock_def = FactorDefinition(
            factor_id="__test__",
            description="test",
            func=compute_func,
            depends_on=depends_on,
            category="test",
            params=func_params,
            compute_mode="full",
            storage=StorageConfig(),
        )
        df = prod_engine._load_data(mock_def, req.start_date, req.end_date, adjust_price=opts["adjust_price"])
        if df is None or df.is_empty():
            return make_error("data", f"日期范围 {req.start_date}~{req.end_date} 内无数据")

        # 应用预处理过滤
        if opts["filter_st"] or opts["filter_new_stock"] or opts["mark_limit"]:
            df = prod_engine._apply_stock_status(
                df, req.start_date, req.end_date,
                filter_st=opts["filter_st"],
                filter_new_stock=opts["filter_new_stock"],
                new_stock_days=opts["new_stock_days"],
                mark_limit=opts["mark_limit"],
            )

    except Exception as e:
        return make_error("data", f"数据加载失败: {e}")
    log("data", f"加载完成: {len(df)} 行, {df['ts_code'].n_unique()} 只股票, 列: {df.columns} ({(time.time()-t0)*1000:.0f}ms)")

    # 4. 执行因子计算（捕获 print）
    log("compute", f"执行因子计算 (params={func_params})...")
    stdout_capture.truncate(0)
    stdout_capture.seek(0)
    t0 = time.time()
    try:
        result = compute_func(df, func_params)
    except Exception:
        return make_error("compute", f"因子计算错误:\n{traceback.format_exc()}")

    compute_stdout = stdout_capture.getvalue()
    if compute_stdout.strip():
        log("compute", f"[stdout]\n{compute_stdout.strip()}")
    log("compute", f"计算完成 ({(time.time()-t0)*1000:.0f}ms)")

    if result is None or not hasattr(result, "is_empty") or result.is_empty():
        return make_error("compute", "因子计算返回空结果")

    # 4.5 停牌处理
    if opts["handle_suspension"] and "factor_value" in result.columns:
        window = func_params.get("window", 20)
        result = prod_engine._handle_suspension_from_status(result, req.start_date, req.end_date, window)

    # 5. 校验结果格式
    log("validate", f"结果列: {result.columns}, 行数: {len(result)}")
    required_cols = {"ts_code", "trade_date", "factor_value"}
    missing = required_cols - set(result.columns)
    if missing:
        return make_error("validate", f"结果缺少必要列: {', '.join(missing)}。因子函数必须返回包含 ts_code, trade_date, factor_value 的 DataFrame。")
    log("validate", "结果格式校验通过")

    # 6. 构建返回数据
    import polars as pl_mod

    result = result.select(["ts_code", "trade_date", "factor_value"]).sort(["ts_code", "trade_date"])

    numeric_types = [pl_mod.Float64, pl_mod.Float32, pl_mod.Int64, pl_mod.Int32]
    is_numeric = result["factor_value"].dtype in numeric_types
    stats = {
        "total_rows": len(result),
        "stock_count": result["ts_code"].n_unique(),
        "date_count": result["trade_date"].n_unique(),
        "factor_mean": round(float(result["factor_value"].mean()), 6) if is_numeric else None,
        "factor_std": round(float(result["factor_value"].std()), 6) if is_numeric else None,
        "factor_min": round(float(result["factor_value"].min()), 6) if is_numeric else None,
        "factor_max": round(float(result["factor_value"].max()), 6) if is_numeric else None,
        "null_count": int(result["factor_value"].null_count()),
    }
    log("result", f"统计: {stats['total_rows']} 行, {stats['stock_count']} 只股票, "
        f"均值={stats['factor_mean']}, 标准差={stats['factor_std']}, 空值={stats['null_count']}")

    preview_limit = 2000
    preview = result.head(preview_limit).to_dicts()
    for row in preview:
        if row.get("factor_value") is not None:
            row["factor_value"] = round(float(row["factor_value"]), 6)

    stocks = sorted(result["ts_code"].unique().to_list())
    dates = sorted(result["trade_date"].unique().to_list())

    return {
        "status": "success",
        "data": {
            "stats": stats,
            "preview": preview,
            "stocks": stocks,
            "dates": dates,
            "truncated": len(result) > preview_limit,
            "logs": logs,
            "stdout": stdout_capture.getvalue(),
        },
    }


# ==================== 数据探查 ====================

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


# ==================== 数据配置 ====================

class DataFieldMapping(BaseModel):
    field_key: str
    description: str = ""
    table_name: str = ""
    column_name: str = ""
    extra_config: str = "{}"


class DataConfigUpdateRequest(BaseModel):
    mappings: List[DataFieldMapping]


@router.get("/production/data-config")
async def get_data_config():
    """获取所有字段映射配置"""
    cached = api_cache.get("production:data-config")
    if cached is not None:
        return cached
    try:
        df = db_client.query("SELECT * FROM factor_data_config ORDER BY field_key")
        rows = df.to_dicts() if not df.is_empty() else []
        result = {"status": "success", "data": rows}
        api_cache.set("production:data-config", result, ttl=120)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/production/data-config")
async def update_data_config(req: DataConfigUpdateRequest):
    """批量更新字段映射配置"""
    try:
        now = datetime.now()
        update_df = pl.DataFrame({
            "field_key": [m.field_key for m in req.mappings],
            "description": [m.description for m in req.mappings],
            "table_name": [m.table_name for m in req.mappings],
            "column_name": [m.column_name for m in req.mappings],
            "extra_config": [m.extra_config for m in req.mappings],
            "updated_at": [now] * len(req.mappings),
        })
        db_client.upsert("factor_data_config", update_df, ["field_key"])
        api_cache.invalidate("production:data-config")
        return {"status": "success", "message": f"已更新 {len(req.mappings)} 条配置"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/data-config/resolved")
async def get_resolved_data_config():
    """返回简化的 field_key → source_label + values 字典，供前端注解显示"""
    cached = api_cache.get("production:data-config:resolved")
    if cached is not None:
        return cached
    try:
        df = db_client.query("SELECT field_key, table_name, column_name, extra_config FROM factor_data_config")
        result = {}
        if not df.is_empty():
            for row in df.to_dicts():
                fk = row["field_key"]
                tbl = row.get("table_name", "") or ""
                col = row.get("column_name", "") or ""
                extra = row.get("extra_config", "{}") or "{}"
                values = None
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
                # 解析枚举值
                try:
                    cfg = json.loads(extra)
                    if "values" in cfg:
                        values = cfg["values"]
                except Exception:
                    pass
                result[fk] = {"table_name": tbl, "column_name": col, "source_label": source_label, "values": values}
        result_resp = {"status": "success", "data": result}
        api_cache.set("production:data-config:resolved", result_resp, ttl=120)
        return result_resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 指数股票池 CRUD ====================

class IndexPoolBatchUploadRequest(BaseModel):
    """批量上传指数成分股请求"""
    index_code: str
    index_name: str = ""
    description: str = ""
    data: List[Dict[str, Any]]  # [{"trade_date": "20240101", "ts_code": "000001.SZ", "weight": 0.05}]


class IndexPoolCSVUploadRequest(BaseModel):
    """CSV 上传请求"""
    index_code: str
    index_name: str = ""
    description: str = ""
    csv_content: str  # CSV 文件内容


@router.post("/index-pool/batch-upload")
async def batch_upload_index_pool(req: IndexPoolBatchUploadRequest):
    """批量上传指数成分股（JSON 格式）

    请求示例:
    {
        "index_code": "000300.SH",
        "index_name": "沪深300",
        "description": "沪深300指数成分股",
        "data": [
            {"trade_date": "20240101", "ts_code": "000001.SZ", "weight": 0.05},
            {"trade_date": "20240101", "ts_code": "000002.SZ", "weight": 0.03}
        ]
    }
    """
    try:
        if not req.data:
            raise HTTPException(status_code=400, detail="数据不能为空")

        # 验证数据格式
        required_fields = ["trade_date", "ts_code"]
        for item in req.data:
            for field in required_fields:
                if field not in item:
                    raise HTTPException(status_code=400, detail=f"缺少必需字段: {field}")

        # 添加 index_code 到每条记录
        for item in req.data:
            item["index_code"] = req.index_code
            if "weight" not in item:
                item["weight"] = 0.0

        # 转换为 DataFrame
        constituents_df = pl.DataFrame(req.data)

        # 插入成分股数据
        db_client.upsert(
            "index_constituents",
            constituents_df,
            key_columns=["trade_date", "ts_code", "index_code"]
        )

        # 更新或插入元数据
        latest_date = constituents_df["trade_date"].max()
        stock_count = constituents_df.filter(pl.col("trade_date") == latest_date)["ts_code"].n_unique()

        metadata_df = pl.DataFrame({
            "index_code": [req.index_code],
            "index_name": [req.index_name],
            "description": [req.description],
            "stock_count": [stock_count],
            "latest_date": [latest_date],
            "created_at": [datetime.now()],
            "updated_at": [datetime.now()]
        })

        db_client.upsert(
            "index_metadata",
            metadata_df,
            key_columns=["index_code"]
        )

        logger.info(f"Uploaded {len(req.data)} records for index {req.index_code}")

        return {
            "status": "success",
            "message": f"成功上传 {len(req.data)} 条成分股数据",
            "data": {
                "index_code": req.index_code,
                "records_count": len(req.data),
                "stock_count": stock_count,
                "latest_date": latest_date
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload index pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/index-pool/csv-upload")
async def csv_upload_index_pool(req: IndexPoolCSVUploadRequest):
    """CSV 上传指数成分股

    CSV 格式要求:
    trade_date,ts_code,weight
    20240101,000001.SZ,0.05
    20240101,000002.SZ,0.03
    """
    try:
        import io
        import csv

        # 解析 CSV
        csv_file = io.StringIO(req.csv_content)
        reader = csv.DictReader(csv_file)
        data = []

        for row in reader:
            if "trade_date" not in row or "ts_code" not in row:
                raise HTTPException(status_code=400, detail="CSV 必须包含 trade_date 和 ts_code 列")

            data.append({
                "trade_date": row["trade_date"],
                "ts_code": row["ts_code"],
                "weight": float(row.get("weight", 0.0))
            })

        if not data:
            raise HTTPException(status_code=400, detail="CSV 文件为空")

        # 调用批量上传逻辑
        batch_req = IndexPoolBatchUploadRequest(
            index_code=req.index_code,
            index_name=req.index_name,
            description=req.description,
            data=data
        )

        return await batch_upload_index_pool(batch_req)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to parse CSV: {e}")
        raise HTTPException(status_code=500, detail=f"CSV 解析失败: {str(e)}")


@router.get("/index-pool/list")
async def list_index_pools():
    """列出所有指数股票池"""
    try:
        df = db_client.query("""
            SELECT index_code, index_name, description, stock_count, latest_date, updated_at
            FROM loadTable("dfs://quant", "index_metadata")
            ORDER BY updated_at DESC
        """)

        if df.is_empty():
            return {"status": "success", "data": []}

        return {"status": "success", "data": df.to_dicts()}

    except Exception as e:
        logger.error(f"Failed to list index pools: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/index-pool/template")
async def download_index_pool_template():
    """下载 CSV 模板"""
    from fastapi.responses import Response

    csv_template = """trade_date,ts_code,weight
20240101,000001.SZ,0.05
20240101,000002.SZ,0.03
20240101,600000.SH,0.04
"""

    return Response(
        content=csv_template,
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=index_pool_template.csv"
        }
    )


@router.get("/index-pool/{index_code}")
async def get_index_pool(index_code: str, trade_date: Optional[str] = None):
    """查询指数成分股

    Args:
        index_code: 指数代码
        trade_date: 交易日期（可选），如果不指定则返回最新日期的成分股
    """
    try:
        # 如果未指定日期，获取最新日期
        if not trade_date:
            latest_df = db_client.query("""
                SELECT MAX(trade_date) as latest_date
                FROM loadTable("dfs://quant", "index_constituents")
                WHERE index_code = %s
            """, (index_code,))

            if latest_df.is_empty() or latest_df["latest_date"][0] is None:
                raise HTTPException(status_code=404, detail=f"指数 {index_code} 无数据")

            trade_date = latest_df["latest_date"][0]

        # 查询成分股
        df = db_client.query("""
            SELECT ts_code, trade_date, weight
            FROM loadTable("dfs://quant", "index_constituents")
            WHERE index_code = %s AND trade_date = %s
            ORDER BY weight DESC
        """, (index_code, trade_date))

        if df.is_empty():
            raise HTTPException(status_code=404, detail=f"指数 {index_code} 在 {trade_date} 无数据")

        # 获取元数据
        metadata_df = db_client.query("""
            SELECT index_code, index_name, description, stock_count, latest_date
            FROM loadTable("dfs://quant", "index_metadata")
            WHERE index_code = %s
        """, (index_code,))

        metadata = metadata_df.to_dicts()[0] if not metadata_df.is_empty() else {
            "index_code": index_code,
            "index_name": "",
            "description": "",
            "stock_count": len(df),
            "latest_date": trade_date
        }

        return {
            "status": "success",
            "data": {
                "metadata": metadata,
                "constituents": df.to_dicts(),
                "query_date": trade_date
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get index pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/index-pool/{index_code}")
async def delete_index_pool(index_code: str):
    """删除指数股票池"""
    try:
        # DolphinDB 删除数据的方式：使用 delete 函数
        # 删除成分股数据
        db_client._session.run(f"""
            constituents_table = loadTable("dfs://quant", "index_constituents");
            delete from constituents_table where index_code = "{index_code}";
        """)

        # 删除元数据
        db_client._session.run(f"""
            metadata_table = loadTable("dfs://quant", "index_metadata");
            delete from metadata_table where index_code = "{index_code}";
        """)

        logger.info(f"Deleted index pool: {index_code}")

        return {
            "status": "success",
            "message": f"成功删除指数 {index_code}"
        }

    except Exception as e:
        logger.error(f"Failed to delete index pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Alphalens 分析 API ====================

class AlphalensAnalysisRequest(BaseModel):
    """Alphalens 分析请求"""
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None


class AlphalensAnalysisHistoryQuery(BaseModel):
    """Alphalens 分析历史查询参数"""
    limit: int = 20
    offset: int = 0


@router.post("/analysis/alphalens")
async def run_alphalens_analysis(req: AlphalensAnalysisRequest):
    """运行 Alphalens 因子分析

    Args:
        factor_id: 因子ID
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        periods: 持有周期列表，默认 [1, 5, 10]
        quantiles: 分位数，默认 5
        index_pool: 指数股票池代码（可选），如 '000300.SH'
        groupby_field: 分组字段（可选），如 'industry', 'market_cap'

    Returns:
        完整的 Alphalens 分析结果
    """
    try:
        logger.info(f"Starting Alphalens analysis: factor_id={req.factor_id}, "
                   f"date_range={req.start_date}~{req.end_date}, "
                   f"index_pool={req.index_pool}, groupby={req.groupby_field}")

        # 运行分析
        results = analyzer.analyze(
            factor_id=req.factor_id,
            start_date=req.start_date,
            end_date=req.end_date,
            periods=req.periods,
            quantiles=req.quantiles,
            use_alphalens=True,
            index_pool=req.index_pool,
            groupby_field=req.groupby_field
        )

        if not results:
            raise HTTPException(status_code=500, detail="分析失败，未返回结果")

        # 清理 NaN/Inf 值以确保 JSON 序列化
        import math
        import numpy as np

        def clean_value(v):
            if isinstance(v, (float, np.floating)):
                if math.isnan(v) or math.isinf(v):
                    return None
            elif isinstance(v, (np.integer, np.int64, np.int32)):
                return int(v)
            return v

        def clean_dict(d):
            if isinstance(d, dict):
                return {k: clean_dict(v) for k, v in d.items()}
            elif isinstance(d, (list, tuple)):
                return [clean_dict(item) for item in d]
            else:
                return clean_value(d)

        results = clean_dict(results)

        logger.info(f"Alphalens analysis completed for {req.factor_id}")

        return {
            "status": "success",
            "message": f"成功完成 Alphalens 分析",
            "data": results
        }

    except Exception as e:
        logger.error(f"Alphalens analysis failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/alphalens/{factor_id}/latest")
async def get_latest_alphalens_analysis(factor_id: str):
    """获取指定因子的最新 Alphalens 分析结果

    Args:
        factor_id: 因子ID

    Returns:
        最新的分析结果，包含完整的 Alphalens 输出
    """
    try:
        # 查询最新的分析记录
        df = db_client.query("""
            SELECT *
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s
            ORDER BY analysis_date DESC
            LIMIT 1
        """, (factor_id,))

        if df.is_empty():
            raise HTTPException(
                status_code=404,
                detail=f"未找到因子 {factor_id} 的分析结果"
            )

        record = df.to_dicts()[0]

        # 解析 JSON 字段
        json_fields = [
            'ic_summary', 'ic_by_period', 'ic_ts', 'quantile_returns',
            'cumulative_returns', 'ic_by_group', 'returns_by_group',
            'turnover', 'decay_analysis', 'chart_data'
        ]

        for field in json_fields:
            if field in record and record[field]:
                try:
                    record[field] = safe_json_parse(record[field])
                except:
                    logger.warning(f"Failed to parse {field} for {factor_id}")
                    record[field] = None

        return {
            "status": "success",
            "data": record
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get latest analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis/alphalens/{factor_id}/history")
async def get_alphalens_analysis_history(
    factor_id: str,
    limit: int = 20,
    offset: int = 0
):
    """获取指定因子的 Alphalens 分析历史记录

    Args:
        factor_id: 因子ID
        limit: 返回记录数，默认 20
        offset: 偏移量，默认 0

    Returns:
        分析历史记录列表（元数据，不包含完整结果）
    """
    try:
        # DolphinDB doesn't support OFFSET, use LIMIT offset, count instead
        df = db_client.query("""
            SELECT
                id, factor_id, analysis_date, start_date, end_date,
                config, task_status
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s
            ORDER BY analysis_date DESC
            LIMIT %s, %s
        """, (factor_id, offset, limit))

        if df.is_empty():
            return {
                "status": "success",
                "data": {
                    "records": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset
                }
            }

        records = df.to_dicts()

        # 解析 config 字段（JSON）
        for record in records:
            if 'config' in record and record['config']:
                try:
                    config = safe_json_parse(record['config'])
                    # 提取关键配置到顶层
                    record['periods'] = config.get('periods')
                    record['quantiles'] = config.get('quantiles')
                    record['index_pool'] = config.get('index_pool')
                    record['groupby_field'] = config.get('groupby_field')
                except:
                    record['periods'] = None
                    record['quantiles'] = None
                    record['index_pool'] = None
                    record['groupby_field'] = None

        # 获取总数
        count_df = db_client.query("""
            SELECT COUNT(*) as total
            FROM loadTable("dfs://quant", "factor_analysis_extended")
            WHERE factor_id = %s
        """, (factor_id,))

        total = count_df["total"][0] if not count_df.is_empty() else 0

        return {
            "status": "success",
            "data": {
                "records": records,
                "total": total,
                "limit": limit,
                "offset": offset
            }
        }

    except Exception as e:
        logger.error(f"Failed to get analysis history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

