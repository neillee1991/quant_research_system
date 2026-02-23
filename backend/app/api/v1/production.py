"""生产系统 API：因子分析、生产任务、DAG 管理"""
import json
import os
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from store.postgres_client import db_client
from engine.analysis.analyzer import FactorAnalyzer
from engine.production.registry import list_factors, get_registry, discover_factors, unregister_factor
from engine.production.engine import ProductionEngine
from data_manager.dag_executor import DAGConfigManager, DAGExecutor
from data_manager.refactored_sync_engine import sync_engine as _sync_engine
from app.core.logger import logger
from app.core.utils import DateUtils

router = APIRouter()
analyzer = FactorAnalyzer(db_client)
prod_engine = ProductionEngine(db_client)
dag_config_mgr = DAGConfigManager()
_dag_executor: Optional[DAGExecutor] = None

FACTORS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "engine", "production", "factors")


def _get_dag_executor() -> DAGExecutor:
    global _dag_executor
    if _dag_executor is None:
        _dag_executor = DAGExecutor(
            sync_engine=_sync_engine,
            production_engine=prod_engine,
            db_client=db_client,
        )
    return _dag_executor


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

    periods = row.get("periods") or []
    if isinstance(periods, str):
        periods = json.loads(periods)

    quantile_returns = row.get("quantile_returns")
    if isinstance(quantile_returns, str):
        quantile_returns = json.loads(quantile_returns)

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


def _parse_params(raw) -> dict:
    """安全解析 params 字段：兼容 str / dict / None"""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


@router.get("/production/factors")
async def list_registered_factors():
    """列出所有因子（合并装饰器注册 + 数据库手动注册）- 带缓存"""
    from store.redis_client import redis_client
    from app.core.config import settings

    # 尝试从缓存获取
    cache_key = "production:factors:list"
    cached_result = redis_client.get(cache_key)
    if cached_result is not None:
        logger.debug("Cache hit for factor list")
        return cached_result

    discover_factors()

    # 装饰器注册的因子
    code_factors = {f["factor_id"]: f for f in list_factors()}

    # 优化：使用单个查询获取元数据和最新日期（消除N+1查询）
    db_meta = {}
    latest_dates: Dict[str, str] = {}
    try:
        # 合并查询：一次性获取元数据和最新日期
        df = db_client.query("""
            SELECT
                fm.*,
                fv.latest_date
            FROM factor_metadata fm
            LEFT JOIN (
                SELECT factor_id, MAX(trade_date) AS latest_date
                FROM factor_values
                GROUP BY factor_id
            ) fv ON fm.factor_id = fv.factor_id
            ORDER BY fm.factor_id
        """)
        if not df.is_empty():
            for row in df.to_dicts():
                factor_id = row["factor_id"]
                db_meta[factor_id] = row
                if row.get("latest_date"):
                    latest_dates[factor_id] = row["latest_date"]
    except Exception:
        pass

    # 自动种子：代码因子不在 DB 中时自动写入
    for fid, fdef in code_factors.items():
        if fid not in db_meta:
            try:
                db_client.execute("""
                    INSERT INTO factor_metadata (factor_id, description, category, compute_mode, storage_target, params)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (factor_id) DO NOTHING
                """, (fid, fdef.get("description", ""), fdef.get("category", "custom"),
                      fdef.get("compute_mode", "incremental"),
                      fdef.get("storage_target", "factor_values"),
                      json.dumps(fdef.get("params", {}))))
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
        db_params = _parse_params(meta.get("params"))
        merged.append({
            "factor_id": fid,
            "description": meta.get("description") or item.get("description", ""),
            "category": meta.get("category") or item.get("category", "custom"),
            "compute_mode": meta.get("compute_mode") or item.get("compute_mode", "incremental"),
            "depends_on": meta.get("depends_on") or item.get("depends_on", []),
            "storage_target": meta.get("storage_target") or item.get("storage_target", "factor_values"),
            "params": db_params if db_params else item.get("params", {}),
            "last_computed_date": meta.get("last_computed_date"),
            "last_computed_at": str(meta["last_computed_at"]) if meta.get("last_computed_at") else None,
            "latest_data_date": latest_dates.get(fid),
        })

    result = {"status": "success", "data": merged}

    # 缓存结果（1小时）
    redis_client.set(cache_key, result, ttl=settings.redis.cache_ttl_factor_metadata)
    logger.debug("Cached factor list")

    return result


@router.post("/production/factors")
async def create_factor(req: FactorCreateRequest):
    """手动注册因子（写入 factor_metadata），可选同时创建代码文件"""
    try:
        existing = db_client.query(
            "SELECT factor_id FROM factor_metadata WHERE factor_id = %s", (req.factor_id,)
        )
        if not existing.is_empty():
            raise HTTPException(status_code=409, detail=f"因子 {req.factor_id} 已存在")

        db_client.execute("""
            INSERT INTO factor_metadata (factor_id, description, category, compute_mode, storage_target, params)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (req.factor_id, req.description, req.category, req.compute_mode,
              req.storage_target, json.dumps(req.params)))

        # 保存因子代码文件
        filename = None
        if req.code and req.code.strip():
            filename = f"{req.factor_id}.py"
            factors_dir = os.path.normpath(FACTORS_DIR)
            fpath = os.path.join(factors_dir, filename)
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(req.code)
            logger.info(f"Created factor code file: {filename}")

        # 清除因子列表缓存
        from store.redis_client import redis_client
        cache_key = "production:factors:list"
        redis_client.delete(cache_key)
        logger.debug(f"Cleared factor list cache after creating {req.factor_id}")

        return {"status": "success", "data": {"factor_id": req.factor_id, "filename": filename}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/production/factors/{factor_id}")
async def update_factor(factor_id: str, req: FactorUpdateRequest):
    """修改因子元数据"""
    try:
        updates = []
        values = []
        if req.description is not None:
            updates.append("description = %s")
            values.append(req.description)
        if req.category is not None:
            updates.append("category = %s")
            values.append(req.category)
        if req.compute_mode is not None:
            updates.append("compute_mode = %s")
            values.append(req.compute_mode)
        if req.storage_target is not None:
            updates.append("storage_target = %s")
            values.append(req.storage_target)
        if req.params is not None:
            updates.append("params = %s")
            values.append(json.dumps(req.params))
        if not updates:
            return {"status": "success", "data": {"factor_id": factor_id}}

        updates.append("updated_at = CURRENT_TIMESTAMP")

        # UPSERT: 如果不存在则插入
        db_client.execute(f"""
            INSERT INTO factor_metadata (factor_id, description, category, compute_mode, storage_target, params)
            VALUES (%s, '', 'custom', 'incremental', 'factor_values', '{{}}')
            ON CONFLICT (factor_id) DO UPDATE SET {', '.join(updates)}
        """, (factor_id, *values))

        # 清除因子列表缓存
        from store.redis_client import redis_client
        cache_key = "production:factors:list"
        redis_client.delete(cache_key)
        logger.debug(f"Cleared factor list cache after updating {factor_id}")

        return {"status": "success", "data": {"factor_id": factor_id}}
    except Exception as e:
        logger.error(f"Update factor failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/production/factors/{factor_id}")
async def delete_factor(factor_id: str, delete_data: bool = False):
    """删除因子元数据、代码文件和注册表条目，可选删除因子值数据"""
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

        # 清除因子列表缓存
        from store.redis_client import redis_client
        cache_key = "production:factors:list"
        redis_client.delete(cache_key)
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
            SELECT * FROM production_task_run
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
    """获取因子源代码"""
    factors_dir = os.path.normpath(FACTORS_DIR)
    # 在 factors 目录下搜索包含该 factor_id 的文件
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
    """更新因子源代码"""
    # 安全检查：只允许写入 factors 目录下的 .py 文件
    if not req.filename.endswith(".py") or "/" in req.filename or "\\" in req.filename:
        raise HTTPException(status_code=400, detail="非法文件名")
    factors_dir = os.path.normpath(FACTORS_DIR)
    fpath = os.path.join(factors_dir, req.filename)
    try:
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(req.code)
        return {"status": "success", "data": {"filename": req.filename}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 因子代码测试 ====================

class FactorTestRequest(BaseModel):
    """因子代码测试请求"""
    code: str                          # 因子计算代码
    start_date: str                    # 测试数据起始日期 YYYYMMDD
    end_date: str                      # 测试数据结束日期 YYYYMMDD
    depends_on: List[str] = ["daily_data"]  # 依赖数据源
    params: Dict[str, Any] = {}        # 因子参数


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
            deps = kwargs.get("depends_on", ["daily_data"])
            params = kwargs.get("params", {})
            captured_definitions.append({
                "factor_id": fid, "func": func,
                "depends_on": deps, "params": params,
            })
            log("exec", f"注册因子: {fid} (depends_on={deps}, params={params})")
            return func
        if len(args) == 1 and callable(args[0]) and not kwargs:
            captured_definitions.append({"factor_id": "unknown", "func": args[0], "depends_on": ["daily_data"], "params": {}})
            return args[0]
        return decorator

    mock_registry = types.ModuleType("engine.production.registry")
    mock_registry.factor = mock_factor_decorator

    namespace = {
        "__builtins__": __builtins__,
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
        df = prod_engine._load_data(mock_def, req.start_date, req.end_date)
        if df is None or df.is_empty():
            return make_error("data", f"日期范围 {req.start_date}~{req.end_date} 内无数据")
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
                STDDEV(factor_value) AS std_val,
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


# ==================== DAG 管理 ====================

class DAGRunRequest(BaseModel):
    dag_id: str
    target_date: Optional[str] = None
    start_date: Optional[str] = None   # 回溯起始日期
    end_date: Optional[str] = None     # 回溯结束日期
    run_type: str = "today"            # today | single | backfill


class DAGCreateRequest(BaseModel):
    dag_id: str
    description: str = ""
    schedule: str = "manual"
    tasks: List[Dict[str, Any]] = []


class DAGUpdateRequest(BaseModel):
    description: Optional[str] = None
    schedule: Optional[str] = None
    tasks: Optional[List[Dict[str, Any]]] = None


def _validate_dag_tasks(tasks: List[Dict[str, Any]]):
    """校验 DAG 任务列表中的 task_id 是否在 sync/production 注册表中"""
    # 先发现所有已注册的因子
    discover_factors()

    sync_tasks = {t["task_id"] for t in _sync_engine.get_all_tasks()}
    prod_factors = {f["factor_id"] for f in list_factors()}
    valid_ids = sync_tasks | prod_factors
    invalid = [t.get("task_id", "?") for t in tasks if t.get("task_id") not in valid_ids]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"未知的 task_id: {', '.join(invalid)}。可用: {', '.join(sorted(valid_ids))}",
        )


@router.get("/dag/list")
async def list_dags():
    """列出所有 DAG，附带最近成功执行时间"""
    dags = dag_config_mgr.get_all_dags()
    # 查询每个 DAG 最近一次成功运行时间
    try:
        df = db_client.query("""
            SELECT dag_id, MAX(finished_at) as last_success
            FROM dag_run_log WHERE status = 'success'
            GROUP BY dag_id
        """)
        success_map = {}
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                success_map[row["dag_id"]] = str(row["last_success"])
        for dag in dags:
            dag["last_success"] = success_map.get(dag["dag_id"])
    except Exception as e:
        logger.warning(f"Failed to query last success time: {e}")
    return {"status": "success", "data": dags}


@router.post("/dag/create")
async def create_dag(req: DAGCreateRequest):
    """创建 DAG"""
    try:
        if req.tasks:
            _validate_dag_tasks(req.tasks)
        dag_config = {
            "dag_id": req.dag_id,
            "description": req.description,
            "schedule": req.schedule,
            "tasks": req.tasks,
        }
        dag_config_mgr.add_dag(dag_config)
        return {"status": "success", "data": dag_config}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Create DAG failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/dag/{dag_id}")
async def update_dag(dag_id: str, req: DAGUpdateRequest):
    """修改 DAG"""
    try:
        existing = dag_config_mgr.get_dag(dag_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"DAG {dag_id} 不存在")
        updated = {**existing}
        if req.description is not None:
            updated["description"] = req.description
        if req.schedule is not None:
            updated["schedule"] = req.schedule
        if req.tasks is not None:
            _validate_dag_tasks(req.tasks)
            updated["tasks"] = req.tasks
        dag_config_mgr.update_dag(dag_id, updated)
        return {"status": "success", "data": updated}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Update DAG failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/dag/{dag_id}")
async def delete_dag(dag_id: str):
    """删除 DAG"""
    try:
        dag_config_mgr.delete_dag(dag_id)
        return {"status": "success", "data": {"dag_id": dag_id}}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Delete DAG failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _build_dag_run_result(dag_run) -> dict:
    """从 DAGRun 对象构建返回结果"""
    dag_status = dag_run.status.value if hasattr(dag_run.status, 'value') else str(dag_run.status)
    task_results = []
    for node in dag_run.tasks.values():
        task_results.append({
            "task_id": node.task_id,
            "task_type": node.task_type,
            "status": node.status.value if hasattr(node.status, 'value') else str(node.status),
            "error_message": node.error_message,
            "started_at": str(node.started_at) if node.started_at else None,
            "finished_at": str(node.finished_at) if node.finished_at else None,
        })
    failed_tasks = [t for t in task_results if t["status"] in ("failed", "skipped")]
    return {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "status": dag_status,
        "tasks": task_results,
        "failed_tasks": failed_tasks,
        "summary": f"{len(task_results)} 个任务: "
                   f"{sum(1 for t in task_results if t['status'] == 'success')} 成功, "
                   f"{sum(1 for t in task_results if t['status'] == 'failed')} 失败, "
                   f"{sum(1 for t in task_results if t['status'] == 'skipped')} 跳过",
    }


def _generate_trading_dates(start_date: str, end_date: str) -> List[str]:
    """生成日期范围内的交易日（简单实现：跳过周末），返回 YYYYMMDD 格式"""
    norm_start = DateUtils.normalize_date(start_date)
    norm_end = DateUtils.normalize_date(end_date)
    start = datetime.strptime(norm_start, "%Y%m%d")
    end = datetime.strptime(norm_end, "%Y%m%d")
    dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 周一到周五
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


@router.post("/dag/run")
async def run_dag(req: DAGRunRequest):
    """运行 DAG，支持单日执行和日期范围回溯"""
    try:
        executor = _get_dag_executor()

        # 日期范围回溯模式
        if req.start_date and req.end_date:
            dates = _generate_trading_dates(req.start_date, req.end_date)
            if not dates:
                raise HTTPException(status_code=400, detail="日期范围内无交易日")

            backfill_id = f"{req.dag_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            logger.info(f"DAG backfill: {req.dag_id}, {len(dates)} trading days from {req.start_date} to {req.end_date}, backfill_id={backfill_id}")
            results = []
            total_success = 0
            total_failed = 0
            for d in dates:
                dag_run = executor.execute_dag(req.dag_id, target_date=d,
                                               run_type="backfill", backfill_id=backfill_id)
                result = _build_dag_run_result(dag_run)
                result["target_date"] = d
                results.append(result)
                if result["status"] == "success":
                    total_success += 1
                else:
                    total_failed += 1

            return {
                "status": "success",
                "data": {
                    "mode": "backfill",
                    "dag_id": req.dag_id,
                    "backfill_id": backfill_id,
                    "date_range": [req.start_date, req.end_date],
                    "total_days": len(dates),
                    "success_days": total_success,
                    "failed_days": total_failed,
                    "summary": f"回溯 {len(dates)} 个交易日: {total_success} 天成功, {total_failed} 天失败",
                    "runs": results,
                }
            }

        # 单日执行模式
        run_type = req.run_type if req.run_type != "backfill" else "single"
        dag_run = executor.execute_dag(req.dag_id, target_date=req.target_date, run_type=run_type)
        result = _build_dag_run_result(dag_run)
        return {"status": "success", "data": result}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"DAG run failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dag/{dag_id}/history")
async def get_dag_history(dag_id: str, limit: int = 10, run_type: Optional[str] = None):
    """获取 DAG 运行历史，支持按 run_type 过滤"""
    try:
        executor = _get_dag_executor()
        records = executor.get_dag_runs(dag_id, limit, run_type=run_type)
        return {"status": "success", "data": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dag/backfill/{backfill_id}")
async def get_backfill_detail(backfill_id: str):
    """获取回溯批次详情"""
    try:
        executor = _get_dag_executor()
        summary = executor.get_backfill_summary(backfill_id)
        if not summary:
            raise HTTPException(status_code=404, detail="回溯批次不存在")
        return {"status": "success", "data": summary}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
