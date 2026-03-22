"""因子计算执行 API 端点"""
import io
import sys
import time
import uuid
import types
import traceback
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from store.dolphindb_client import db_client
from services.factor_compute_service import FactorComputeService, DEFAULT_PREPROCESS as _DEFAULT_PREPROCESS
from engine.production.registry import FactorDefinition, StorageConfig
from app.core.logger import logger
from app.core.utils import (
    DateUtils,
    safe_json_parse,
    unify_record_fields,
    safe_str_datetime,
    normalize_trade_date_pl,
)

router = APIRouter()
factor_service = FactorComputeService(db_client)


# ==================== Helper Functions ====================

def _format_run_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """格式化 factor_run_log 记录（统一日期格式、字段名）"""
    result = dict(record)

    # 格式化日期字段
    for date_field in ["start_date", "end_date"]:
        if result.get(date_field):
            result[date_field] = DateUtils.format_date_for_display(result[date_field])

    # 格式化时间戳
    result["created_at"] = safe_str_datetime(result.get("created_at"))
    result["finished_at"] = safe_str_datetime(result.get("finished_at"))

    # 统一字段名
    result = unify_record_fields(result)
    return result


# ==================== Pydantic Models ====================

class PreprocessOptions(BaseModel):
    """因子计算预处理选项"""
    adjust_price: str = "forward"       # 复权方式: "none"=不复权, "forward"=前复权, "backward"=后复权
    filter_st: bool = True              # 过滤 ST/*ST 股票
    filter_new_stock: bool = True       # 过滤新股（上市不足 N 天）
    new_stock_days: int = 60            # 新股排除天数
    mark_limit: bool = True             # 标记一字涨跌停


class ProductionRunRequest(BaseModel):
    """生产任务运行请求"""
    factor_id: str
    mode: str = "incremental"
    target_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    preprocess: Optional[PreprocessOptions] = None


class BatchRunRequest(BaseModel):
    """批量运行请求"""
    factor_ids: List[str]
    mode: str = "incremental"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    preprocess: Optional[PreprocessOptions] = None


class FactorTestRequest(BaseModel):
    """因子测试请求"""
    code: str
    start_date: str  # 因子计算起始日期
    end_date: str    # 因子计算结束日期
    params: Dict[str, Any] = {}
    depends_on: List[str] = ["sync_daily_data"]
    preprocess: Optional[Dict[str, Any]] = None
    lookback_days: int = 60  # 向前回溯天数，用于自动计算数据加载起始日期


# ==================== API Endpoints ====================

def _run_factor_background(
    factor_id: str,
    mode: str,
    target_date: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    preprocess: Optional[Dict],
    run_id: str
):
    """后台执行因子计算"""
    try:
        result = factor_service.compute_factor(
            factor_id=factor_id,
            mode=mode,
            target_date=target_date,
            start_date=start_date,
            end_date=end_date,
            preprocess=preprocess,
        )
        logger.info(f"Factor {factor_id} computation completed: run_id={run_id}, success={result.success}, rows={result.rows}")
    except Exception as e:
        logger.error(f"Factor {factor_id} computation failed: run_id={run_id}, error={e}")


@router.post("/production/run")
async def run_production(req: ProductionRunRequest, background_tasks: BackgroundTasks):
    """运行生产任务（异步）

    立即返回 run_id，后台执行计算。使用 /production/status/{run_id} 查询状态。
    """
    try:
        # 生成 run_id
        run_id = f"{req.factor_id}_{int(time.time() * 1000)}"

        preprocess = req.preprocess.model_dump() if req.preprocess else None

        # 添加后台任务
        background_tasks.add_task(
            _run_factor_background,
            factor_id=req.factor_id,
            mode=req.mode,
            target_date=req.target_date,
            start_date=req.start_date,
            end_date=req.end_date,
            preprocess=preprocess,
            run_id=run_id
        )

        return {
            "status": "success",
            "data": {
                "run_id": run_id,
                "factor_id": req.factor_id,
                "status": "pending",
                "message": "Factor computation started in background"
            }
        }
    except Exception as e:
        logger.error(f"Failed to start factor computation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/production/batch-run")
async def batch_run_production(req: BatchRunRequest, background_tasks: BackgroundTasks):
    """批量计算因子（异步）

    立即返回所有 run_id，后台并行执行。使用 /production/status/{run_id} 查询各因子状态。
    """
    try:
        preprocess = req.preprocess.model_dump() if req.preprocess else None
        run_ids = []

        for fid in req.factor_ids:
            # 使用 UUID 替代 timestamp + sleep，避免冲突且无需延迟
            run_id = f"{fid}_{uuid.uuid4().hex[:12]}"
            run_ids.append({"factor_id": fid, "run_id": run_id, "status": "pending"})

            # 每个因子独立后台任务
            background_tasks.add_task(
                _run_factor_background,
                factor_id=fid,
                mode=req.mode,
                target_date=None,
                start_date=req.start_date,
                end_date=req.end_date,
                preprocess=preprocess,
                run_id=run_id
            )

        return {
            "status": "success",
            "data": {
                "tasks": run_ids,
                "message": f"{len(req.factor_ids)} factor computations started in background"
            }
        }
    except Exception as e:
        logger.error(f"Failed to start batch computation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/status/{run_id}")
async def get_run_status(run_id: str):
    """查询因子计算任务状态

    返回任务的执行状态、耗时、数据条数等信息。
    """
    try:
        # 只查询需要的列，而不是 SELECT *
        df = db_client.query("""
            SELECT id, factor_id, run_id, start_date, end_date, status,
                   created_at, finished_at, rows_affected, duration_seconds, error_message
            FROM factor_run_log
            WHERE run_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (run_id,))

        if df.is_empty():
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found"
            )

        record = df.to_dicts()[0]
        record = _format_run_record(record)

        return {"status": "success", "data": record}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get run status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/history")
async def get_production_history(
    factor_id: Optional[str] = None,
    limit: int = 20,
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD")
):
    """获取生产运行历史"""
    try:
        conditions = []
        params = []
        if factor_id:
            conditions.append("factor_id = %s")
            params.append(factor_id)
        if start_date:
            if not DateUtils.validate_yyyymmdd(start_date):
                raise HTTPException(status_code=400, detail="start_date must be YYYYMMDD")
            conditions.append(f"date(created_at) >= {start_date[:4]}.{start_date[4:6]}.{start_date[6:8]}")
        if end_date:
            if not DateUtils.validate_yyyymmdd(end_date):
                raise HTTPException(status_code=400, detail="end_date must be YYYYMMDD")
            conditions.append(f"date(created_at) <= {end_date[:4]}.{end_date[4:6]}.{end_date[6:8]}")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        # 只查询需要的列
        df = db_client.query(f"""
            SELECT id, factor_id, run_id, start_date, end_date, status,
                   created_at, finished_at, rows_affected, duration_seconds, error_message
            FROM factor_run_log
            {where}
            ORDER BY created_at DESC LIMIT %s
        """, tuple(params))

        data = []
        if not df.is_empty():
            for row in df.to_dicts():
                data.append(_format_run_record(row))

        return {"status": "success", "data": data}
    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": []}
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/production/factors/test")
async def test_factor_code(req: FactorTestRequest):
    """编译并测试因子代码，返回计算结果预览

    流程：
    1. 编译代码，提取 compute 函数（通过 @factor 装饰器或直接定义）
    2. 加载指定日期范围的真实数据
    3. 执行因子计算
    4. 返回结果预览（含统计信息）
    """
    logs: list = []
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
    depends_on = req.depends_on

    if captured_definitions:
        defn = captured_definitions[0]
        compute_func = defn["func"]
        func_params = {**defn["params"], **req.params}
        depends_on = defn["depends_on"]
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
    # 计算数据加载起始日期：因子计算起始日期 - lookback_days
    from app.core.utils import TradingCalendar
    lookback_days = req.lookback_days
    trading_cal = TradingCalendar.get_instance(db_client)
    data_start_date = trading_cal.offset_trading_days(req.start_date, -lookback_days)

    log("data", f"因子计算区间: {req.start_date}~{req.end_date}")
    log("data", f"数据加载区间: {data_start_date}~{req.end_date} (向前回溯 {lookback_days} 个交易日)")
    log("data", f"依赖数据源: {depends_on}")
    t0 = time.time()

    preprocess_opts = func_params.get("preprocess", {})
    if req.preprocess:
        preprocess_opts = {**preprocess_opts, **req.preprocess}
    opts = {**_DEFAULT_PREPROCESS, **preprocess_opts}
    log("data", f"预处理配置: adjust_price={opts['adjust_price']}, filter_st={opts['filter_st']}, filter_new_stock={opts['filter_new_stock']}, mark_limit={opts['mark_limit']}")

    try:
        # 直接从数据库加载数据（使用计算后的数据起始日期）
        df = None
        for table in depends_on:
            table_df = db_client.query(
                f"SELECT * FROM {table} WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date",
                (data_start_date, req.end_date)
            )
            if df is None:
                df = table_df
            else:
                # 合并数据（使用 outer join 保留所有记录）
                df = df.join(table_df, on=["ts_code", "trade_date"], how="outer")

        if df is None or df.is_empty():
            return make_error("data", f"数据加载区间 {data_start_date}~{req.end_date} 无数据")

        log("data", f"加载完成: {df.shape[0]} 行 × {df.shape[1]} 列 ({(time.time()-t0)*1000:.0f}ms)")
    except Exception as e:
        return make_error("data", f"数据加载失败:\n{traceback.format_exc()}")

    # 4. 执行因子计算
    log("compute", "执行因子计算...")
    t0 = time.time()
    try:
        result = compute_func(df, func_params)
        if result is None:
            return make_error("compute", "因子函数返回 None")
        if not hasattr(result, "shape"):
            return make_error("compute", f"因子函数返回类型错误: {type(result)}")
        log("compute", f"计算完成: {result.shape[0]} 行 × {result.shape[1]} 列 ({(time.time()-t0)*1000:.0f}ms)")
    except Exception as e:
        return make_error("compute", f"因子计算失败:\n{traceback.format_exc()}")

    # 5. 生成预览和统计
    log("stats", "生成统计信息...")
    try:
        import polars as pl
        if "factor_value" not in result.columns:
            return make_error("stats", "结果缺少 'factor_value' 列")

        log("stats", f"计算结果: {result.shape[0]} 行 × {result.shape[1]} 列")

        # 过滤结果：只保留测试区间内的数据
        if "trade_date" in result.columns:
            # 使用统一的日期归一化工具
            result = normalize_trade_date_pl(result, "trade_date")
            log("stats", f"已将 trade_date 转换为 YYYYMMDD 格式")
            log("stats", f"过滤前行数: {result.shape[0]}, 过滤条件: {req.start_date} <= trade_date <= {req.end_date}")

            result = result.filter(
                (pl.col("trade_date") >= req.start_date) &
                (pl.col("trade_date") <= req.end_date)
            )
            log("stats", f"过滤后行数: {result.shape[0]}")

        preview = result.head(100).to_dicts()

        # 使用 DolphinDB 的 stat 函数计算统计指标
        total_rows = result.shape[0]
        null_count = result["factor_value"].null_count()
        null_ratio = null_count / total_rows if total_rows > 0 else 0

        stats = {
            "total_rows": total_rows,
            "null_count": null_count,
            "null_ratio": null_ratio,
        }

        # 使用 Polars 计算统计指标
        valid = result.filter(pl.col("factor_value").is_not_null())
        if valid.shape[0] > 0:
            valid_values = valid["factor_value"].drop_nulls()
            if len(valid_values) > 0:
                stats.update({
                    "count": len(valid_values),
                    "mean": float(valid_values.mean()),
                    "std": float(valid_values.std()),
                    "min": float(valid_values.min()),
                    "max": float(valid_values.max()),
                    "median": float(valid_values.median()),
                })
                log("stats", f"统计结果: count={stats['count']}, mean={stats['mean']:.6f}, std={stats['std']:.6f}")
        else:
            stats.update({"count": 0, "mean": None, "std": None, "min": None, "max": None, "median": None})

        log("stats", f"统计完成: {stats['total_rows']} 行, {stats['null_count']} 空值 ({stats['null_ratio']:.2%})")

        return {
            "status": "success",
            "data": {
                "preview": preview,
                "stats": stats,
                "logs": logs,
                "stdout": stdout_capture.getvalue(),
            }
        }
    except Exception as e:
        return make_error("stats", f"统计失败:\n{traceback.format_exc()}")
