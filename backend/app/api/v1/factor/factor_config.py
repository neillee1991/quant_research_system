"""因子配置和指数池管理 API 端点"""
import json
import io
import csv
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from store.dolphindb_client import db_client
from app.core.logger import logger
from app.core.cache import api_cache

router = APIRouter()


# ==================== Pydantic Models ====================

class DataFieldMapping(BaseModel):
    field_key: str
    description: str = ""
    table_name: str = ""
    column_name: str = ""
    extra_config: str = "{}"


class DataConfigUpdateRequest(BaseModel):
    mappings: List[DataFieldMapping]


class IndexPoolBatchUploadRequest(BaseModel):
    index_code: str
    index_name: str = ""
    description: str = ""
    data: List[Dict[str, Any]]


class IndexPoolCSVUploadRequest(BaseModel):
    index_code: str
    index_name: str = ""
    description: str = ""
    csv_content: str


# ==================== Data Config Endpoints ====================

@router.get("/factor/data-config")
async def get_data_config():
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


@router.put("/factor/data-config")
async def update_data_config(req: DataConfigUpdateRequest):
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


@router.get("/factor/data-config/resolved")
async def get_resolved_data_config():
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


@router.get("/factor/available-tables")
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


# ==================== Index Pool Endpoints ====================

@router.post("/factor/index-pool/batch-upload")
async def batch_upload_index_pool(req: IndexPoolBatchUploadRequest):
    """批量上传指数成分股（index_constituents → DolphinDB，index_configs → PostgreSQL）"""
    from scheduler.db import DatabasePool
    import polars as pl

    try:
        if not req.data:
            raise HTTPException(status_code=400, detail="数据不能为空")

        required_fields = ["trade_date", "ts_code"]
        for item in req.data:
            for field in required_fields:
                if field not in item:
                    raise HTTPException(status_code=400, detail=f"缺少必需字段: {field}")

        for item in req.data:
            item["index_code"] = req.index_code
            if "weight" not in item:
                item["weight"] = 0.0

        # index_constituents 是时序数据，保留在 DolphinDB
        constituents_df = pl.DataFrame(req.data)
        db_client.upsert(
            "index_constituents",
            constituents_df,
            key_columns=["trade_date", "ts_code", "index_code"]
        )

        latest_date = constituents_df["trade_date"].max()
        stock_count = constituents_df.filter(
            pl.col("trade_date") == latest_date
        )["ts_code"].n_unique()

        # index_configs (metadata) → PostgreSQL
        now = datetime.now()
        await DatabasePool.execute("""
            INSERT INTO index_configs
              (index_code, index_name, description, stock_count, latest_date, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (index_code) DO UPDATE SET
              index_name  = EXCLUDED.index_name,
              description = EXCLUDED.description,
              stock_count = EXCLUDED.stock_count,
              latest_date = EXCLUDED.latest_date,
              updated_at  = EXCLUDED.updated_at
        """,
            req.index_code, req.index_name, req.description,
            stock_count, latest_date, now, now,
        )

        logger.info(f"Uploaded {len(req.data)} records for index {req.index_code}")
        return {
            "status": "success",
            "message": f"成功上传 {len(req.data)} 条成分股数据",
            "data": {
                "index_code": req.index_code,
                "records_count": len(req.data),
                "stock_count": stock_count,
                "latest_date": latest_date,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload index pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/factor/index-pool/csv-upload")
async def csv_upload_index_pool(req: IndexPoolCSVUploadRequest):
    """CSV 上传指数成分股"""
    try:
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

        batch_req = IndexPoolBatchUploadRequest(
            index_code=req.index_code,
            index_name=req.index_name,
            description=req.description,
            data=data,
        )
        return await batch_upload_index_pool(batch_req)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to parse CSV: {e}")
        raise HTTPException(status_code=500, detail=f"CSV 解析失败: {str(e)}")


@router.get("/factor/index-pool/list")
async def list_index_pools():
    """列出所有指数股票池（PostgreSQL index_configs）"""
    from scheduler.db import DatabasePool

    try:
        rows = await DatabasePool.fetch(
            "SELECT index_code, index_name, description, stock_count, latest_date, "
            "created_at, updated_at FROM index_configs ORDER BY index_code"
        )
        records = []
        for row in rows:
            r = dict(row)
            for ts_field in ["created_at", "updated_at"]:
                if r.get(ts_field):
                    r[ts_field] = str(r[ts_field])
            if r.get("latest_date"):
                r["latest_date"] = str(r["latest_date"])
            records.append(r)
        return {"status": "success", "data": records}
    except Exception as e:
        logger.error(f"Failed to list index pools: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor/index-pool/template")
async def get_index_pool_template():
    """获取指数成分股上传模板（CSV 格式）"""
    template = "trade_date,ts_code,weight\n20240101,000001.SZ,0.05\n20240101,000002.SZ,0.03\n"
    return {
        "status": "success",
        "data": {
            "template": template,
            "description": "CSV 格式模板，包含 trade_date（交易日期）、ts_code（股票代码）、weight（权重，可选）"
        }
    }


@router.get("/factor/index-pool/{index_code}")
async def get_index_pool_detail(index_code: str, trade_date: Optional[str] = None):
    """获取指定指数的成分股详情（metadata from PG, constituents from DolphinDB）"""
    from scheduler.db import DatabasePool

    try:
        row = await DatabasePool.fetchrow(
            "SELECT * FROM index_configs WHERE index_code = $1", index_code
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"未找到指数 {index_code}")

        metadata = dict(row)
        for ts_field in ["created_at", "updated_at"]:
            if metadata.get(ts_field):
                metadata[ts_field] = str(metadata[ts_field])
        if metadata.get("latest_date"):
            metadata["latest_date"] = str(metadata["latest_date"])

        if not trade_date:
            trade_date = metadata.get("latest_date")
            if not trade_date:
                return {"status": "success", "data": {"metadata": metadata, "constituents": [], "trade_date": None}}

        # index_constituents 是时序数据，保留在 DolphinDB
        constituents_df = db_client.query("""
            SELECT ts_code, weight
            FROM index_constituents
            WHERE index_code = %s AND trade_date = %s
            ORDER BY weight DESC
        """, (index_code, trade_date))

        constituents = constituents_df.to_dicts() if not constituents_df.is_empty() else []
        return {"status": "success", "data": {"metadata": metadata, "constituents": constituents, "trade_date": trade_date}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get index pool detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/factor/index-pool/{index_code}")
async def delete_index_pool(index_code: str):
    """删除指定指数及其所有成分股数据"""
    from scheduler.db import DatabasePool

    try:
        # index_constituents 在 DolphinDB
        db_client.execute(
            "DELETE FROM index_constituents WHERE index_code = %s", (index_code,)
        )
        # index_configs 在 PostgreSQL
        await DatabasePool.execute(
            "DELETE FROM index_configs WHERE index_code = $1", index_code
        )
        logger.info(f"Deleted index pool: {index_code}")
        return {"status": "success", "message": f"成功删除指数 {index_code}"}
    except Exception as e:
        logger.error(f"Failed to delete index pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== DataFrame Schema Preview ====================

class DataFrameSchemaRequest(BaseModel):
    depends_on: List[str]


@router.post("/factor/dataframe-schema")
async def get_dataframe_schema(req: DataFrameSchemaRequest):
    """根据依赖的数据源，返回预期的 DataFrame schema（列名和类型）"""
    try:
        if not req.depends_on:
            return {"status": "success", "data": {"columns": []}}

        columns = []
        seen_columns = set()

        columns.append({"name": "ts_code", "type": "SYMBOL", "source": "所有表", "description": "股票代码"})
        columns.append({"name": "trade_date", "type": "DATE", "source": "所有表", "description": "交易日期"})
        seen_columns.update(["ts_code", "trade_date"])

        for dep in req.depends_on:
            if dep.startswith("factor:"):
                factor_id = dep[7:]
                if factor_id not in seen_columns:
                    columns.append({
                        "name": factor_id, "type": "Float64",
                        "source": f"因子: {factor_id}", "description": "因子值"
                    })
                    seen_columns.add(factor_id)
            else:
                try:
                    schema_df = db_client.query(
                        f"SELECT name, typeString FROM schema(loadTable('dfs://quant', '{dep}')).colDefs"
                    )
                    if not schema_df.is_empty():
                        for row in schema_df.to_dicts():
                            col_name = row["name"]
                            if col_name in ["ts_code", "trade_date"]:
                                continue
                            if col_name not in seen_columns:
                                columns.append({
                                    "name": col_name, "type": row["typeString"],
                                    "source": dep, "description": ""
                                })
                                seen_columns.add(col_name)
                            else:
                                columns.append({
                                    "name": f"{col_name}_{dep}", "type": row["typeString"],
                                    "source": dep, "description": f"重命名（原: {col_name}）"
                                })
                except Exception as e:
                    logger.warning(f"Failed to get schema for {dep}: {e}")
                    continue

        return {
            "status": "success",
            "data": {
                "columns": columns,
                "total_columns": len(columns),
                "note": "实际列名可能因数据源冲突而自动重命名"
            }
        }
    except Exception as e:
        logger.error(f"Failed to get dataframe schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))
