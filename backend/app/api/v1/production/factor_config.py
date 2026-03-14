"""因子配置和指数池管理 API 端点"""
import json
import io
import csv
from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import polars as pl

from store.dolphindb_client import db_client
from app.core.logger import logger
from app.core.cache import api_cache

router = APIRouter()


# ==================== Pydantic Models ====================

class DataFieldMapping(BaseModel):
    """数据字段映射"""
    field_key: str
    description: str = ""
    table_name: str = ""
    column_name: str = ""
    extra_config: str = "{}"


class DataConfigUpdateRequest(BaseModel):
    """数据配置更新请求"""
    mappings: List[DataFieldMapping]


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
    csv_content: str


# ==================== Data Config Endpoints ====================

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
    """返回简化的 field_key → source_label + values 字典，供前端注解显示

    注意：只返回 factor_data_config 中配置的特殊字段
    depends_on 表的字段会自动可用，不需要在这里配置
    """
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
                    except Exception as e:
                        logger.debug(f"解析数据源配置失败: {e}")
                        source_label = "未配置"
                else:
                    source_label = "未配置"

                result[fk] = {"source_label": source_label, "values": values}

        response = {"status": "success", "data": result}
        api_cache.set("production:data-config:resolved", response, ttl=120)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/production/available-tables")
async def get_available_tables():
    """获取所有可用的数据表（sync任务表 + ETL任务表）"""
    cached = api_cache.get("production:available-tables")
    if cached is not None:
        return cached
    try:
        tables = []

        # 获取所有 sync 任务表
        try:
            sync_df = db_client.query("SELECT task_id, table_name, description FROM sync_task_config WHERE enabled = true ORDER BY task_id")
            if not sync_df.is_empty():
                for row in sync_df.to_dicts():
                    tables.append({
                        "value": row["table_name"],
                        "label": row["table_name"],
                        "description": row.get("description", ""),
                        "type": "sync"
                    })
        except Exception as e:
            logger.warning(f"Failed to load sync tasks: {e}")

        # 获取所有 ETL 任务表
        try:
            etl_df = db_client.query("SELECT task_id, table_name, description FROM etl_task_config WHERE enabled = true ORDER BY task_id")
            if not etl_df.is_empty():
                for row in etl_df.to_dicts():
                    tables.append({
                        "value": row["table_name"],
                        "label": row["table_name"],
                        "description": row.get("description", ""),
                        "type": "etl"
                    })
        except Exception as e:
            logger.warning(f"Failed to load ETL tasks: {e}")

        response = {"status": "success", "data": tables}
        api_cache.set("production:available-tables", response, ttl=300)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Index Pool Endpoints ====================

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
            SELECT index_code, index_name, description, stock_count, latest_date, created_at, updated_at
            FROM index_metadata
            ORDER BY index_code
        """)

        if df.is_empty():
            return {"status": "success", "data": []}

        records = df.to_dicts()
        for record in records:
            if "created_at" in record and record["created_at"]:
                record["created_at"] = str(record["created_at"])
            if "updated_at" in record and record["updated_at"]:
                record["updated_at"] = str(record["updated_at"])

        return {"status": "success", "data": records}

    except Exception as e:
        if "does not exist" in str(e):
            return {"status": "success", "data": []}
        logger.error(f"Failed to list index pools: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/index-pool/template")
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


@router.get("/index-pool/{index_code}")
async def get_index_pool_detail(index_code: str, trade_date: Optional[str] = None):
    """获取指定指数的成分股详情

    Args:
        index_code: 指数代码，如 '000300.SH'
        trade_date: 交易日期（可选），如 '20240101'。不指定则返回最新日期的成分股
    """
    try:
        # 查询元数据
        metadata_df = db_client.query("""
            SELECT * FROM index_metadata WHERE index_code = %s
        """, (index_code,))

        if metadata_df.is_empty():
            raise HTTPException(status_code=404, detail=f"未找到指数 {index_code}")

        metadata = metadata_df.to_dicts()[0]
        if "created_at" in metadata and metadata["created_at"]:
            metadata["created_at"] = str(metadata["created_at"])
        if "updated_at" in metadata and metadata["updated_at"]:
            metadata["updated_at"] = str(metadata["updated_at"])

        # 确定查询日期
        if not trade_date:
            trade_date = metadata.get("latest_date")
            if not trade_date:
                return {
                    "status": "success",
                    "data": {
                        "metadata": metadata,
                        "constituents": [],
                        "trade_date": None
                    }
                }

        # 查询成分股
        constituents_df = db_client.query("""
            SELECT ts_code, weight
            FROM index_constituents
            WHERE index_code = %s AND trade_date = %s
            ORDER BY weight DESC
        """, (index_code, trade_date))

        constituents = constituents_df.to_dicts() if not constituents_df.is_empty() else []

        return {
            "status": "success",
            "data": {
                "metadata": metadata,
                "constituents": constituents,
                "trade_date": trade_date
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get index pool detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/index-pool/{index_code}")
async def delete_index_pool(index_code: str):
    """删除指定指数及其所有成分股数据"""
    try:
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
