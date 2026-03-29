"""
指数订阅 API

提供指数发现和订阅功能：
- 查询可订阅的指数列表
- 获取市场和发布机构筛选选项
- 订阅/取消订阅指数
"""
import json
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
import polars as pl

from app.core.logger import logger
from store.dolphindb_client import db_client
from app.services.task_service import sync_service
from app.models.base_task import SyncTaskConfig


router = APIRouter()


# ==================== 请求/响应模型 ====================

class IndexBasicInfo(BaseModel):
    """指数基础信息"""
    ts_code: str
    name: str
    market: str
    publisher: Optional[str] = None
    list_date: Optional[str] = None
    weight_rule: Optional[str] = None
    desc: Optional[str] = None
    exp_date: Optional[str] = None
    is_subscribed: bool = False
    subscribed_task_id: Optional[str] = None


class IndexListResponse(BaseModel):
    """指数列表响应"""
    indices: List[IndexBasicInfo]
    total: int
    page: int
    limit: int


class FilterOptionsResponse(BaseModel):
    """筛选选项响应"""
    markets: List[str]
    publishers: List[str]


class IndexSubscribeRequest(BaseModel):
    """指数订阅请求"""
    index_code: str = Field(..., description="指数代码，如 000001.SH")


class IndexSubscribeResponse(BaseModel):
    """指数订阅响应"""
    task_id: str
    index_code: str
    table_name: str
    status: str
    message: str


class IndexUnsubscribeResponse(BaseModel):
    """指数取消订阅响应"""
    index_code: str
    status: str
    message: str


class FilterFieldConfig(BaseModel):
    """单个筛选字段配置"""
    field: str = Field(..., description="字段名")
    label: str = Field(..., description="显示标签")
    enabled: bool = Field(default=True, description="是否启用为筛选项")
    default_value: Optional[str] = Field(default=None, description="默认筛选值")


class UserSyncPreference(BaseModel):
    """用户同步偏好配置"""
    index_basic_table: str = Field(..., description="指数基础信息表名")
    filter_config: Optional[List[FilterFieldConfig]] = Field(default=None, description="筛选字段配置")


class UserSyncPreferenceResponse(BaseModel):
    """用户同步偏好响应"""
    user_id: str
    index_basic_table: str
    filter_config: Optional[List[FilterFieldConfig]] = None


# ==================== 辅助函数 ====================

def _sanitize_index_code(index_code: str) -> str:
    """
    清理指数代码，生成合法的表名后缀

    将 . 和 _ 替换为空，避免表名问题
    """
    return index_code.replace(".", "").replace("_", "")


def _get_subscribed_indices() -> List[str]:
    """获取已订阅的指数列表"""
    try:
        # 查询所有同步任务，找出指数权重任务
        tasks = sync_service.list_tasks(enabled_only=False)
        subscribed = []
        for task in tasks:
            task_id = task.task_id
            if task_id.startswith("sync_index_weight_"):
                # 从任务配置中提取指数代码
                try:
                    params = json.loads(task.params_json) if task.params_json else {}
                    index_code = params.get("index_code")
                    if index_code:
                        subscribed.append(index_code)
                except json.JSONDecodeError:
                    continue
        return subscribed
    except Exception as e:
        logger.error(f"Failed to get subscribed indices: {e}")
        return []


def _get_subscription_task_map() -> dict:
    """获取指数代码到任务ID的映射"""
    try:
        tasks = sync_service.list_tasks(enabled_only=False)
        mapping = {}
        for task in tasks:
            task_id = task.task_id
            if task_id.startswith("sync_index_weight_"):
                try:
                    params = json.loads(task.params_json) if task.params_json else {}
                    index_code = params.get("index_code")
                    if index_code:
                        mapping[index_code] = task_id
                except json.JSONDecodeError:
                    continue
        return mapping
    except Exception as e:
        logger.error(f"Failed to get subscription task map: {e}")
        return {}


# ==================== API 端点 ====================

@router.get("/data/index/available", response_model=IndexListResponse)
async def list_available_indices(
    search: Optional[str] = Query(None, description="搜索关键词（指数名称或代码）"),
    filters: Optional[str] = Query(None, description="JSON格式筛选条件，如 {\"market\":\"SSE\"}"),
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(20, ge=1, le=100, description="每页数量"),
    show_subscribed_only: bool = Query(False, description="仅显示已订阅的指数"),
):
    """
    查询可订阅的指数列表

    支持搜索、筛选和分页功能
    """
    try:
        # 构建查询条件
        conditions = []
        params = []

        try:
            allowed_columns = set(db_client.get_table_columns("sync_index_basic"))
        except Exception:
            allowed_columns = {"market", "publisher", "ts_code", "name", "list_date"}

        if search:
            conditions.append("(name LIKE %s OR ts_code LIKE %s)")
            search_pattern = f"%{search}%"
            params.extend([search_pattern, search_pattern])

        if filters:
            try:
                filter_dict = json.loads(filters)
                for field, value in filter_dict.items():
                    if field in allowed_columns and value:
                        conditions.append(f"{field} = %s")
                        params.append(value)
            except json.JSONDecodeError:
                pass

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 查询总数
        count_sql = f"SELECT COUNT(*) as total FROM sync_index_basic WHERE {where_clause}"
        count_df = db_client.query(count_sql, tuple(params) if params else None)
        total = count_df["total"][0] if not count_df.is_empty() else 0

        # 查询数据（先查全部，再在Python中分页）
        data_sql = f"""
            SELECT ts_code, name, market, publisher, list_date, weight_rule, desc, exp_date
            FROM sync_index_basic
            WHERE {where_clause}
            ORDER BY ts_code
        """
        df = db_client.query(data_sql, tuple(params) if params else None)

        # 获取已订阅的指数
        subscribed_set = set(_get_subscribed_indices())
        task_map = _get_subscription_task_map()

        # 已订阅的排在前面，再按 ts_code 排序
        all_rows = df.to_dicts() if not df.is_empty() else []
        all_rows.sort(key=lambda r: (0 if r.get("ts_code", "") in subscribed_set else 1, r.get("ts_code", "")))

        offset = (page - 1) * limit
        paginated_rows = all_rows[offset:offset + limit]

        # 转换为响应模型
        indices = []
        for row in paginated_rows:
            index_code = row.get("ts_code", "")
            is_subscribed = index_code in subscribed_set

            # 如果只显示已订阅的，跳过未订阅的
            if show_subscribed_only and not is_subscribed:
                continue

            # 格式化日期字段
            list_date = row.get("list_date")
            if list_date and hasattr(list_date, 'strftime'):
                list_date = list_date.strftime("%Y%m%d")
            elif list_date:
                list_date = str(list_date)

            exp_date = row.get("exp_date")
            if exp_date and hasattr(exp_date, 'strftime'):
                exp_date = exp_date.strftime("%Y%m%d")
            elif exp_date:
                exp_date = str(exp_date)

            indices.append(IndexBasicInfo(
                ts_code=index_code,
                name=row.get("name", ""),
                market=row.get("market", ""),
                publisher=row.get("publisher"),
                list_date=list_date,
                weight_rule=row.get("weight_rule"),
                desc=row.get("desc"),
                exp_date=exp_date,
                is_subscribed=is_subscribed,
                subscribed_task_id=task_map.get(index_code) if is_subscribed else None
            ))

        # 如果是仅显示已订阅，需要重新计算总数
        if show_subscribed_only:
            total = len(indices)

        return IndexListResponse(
            indices=indices,
            total=total,
            page=page,
            limit=limit
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list available indices: {e}")
        raise HTTPException(status_code=500, detail=f"查询指数列表失败: {str(e)}")


@router.get("/data/index/filter-options", response_model=FilterOptionsResponse)
async def get_filter_options():
    """
    获取市场和发布机构的筛选选项

    用于前端构建筛选下拉菜单
    """
    try:
        # 查询所有市场
        market_sql = "SELECT DISTINCT market FROM sync_index_basic WHERE market IS NOT NULL ORDER BY market"
        market_df = db_client.query(market_sql)
        markets = [row["market"] for row in market_df.to_dicts()] if not market_df.is_empty() else []

        # 查询所有发布机构
        publisher_sql = "SELECT DISTINCT publisher FROM sync_index_basic WHERE publisher IS NOT NULL ORDER BY publisher"
        publisher_df = db_client.query(publisher_sql)
        publishers = [row["publisher"] for row in publisher_df.to_dicts()] if not publisher_df.is_empty() else []

        return FilterOptionsResponse(
            markets=markets,
            publishers=publishers
        )
    except Exception as e:
        logger.error(f"Failed to get filter options: {e}")
        raise HTTPException(status_code=500, detail=f"获取筛选选项失败: {str(e)}")


@router.post("/data/index/subscribe", response_model=IndexSubscribeResponse)
async def subscribe_index(request: IndexSubscribeRequest):
    """
    订阅指数

    1. 从模板 sync_index_weight_template 复制任务配置
    2. 替换 $$INDEX_CODE$$ 为实际指数代码
    3. 动态表名：sync_index_weight_{index_code}（清理后的代码）
    4. 创建同步任务到 sync_task_config 表
    """
    index_code = request.index_code.strip()

    try:
        # 1. 验证指数存在
        index_df = db_client.query(
            "SELECT ts_code, name FROM sync_index_basic WHERE ts_code = %s",
            (index_code,)
        )
        if index_df.is_empty():
            raise HTTPException(status_code=404, detail=f"指数 {index_code} 不存在")

        # 2. 检查是否已订阅
        task_map = _get_subscription_task_map()
        if index_code in task_map:
            raise HTTPException(
                status_code=400,
                detail=f"指数 {index_code} 已订阅，任务ID: {task_map[index_code]}"
            )

        # 3. 获取模板配置
        template = sync_service.get_task("sync_index_weight_template")
        if not template:
            raise HTTPException(status_code=500, detail="指数权重同步任务模板不存在")

        # 4. 生成任务ID和表名
        sanitized_code = _sanitize_index_code(index_code)
        task_id = f"sync_index_weight_{sanitized_code}"
        table_name = f"sync_index_weight_{sanitized_code}"

        # 5. 准备参数
        try:
            template_params = json.loads(template.params_json) if template.params_json else {}
        except json.JSONDecodeError:
            template_params = {}

        # 替换指数代码参数
        template_params["index_code"] = index_code

        # 6. 创建任务配置数据
        task_config_data = {
            "task_id": task_id,
            "description": f"指数 {index_code} 成分股权重同步",
            "api_name": template.api_name,
            "api_limit": template.api_limit,
            "sync_type": template.sync_type,
            "params_json": json.dumps(template_params),
            "date_field": template.date_field,
            "primary_keys_json": template.primary_keys_json,
            "table_name": table_name,
            "schema_json": template.schema_json,
            "column_mapping_json": template.column_mapping_json,
            "enabled": True,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }

        # 7. 保存任务配置
        sync_service.create_task(
            task_config_data,
            changed_by="api",
            change_reason=f"Subscribe index {index_code}"
        )

        # 8. 确保表存在（使用schema创建表）
        try:
            schema = json.loads(template.schema_json) if template.schema_json else {}
            if schema:
                db_client.create_table(table_name, schema, if_not_exists=True)
                logger.info(f"Created table {table_name} for index {index_code}")
        except Exception as table_err:
            logger.warning(f"Failed to create table {table_name}: {table_err}")

        logger.info(f"Successfully subscribed to index {index_code}, task_id={task_id}")

        return IndexSubscribeResponse(
            task_id=task_id,
            index_code=index_code,
            table_name=table_name,
            status="success",
            message=f"成功订阅指数 {index_code}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to subscribe index {index_code}: {e}")
        raise HTTPException(status_code=500, detail=f"订阅指数失败: {str(e)}")


@router.delete("/data/index/subscribe/{index_code}", response_model=IndexUnsubscribeResponse)
async def unsubscribe_index(index_code: str):
    """
    取消订阅指数

    1. 删除对应的同步任务
    2. 删除对应的数据表（可选，默认保留数据）
    """
    index_code = index_code.strip()

    try:
        # 1. 获取任务映射
        task_map = _get_subscription_task_map()
        if index_code not in task_map:
            raise HTTPException(status_code=404, detail=f"指数 {index_code} 未订阅")

        task_id = task_map[index_code]

        # 2. 删除任务配置及数据表（硬删除）
        sync_service.delete_task(
            task_id,
            changed_by="api",
            change_reason=f"Unsubscribe index {index_code}",
            drop_table=True,
            hard_delete=True
        )

        logger.info(f"Successfully unsubscribed from index {index_code}, task_id={task_id}")

        return IndexUnsubscribeResponse(
            index_code=index_code,
            status="success",
            message=f"成功取消订阅指数 {index_code}（同步任务及数据表已删除）"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unsubscribe index {index_code}: {e}")
        raise HTTPException(status_code=500, detail=f"取消订阅指数失败: {str(e)}")


@router.get("/data/index/preference", response_model=UserSyncPreferenceResponse)
async def get_user_preference():
    """
    获取用户的同步偏好配置

    使用固定的 "default" 用户，因为没有用户系统
    """
    try:
        # 查询用户偏好
        df = db_client.query(
            "SELECT user_id, index_table, filter_config FROM user_sync_preference WHERE user_id = %s",
            ("default",)
        )

        if df.is_empty():
            # 如果没有配置，返回默认值
            logger.warning("No user preference found, returning default")
            return UserSyncPreferenceResponse(
                user_id="default",
                index_basic_table="sync_index_basic"
            )

        row = df.to_dicts()[0]

        # 解析 filter_config JSON 字符串
        filter_config = None
        raw_filter = row.get("filter_config")
        if raw_filter:
            try:
                parsed = json.loads(raw_filter)
                filter_config = [FilterFieldConfig(**item) for item in parsed]
            except Exception:
                filter_config = None

        return UserSyncPreferenceResponse(
            user_id=row.get("user_id", "default"),
            index_basic_table=row.get("index_table", "sync_index_basic"),
            filter_config=filter_config
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get user preference: {e}")
        raise HTTPException(status_code=500, detail=f"获取用户偏好失败: {str(e)}")


@router.post("/data/index/preference", response_model=UserSyncPreferenceResponse)
async def save_user_preference(request: UserSyncPreference):
    """
    保存用户的同步偏好配置

    使用固定的 "default" 用户，因为没有用户系统
    """
    try:
        # 验证表名（基本的安全检查）
        index_table = request.index_basic_table.strip()
        if not index_table or len(index_table) > 100:
            raise HTTPException(status_code=400, detail="表名不能为空且长度不能超过100个字符")

        # 只允许字母、数字和下划线
        if not index_table.replace("_", "").isalnum():
            raise HTTPException(status_code=400, detail="表名只能包含字母、数字和下划线")

        now = datetime.now()

        # 序列化 filter_config
        filter_config_json = ""
        if request.filter_config is not None:
            filter_config_json = json.dumps(
                [f.model_dump() for f in request.filter_config], ensure_ascii=False
            )

        # 检查是否已存在配置
        check_df = db_client.query(
            "SELECT user_id FROM user_sync_preference WHERE user_id = %s",
            ("default",)
        )

        if check_df.is_empty():
            # 插入新配置
            data = {
                "user_id": "default",
                "index_table": index_table,
                "filter_config": filter_config_json or "",
                "created_at": now,
                "updated_at": now,
            }
            db_client.upsert("user_sync_preference", pl.DataFrame([data]), ["user_id"])
            logger.info(f"Created user preference for default user: index_table={index_table}")
        else:
            # 更新现有配置 - 使用upsert代替UPDATE语句
            data = {
                "user_id": "default",
                "index_table": index_table,
                "filter_config": filter_config_json or "",
                "created_at": check_df.to_dicts()[0].get("created_at", now),
                "updated_at": now,
            }
            db_client.upsert("user_sync_preference", pl.DataFrame([data]), ["user_id"])
            logger.info(f"Updated user preference for default user: index_table={index_table}")

        return UserSyncPreferenceResponse(
            user_id="default",
            index_basic_table=index_table,
            filter_config=request.filter_config
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save user preference: {e}")
        raise HTTPException(status_code=500, detail=f"保存用户偏好失败: {str(e)}")
