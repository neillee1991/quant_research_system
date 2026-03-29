# 股票池模块最终设计方案

**日期**: 2026-03-28
**版本**: v2.0（整合专家评审意见）
**状态**: 待审核

---

## 一、设计原则

基于四位专家的评审意见，确定以下核心设计原则：

1. **复用优先，扩展为辅** - 深度融合现有同步任务系统，不重建
2. **明确职责边界** - 原始层、业务层、查询层职责清晰
3. **状态机完备** - 完整的状态流转和错误处理
4. **性能可扩展** - 为大数据量做好准备
5. **用户体验优先** - API友好，反馈及时

---

## 二、数据库表设计（最终版）

### 核心表

#### 表1: `stock_pool_metadata`（股票池元数据表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `pool_id` | SYMBOL | 股票池ID（主键） |
| `pool_type` | SYMBOL | static/dynamic/index/composite |
| `pool_name` | STRING | 名称 |
| `description` | STRING | 描述 |
| `status` | SYMBOL | draft/active/paused/error/archived |
| `version` | INT | 版本号 |
| `weight_method` | SYMBOL | equal/market_cap/custom/index_native |
| `definition` | STRING | JSON定义 |
| `created_at` | TIMESTAMP | 创建时间 |
| `updated_at` | TIMESTAMP | 更新时间 |
| `created_by` | SYMBOL | 创建人 |
| `updated_by` | SYMBOL | 更新人 |

---

#### 表2: `stock_pool_sync_task`（股票池同步任务关联表）- 新增

| 字段 | 类型 | 说明 |
|------|------|------|
| `pool_id` | SYMBOL | 股票池ID（主键） |
| `task_id` | SYMBOL | 关联的同步任务ID |
| `sync_status` | SYMBOL | idle/syncing/success/failed |
| `last_sync_date` | DATE | 最后同步日期 |
| `last_sync_time` | TIMESTAMP | 最后同步时间 |
| `error_message` | STRING | 错误信息 |
| `created_at` | TIMESTAMP | 创建时间 |
| `updated_at` | TIMESTAMP | 更新时间 |

---

#### 表3: `stock_pool_constituents`（股票池成分股表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `pool_id` | SYMBOL | 股票池ID |
| `trade_date` | DATE | 交易日 |
| `ts_code` | SYMBOL | 股票代码 |
| `weight` | DOUBLE | 权重 |
| `rank` | INT | 排序（可选） |
| `inclusion_reason` | STRING | 纳入原因（可选） |
| `created_at` | TIMESTAMP | 创建时间 |

**分区策略**: HASH(pool_id, 32) + RANGE(trade_date, MONTHLY) + HASH(ts_code, 10)

---

#### 表4: `stock_pool_latest`（股票池最新状态表）- 新增查询优化表

| 字段 | 类型 | 说明 |
|------|------|------|
| `pool_id` | SYMBOL | 股票池ID |
| `ts_code` | SYMBOL | 股票代码 |
| `weight` | DOUBLE | 权重 |
| `as_of_date` | DATE | 生效日期 |
| `updated_at` | TIMESTAMP | 更新时间 |

**主键**: (`pool_id`, `ts_code`)

---

#### 表5: `stock_pool_event`（股票池事件日志表）- 新增事件溯源

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_id` | SYMBOL | 事件ID |
| `pool_id` | SYMBOL | 股票池ID |
| `event_type` | SYMBOL | CREATED/UPDATED/SYNC_STARTED/SYNC_COMPLETED等 |
| `event_data` | STRING | JSON格式事件数据 |
| `occurred_at` | TIMESTAMP | 发生时间 |
| `operator` | SYMBOL | 操作者 |

**分区策略**: RANGE(occurred_at, MONTHLY)

---

#### 表6: `sync_index_basic`（指数基础信息表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `ts_code` | SYMBOL | 指数代码（主键） |
| `name` | STRING | 指数名称 |
| `market` | SYMBOL | 市场: SSE/SZSE/CICC |
| `publisher` | STRING | 发布机构 |
| `list_date` | DATE | 发布日期 |
| `weight_rule` | STRING | 加权规则 |
| `desc` | STRING | 描述 |
| `exp_date` | DATE | 终止日期 |
| `updated_at` | TIMESTAMP | 更新时间 |

---

#### 表7: `sync_index_weight`（统一指数权重表）- 避免表膨胀

| 字段 | 类型 | 说明 |
|------|------|------|
| `index_code` | SYMBOL | 指数代码 |
| `ts_code` | SYMBOL | 股票代码 |
| `trade_date` | DATE | 交易日 |
| `weight` | DOUBLE | 权重 |
| `created_at` | TIMESTAMP | 创建时间 |

**主键**: (`index_code`, `ts_code`, `trade_date`)
**分区策略**: HASH(index_code, 32) + RANGE(trade_date, QUARTERLY)

---

### 保留现有表（复用）

- `sync_task_config` - 同步任务配置（扩展使用）
- `sync_log` / `sync_log_history` - 同步日志
- `stock_pool_version` - 股票池版本历史（优化字段）

---

## 三、状态机设计（最终版）

```python
from enum import Enum

class PoolStatus(Enum):
    DRAFT = "draft"           # 草稿状态
    ACTIVE = "active"         # 正常使用中
    PAUSED = "paused"         # 已暂停（仅 index 类型）
    ERROR = "error"           # 错误状态
    ARCHIVED = "archived"     # 已归档（软删除）

class SyncStatus(Enum):
    IDLE = "idle"             # 空闲
    SYNCING = "syncing"       # 同步中
    SUCCESS = "success"       # 同步成功
    FAILED = "failed"         # 同步失败

# 状态流转图
ALLOWED_TRANSITIONS = {
    PoolStatus.DRAFT: {PoolStatus.ACTIVE, PoolStatus.ARCHIVED},
    PoolStatus.ACTIVE: {PoolStatus.PAUSED, PoolStatus.ERROR, PoolStatus.ARCHIVED},
    PoolStatus.PAUSED: {PoolStatus.ACTIVE, PoolStatus.ARCHIVED},
    PoolStatus.ERROR: {PoolStatus.ACTIVE, PoolStatus.PAUSED, PoolStatus.ARCHIVED},
    PoolStatus.ARCHIVED: {PoolStatus.ACTIVE},
}
```

---

## 四、与现有同步任务系统的融合方案

### 方案概述

**不重建，而是扩展现有同步系统**，实现深度融合：

1. **扩展同步任务配置** - 添加指数权重同步任务模板
2. **动态生成任务** - 订阅指数时动态生成任务配置
3. **Post-Sync Hook** - 同步完成后自动填充股票池表
4. **Prefect Flow 协调** - 在现有 flow 中嵌入股票池同步

---

### 1. 新增同步任务配置

在 `config/seed_data/sync_tasks.json` 中添加：

```json
{
  "task_id": "sync_index_basic",
  "api_name": "index_basic",
  "description": "指数基础信息列表",
  "sync_type": "full",
  "date_field": "",
  "table_name": "sync_index_basic",
  "params": {
    "market": "",
    "publisher": "",
    "fields": "ts_code,name,market,publisher,list_date,weight_rule,desc,exp_date"
  },
  "primary_keys": ["ts_code"],
  "api_limit": 5000,
  "schema": {
    "ts_code": {"type": "SYMBOL"},
    "name": {"type": "STRING"},
    "market": {"type": "SYMBOL"},
    "publisher": {"type": "STRING"},
    "list_date": {"type": "DATE"},
    "weight_rule": {"type": "STRING"},
    "desc": {"type": "STRING"},
    "exp_date": {"type": "DATE"},
    "updated_at": {"type": "TIMESTAMP"}
  }
},
{
  "task_id": "sync_index_weight_template",
  "api_name": "index_weight",
  "description": "指数成分股权重（模板，订阅时动态生成实际任务）",
  "sync_type": "incremental",
  "date_field": "trade_date",
  "table_name": "sync_index_weight",
  "params": {
    "index_code": "$$INDEX_CODE$$",
    "trade_date": "{date}",
    "fields": "index_code,con_code,trade_date,weight"
  },
  "primary_keys": ["index_code", "con_code", "trade_date"],
  "api_limit": 5000,
  "schema": {
    "index_code": {"type": "SYMBOL"},
    "con_code": {"type": "SYMBOL"},
    "trade_date": {"type": "DATE"},
    "weight": {"type": "DOUBLE"}
  },
  "_is_template": true
}
```

---

### 2. 动态创建同步任务

```python
# stock_pool/services/pool_service.py

def _create_index_sync_task(self, index_code: str, pool_id: str) -> str:
    """
    从模板创建指数权重同步任务
    所有指数共享同一个 sync_index_weight 表，避免表膨胀
    """
    # 加载模板
    template = self._load_sync_task_template("sync_index_weight_template")

    # 替换变量
    task_id = f"sync_pool_{pool_id}"

    task_config = {
        **template,
        "task_id": task_id,
        "description": f"股票池 {pool_id} 指数成分股同步: {index_code}",
        "params": {
            **template["params"],
            "index_code": index_code
        },
        "enabled": True,
        "_metadata": {
            "pool_id": pool_id,
            "index_code": index_code,
            "task_type": "index_pool_sync"
        }
    }

    # 保存到 sync_task_config 表
    self.sync_engine.save_task_config(task_config)

    # 记录关联关系
    self._save_pool_sync_task_mapping(pool_id, task_id)

    return task_id
```

---

### 3. Post-Sync Hook 机制

```python
# 在同步任务完成后自动执行

class IndexPoolSyncPostHook:
    """指数股票池同步后钩子"""

    def after_sync(
        self,
        task_id: str,
        trade_date: str,
        synced_data: pl.DataFrame
    ) -> None:
        """同步完成后的处理"""
        # 从任务配置获取 pool_id
        task_config = self.sync_engine.get_task_config(task_id)
        metadata = task_config.get("_metadata", {})
        pool_id = metadata.get("pool_id")

        if not pool_id:
            return

        # 转换为统一格式
        constituents = synced_data.select([
            pl.lit(pool_id).alias("pool_id"),
            pl.col("trade_date"),
            pl.col("con_code").alias("ts_code"),
            (pl.col("weight") / 100.0).alias("weight"),  # Tushare 返回百分比
            pl.lit(None).cast(pl.Int32).alias("rank"),
            pl.lit(f"index_sync:{metadata.get('index_code')}").alias("inclusion_reason"),
            pl.lit(datetime.now()).alias("created_at")
        ])

        # 写入统一股票池表（幂等操作）
        with self.db_client.transaction():
            # 先删除该日期的旧数据
            self.db_client.execute("""
                DELETE FROM stock_pool_constituents
                WHERE pool_id = %s AND trade_date = %s
            """, (pool_id, trade_date))

            # 再写入新数据
            self.db_client.upsert(
                "stock_pool_constituents",
                constituents,
                ["pool_id", "trade_date", "ts_code"]
            )

            # 更新最新状态表
            self._update_latest_state(pool_id, constituents)

        # 失效缓存
        self.cache.invalidate_pool(pool_id)

        # 更新同步状态
        self._update_pool_sync_status(pool_id, "success", trade_date)
```

---

### 4. Prefect Flow 集成

```python
# flows/data_sync_flow.py - 修改现有 flow

@flow(name="daily-data-sync", log_prints=True)
def sync_daily_data(target_date: Optional[str] = None):
    """现有每日同步 flow，增加股票池步骤"""
    logger = get_run_logger()

    if target_date is None:
        target_date = datetime.now().strftime("%Y%m%d")

    # ========== 第一层：并行同步基础数据 ==========
    daily_future = sync_task.submit("sync_daily", target_date)
    daily_basic_future = sync_task.submit("sync_daily_basic", target_date)
    adj_factor_future = sync_task.submit("sync_adj_factor", target_date)
    # ... 其他基础数据

    # 等待基础数据完成
    daily_result = daily_future.result()
    daily_basic_result = daily_basic_future.result()
    adj_factor_future.result()

    # ========== 第二层：同步指数股票池 ==========
    if daily_result:
        sync_index_pools.submit(target_date)

    # ========== 第三层：计算因子 ==========
    # ... 现有因子计算逻辑

    logger.info("每日数据同步流水线完成")


@flow(name="sync-index-pools", log_prints=True)
def sync_index_pools(target_date: str):
    """同步所有活跃指数股票池"""
    from stock_pool.services.pool_service import StockPoolService
    from store.dolphindb_client import db_client

    logger = get_run_logger()

    # 获取所有活跃的指数股票池
    service = StockPoolService(db_client)
    active_pools = service.list_pools(pool_type="index", status="active")

    logger.info(f"发现 {len(active_pools)} 个活跃指数股票池")

    # 并行同步
    for pool in active_pools:
        sync_task.submit(pool.sync_task_id, target_date)
```

---

## 五、API 设计（最终版，前端友好）

### 统一响应格式

```typescript
interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
    detail?: string;
    suggestion?: string;
  };
  message?: string;
  meta?: {
    total?: number;
    page?: number;
    limit?: number;
    [key: string]: any;
  };
}
```

---

### 核心 API 端点

#### 1. 指数发现

```typescript
// 获取可订阅的指数列表
GET /api/v1/stock-pool/index/available
Query: {
  search?: string;
  page?: number;
  limit?: number;
  category?: string;
}

// 响应
interface AvailableIndex {
  tsCode: string;
  name: string;
  market: string;
  publisher: string;
  listDate: string;
  weightRule: string;
  isSubscribed: boolean;
  poolId?: string;
  poolStatus?: string;
}
```

---

#### 2. 订阅指数（创建 index 类型股票池）

```typescript
POST /api/v1/stock-pool/pools/index-subscribe
Body: {
  indexCode: string;
  poolName?: string;
  description?: string;
  subscriptionStartDate?: string;
  autoSync?: boolean;
  syncSchedule?: 'daily' | 'weekly';
}

// 响应
interface SubscribeResult {
  poolId: string;
  poolType: 'index';
  poolName: string;
  status: 'subscribing' | 'active';
  indexCode: string;
  syncTaskId: string;
  createdAt: string;
  estimatedTime?: number;
}
```

---

#### 3. 股票池列表（复用，支持筛选）

```typescript
GET /api/v1/stock-pool/pools
Query: {
  poolType?: 'static' | 'dynamic' | 'index' | 'composite';
  status?: 'draft' | 'active' | 'paused' | 'error' | 'archived';
  search?: string;
  page?: number;
  limit?: number;
}
```

---

#### 4. 股票池详情

```typescript
GET /api/v1/stock-pool/pools/{poolId}
Query: {
  includeConstituents?: boolean;
  tradeDate?: string;
  includeSyncStatus?: boolean;
}

// 响应
interface PoolDetail {
  metadata: PoolMetadata;
  constituents?: Constituent[];
  syncStatus?: {
    status: 'idle' | 'syncing' | 'success' | 'failed';
    lastSyncDate?: string;
    lastSyncTime?: string;
    error?: string;
  };
  availableDates?: string[];
}
```

---

#### 5. 暂停/恢复/手动同步

```typescript
POST /api/v1/stock-pool/pools/{poolId}/pause
POST /api/v1/stock-pool/pools/{poolId}/resume
POST /api/v1/stock-pool/pools/{poolId}/sync
Body: {
  startDate?: string;
  endDate?: string;
}
```

---

#### 6. 取消订阅（归档股票池）

```typescript
DELETE /api/v1/stock-pool/pools/{poolId}
```

---

## 六、核心业务逻辑（最终版）

### 订阅指数流程（事件溯源保证原子性）

```python
def subscribe_index(
    self,
    index_code: str,
    pool_name: str = None,
    description: str = None,
    subscription_start_date: str = None,
    auto_sync: bool = True,
    operator: str = "system"
) -> Dict:
    """
    订阅指数 = 创建股票池 + 创建同步任务 + 首次同步
    使用事件溯源保证原子性
    """
    # 1. 验证指数存在
    index_info = self._get_index_basic(index_code)
    if not index_info:
        raise ValueError(f"指数 {index_code} 不存在，请先运行 sync_index_basic 任务")

    # 2. 检查是否已订阅
    existing = self._get_existing_index_pool(index_code)
    if existing:
        raise ValueError(f"指数 {index_code} 已订阅，股票池ID: {existing['pool_id']}")

    pool_id = f"POOL_INDEX_{index_code.replace('.', '_')}"

    # 3. 开始事务 + 事件溯源
    with self.db_client.transaction():
        # 生成事件
        events = [
            StockPoolCreatedEvent(
                pool_id=pool_id,
                pool_type="index",
                pool_name=pool_name or f"{index_info['name']}指数成分股",
                description=description or f"{index_info['name']}指数成分股，自动每日同步",
                definition={
                    "index_code": index_code,
                    "index_name": index_info["name"],
                    "source": "tushare",
                    "subscription_start_date": subscription_start_date
                }
            ),
            SyncTaskCreatedEvent(
                pool_id=pool_id,
                index_code=index_code
            )
        ]

        # 持久化事件
        for event in events:
            self._persist_event(event, operator)

        # 应用事件到物化视图
        self._apply_events_to_materialized_view(events)

    # 4. 异步触发首次同步（不在事务内）
    if auto_sync:
        self._trigger_initial_sync.send(pool_id, subscription_start_date)

    return self.get_pool(pool_id)
```

---

## 七、缓存策略（最终版）

```python
# 多层缓存策略

class StockPoolCache:
    """股票池多层缓存"""

    def __init__(self):
        # L1: 内存缓存（热点数据，1分钟TTL）
        self._local_cache = {}

        # L2: Redis 缓存（全量数据，1小时TTL，可选）
        self._redis_client = None

    def get_constituents(self, pool_id: str, trade_date: str) -> Optional[pl.DataFrame]:
        """获取成分股"""
        cache_key = f"pool:{pool_id}:{trade_date}"

        # L1 缓存
        if cache_key in self._local_cache:
            expire_time, data = self._local_cache[cache_key]
            if datetime.now() < expire_time:
                return data

        # L2 缓存
        if self._redis_client:
            data = self._redis_get(cache_key)
            if data:
                # 回填 L1
                self._local_cache[cache_key] = (
                    datetime.now() + timedelta(minutes=1),
                    data
                )
                return data

        return None

    def invalidate_pool(self, pool_id: str) -> None:
        """失效某个股票池的所有缓存"""
        # 清除 L1
        keys_to_remove = [
            k for k in self._local_cache
            if k.startswith(f"pool:{pool_id}:")
        ]
        for k in keys_to_remove:
            del self._local_cache[k]

        # 清除 L2
        if self._redis_client:
            self._redis_delete_pattern(f"pool:{pool_id}:*")
```

---

## 八、实施计划

### Phase 1: 基础设施（1周）

1. 创建/修改数据库表
2. 添加 sync_index_basic 同步任务
3. 实现基础的股票池 CRUD
4. 实现事件日志基础框架

### Phase 2: 指数订阅（1周）

1. 实现指数发现和列表 API
2. 实现订阅指数流程（事件溯源）
3. 实现动态同步任务创建
4. 实现 Post-Sync Hook

### Phase 3: 同步集成（1周）

1. 扩展 Prefect Flow
2. 实现同步状态管理
3. 实现暂停/恢复/手动同步
4. 实现缓存策略

### Phase 4: 完整功能（2周）

1. 实现静态股票池
2. 实现动态条件选股
3. 实现股票池组合运算
4. 实现版本历史和对比
5. 与因子计算/回测集成

### Phase 5: 优化完善（1周）

1. 性能优化（分区、缓存、物化视图）
2. 错误处理和故障恢复
3. 前端友好的 API 完善
4. 文档和测试

---

## 九、关键文件清单

| 文件 | 说明 |
|------|------|
| `stock_pool/__init__.py` | 模块初始化 |
| `stock_pool/api/__init__.py` | API 路由聚合 |
| `stock_pool/api/pool_management.py` | 股票池 CRUD |
| `stock_pool/api/index_discovery.py` | 指数发现和订阅 |
| `stock_pool/api/pool_selection.py` | 动态选股 |
| `stock_pool/api/pool_version.py` | 版本历史 |
| `stock_pool/services/pool_service.py` | 核心服务 |
| `stock_pool/services/index_sync_service.py` | 指数同步服务 |
| `stock_pool/engine/selector.py` | 选股引擎 |
| `stock_pool/engine/composer.py` | 组合运算 |
| `stock_pool/models/pool.py` | Pydantic 模型 |
| `stock_pool/state_machine.py` | 状态机 |
| `stock_pool/cache.py` | 缓存管理 |
| `stock_pool/events.py` | 事件定义 |
| `app/api/v1/stock_pool.py` | 路由聚合 |
| `flows/stock_pool_sync.py` | Prefect Flow |
| `database/init_stock_pool.py` | 数据库初始化 |

---

## 十、风险与缓解措施

| 风险 | 影响 | 概率 | Mitigation |
|------|------|------|------------|
| 双重存储导致数据不一致 | 高 | 中 | 使用事件溯源+发件箱模式，明确主从关系 |
| 状态机设计不完善导致死锁 | 高 | 低 | 显式状态机验证，所有转换必须经过 check |
| 与现有同步任务冲突 | 中 | 中 | 使用命名空间（task_id 前缀 `pool_sync_`） |
| 缓存失效不及时 | 中 | 高 | 细粒度缓存失效，版本号+TTL双保险 |
| 大数据量性能问题 | 中 | 中 | 预分区+物化视图+分页查询 |

---

**文档结束**
