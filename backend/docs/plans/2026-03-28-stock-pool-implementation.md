# 股票池模块实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建完整的股票池模块，包括静态股票池、动态选股、指数订阅、版本管理、与因子计算/回测集成等功能。

**Architecture:** 基于现有系统架构，采用分层设计（API → Service → Repository → Database），深度融合现有同步任务系统，使用事件溯源保证数据一致性。

**Tech Stack:** FastAPI, DolphinDB, Polars, Prefect 3.x, Pydantic

---

## Phase 1: 基础设施（1周）

### Task 1.1: 创建数据库初始化脚本

**Files:**
- Create: `database/init_stock_pool_tables.py`
- Modify: `database/init_meta_tables.py`

**Step 1: Write the failing test**

```python
# tests/database/test_init_stock_pool.py
import pytest
from database.init_stock_pool_tables import init_stock_pool_tables
from store.dolphindb_client import db_client

def test_stock_pool_tables_exist():
    init_stock_pool_tables()
    tables = db_client.query("SHOW TABLES FROM dfs://quant")
    table_names = [t["name"] for t in tables.to_dicts()]
    assert "stock_pool_metadata" in table_names
    assert "stock_pool_constituents" in table_names
    assert "stock_pool_sync_task" in table_names
    assert "stock_pool_latest" in table_names
    assert "stock_pool_event" in table_names
    assert "sync_index_basic" in table_names
    assert "sync_index_weight" in table_names
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/database/test_init_stock_pool.py::test_stock_pool_tables_exist -v`
Expected: FAIL with "No module named 'database.init_stock_pool_tables'"

**Step 3: Write minimal implementation**

```python
# database/init_stock_pool_tables.py
#!/usr/bin/env python3
"""
初始化股票池模块数据库表
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from store.dolphindb_client import db_client
from app.core.logger import logger

def init_stock_pool_tables():
    """创建股票池模块所有表"""
    logger.info("开始创建股票池模块表...")

    # 1. stock_pool_metadata - 股票池元数据表
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS stock_pool_metadata (
            pool_id SYMBOL,
            pool_type SYMBOL,
            pool_name STRING,
            description STRING,
            status SYMBOL,
            version INT,
            weight_method SYMBOL,
            definition STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            created_by SYMBOL,
            updated_by SYMBOL,
            PRIMARY KEY (pool_id)
        )
    """)
    logger.info("✓ stock_pool_metadata 创建成功")

    # 2. stock_pool_sync_task - 股票池同步任务关联表
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS stock_pool_sync_task (
            pool_id SYMBOL,
            task_id SYMBOL,
            sync_status SYMBOL,
            last_sync_date DATE,
            last_sync_time TIMESTAMP,
            error_message STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY (pool_id)
        )
    """)
    logger.info("✓ stock_pool_sync_task 创建成功")

    # 3. stock_pool_constituents - 股票池成分股表（分区表）
    # 先创建维度表，再创建分区表
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS stock_pool_constituents (
            pool_id SYMBOL,
            trade_date DATE,
            ts_code SYMBOL,
            weight DOUBLE,
            rank INT,
            inclusion_reason STRING,
            created_at TIMESTAMP,
            PRIMARY KEY (pool_id, trade_date, ts_code)
        )
    """)
    logger.info("✓ stock_pool_constituents 维度表创建成功")

    # 4. stock_pool_latest - 股票池最新状态表
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS stock_pool_latest (
            pool_id SYMBOL,
            ts_code SYMBOL,
            weight DOUBLE,
            as_of_date DATE,
            updated_at TIMESTAMP,
            PRIMARY KEY (pool_id, ts_code)
        )
    """)
    logger.info("✓ stock_pool_latest 创建成功")

    # 5. stock_pool_event - 股票池事件日志表
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS stock_pool_event (
            event_id SYMBOL,
            pool_id SYMBOL,
            event_type SYMBOL,
            event_data STRING,
            occurred_at TIMESTAMP,
            operator SYMBOL,
            PRIMARY KEY (event_id, occurred_at)
        )
    """)
    logger.info("✓ stock_pool_event 创建成功")

    # 6. sync_index_basic - 指数基础信息表
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS sync_index_basic (
            ts_code SYMBOL,
            name STRING,
            market SYMBOL,
            publisher STRING,
            list_date DATE,
            weight_rule STRING,
            desc STRING,
            exp_date DATE,
            updated_at TIMESTAMP,
            PRIMARY KEY (ts_code)
        )
    """)
    logger.info("✓ sync_index_basic 创建成功")

    # 7. sync_index_weight - 统一指数权重表（分区表）
    db_client.execute("""
        CREATE TABLE IF NOT EXISTS sync_index_weight (
            index_code SYMBOL,
            ts_code SYMBOL,
            trade_date DATE,
            weight DOUBLE,
            created_at TIMESTAMP,
            PRIMARY KEY (index_code, ts_code, trade_date)
        )
    """)
    logger.info("✓ sync_index_weight 维度表创建成功")

    logger.info("股票池模块表创建完成！")

if __name__ == "__main__":
    try:
        init_stock_pool_tables()
    except Exception as e:
        logger.error(f"创建表失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
```

**Step 4: 修改 init_meta_tables.py**

```python
# database/init_meta_tables.py
# 在 main() 函数中添加股票池表初始化
def main():
    print("开始初始化元数据表...")
    try:
        # 创建所有元数据表
        db_client.ensure_meta_tables()
        print("✓ 元数据表创建成功")

        # 初始化股票池表（新增）
        from database.init_stock_pool_tables import init_stock_pool_tables
        init_stock_pool_tables()
        print("✓ 股票池模块表创建成功")

        # 写入默认同步任务配置
        db_client.seed_sync_task_config()
        print("✓ 同步任务配置种子数据已写入")

        # 写入默认 ETL 任务配置
        db_client.seed_etl_task_config()
        print("✓ ETL 任务配置种子数据已写入")

        # 写入因子数据配置
        db_client.seed_factor_data_config()
        print("✓ 因子数据配置种子数据已写入")

        # 写入默认种子因子定义
        db_client.seed_factor_metadata()
        print("✓ 种子因子定义已写入")

        print("\n所有元数据表初始化完成！")

    except Exception as e:
        print(f"✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
```

**Step 5: Run test to verify it passes**

Run: `pytest tests/database/test_init_stock_pool.py::test_stock_pool_tables_exist -v`
Expected: PASS

**Step 6: Commit**

```bash
git add database/init_stock_pool_tables.py database/init_meta_tables.py tests/database/test_init_stock_pool.py
git commit -m "feat: add stock pool database tables initialization"
```

---

### Task 1.2: 添加 sync_index_basic 同步任务配置

**Files:**
- Modify: `config/seed_data/sync_tasks.json`

**Step 1: Write the failing test**

```python
# tests/config/test_sync_tasks.py
import json
from pathlib import Path

def test_sync_index_basic_task_exists():
    config_path = Path(__file__).parent.parent / "config/seed_data/sync_tasks.json"
    with open(config_path, "r", encoding="utf-8") as f:
        tasks = json.load(f)

    task_ids = [t["task_id"] for t in tasks]
    assert "sync_index_basic" in task_ids
    assert "sync_index_weight_template" in task_ids
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/config/test_sync_tasks.py::test_sync_index_basic_task_exists -v`
Expected: FAIL

**Step 3: Write minimal implementation**

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

**Step 4: Run test to verify it passes**

Run: `pytest tests/config/test_sync_tasks.py::test_sync_index_basic_task_exists -v`
Expected: PASS

**Step 5: Commit**

```bash
git add config/seed_data/sync_tasks.json tests/config/test_sync_tasks.py
git commit -m "feat: add index basic and weight template sync tasks"
```

---

### Task 1.3: 创建股票池 Pydantic 模型

**Files:**
- Create: `stock_pool/models/pool.py`
- Create: `stock_pool/models/__init__.py`

**Step 1: Write the failing test**

```python
# tests/stock_pool/test_models.py
from stock_pool.models.pool import (
    PoolType, PoolStatus, SyncStatus,
    PoolCreateRequest, PoolUpdateRequest,
    ConstituentItem, IndexSubscribeRequest
)

def test_pool_type_enum():
    assert PoolType.STATIC == "static"
    assert PoolType.DYNAMIC == "dynamic"
    assert PoolType.INDEX == "index"
    assert PoolType.COMPOSITE == "composite"

def test_pool_status_enum():
    assert PoolStatus.DRAFT == "draft"
    assert PoolStatus.ACTIVE == "active"
    assert PoolStatus.PAUSED == "paused"
    assert PoolStatus.ERROR == "error"
    assert PoolStatus.ARCHIVED == "archived"

def test_pool_create_request_validation():
    req = PoolCreateRequest(
        pool_id="POOL_TEST_001",
        pool_type=PoolType.STATIC,
        pool_name="测试股票池"
    )
    assert req.pool_id == "POOL_TEST_001"
    assert req.pool_name == "测试股票池"

def test_index_subscribe_request():
    req = IndexSubscribeRequest(
        index_code="000300.SH",
        pool_name="沪深300指数成分股",
        auto_sync=True
    )
    assert req.index_code == "000300.SH"
    assert req.auto_sync is True
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/stock_pool/test_models.py -v`
Expected: FAIL with "No module named 'stock_pool.models'"

**Step 3: Write minimal implementation**

```python
# stock_pool/models/pool.py
from enum import Enum
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, date


class PoolType(str, Enum):
    """股票池类型"""
    STATIC = "static"
    DYNAMIC = "dynamic"
    INDEX = "index"
    COMPOSITE = "composite"


class PoolStatus(str, Enum):
    """股票池状态"""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"
    ARCHIVED = "archived"


class SyncStatus(str, Enum):
    """同步状态"""
    IDLE = "idle"
    SYNCING = "syncing"
    SUCCESS = "success"
    FAILED = "failed"


class WeightMethod(str, Enum):
    """权重方式"""
    EQUAL = "equal"
    MARKET_CAP = "market_cap"
    CUSTOM = "custom"
    INDEX_NATIVE = "index_native"


# ========== 请求/响应模型 ==========

class ConstituentItem(BaseModel):
    """成分股项"""
    trade_date: str
    ts_code: str
    weight: Optional[float] = 0.0
    rank: Optional[int] = None
    inclusion_reason: Optional[str] = None


class PoolCreateRequest(BaseModel):
    """创建股票池请求"""
    pool_id: str = Field(..., description="股票池ID")
    pool_type: PoolType = Field(..., description="股票池类型")
    pool_name: str = Field(..., description="股票池名称")
    description: Optional[str] = ""
    weight_method: Optional[WeightMethod] = WeightMethod.EQUAL
    definition: Optional[Dict[str, Any]] = None
    constituents: Optional[List[ConstituentItem]] = None


class PoolUpdateRequest(BaseModel):
    """更新股票池请求"""
    pool_name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[PoolStatus] = None
    weight_method: Optional[WeightMethod] = None
    definition: Optional[Dict[str, Any]] = None


class IndexSubscribeRequest(BaseModel):
    """订阅指数请求"""
    index_code: str = Field(..., description="指数代码，如 000300.SH")
    pool_name: Optional[str] = Field(None, description="股票池名称，默认用指数名称")
    description: Optional[str] = ""
    subscription_start_date: Optional[str] = Field(None, description="订阅开始日期，YYYYMMDD")
    auto_sync: bool = Field(True, description="是否自动同步")
    sync_schedule: Optional[str] = Field("daily", description="同步频率: daily/weekly")


class AvailableIndex(BaseModel):
    """可用指数信息"""
    ts_code: str
    name: str
    market: Optional[str] = None
    publisher: Optional[str] = None
    list_date: Optional[str] = None
    weight_rule: Optional[str] = None
    is_subscribed: bool = False
    pool_id: Optional[str] = None
    pool_status: Optional[str] = None


class PoolMetadata(BaseModel):
    """股票池元数据"""
    pool_id: str
    pool_type: PoolType
    pool_name: str
    description: str
    status: PoolStatus
    version: int
    weight_method: WeightMethod
    definition: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


class PoolDetail(BaseModel):
    """股票池详情"""
    metadata: PoolMetadata
    constituents: Optional[List[ConstituentItem]] = None
    sync_status: Optional[Dict[str, Any]] = None
    available_dates: Optional[List[str]] = None


class SubscribeResult(BaseModel):
    """订阅指数结果"""
    pool_id: str
    pool_type: PoolType
    pool_name: str
    status: PoolStatus
    index_code: str
    sync_task_id: Optional[str] = None
    created_at: datetime
    estimated_time: Optional[int] = None


# ========== API 响应模型 ==========

class ApiResponse(BaseModel):
    """统一API响应"""
    success: bool
    data: Optional[Any] = None
    error: Optional[Dict[str, str]] = None
    message: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class ListResponse(BaseModel):
    """列表响应"""
    items: List[Any]
    total: int
    page: int
    limit: int
    has_more: bool
```

```python
# stock_pool/models/__init__.py
from .pool import (
    PoolType,
    PoolStatus,
    SyncStatus,
    WeightMethod,
    ConstituentItem,
    PoolCreateRequest,
    PoolUpdateRequest,
    IndexSubscribeRequest,
    AvailableIndex,
    PoolMetadata,
    PoolDetail,
    SubscribeResult,
    ApiResponse,
    ListResponse,
)

__all__ = [
    "PoolType",
    "PoolStatus",
    "SyncStatus",
    "WeightMethod",
    "ConstituentItem",
    "PoolCreateRequest",
    "PoolUpdateRequest",
    "IndexSubscribeRequest",
    "AvailableIndex",
    "PoolMetadata",
    "PoolDetail",
    "SubscribeResult",
    "ApiResponse",
    "ListResponse",
]
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/stock_pool/test_models.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add stock_pool/models/pool.py stock_pool/models/__init__.py tests/stock_pool/test_models.py
git commit -m "feat: add stock pool Pydantic models"
```

---

### Task 1.4: 创建状态机和事件定义

**Files:**
- Create: `stock_pool/state_machine.py`
- Create: `stock_pool/events.py`

**Step 1: Write the failing test**

```python
# tests/stock_pool/test_state_machine.py
from stock_pool.state_machine import StockPoolStateMachine, PoolStatus

def test_valid_state_transitions():
    sm = StockPoolStateMachine()

    # DRAFT -> ACTIVE
    assert sm.can_transition(PoolStatus.DRAFT, PoolStatus.ACTIVE) is True
    # DRAFT -> ARCHIVED
    assert sm.can_transition(PoolStatus.DRAFT, PoolStatus.ARCHIVED) is True
    # DRAFT -> PAUSED (invalid)
    assert sm.can_transition(PoolStatus.DRAFT, PoolStatus.PAUSED) is False

    # ACTIVE -> PAUSED
    assert sm.can_transition(PoolStatus.ACTIVE, PoolStatus.PAUSED) is True
    # ACTIVE -> ERROR
    assert sm.can_transition(PoolStatus.ACTIVE, PoolStatus.ERROR) is True
    # ACTIVE -> ARCHIVED
    assert sm.can_transition(PoolStatus.ACTIVE, PoolStatus.ARCHIVED) is True

    # PAUSED -> ACTIVE
    assert sm.can_transition(PoolStatus.PAUSED, PoolStatus.ACTIVE) is True
    # PAUSED -> ARCHIVED
    assert sm.can_transition(PoolStatus.PAUSED, PoolStatus.ARCHIVED) is True

    # ERROR -> ACTIVE
    assert sm.can_transition(PoolStatus.ERROR, PoolStatus.ACTIVE) is True
    # ERROR -> PAUSED
    assert sm.can_transition(PoolStatus.ERROR, PoolStatus.PAUSED) is True
    # ERROR -> ARCHIVED
    assert sm.can_transition(PoolStatus.ERROR, PoolStatus.ARCHIVED) is True

    # ARCHIVED -> ACTIVE
    assert sm.can_transition(PoolStatus.ARCHIVED, PoolStatus.ACTIVE) is True
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/stock_pool/test_state_machine.py -v`
Expected: FAIL

**Step 3: Write minimal implementation**

```python
# stock_pool/state_machine.py
from typing import Dict, Set
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class PoolStatus(str, Enum):
    """股票池状态"""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"
    ARCHIVED = "archived"


class SyncStatus(str, Enum):
    """同步状态"""
    IDLE = "idle"
    SYNCING = "syncing"
    SUCCESS = "success"
    FAILED = "failed"


class StockPoolStateMachine:
    """股票池状态机"""

    # 允许的状态转换
    ALLOWED_TRANSITIONS: Dict[PoolStatus, Set[PoolStatus]] = {
        PoolStatus.DRAFT: {
            PoolStatus.ACTIVE,
            PoolStatus.ARCHIVED,
        },
        PoolStatus.ACTIVE: {
            PoolStatus.PAUSED,
            PoolStatus.ERROR,
            PoolStatus.ARCHIVED,
        },
        PoolStatus.PAUSED: {
            PoolStatus.ACTIVE,
            PoolStatus.ARCHIVED,
        },
        PoolStatus.ERROR: {
            PoolStatus.ACTIVE,
            PoolStatus.PAUSED,
            PoolStatus.ARCHIVED,
        },
        PoolStatus.ARCHIVED: {
            PoolStatus.ACTIVE,
        },
    }

    @classmethod
    def can_transition(cls, from_status: PoolStatus, to_status: PoolStatus) -> bool:
        """验证状态转换是否合法"""
        return to_status in cls.ALLOWED_TRANSITIONS.get(from_status, set())

    @classmethod
    def get_allowed_transitions(cls, status: PoolStatus) -> Set[PoolStatus]:
        """获取当前状态允许的转换"""
        return cls.ALLOWED_TRANSITIONS.get(status, set())
```

```python
# stock_pool/events.py
from typing import Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import json


@dataclass
class StockPoolEvent:
    """股票池事件基类"""
    event_id: str
    pool_id: str
    event_type: str
    event_data: Dict[str, Any]
    occurred_at: datetime
    operator: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "pool_id": self.pool_id,
            "event_type": self.event_type,
            "event_data": json.dumps(self.event_data, ensure_ascii=False),
            "occurred_at": self.occurred_at,
            "operator": self.operator,
        }


@dataclass
class StockPoolCreatedEvent(StockPoolEvent):
    """股票池创建事件"""
    def __init__(self, pool_id: str, pool_type: str, pool_name: str,
                 description: str, definition: Dict[str, Any],
                 operator: str = "system"):
        import uuid
        super().__init__(
            event_id=f"EVT_{uuid.uuid4().hex[:8]}",
            pool_id=pool_id,
            event_type="POOL_CREATED",
            event_data={
                "pool_type": pool_type,
                "pool_name": pool_name,
                "description": description,
                "definition": definition,
            },
            occurred_at=datetime.now(),
            operator=operator,
        )


@dataclass
class StockPoolUpdatedEvent(StockPoolEvent):
    """股票池更新事件"""
    def __init__(self, pool_id: str, updates: Dict[str, Any],
                 operator: str = "system"):
        import uuid
        super().__init__(
            event_id=f"EVT_{uuid.uuid4().hex[:8]}",
            pool_id=pool_id,
            event_type="POOL_UPDATED",
            event_data={"updates": updates},
            occurred_at=datetime.now(),
            operator=operator,
        )


@dataclass
class SyncTaskCreatedEvent(StockPoolEvent):
    """同步任务创建事件"""
    def __init__(self, pool_id: str, index_code: str,
                 operator: str = "system"):
        import uuid
        super().__init__(
            event_id=f"EVT_{uuid.uuid4().hex[:8]}",
            pool_id=pool_id,
            event_type="SYNC_TASK_CREATED",
            event_data={"index_code": index_code},
            occurred_at=datetime.now(),
            operator=operator,
        )


@dataclass
class SyncStartedEvent(StockPoolEvent):
    """同步开始事件"""
    def __init__(self, pool_id: str, trade_date: str,
                 operator: str = "system"):
        import uuid
        super().__init__(
            event_id=f"EVT_{uuid.uuid4().hex[:8]}",
            pool_id=pool_id,
            event_type="SYNC_STARTED",
            event_data={"trade_date": trade_date},
            occurred_at=datetime.now(),
            operator=operator,
        )


@dataclass
class SyncCompletedEvent(StockPoolEvent):
    """同步完成事件"""
    def __init__(self, pool_id: str, trade_date: str,
                 stock_count: int, operator: str = "system"):
        import uuid
        super().__init__(
            event_id=f"EVT_{uuid.uuid4().hex[:8]}",
            pool_id=pool_id,
            event_type="SYNC_COMPLETED",
            event_data={"trade_date": trade_date, "stock_count": stock_count},
            occurred_at=datetime.now(),
            operator=operator,
        )
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/stock_pool/test_state_machine.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add stock_pool/state_machine.py stock_pool/events.py tests/stock_pool/test_state_machine.py
git commit -m "feat: add state machine and event definitions"
```

---

## Phase 1 总结

Phase 1 完成了股票池模块的基础设施建设：
- ✅ 数据库表创建脚本
- ✅ 同步任务配置
- ✅ Pydantic 数据模型
- ✅ 状态机和事件定义

---

**Note:** 本实施计划采用 TDD 方式，每个任务都是独立的、可测试的小步骤。由于篇幅限制，此处仅展示 Phase 1 的详细任务。完整的 5 阶段计划请参考 `docs/plans/2026-03-28-stock-pool-final-design.md` 中的实施计划章节。

---

Plan complete and saved to `docs/plans/2026-03-28-stock-pool-implementation.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
