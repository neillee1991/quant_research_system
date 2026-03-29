# 股票池模块设计评审报告

**评审日期**: 2026-03-28
**评审人**: 量化研究团队
**项目**: QuantResearchSystem

---

## 一、评审概览

本文档从量化研究实际使用角度，评审融合指数订阅功能的完整股票池模块设计。

### 当前项目基础架构：
- 数据层：DolphinDB (时间序列数据库)
- 计算层：Polars (向量化计算)
- 编排层：Prefect 3.x
- API层：FastAPI

---

## 二、核心功能评审

### 1. 指数快速订阅（沪深300、中证500等）

#### ✅ 优点
- 直接对接Tushare已有指数成分股数据接口
- 现有行业分类体系完善（申万+中信）
- 已有的ETL框架可复用

#### ⚠️ 潜在问题与建议

**问题1：指数成分股时效性**
```
现状：项目目前只有行业分类，缺少指数成分股数据表
风险：
- 无法获取沪深300、中证500等主流指数的历史成分
- 无法处理指数调仓的历史回溯
- 回测时存在 survivorship bias（生存者偏差）
```

**建议方案**：
```python
# 新增指数成分股同步任务
# 需要的数据表结构：
index_member (
    index_code: SYMBOL,      # 指数代码 (000300.SH, 000905.SH)
    index_name: STRING,         # 指数名称
    ts_code: SYMBOL,         # 股票代码
    trade_date: DATE,          # 交易日期
    weight: DOUBLE,           # 权重（如果有）
    in_date: DATE,           # 纳入日期
    out_date: DATE,          # 剔除日期
    is_new: STRING           # 是否新版
)

# 分区策略：
# 一级分区：HASH(index_code, 10)
# 二级分区：RANGE(trade_date, 季度)
```

**问题2：指数权重处理**
```
现状：没有指数成分股权重数据缺失
风险：
- 无法精确复制指数表现
- 无法进行指数增强策略研究
```

**建议**：
- 支持多种权重模式：指数原生权重、等权、市值加权、自由流通市值加权
- 提供权重再平衡频率配置（日、周、月、季、年）
- 支持自定义权重计算函数

---

### 2. 静态股票池管理

#### ✅ 优点
- 版本历史和回滚功能设计良好
- 支持股票池组合运算

#### ⚠️ 建议增强

**数据表设计建议**：
```sql
-- 股票池定义表
universes (
    universe_id: SYMBOL,      PK
    universe_name: STRING,
    universe_type: STRING,     -- static, dynamic, index, composite
    description: STRING,
    created_at: TIMESTAMP,
    created_by: STRING,
    is_active: BOOLEAN
)

-- 股票池版本表
universe_versions (
    version_id: SYMBOL,       PK
    universe_id: SYMBOL,      FK
    version_number: INT,
    version_date: DATE,
    change_reason: STRING,
    created_at: TIMESTAMP
)

-- 股票池成分表（每个版本的具体成分）
universe_components (
    version_id: SYMBOL,       PK
    ts_code: SYMBOL,          PK
    weight: DOUBLE,           -- 权重（可选）
    include_reason: STRING    -- 纳入原因
)
```

---

### 3. 动态条件选股

#### ✅ 优点
- 现有因子引擎可以直接复用
- Polars向量化计算支持高效的条件筛选

#### ⚠️ 设计建议

**动态选股条件DSL**：
```python
# 建议的选股条件表达式
conditions = [
    # 基础条件
    {"field": "pe_ttm", "operator": "<", "value": 30},
    {"field": "pb", "operator": "<", "value": 3},
    {"field": "total_mv", "operator": ">", "value": 10000000000},  # 100亿

    # 技术条件
    {"field": "close", "operator": ">", "value": "ma_20"},

    # 复合条件
    {"logic": "OR", "conditions": [
        {"field": "industry", "operator": "in", "value": ["银行", "非银金融"]},
        {"field": "pct_chg_5d", "operator": ">", "value": 0}
    ]}
]

# 选股执行流程：
# 1. 加载基础数据 + 因子数据
# 2. 应用条件过滤
# 3. 排序（可选）
# 4. 选择Top N
# 5. 保存结果到股票池
```

---

### 4. 股票池组合运算（交/并/差）

#### ✅ 优点
- 组合运算功能完整
- 版本历史支持审计追踪

#### ⚠️ 建议增强

**运算类型建议**：
```python
class UniverseOperation:
    UNION = "union"              # 并集
    INTERSECTION = "intersection"  # 交集
    DIFFERENCE = "difference"    # 差集
    SYMMETRIC_DIFFERENCE = "symmetric_difference"  # 对称差
    WEIGHTED_COMBINE = "weighted_combine"  # 加权合并
```

**运算示例**：
```python
# 沪深300 + 中证500 = 沪深800
universe_800 = combine_universes(
    [universe_hs300, universe_zz500],
    operation="union"
)

# 沪深300 - ST股票
universe_hs300_clean = combine_universes(
    [universe_hs300, universe_st],
    operation="difference"
)

# 多策略股票池加权合并
universe_combined = combine_universes(
    [universe_strategy1, universe_strategy2],
    operation="weighted_combine",
    weights=[0.6, 0.4]
)
```

---

### 5. 版本历史和回滚

#### ✅ 优点
- 版本管理设计完善
- 支持审计追踪

#### ⚠️ 建议增强

**版本元数据建议**：
```python
# 每个版本应该记录：
- version_id: 唯一标识
- version_number: 版本号
- version_date: 版本日期
- change_type: 创建/添加/删除/修改/合并/回滚
- change_reason: 变更原因
- created_by: 创建人
- parent_version: 父版本（用于回滚追踪）
- snapshot_data: 完整成分快照（用于快速恢复）
```

---

### 6. 与因子计算、策略回测的集成

#### ✅ 优点
- 现有因子引擎架构良好
- 回测引擎已准备就绪

#### ⚠️ 集成设计建议

**因子计算集成**：
```python
# 因子计算时可以指定股票池
factor_engine.compute_factor(
    factor_id="momentum_20d",
    universe_id="universe_hs300",  # 只在沪深300成分股中计算
    start_date="20200101",
    end_date="20251231"
)

# 或者动态选股时可以使用已计算的因子
universe = create_dynamic_universe(
    name="低估值",
    conditions=[
        {"factor": "pe_ttm", "operator": "<", "value": 30, "percentile": 0.3},
        {"factor": "momentum_20d", "operator": ">", "value": 0}
    ],
    base_universe="universe_hs300"
)
```

**回测集成**：
```python
# 回测时指定股票池
backtest_engine.run_backtest(
    strategy="my_strategy",
    universe_id="universe_hs300",  # 在沪深300内回测
    start_date="20200101",
    end_date="20251231",
    initial_capital=10000000
)

# 或者使用动态股票池（随时间变化）
backtest_engine.run_backtest(
    strategy="my_strategy",
    universe_id="universe_dynamic_low_valuation",  # 动态更新
    universe_rebalance_freq="monthly",  # 每月重新选股
    start_date="20200101",
    end_date="20251231"
)
```

---

## 三、关键技术架构设计

### 3.1 数据库表设计完整设计

```sql
-- ============================================
-- 指数基础信息表
-- ============================================
index_basic (
    index_code: SYMBOL,      PK
    index_name: STRING,
    market: STRING,          # SSE, SZSE, CSI
    publisher: STRING,           # 中证指数公司, 上交所, 深交所
    list_date: DATE,
    description: STRING,
    base_date: DATE,
    base_point: DOUBLE,
    is_active: BOOLEAN
)

-- ============================================
-- 指数成分股历史表（支持历史回溯）
-- ============================================
index_member_history (
    index_code: SYMBOL,      PK
    ts_code: SYMBOL,         PK
    trade_date: DATE,          PK
    weight: DOUBLE,           # 指数权重
    in_date: DATE,
    out_date: DATE,
    is_current: BOOLEAN
)

-- ============================================
-- 股票池定义表
-- ============================================
universes (
    universe_id: SYMBOL,      PK
    universe_name: STRING,
    universe_type: STRING,     # static, dynamic, index, composite
    description: STRING,
    config: STRING,          # JSON配置（动态选股条件等）
    created_at: TIMESTAMP,
    created_by: STRING,
    updated_at: TIMESTAMP,
    is_active: BOOLEAN,
    tags: STRING           # JSON数组
)

-- ============================================
-- 股票池版本表
-- ============================================
universe_versions (
    version_id: SYMBOL,       PK
    universe_id: SYMBOL,      FK
    version_number: INT,
    version_date: DATE,
    change_type: STRING,     # create, add, remove, update, rollback
    change_reason: STRING,
    parent_version: SYMBOL,
    created_at: TIMESTAMP,
    created_by: STRING,
    snapshot_hash: STRING    # 成分快照哈希值
)

-- ============================================
-- 股票池成分表
-- ============================================
universe_components (
    version_id: SYMBOL,       PK
    ts_code: SYMBOL,          PK
    weight: DOUBLE,
    include_reason: STRING,
    source_universe: SYMBOL    # 来源股票池（组合运算时）
)

-- ============================================
-- 股票池运算日志表
-- ============================================
universe_operations (
    operation_id: SYMBOL,     PK
    operation_type: STRING,
    result_universe: SYMBOL,
    operand_universes: STRING,  # JSON数组
    operation_config: STRING,     # JSON配置
    created_at: TIMESTAMP,
    created_by: STRING
)
```

### 3.2 API 接口设计

```python
# 股票池模块API设计
# 文件路径: app/api/v1/universe.py

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import date
from enum import Enum

router = APIRouter()

class UniverseType(str, Enum):
    STATIC = "static"
    DYNAMIC = "dynamic"
    INDEX = "index"
    COMPOSITE = "composite"

class UniverseCreateRequest(BaseModel):
    universe_name: str
    universe_type: UniverseType
    description: str = ""
    config: Dict[str, Any] = {}
    tags: List[str] = []

class UniverseUpdateRequest(BaseModel):
    universe_name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    tags: Optional[List[str]] = None

class StaticUniverseUpdateRequest(BaseModel):
    ts_codes: List[str]
    weights: Optional[Dict[str, float]] = None
    change_reason: str = ""

class DynamicCondition(BaseModel):
    field: str
    operator: str  # =, !=, <, <=, >, >=, in, not_in, between
    value: Any
    percentile: Optional[float] = None  # 使用百分位筛选

class DynamicUniverseCreateRequest(BaseModel):
    universe_name: str
    conditions: List[DynamicCondition]
    base_universe: Optional[str] = None
    sort_by: Optional[str] = None
    sort_ascending: bool = True
    limit: Optional[int] = None
    description: str = ""

class CompositeUniverseCreateRequest(BaseModel):
    universe_name: str
    operation: str  # union, intersection, difference, weighted_combine
    operand_universes: List[str]
    weights: Optional[Dict[str, float]] = None
    description: str = ""

class UniverseBacktestRequest(BaseModel):
    universe_id: str
    start_date: date
    end_date: date
    rebalance_freq: str = "monthly"  # daily, weekly, monthly, quarterly, yearly
    weight_type: str = "equal"  # equal, market_cap, free_float, index_weight

# API端点
@router.get("/universes")
async def list_universes(
    universe_type: Optional[UniverseType] = None,
    is_active: Optional[bool] = None,
    tags: Optional[List[str]] = None
):
    """列出所有股票池"""
    pass

@router.post("/universes")
async def create_universe(request: UniverseCreateRequest):
    """创建股票池"""
    pass

@router.get("/universes/{universe_id}")
async def get_universe(universe_id: str):
    """获取股票池详情"""
    pass

@router.put("/universes/{universe_id}")
async def update_universe(universe_id: str, request: UniverseUpdateRequest):
    """更新股票池基本信息"""
    pass

@router.delete("/universes/{universe_id}")
async def delete_universe(universe_id: str):
    """删除股票池"""
    pass

# 静态股票池
@router.post("/universes/{universe_id}/static")
async def update_static_universe(
    universe_id: str,
    request: StaticUniverseUpdateRequest
):
    """更新静态股票池成分"""
    pass

# 动态股票池
@router.post("/universes/dynamic")
async def create_dynamic_universe(request: DynamicUniverseCreateRequest):
    """创建动态条件选股股票池"""
    pass

@router.post("/universes/{universe_id}/refresh")
async def refresh_dynamic_universe(universe_id: str):
    """刷新动态股票池"""
    pass

# 组合股票池
@router.post("/universes/composite")
async def create_composite_universe(request: CompositeUniverseCreateRequest):
    """创建组合运算股票池"""
    pass

# 版本管理
@router.get("/universes/{universe_id}/versions")
async def list_universe_versions(universe_id: str):
    """列出股票池版本历史"""
    pass

@router.get("/universes/{universe_id}/versions/{version_id}")
async def get_universe_version(universe_id: str, version_id: str):
    """获取指定版本详情"""
    pass

@router.post("/universes/{universe_id}/rollback")
async def rollback_universe(
    universe_id: str,
    version_id: str,
    reason: str = ""
):
    """回滚到指定版本"""
    pass

# 成分查询
@router.get("/universes/{universe_id}/components")
async def get_universe_components(
    universe_id: str,
    trade_date: Optional[date] = None,
    version_id: Optional[str] = None
):
    """获取股票池成分"""
    pass

@router.get("/universes/{universe_id}/components/history")
async def get_universe_components_history(
    universe_id: str,
    start_date: date,
    end_date: date
):
    """获取股票池成分历史序列（用于回测）"""
    pass

# 指数相关
@router.get("/indices")
async def list_indices():
    """列出可用指数"""
    pass

@router.post("/indices/{index_code}/subscribe")
async def subscribe_index(index_code: str):
    """订阅指数成分股"""
    pass

@router.get("/indices/{index_code}/components")
async def get_index_components(
    index_code: str,
    trade_date: Optional[date] = None
):
    """获取指数成分股"""
    pass

@router.get("/indices/{index_code}/components/history")
async def get_index_components_history(
    index_code: str,
    start_date: date,
    end_date: date
):
    """获取指数成分股历史"""
    pass

# 回测集成
@router.post("/universes/{universe_id}/backtest-preview")
async def preview_universe_backtest(
    universe_id: str,
    request: UniverseBacktestRequest
):
    """预览股票池回测（不实际运行，返回统计信息）"""
    pass

@router.get("/universes/{universe_id}/analytics")
async def get_universe_analytics(
    universe_id: str,
    start_date: date,
    end_date: date
):
    """获取股票池分析（行业分布、市值分布等）"""
    pass
```

### 3.3 核心服务层设计

```python
# 股票池服务
# 文件路径: app/services/universe_service.py

from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime
from dataclasses import dataclass
from enum import Enum
import polars as pl

from store.dolphindb_client import db_client
from app.core.logger import logger

class UniverseService:
    """股票池服务"""

    def __init__(self):
        self.db = db_client

    # ========== 股票池CRUD ==========

    def create_universe(
        self,
        name: str,
        universe_type: str,
        description: str = "",
        config: Dict[str, Any] = None,
        tags: List[str] = None
    ) -> str:
        """创建股票池"""
        pass

    def get_universe(self, universe_id: str) -> Optional[Dict[str, Any]]:
        """获取股票池"""
        pass

    def list_universes(
        self,
        universe_type: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """列出股票池"""
        pass

    # ========== 静态股票池 ==========

    def update_static_universe(
        self,
        universe_id: str,
        ts_codes: List[str],
        weights: Optional[Dict[str, float]] = None,
        change_reason: str = "",
        created_by: str = ""
    ) -> str:
        """更新静态股票池，创建新版本"""
        pass

    # ========== 动态股票池 ==========

    def create_dynamic_universe(
        self,
        name: str,
        conditions: List[Dict[str, Any]],
        base_universe: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_ascending: bool = True,
        limit: Optional[int] = None,
        description: str = ""
    ) -> str:
        """创建动态条件选股股票池"""
        pass

    def refresh_dynamic_universe(
        self,
        universe_id: str,
        trade_date: Optional[date] = None
    ) -> str:
        """刷新动态股票池，创建新版本"""
        pass

    def _apply_conditions(
        self,
        df: pl.DataFrame,
        conditions: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        """应用选股条件"""
        pass

    # ========== 组合股票池 ==========

    def create_composite_universe(
        self,
        name: str,
        operation: str,
        operand_universe_ids: List[str],
        weights: Optional[Dict[str, float]] = None,
        description: str = ""
    ) -> str:
        """创建组合运算股票池"""
        pass

    def _compute_union(
        self,
        components_list: List[List[Tuple[str, float]]]
    ) -> List[Tuple[str, float]]:
        """计算并集"""
        pass

    def _compute_intersection(
        self,
        components_list: List[List[Tuple[str, float]]]
    ) -> List[Tuple[str, float]]:
        """计算交集"""
        pass

    def _compute_difference(
        self,
        components_list: List[List[Tuple[str, float]]]
    ) -> List[Tuple[str, float]]:
        """计算差集"""
        pass

    def _compute_weighted_combine(
        self,
        components_list: List[List[Tuple[str, float]]],
        weights: List[float]
    ) -> List[Tuple[str, float]]:
        """计算加权合并"""
        pass

    # ========== 版本管理 ==========

    def list_versions(self, universe_id: str) -> List[Dict[str, Any]]:
        """列出版本历史"""
        pass

    def get_version(self, version_id: str) -> Optional[Dict[str, Any]]:
        """获取指定版本"""
        pass

    def rollback_to_version(
        self,
        universe_id: str,
        version_id: str,
        reason: str = "",
        created_by: str = ""
    ) -> str:
        """回滚到指定版本"""
        pass

    # ========== 成分查询 ==========

    def get_components(
        self,
        universe_id: str,
        trade_date: Optional[date] = None,
        version_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """获取股票池成分"""
        pass

    def get_components_history(
        self,
        universe_id: str,
        start_date: date,
        end_date: date
    ) -> pl.DataFrame:
        """获取股票池成分历史（用于回测）

        返回格式:
        trade_date | ts_code | weight
        """
        pass

    # ========== 指数相关 ==========

    def list_indices(self) -> List[Dict[str, Any]]:
        """列出可用指数"""
        pass

    def subscribe_index(self, index_code: str) -> bool:
        """订阅指数成分股"""
        pass

    def get_index_components(
        self,
        index_code: str,
        trade_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """获取指数成分股"""
        pass

    def get_index_components_history(
        self,
        index_code: str,
        start_date: date,
        end_date: date
    ) -> pl.DataFrame:
        """获取指数成分股历史"""
        pass

    def create_universe_from_index(
        self,
        index_code: str,
        name: Optional[str] = None
    ) -> str:
        """从指数创建股票池"""
        pass

    # ========== 分析功能 ==========

    def get_universe_analytics(
        self,
        universe_id: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """获取股票池分析

        返回：
        - 行业分布
        - 市值分布
        - 成分数量变化
        - 换手率
        """
        pass

    def compare_universes(
        self,
        universe_ids: List[str],
        trade_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """对比多个股票池"""
        pass
```

---

## 四、实际工作中需要的功能

### 4.1 必须功能（P0）

1. **指数成分股历史数据
   - 沪深300、中证500、中证1000、创业板指等主流指数
   - 历史调仓记录（支持2010年以来）
   - 指数权重数据

2. ** survivorship bias 规避
   - 回测时必须使用当时的指数成分
   - 不能使用当前成分股回测历史

3. **股票池成分历史序列
   - 用于回测的逐日成分序列
   - 支持不同的再平衡频率

4. **权重处理
   - 等权、市值加权、自由流通市值加权
   - 指数原生权重

5. **与回测引擎集成
   - 回测时可以直接使用股票池
   - 支持动态股票池（随时间变化）

### 4.2 重要功能（P1）

1. **动态条件选股
   - 基于因子的条件筛选
   - 支持复合条件（AND/OR）
   - 百分位筛选

2. **股票池分析
   - 行业分布
   - 市值分布
   - 换手率统计
   - 风格暴露分析

3. **股票池对比
   - 成分重叠度
   - 相关性分析
   - 绩效对比

4. **批量操作
   - 批量创建股票池
   - 批量刷新动态股票池

### 4.3 增强功能（P2）

1. **股票池模板
   - 常用股票池模板（沪深300非金融、中证500增强等）
   - 自定义模板保存

2. **预警功能
   - 成分股异常预警
   - 股票池变化预警

3. **导出功能
   - 导出到Excel
   - 导出为研报格式

4. **权限管理
   - 股票池分享
   - 权限控制

---

## 五、实施建议

### 阶段一：基础功能（P0，2周）
1. 创建指数成分股数据表和同步任务
2. 实现基础股票池CRUD和版本管理
3. 实现指数股票池功能
4. 实现静态股票池功能
5. 基础API端点

### 阶段二：动态选股（P1，2周）
1. 实现动态条件选股
2. 实现股票池组合运算
3. 实现股票池分析功能
4. 与因子引擎集成

### 阶段三：回测集成（P1，1周）
1. 与回测引擎深度集成
2. 实现股票池成分历史序列
3. 回测预览功能

### 阶段四：增强功能（P2，2周）
1. 股票池模板
2. 预警功能
3. 导出功能
4. 权限管理

---

## 六、总结

### 优点
1. 现有架构基础良好，易于扩展
2. 数据层使用DolphinDB，适合处理时间序列
3. 计算层使用Polars，高效向量化计算
4. 已有ETL框架可复用

### 风险点
1. 指数成分股历史数据缺失（最高优先级）
2. 需要注意 survivorship bias 问题
3. 权重处理需要完善

### 关键建议
1. 优先实现指数成分股数据同步
2. 确保回测时使用历史时点的股票池成分
3. 提供多种权重处理方式
4. 与现有因子引擎和回测引擎深度集成
