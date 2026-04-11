# 股票池模块设计文档

**日期**: 2026-03-28
**版本**: v1.0
**状态**: 待审核

---

## 一、概述

### 1.1 项目背景

股票池是量化研究系统的核心模块，用于管理和维护股票集合，支持因子计算和策略回测的标的范围选择。

### 1.2 功能范围

- **静态股票池管理**：手动创建和维护股票列表
- **动态条件选股**：基于规则/表达式动态筛选股票
- **指数成分股管理**：同步和管理指数成分股（支持Tushare快速查询）
- **股票池组合运算**：交集、并集、差集
- **版本历史管理**：完整的变更历史、对比、回滚
- **与现有系统集成**：因子计算、策略回测

### 1.3 执行策略

采用**混合模式**：
- 预计算模式：常用股票池每日定时计算并保存
- 实时计算模式：按需实时选股，支持保存快照

---

## 二、系统架构

### 2.1 整体架构

```
backend/
├── stock_pool/                    # 股票池根模块
│   ├── __init__.py
│   ├── api/                       # API 层
│   │   ├── __init__.py
│   │   ├── pool_management.py     # 股票池 CRUD
│   │   ├── pool_selection.py      # 动态选股执行
│   │   ├── index_constituent.py   # 指数成分股管理
│   │   └── pool_version.py        # 版本历史查询
│   ├── engine/                     # 选股引擎
│   │   ├── __init__.py
│   │   ├── selector.py             # 条件选股执行器
│   │   ├── parser.py               # 选股表达式解析器
│   │   ├── composer.py             # 股票池组合器
│   │   ├── weight.py               # 权重计算器
│   │   └── functions.py            # 内置选股函数库
│   ├── services/                   # 服务层
│   │   ├── __init__.py
│   │   ├── pool_service.py         # 股票池服务
│   │   ├── index_sync_service.py   # 指数同步服务
│   │   └── version_service.py      # 版本管理服务
│   ├── models/                     # Pydantic 模型
│   │   ├── __init__.py
│   │   ├── pool.py                 # 股票池模型
│   │   ├── selection.py            # 选股请求/响应模型
│   │   └── index.py                # 指数模型
│   ├── validators/                 # 数据验证
│   │   ├── __init__.py
│   │   └── pool_validator.py      # 股票池数据验证
│   └── registry.py                 # 选股函数注册表
├── app/
│   ├── api/v1/
│   │   └── stock_pool.py          # API 路由聚合
│   ├── services/
│   │   └── stock_pool_service.py  # 服务层（可选）
│   └── models/
│       └── stock_pool.py          # Pydantic模型（可选）
├── store/
│   └── repositories/
│       └── stock_pool_repository.py  # 数据访问封装
├── flows/
│   └── stock_pool_sync.py          # Prefect 同步工作流
└── database/
    └── init_stock_pool.py         # 数据库表初始化
```

### 2.2 架构原则

1. **与现有系统保持一致**：参考 `production` 因子模块架构
2. **分层清晰**：API → Service → Repository → Database
3. **职责分离**：选股引擎与数据访问分离
4. **可扩展性**：便于未来扩展高级功能

---

## 三、数据库设计

### 3.1 数据库表

#### 表1: `stock_pool_metadata`（股票池元数据表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `pool_id` | SYMBOL | 股票池ID（主键），格式: POOL_[TYPE]_[CODE] |
| `pool_type` | SYMBOL | 类型: static/dynamic/index/composite |
| `pool_name` | STRING | 名称 |
| `description` | STRING | 描述 |
| `status` | SYMBOL | 状态: draft/active/archived |
| `version` | INT | 版本号 |
| `weight_method` | SYMBOL | 权重方式: equal/market_cap/custom |
| `definition` | STRING | 定义（JSON，根据类型不同） |
| `created_at` | TIMESTAMP | 创建时间 |
| `updated_at` | TIMESTAMP | 更新时间 |
| `created_by` | SYMBOL | 创建人 |
| `updated_by` | SYMBOL | 更新人 |

**索引**: `pool_type`, `status`, `updated_at`

---

#### 表2: `stock_pool_constituents`（股票池成分股表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `pool_id` | SYMBOL | 股票池ID |
| `trade_date` | DATE | 交易日 |
| `ts_code` | SYMBOL | 股票代码 |
| `weight` | DOUBLE | 权重（0-1） |
| `rank` | INT | 排序（可选） |
| `inclusion_reason` | STRING | 纳入原因（可选） |
| `created_at` | TIMESTAMP | 创建时间 |

**分区策略**: HASH(pool_id, 16) + RANGE(trade_date, 季度)
**排序字段**: `pool_id`, `trade_date`, `ts_code`
**去重策略**: LAST（保留最新）

---

#### 表3: `stock_pool_version`（股票池版本历史表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `version_id` | SYMBOL | 版本ID（主键） |
| `pool_id` | SYMBOL | 股票池ID |
| `version_num` | INT | 版本号 |
| `change_type` | SYMBOL | 变更类型: create/update/refresh/rollback |
| `change_reason` | STRING | 变更原因 |
| `snapshot` | STRING | 成分股快照（JSON） |
| `created_at` | TIMESTAMP | 创建时间 |
| `created_by` | SYMBOL | 创建人 |

**索引**: `pool_id`, `version_num`

---

#### 表4: `index_constituent_cache`（指数成分股缓存表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `index_code` | SYMBOL | 指数代码 |
| `trade_date` | DATE | 交易日 |
| `ts_code` | SYMBOL | 股票代码 |
| `weight` | DOUBLE | 权重 |
| `synced_at` | TIMESTAMP | 同步时间 |

**分区策略**: HASH(index_code, 10) + RANGE(trade_date, 季度)
**排序字段**: `index_code`, `trade_date`, `ts_code`

---

#### 表5: `stock_pool_audit_log`（股票池操作审计日志表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `event_time` | TIMESTAMP | 事件时间（主键） |
| `pool_id` | SYMBOL | 股票池ID |
| `event_type` | SYMBOL | 事件类型: CREATE/UPDATE/DELETE/ACTIVATE/ARCHIVE |
| `event_detail` | STRING | 事件详情（JSON） |
| `operated_by` | SYMBOL | 操作人 |
| `comment` | STRING | 备注 |

**分区策略**: RANGE(event_time, 月度)
**排序字段**: `pool_id`, `event_time`

---

### 3.2 定义字段格式

#### 静态股票池 (pool_type = "static"

```json
{
  "source": "manual"
}
```

#### 动态股票池 (pool_type = "dynamic")

```json
{
  "expression": "market_cap > 10000000000 AND pe_ttm < 30",
  "universe": "all_a_shares",
  "lookback_days": 1
}
```

#### 指数股票池 (pool_type = "index")

```json
{
  "index_code": "000300.SH",
  "source": "tushare"
}
```

#### 组合股票池 (pool_type = "composite")

```json
{
  "operation": "intersection",
  "pool_ids": ["POOL_INDEX_000300", "POOL_DYNAMIC_LARGE_CAP"]
}
```

---

## 四、API 设计

### 4.1 统一响应格式

```typescript
interface ApiResponse {
    success: boolean;
    data: any;
    error: string | null;
    message: string | null;
    metadata: PaginationMeta | null;
}

interface PaginationMeta {
    total: number;
    page: number;
    page_size: number;
    total_pages: number;
}
```

### 4.2 错误响应格式

```typescript
interface ErrorResponse {
    success: false;
    error: {
        code: string;
        message: string;
        details?: Array<{
            field?: string;
            issue?: string;
            hint?: string;
        }>;
    };
    data: null;
}
```

### 4.3 API 端点列表

| 方法 | 端点 | 说明 |
|------|------|------|
| GET | `/api/v1/stock-pool/pools` | 列出股票池 |
| POST | `/api/v1/stock-pool/pools` | 创建股票池 |
| GET | `/api/v1/stock-pool/pools/{pool_id}` | 获取股票池详情 |
| PUT | `/api/v1/stock-pool/pools/{pool_id}` | 更新股票池 |
| DELETE | `/api/v1/stock-pool/pools/{pool_id}` | 归档股票池 |
| GET | `/api/v1/stock-pool/pools/{pool_id}/constituents/{trade_date}` | 获取成分股 |
| POST | `/api/v1/stock-pool/pools/{pool_id}/constituents` | 上传成分股 |
| POST | `/api/v1/stock-pool/pools/{pool_id}/constituents/csv` | CSV上传成分股 |
| GET | `/api/v1/stock-pool/pools/{pool_id}/export` | 导出成分股 |
| GET | `/api/v1/stock-pool/pools/{pool_id}/versions` | 版本历史列表 |
| GET | `/api/v1/stock-pool/pools/{pool_id}/versions/{version}` | 获取特定版本 |
| GET | `/api/v1/stock-pool/pools/{pool_id}/versions/{v1}/diff/{v2}` | 版本对比 |
| POST | `/api/v1/stock-pool/pools/{pool_id}/versions/{version}/rollback` | 回滚到版本 |
| POST | `/api/v1/stock-pool/index/fetch-constituents` | Tushare快速查询指数成分股 |
| GET | `/api/v1/stock-pool/index/available` | 可用指数列表 |
| POST | `/api/v1/stock-pool/index/sync` | 同步指数 |
| POST | `/api/v1/stock-pool/selection/evaluate` | 实时选股评估 |
| POST | `/api/v1/stock-pool/composite/compute` | 股票池组合运算 |

---

## 五、核心功能设计

### 5.1 指数成分股快速查询（Tushare）

#### 功能说明：用户输入指数代码，系统从Tushare获取成分股并保存。

**流程**:
1. 用户输入指数代码（如 `000300.SH`）
2. 调用 Tushare `index_weight` 接口获取成分股
3. 保存到 `index_constituent_cache` 表
4. 可选：创建 `index` 类型的股票池

**API**:
```
POST /api/v1/stock-pool/index/fetch-constituents
{
  "index_code": "000300.SH",
  "trade_date": "20240328",
  "create_pool": true
}
```

### 5.2 动态选股表达式（混合模式

#### 预计算模式（定时任务）

- 每日收盘后定时计算常用动态股票池
- 保存结果到 `stock_pool_constituents`
- 支持历史回补

#### 实时计算模式（按需查询）

- 用户提交选股表达式
- 实时执行选股
- 支持保存为股票池快照

#### 选股表达式示例

```
# 简单条件
market_cap > 10000000000 AND pe_ttm < 30

# 复杂条件
(market_cap > 10000000000 OR circ_mv > 5000000000) AND pe_ttm < 30 AND turnover_rate > 0.02

# 包含技术指标
rsi_14 < 30 AND close > ma_20
```

### 5.3 股票池组合运算

支持的运算类型：
- `intersection`（交集）- 同时在多个股票池中
- `union`（并集）- 在任意一个股票池中
- `difference`（差集）- 在A中但不在B中

```json
{
  "operation": "intersection",
  "pool_ids": ["POOL_INDEX_000300", "POOL_DYNAMIC_LARGE_CAP"],
  "trade_date": "20240328"
}
```

### 5.4 权重计算

支持的权重方式：
- `equal`（等权）- 每只股票权重相同
- `market_cap`（市值加权）- 按市值加权
- `custom`（自定义）- 用户自定义权重

---

## 六、与现有系统集成

### 6.1 与因子计算集成

修改 `ProductionEngine` 支持股票池参数：

```python
# 原有:
def run_task(self, factor_id: str, start_date: str, end_date: str)

# 新增:
def run_task(self, factor_id: str, start_date: str, end_date: str,
             pool_id: Optional[str] = None)
```

当 `pool_id` 提供时：
1. 加载股票池在日期范围内的成分股
2. 只计算股票池内的股票
3. 过滤停牌/ST等处理保持不变

### 6.2 与策略回测集成

修改 `BacktestEngine` 支持动态股票池：

```python
class BacktestConfig:
    pool_id: Optional[str] = None
    # 回测期间根据日期切换股票池
```

### 6.3 与 Prefect 工作流集成

创建 Prefect 工作流：

```python
# flows/stock_pool_sync.py

@flow
def sync_index_constituents(index_codes: List[str]):
    """同步指数成分股
    for code in index_codes:
        sync_one_index(code)

@flow
def refresh_dynamic_pools(pool_ids: List[str]):
    """刷新动态股票池
    for pool_id in pool_ids:
        refresh_one_pool(pool_id)
```

---

## 七、性能优化

### 7.1 缓存策略

- **内存缓存：`IndexPoolCache`
  - 成分股缓存：TTL 5分钟
  - 元数据缓存：TTL 5分钟
  - 列表缓存：TTL 1分钟

- **Redis缓存**（可选，未来扩展）

### 7.2 数据库优化

- **分区裁剪：按 `pool_id` + `trade_date` 分区
- **排序字段**：优化查询性能
- **批量操作**：大数据量分批处理

### 7.3 查询优化

| 场景 | 优化方式 |
|------|----------|
| 获取最新成分股 | 缓存 + 分区裁剪 |
| 获取历史成分股 | 按 trade_date 分区 |
| 多股票池对比 | 并行查询 |
| 规则实时计算 | 预计算 + 缓存 |

---

## 八、实施计划

### Phase 1: 基础功能（高优先级）

1. 创建数据库表结构
2. 实现 Repository 层
3. 实现 Service 层
4. 实现 API 层（CRUD + 指数快速查询）
5. 集成到主应用

### Phase 2: 动态选股（中优先级）

6. 实现表达式解析器
7. 实现选股引擎
8. 实现预计算定时任务
9. 实时选股 API

### Phase 3: 高级功能（中低优先级）

10. 版本对比和回滚
11. 股票池组合运算
12. 与因子/回测集成
13. 审计日志
14. 性能优化和缓存

---

## 九、关键文件清单

| 文件 | 说明 |
|------|------|
| `stock_pool/api/pool_management.py` | 股票池 CRUD API |
| `stock_pool/api/index_constituent.py` | 指数成分股 API |
| `stock_pool/services/pool_service.py` | 股票池服务 |
| `stock_pool/engine/selector.py` | 选股引擎 |
| `stock_pool/models/pool.py` | Pydantic 模型 |
| `stock_pool/validators/pool_validator.py` | 数据验证 |
| `store/repositories/stock_pool_repository.py` | 数据访问 |
| `app/api/v1/stock_pool.py` | 路由聚合 |
| `flows/stock_pool_sync.py` | Prefect 工作流 |
| `database/init_stock_pool.py` | 数据库初始化 |

---

## 十、风险与注意事项

1. **数据一致性**：使用 DolphinDB 事务保证元数据和成分股原子更新
2. **权重验证**：严格验证权重范围和总和
3. **缓存失效**：股票池更新时及时失效相关缓存
4. **向后兼容**：保留现有 `index_constituents` 表作为过渡
5. **性能监控**：监控大股票池的查询和计算性能

---

**文档结束**
