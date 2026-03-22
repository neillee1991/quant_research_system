# 项目重构设计文档

> 日期: 2026-03-22
> 版本: 1.0
> 目标: 从第一性原理出发，清理整合项目，使结构更清晰、无冗余、模块化强、扩展性好

---

## 执行摘要

本文档描述 QuantResearchSystem 项目的全面重构方案，包括：
1. 文档整合与清理
2. 种子数据配置化
3. DolphinDB 客户端统一
4. 脚本清理
5. 前后端 API 一致性检查

---

## 设计原则

### 第一性原理
- 数据与代码分离
- 职责单一，模块化清晰
- 命名规范、科学、易读易理解
- 无冗余、无重复、无歧义

### 命名规范（强制执行）
- **不使用** `new`、`old`、`v2`、`legacy` 这类版本号在名称中
- 使用清晰的领域术语
- 动词开头：`get`、`find`、`create`、`update`、`delete`、`insert`、`query`、`execute`
- 类名使用 PascalCase：`DatabaseClient`、`SeedDataLoader`
- 函数名使用 snake_case：`load_sync_tasks()`、`seed_all_to_database()`
- 文件名使用 snake_case：`database_client.py`、`seed_loader.py`

---

## 阶段一：文档整合与清理

### 1.1 新的文档结构

```
quant_research_system/
├── README.md                    # 快速开始（精简版）
├── CLAUDE.md                    # AI 助手指南（保留并精简）
└── docs/
    ├── ARCHITECTURE.md          # 系统架构（新建）
    ├── API_REFERENCE.md         # API 参考（新建）
    ├── DEPLOYMENT.md           # 部署指南（新建）
    ├── DEVELOPER_GUIDE.md      # 开发者指南（新建）
    └── TROUBLESHOOTING.md      # 故障排查（新建）
```

### 1.2 文档内容规划

#### README.md（根目录）
- 项目简介
- 快速开始（3步启动）
- 核心功能列表
- 链接到 docs/ 下的详细文档

#### ARCHITECTURE.md（新建）
整合来源：
- `PROJECT_STATUS.md`
- `SYSTEM_REFACTORING_STATUS.md`
- `memory/MEMORY.md`（从 Claude memory 提取）

章节：
- 系统概览
- 架构分层（数据层/计算层/API层/前端层）
- 核心模块说明
- 数据库表设计
- 关键数据流

#### DEVELOPER_GUIDE.md（新建）
整合来源：
- `PROJECT_STANDARDS.md`
- `DATA_INSPECTION_FRONTEND_GUIDE.md`
- `backend/README.md`
- `backend/TASK_ABSTRACTION_GUIDE.md`
- `frontend/TASK_ABSTRACTION_GUIDE.md`

章节：
- 开发环境搭建
- 代码规范（来自 PROJECT_STANDARDS.md）
- 开发流程
- 测试指南
- 调试技巧
- 命名规范（本文档 1.2 节）

#### 其他新文档
- `API_REFERENCE.md` - API 端点完整列表
- `DEPLOYMENT.md` - 生产环境部署
- `TROUBLESHOOTING.md` - 常见问题

### 1.3 待删除的临时文档

提取有用信息后删除：
- `DATA_INSPECTION_FEATURE.md`
- `DATA_INSPECTION_FRONTEND_GUIDE.md`
- `DOLPHINDB_DATA_DIR_ROOT_CAUSE.md`
- `DOLPHINDB_PATH_FIX.md`
- `SCRIPTS_STATUS.md`
- `SETUP_FIXES.md`
- `FACTOR_UI_FIXES.md`
- `DATA_DEPENDENCY_THREE_CATEGORIES.md`
- `FACTOR_MODULE_CONSISTENCY_ANALYSIS.md`
- `PROJECT_STATUS.md`
- `SYSTEM_REFACTORING_STATUS.md`

---

## 阶段二：种子数据配置化

### 2.1 种子数据配置结构

```
config/
└── seed_data/
    ├── README.md                    # 种子数据说明
    ├── sync_tasks.json              # 所有同步任务配置（完整）
    ├── etl_tasks.json               # 所有 ETL 任务配置（完整）
    ├── factor_data_config.json       # 因子字段映射配置
    └── factor_metadata.json          # 默认因子定义
```

### 2.2 配置文件包含的完整内容

#### sync_tasks.json 包含（12个任务）
1. `sync_stock_basic` - 股票基础信息
2. `sync_trade_cal` - 交易日历
3. `sync_daily_data` - 日线行情
4. `sync_adj_factor` - 复权因子
5. `sync_daily_basic` - 每日指标
6. `sync_stk_limit` - 涨跌停价格
7. `sync_suspend_d` - 停复牌信息
8. `sync_stock_st` - ST 股票列表
9. `sync_sw_index_member_N` - 申万行业成分（旧）
10. `sync_sw_index_member_Y` - 申万行业成分（新）
11. `sync_ci_index_member_N` - 中信行业成分（旧）
12. `sync_ci_index_member_Y` - 中信行业成分（新）

#### etl_tasks.json 包含（3个任务）
1. `etl_index_member` - 合并申万+中信行业成员表
2. `etl_index_member_daily` - 每只股票每个交易日所属行业
3. `etl_stock_daily_info` - 行情+基本面+行业宽表

#### factor_data_config.json 包含（7个配置）
- `adj_factor` - 复权因子
- `list_date` - 股票上市日期
- `is_st` - 是否 ST
- `is_limit` - 涨跌停状态
- `industry_l1` - 股票一级行业
- `industry_l2` - 股票二级行业
- `market_cap` - 股票总市值

#### factor_metadata.json 包含（8个因子）
1. `factor_ma_5` - 5 日移动平均线
2. `factor_ma_20` - 20 日移动平均线
3. `factor_momentum_20` - 20 日价格动量
4. `factor_volatility_10` - 10 日收益率波动率
5. `factor_volatility_20` - 20 日收益率波动率
6. `factor_rsi_14` - 14 日 RSI
7. `factor_pe_rank` - PE 百分位排名
8. `factor_pb_rank` - PB 百分位排名

### 2.3 配置文件格式

#### sync_tasks.json 格式
```json
{
  "version": "1.0",
  "tasks": [
    {
      "task_id": "sync_daily_data",
      "api_name": "daily",
      "description": "日线行情（开高低收、成交量、成交额）",
      "sync_type": "incremental",
      "date_field": "trade_date",
      "table_name": "sync_daily_data",
      "params": {
        "trade_date": "{date}",
        "fields": "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
      },
      "primary_keys": ["ts_code", "trade_date"],
      "api_limit": 5000,
      "schema": {
        "ts_code": {"type": "SYMBOL"},
        "trade_date": {"type": "DATE"},
        "open": {"type": "DOUBLE"},
        "high": {"type": "DOUBLE"},
        "low": {"type": "DOUBLE"},
        "close": {"type": "DOUBLE"},
        "pre_close": {"type": "DOUBLE"},
        "change": {"type": "DOUBLE"},
        "pct_chg": {"type": "DOUBLE"},
        "vol": {"type": "DOUBLE"},
        "amount": {"type": "DOUBLE"}
      },
      "enabled": true
    }
  ]
}
```

### 2.4 SeedDataLoader 服务

**位置：** `infrastructure/seed/seed_loader.py`

**功能定义：**
```python
"""
种子数据加载器 - 从配置文件加载所有默认配置
"""
from pathlib import Path
from typing import Dict, List, Optional


class SeedDataLoader:
    """种子数据加载器"""

    def __init__(self, config_dir: Optional[Path] = None):
        """
        初始化种子数据加载器

        Args:
            config_dir: 配置文件目录，默认为 config/seed_data/
        """

    def load_sync_tasks(self) -> List[Dict]:
        """加载所有同步任务配置"""

    def load_etl_tasks(self) -> List[Dict]:
        """加载所有 ETL 任务配置"""

    def load_factor_data_config(self) -> List[Dict]:
        """加载因子数据字段映射配置"""

    def load_factor_metadata(self) -> List[Dict]:
        """加载默认因子元数据"""

    def seed_all_to_database(self, db_client):
        """将所有种子数据写入数据库（如果表为空）"""

    def seed_sync_tasks_to_database(self, db_client):
        """仅写入同步任务配置"""

    def seed_etl_tasks_to_database(self, db_client):
        """仅写入 ETL 任务配置"""

    def seed_factor_config_to_database(self, db_client):
        """仅写入因子配置"""
```

### 2.5 更新启动流程

**app/main.py 的变化：**
```python
# 旧代码：
# db_client.seed_sync_task_config()
# db_client.seed_etl_task_config()
# db_client.seed_factor_data_config()
# db_client.seed_factor_metadata()

# 新代码：
from infrastructure.seed.seed_loader import seed_data_loader

seed_data_loader.seed_all_to_database(db_client)
```

---

## 阶段三：DolphinDB 客户端统一

### 3.1 最终目录结构

```
backend/
├── store/
│   ├── __init__.py           # 向后兼容层（重新导出）
│   └── file_storage.py       # 文件存储（保留，与数据库无关）
│
└── infrastructure/
    └── database/
        ├── __init__.py
        ├── connection.py          # 数据库连接管理
        ├── sql_adapter.py         # SQL 语法适配器
        ├── type_converter.py      # 数据类型转换器
        ├── table_manager.py       # 数据库表管理器
        ├── data_operations.py     # 数据操作器
        ├── metadata_manager.py    # 元数据管理器
        └── database_client.py     # 数据库客户端门面（单例）
```

### 3.2 命名规范（强制执行）

| 用途 | 名称 |
|------|------|
| 数据库客户端门面 | `DatabaseClient` (在 `database_client.py` 中) |
| 连接管理 | `DatabaseConnection` |
| SQL 适配 | `SqlAdapter` (注意大小写：Sql 不是 SQL) |
| 类型转换 | `DataTypeConverter` |
| 表管理 | `TableManager` |
| 数据操作 | `DataOperations` |
| 元数据管理 | `MetadataManager` |

### 3.3 模块导出

**infrastructure/database/__init__.py：**
```python
from .database_client import DatabaseClient, database_client

__all__ = ["DatabaseClient", "database_client"]
```

**store/__init__.py（向后兼容层）：**
```python
from infrastructure.database import DatabaseClient, database_client

# 保留旧名称的别名
DolphinDBClient = DatabaseClient
db_client = database_client

__all__ = ["DatabaseClient", "database_client", "DolphinDBClient", "db_client"]
```

### 3.4 统一后的客户端方法命名

```python
class DatabaseClient:
    """统一的数据库客户端"""

    # 连接管理
    def connect(self) -> None:
    def disconnect(self) -> None:
    def ensure_connected(self) -> None:

    # 查询操作
    def query(self, sql: str, params: Optional[Tuple] = None) -> pl.DataFrame:
    def execute(self, sql: str, params: Optional[Tuple] = None) -> None:

    # 数据操作
    def insert(self, table_name: str, df: pl.DataFrame) -> None:
    def upsert(self, table_name: str, df: pl.DataFrame, key_columns: List[str]) -> None:
    def append(self, table_name: str, df: pl.DataFrame) -> int:

    # 表管理
    def table_exists(self, table_name: str) -> bool:
    def create_table(self, table_name: str, schema: Dict, primary_keys: List[str]) -> None:
    def drop_table(self, table_name: str) -> None:
    def list_tables(self) -> List[Dict]:
    def get_table_columns(self, table_name: str) -> List[str]:

    # 元数据管理
    def ensure_metadata_tables(self) -> None:
```

### 3.5 删除的内容

**将被删除：**
- `store/dolphindb_client.py`
- `store/dolphindb/` 整个目录

---

## 阶段四：脚本清理

### 4.1 保留的脚本（激进清理策略）

```
backend/scripts/
├── maintenance/
│   ├── backup_configs.py       # 配置备份
│   └── restore_configs.py      # 配置恢复
├── migrations/
│   └── (未来数据库迁移脚本)
└── validation/
    └── health_check.py         # 健康检查
```

### 4.2 被删除的脚本

以下脚本将被删除（需要时可从 git 历史恢复）：
- `test_partition_performance.py`
- `optimize_factor_values_partition.py`
- `performance_dashboard.py`
- `seed_factors.py`
- `verify_integrity.py`
- `verify_backup.py`
- `verify_migration.py`
- `verify_seed_tasks.py`
- `test_new_api.py`
- `test_task_execution.py`
- `profile_code.py`
- `run_benchmarks.py`
- `migrate_factor.py`
- `reinit_database.py`
- `drop_old_tables.py`
- `fix_factor_data_config.py`
- `cleanup_duplicate_factor_data_config.py`
- `cleanup_duplicate_sync_log.py`
- `migrations/completed/*.py`

---

## 阶段五：前后端 API 一致性检查

### 5.1 确认保留的 API 结构

```
app/api/v1/
├── production/              # ✅ 保留（已良好模块化）
│   ├── __init__.py         # 路由聚合
│   ├── factor_analysis.py   # 因子分析端点
│   ├── factor_compute.py    # 因子计算执行端点
│   ├── factor_registry.py   # 因子注册和元数据管理
│   └── factor_config.py    # 配置和指数池管理
│
├── data/                    # ✅ 保留（已良好模块化）
│   ├── __init__.py
│   ├── config_api.py
│   ├── etl_api.py
│   ├── query_api.py
│   ├── schema_utils.py
│   └── sync_api.py
│
├── factor.py               # 简单因子计算（待确认是否与 production 重复）
├── strategy.py             # 策略回测
├── ml.py                   # AutoML
├── flows.py                # Prefect 工作流
├── tasks.py                # 任务管理
├── generic_task.py         # 通用任务
├── schema_tools.py         # Schema 工具
└── versions.py             # 版本管理
```

### 5.2 检查清单

| 模块 | 后端文件 | 前端调用 |
|------|---------|---------|
| 数据查询 | `app/api/v1/data/` | `dataApi.*` |
| 因子生产 | `app/api/v1/production/` | `productionApi.*` |
| 因子计算 | `app/api/v1/factor.py` | `factorApi.*` |
| 策略回测 | `app/api/v1/strategy.py` | `strategyApi.*` |
| AutoML | `app/api/v1/ml.py` | `mlApi.*` |
| 工作流 | `app/api/v1/flows.py` | `flowApi.*` |
| 任务管理 | `app/api/v1/tasks.py`, `generic_task.py` | 直接调用 |

### 5.3 检查内容

1. 前端 API 调用与后端路由一一对应
2. 请求参数类型匹配
3. 响应数据结构匹配
4. 错误处理一致
5. 没有废弃的端点仍在使用

### 5.4 修复策略

发现不一致时的处理：
1. 如果后端实现正确，更新前端类型定义
2. 如果前端调用正确，更新后端实现
3. 如果双方都正确但类型不匹配，添加类型适配层
4. 删除双方都未使用的端点

---

## 实施计划（5个阶段）

### 阶段 1：文档整合与清理
- 提取临时文档中的有用信息
- 创建新的架构文档
- 删除临时文档
- **预计时间：1天**

### 阶段 2：种子数据配置化
- 创建 `config/seed_data/` 目录
- 将种子数据迁移到 JSON 文件
- 创建 `SeedDataLoader` 服务
- 更新启动流程
- **预计时间：1-2天**

### 阶段 3：DolphinDB 客户端统一
- 重命名模块和类（符合命名规范）
- 更新 `infrastructure/database/` 实现
- 创建兼容层 `store/__init__.py`
- 删除旧实现
- 更新所有引用
- **预计时间：2-3天**

### 阶段 4：脚本清理
- 保留核心运维脚本
- 删除所有临时脚本
- 重新组织脚本目录
- **预计时间：0.5天**

### 阶段 5：API 一致性检查
- 比对前后端 API
- 修复不一致
- **预计时间：1-2天**

**总计：5.5-8.5天**

---

## 风险与缓解

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| DolphinDB 客户端更新引入bug | 高 | 中 | 渐进式更新 + 充分测试 |
| 种子数据迁移遗漏配置 | 中 | 低 | 完整对照现有代码 |
| API 不一致导致前端崩溃 | 高 | 中 | 完整比对 + 回归测试 |
| 删除脚本后需要恢复 | 低 | 低 | Git 历史可恢复 |

---

## 成功标准

- [ ] 文档结构清晰，无冗余
- [ ] 所有种子数据在配置文件中
- [ ] DolphinDB 客户端统一，无重复代码
- [ ] 命名规范、科学、易读易理解
- [ ] 前后端 API 完全一致
- [ ] 所有测试通过
- [ ] 系统可以正常启动和运行
