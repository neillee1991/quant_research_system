# QuantSystem 数据库审查报告

**审查日期**: 2026-04-11  
**审查范围**: DolphinDB + PostgreSQL 双数据库架构

---

## 执行摘要

本项目采用 **DolphinDB (时序数据) + PostgreSQL (元数据/配置)** 的双数据库架构，这是合理的量化系统设计。发现 **3 个严重问题**、**8 个高优先级问题** 和 **12 个中低优先级问题**，需要优先处理。

---

## 1. 数据库设计

### 1.1 架构设计 - 良好 ✅

**当前架构**:
- **DolphinDB**: 时间序列数据表 (TSDB 引擎)
  - `sync_daily_data`, `sync_daily_basic`, `sync_adj_factor`, `sync_index_daily`, `sync_moneyflow`
  - `factor_values` (三维组合分区: HASH(factor_id,20) + RANGE(trade_date,季度) + HASH(ts_code,10))
- **PostgreSQL**: 元数据、配置、调度、结果表
  - 配置表: `sync_task_configs`, `etl_task_configs`, `factor_configs`, `factor_field_mappings`
  - 调度表: `flow_configs`, `flow_runs`, `task_runs`
  - 参考数据: `stocks`, `trading_calendar`, `index_configs`, `user_preferences`
  - 结果表: `factor_analysis_results`, `backtest_results`

**优点**:
- 时序数据和关系数据分离存储，各取所长
- `factor_values` 表分区策略设计合理，24,000 个分区能有效裁剪查询范围

### 1.2 规范化问题 - 严重 🔴

**问题 1.2.1: JSON 字段滥用，违反第一范式**

| 表 | 字段 | 问题 |
|---|---|---|
| `flow_configs` | `tasks`, `tags` | JSONB 存储任务 DAG，无法建立索引和约束 |
| `sync_task_configs` | `params_json`, `schema_json`, `primary_keys_json` | 应使用关联表 |
| `etl_task_configs` | `primary_keys_json` | 应使用关联表 |
| `factor_configs` | `depends_on`, `params` | 应使用关联表 |
| `factor_analysis_results` | `periods`, `ic_summary`, `ic_by_period`, `config` | JSON 存储分析结果 |
| `backtest_results` | `metrics_json`, `equity_curve_json`, `trades_json` | JSON 存储回测结果 |

**建议**:
- 对于配置类 JSON: 拆分成独立关联表，支持查询和约束
- 对于结果类 JSON: 保留，但考虑提取常用查询字段到独立列

---

**问题 1.2.2: 缺少外键约束 - 高 🟠**

PostgreSQL 表之间几乎没有定义外键约束:

```sql
-- 应有但缺失的外键:
flow_runs.flow_name → flow_configs.name
task_runs.flow_run_id → flow_runs.id  (已有 ON DELETE CASCADE)
factor_analysis_results.factor_id → factor_configs.factor_id
sync_task_configs 与其他表无关联
```

**风险**: 数据不一致，存在孤儿记录

---

**问题 1.2.3: 主键设计不一致 - 中 🟡**

| 表 | 主键 | 问题 |
|---|---|---|
| `flow_configs` | `id SERIAL` | 自增主键，有 `name` 唯一键 |
| `sync_task_configs` | `task_id VARCHAR(255)` | 业务主键 |
| `etl_task_configs` | `task_id VARCHAR(255)` | 业务主键 |
| `factor_configs` | `factor_id VARCHAR(255)` | 业务主键 |
| `stocks` | `ts_code VARCHAR(20)` | 业务主键 |
| `trading_calendar` | `(exchange, cal_date)` | 复合主键 |
| `factor_analysis_results` | `id BIGSERIAL` | 自增主键，有 `(factor_id, analysis_date)` 唯一键 |

**建议**: 统一使用 `BIGSERIAL` 代理主键，业务键加 `UNIQUE` 约束

---

### 1.3 冗余字段 - 中 🟡

**问题 1.3.1: `task_runs` 表字段冗余**

```sql
-- 003_migrate_dolphindb_tables.sql 中合并了 DolphinDB 字段:
task_runs (
  run_id VARCHAR(255),        -- 与 id 重复
  task_name TEXT DEFAULT '',   -- 可从 task_id 推断
  rows INT DEFAULT 0,          -- 监控字段应在独立表
  elapsed_sec FLOAT,           -- 监控字段应在独立表
  params TEXT DEFAULT '',      -- 应使用关联表
  extra TEXT DEFAULT '',       -- 未定义用途
  finished_at TIMESTAMPTZ      -- 与 ended_at 重复
)
```

---

## 2. 性能优化

### 2.1 索引设计 - 高 🟠

**问题 2.1.1: 外键列缺少索引**

```sql
-- 缺少的索引:
CREATE INDEX idx_flow_r_parent_flow_run_id ON flow_runs(parent_flow_run_id);
CREATE INDEX idx_far_factor_id ON factor_analysis_results(factor_id);  -- 已有
CREATE INDEX idx_br_task_id ON backtest_results(task_id);  -- 已有
```

**问题 2.1.2: DolphinDB 索引未明确定义**

- `metadata_manager.py` 仅定义主键，未定义二级索引
- 缺少查询模式分析，无法评估索引有效性

---

### 2.2 查询优化 - 严重 🔴

**问题 2.2.1: SQL 注入漏洞 (已知问题)**

根据 `CLAUDE.md`，`data_merged.py` 中有 **18+ 处 f-string SQL 拼接**:

```python
# 错误示例 (推测):
df = db_client.query(f"SELECT * FROM {table} WHERE ts_code = '{ts_code}'")

# 正确做法:
df = db_client.query("SELECT * FROM %s WHERE ts_code = %s", (table, ts_code))
```

---

### 2.3 分区策略 - 良好 ✅

**`factor_values` 三维分区设计**:
- HASH(factor_id, 20) + RANGE(trade_date, 季度) + HASH(ts_code, 10)
- 总分区数: 24,000
- 查询裁剪效果:
  - 按股票查询: ~10 个分区
  - 按日期查询: ~200 个分区
  - 按因子查询: ~1200 个分区

---

## 3. 数据完整性

### 3.1 约束检查 - 高 🟠

**问题 3.1.1: 缺少 CHECK 约束**

```sql
-- 建议添加:
ALTER TABLE flow_runs ADD CHECK (status IN ('pending','running','success','failed','cancelled'));
ALTER TABLE task_runs ADD CHECK (status IN ('pending','running','success','failed'));
ALTER TABLE task_runs ADD CHECK (task_type IN ('sync','etl','factor','flow'));
ALTER TABLE sync_task_configs ADD CHECK (sync_type IN ('incremental','full'));
ALTER TABLE trading_calendar ADD CHECK (is_open IN (0, 1));
```

**问题 3.1.2: 缺少 NOT NULL 约束**

```sql
-- 很多字段定义为 DEFAULT '' 但没有 NOT NULL
-- 建议明确 NOT NULL 约束
```

---

### 3.2 触发器和级联 - 中 🟡

**问题 3.2.1: 无自动更新时间戳触发器**

```sql
-- PostgreSQL 需要触发器来自动更新 updated_at:
CREATE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ language 'plpgsql';

-- 应用到所有有 updated_at 的表
CREATE TRIGGER update_flow_configs_updated_at BEFORE UPDATE ON flow_configs
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

**问题 3.2.2: 级联删除策略不完整**

- `task_runs` 有 `ON DELETE CASCADE` (良好)
- 其他父子关系未定义级联策略

---

## 4. 备份和恢复

### 4.1 备份策略 - 严重 🔴

**问题 4.1.1: 缺少自动化备份**

| 项目 | 状态 |
|---|---|
| 自动化定时备份 | ❌ 无 |
| DolphinDB 完整备份 | ❌ 无 |
| PostgreSQL 完整备份 | ❌ 无 |
| 备份保留策略 | ❌ 未定义 |
| 备份完整性校验 | ❌ 无 |

**仅有的备份**:
- `backups-2026-03-08.tar.gz` (手动备份，旧格式)
- `backup_configs.py` (仅备份配置表，从 DolphinDB，已过时)

---

### 4.2 恢复测试 - 高 🟠

**问题 4.2.1: 无恢复测试记录**

- 没有证据表明备份已被验证可恢复
- 没有灾难恢复演练记录

---

### 4.3 灾难恢复计划 - 高 🟠

**问题 4.3.1: 无 DR 文档**

- RTO (恢复时间目标) 未定义
- RPO (恢复点目标) 未定义
- 故障切换流程未定义

---

## 5. 迁移管理

### 5.1 迁移脚本 - 中 🟡

**优点**:
- 有迁移脚本目录 `scripts/migrations/`
- 有 `run_migrations.py` 执行脚本
- 有 v2.0 详细的迁移说明文档

**问题**:

| 问题 | 详情 |
|---|---|
| 迁移版本控制不完整 | 无 `schema_migrations` 表记录已执行的迁移 |
| 迁移顺序依赖文件名 | `001_`, `002_`, `003_` 前缀，但无迁移表 |
| 幂等性不足 | 部分脚本使用 `CREATE TABLE IF NOT EXISTS`，但 ALTER 不幂等 |
| 回滚支持不完整 | v2.0 有回滚脚本，但其他迁移没有 |

**建议**: 使用成熟的迁移工具 (`Alembic`, `Flyway`, `Liquibase`)

---

### 5.2 迁移进行中 - 高 🟠

**问题 5.2.1: 数据迁移未完成**

根据代码分析，系统处于从 DolphinDB 到 PostgreSQL 的迁移中:

```python
# table_manager.py:
_META_TABLES = frozenset({
    "stock_basic",  # 待迁移（stocks）
    "trade_cal",    # 待迁移（trading_calendar）
})
```

**风险**:
- 两个数据库都有同类数据，可能产生数据不一致
- 应用层可能双重写入或读取混乱

---

## 6. 安全问题

### 6.1 认证和授权 - 严重 🔴

**问题 6.1.1: 数据库默认密码**

```python
# init_dolphindb.dos:
try { login("admin", "123456") } catch(ex) { /* 已通过 session 登录 */ }
```

**问题 6.1.2: 缺少行级安全 (RLS)**

- PostgreSQL 表未启用 RLS
- 多租户场景下数据隔离依赖应用层

---

## 7. 具体改进建议 (按优先级)

### 优先级 0 - 立即处理 (严重)

1. **修复 SQL 注入漏洞** - 替换所有 f-string SQL 为参数化查询
2. **修改默认数据库密码** - DolphinDB `admin/123456` 必须修改
3. **建立自动化备份** - PostgreSQL `pg_dump` + DolphinDB 备份工具

### 优先级 1 - 本周处理 (高)

4. **添加外键约束** - 在 PostgreSQL 中定义参照完整性
5. **添加 CHECK 约束** - 状态字段、类型字段添加约束
6. **添加 `updated_at` 触发器** - 自动更新时间戳
7. **完成数据迁移** - 决定 `stock_basic`/`trade_cal` 的位置
8. **创建 `schema_migrations` 表** - 跟踪迁移状态

### 优先级 2 - 本月处理 (中)

9. **重构 JSON 字段** - 配置类 JSON 拆分为关联表
10. **统一主键设计** - 使用 `BIGSERIAL` 代理主键
11. **清理 `task_runs` 冗余字段** - 移除重复字段
12. **添加缺失索引** - 外键列、常用查询列加索引
13. **编写恢复测试计划** - 定期验证备份可恢复性

### 优先级 3 - 规划中 (低)

14. **考虑使用迁移工具** - Alembic 或 Flyway
15. **启用 PostgreSQL RLS** - 行级安全隔离
16. **添加数据版本控制** - 重要配置的变更历史
17. **建立容量监控** - 跟踪表大小和增长趋势

---

## 8. 表结构清单

### PostgreSQL 表 (16 张)

| 表名 | 用途 | 主键 | 已迁移 |
|---|---|---|---|
| `flow_configs` | Flow 配置 | `id SERIAL` | ✅ |
| `flow_runs` | Flow 执行记录 | `id SERIAL` | ✅ |
| `task_runs` | Task 执行记录 | `id SERIAL` | ✅ |
| `sync_task_configs` | 同步任务配置 | `task_id VARCHAR` | ✅ |
| `etl_task_configs` | ETL 任务配置 | `task_id VARCHAR` | ✅ |
| `factor_configs` | 因子配置 | `factor_id VARCHAR` | ✅ |
| `factor_field_mappings` | 因子字段映射 | `field_key VARCHAR` | ✅ |
| `stocks` | 股票基础信息 | `ts_code VARCHAR` | ✅ |
| `trading_calendar` | 交易日历 | `(exchange, cal_date)` | ✅ |
| `index_configs` | 指数配置 | `index_code VARCHAR` | ✅ |
| `user_preferences` | 用户偏好 | `user_id VARCHAR` | ✅ |
| `factor_analysis_results` | 因子分析结果 | `id BIGSERIAL` | ✅ |
| `backtest_results` | 回测结果 | `run_id VARCHAR` | ✅ |
| `index_constituents` | 指数成分股 | `(trade_date, ts_code, index_code)` | ⚠️ 仅 MySQL |
| `index_metadata` | 指数元数据 | `index_code VARCHAR` | ⚠️ 仅 MySQL |
| `factor_analysis_extended` | 因子分析扩展 | `id INT AUTO_INCREMENT` | ⚠️ 仅 MySQL |

### DolphinDB 表 (8 张)

| 表名 | 类型 | 用途 |
|---|---|---|
| `sync_daily_data` | TSDB | 日线行情 |
| `sync_daily_basic` | TSDB | 日线基本指标 |
| `sync_adj_factor` | TSDB | 复权因子 |
| `sync_index_daily` | TSDB | 指数日线 |
| `sync_moneyflow` | TSDB | 资金流向 |
| `factor_values` | TSDB (分区) | 因子值结果 |
| `stock_basic` | 维度 | 股票基础信息 (待迁移) |
| `trade_cal` | 维度 | 交易日历 (待迁移) |

---

## 9. 文件索引

| 项目 | 文件路径 |
|---|---|
| DolphinDB 初始化 | `/backend/database/init_dolphindb.dos` |
| DolphinDB 客户端 | `/backend/infrastructure/database/dolphindb_client.py` |
| 元数据管理 | `/backend/infrastructure/database/metadata_manager.py` |
| 表管理 | `/backend/infrastructure/database/table_manager.py` |
| PostgreSQL 连接 | `/backend/scheduler/db.py` |
| 迁移 001 | `/backend/scripts/migrations/001_create_scheduler_tables.sql` |
| 迁移 003 | `/backend/scripts/migrations/003_migrate_dolphindb_tables.sql` |
| 迁移运行器 | `/backend/scripts/migrations/run_migrations.py` |
| 配置备份脚本 | `/backend/scripts/maintenance/backup_configs.py` |
| v2.0 迁移文档 | `/backend/database/migrations/v2.0/README.md` |

---

**报告生成完成**  
如有疑问，可查看上述文件获取更多细节。