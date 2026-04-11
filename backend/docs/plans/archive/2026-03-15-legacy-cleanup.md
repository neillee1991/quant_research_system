# Legacy Code Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 彻底删除所有对旧接口的依赖，统一使用新架构（FactorComputeService + infrastructure/database）。

**Architecture:** 分6个独立任务并行执行。每个任务聚焦一个文件或模块，互不依赖。完成后删除死代码目录。

**Tech Stack:** Python 3.11, FastAPI, Polars, DolphinDB

---

## 任务总览

| # | 任务 | 文件 | 改动类型 |
|---|------|------|---------|
| 1 | 删除 ProductionEngine 导入 | `factor_compute.py` | 移常量 + 删导入 |
| 2 | 消除 factor_compute_service 的 _session | `factor_compute_service.py` | 替换私有访问 |
| 3 | 消除 factor_compute.py 的 _session | `app/api/v1/production/factor_compute.py` | 替换私有访问 |
| 4 | 消除 factor_config.py 的 _session | `app/api/v1/production/factor_config.py` | 替换私有访问 |
| 5 | 消除 etl_api.py + config_api.py 的 _session | `etl_api.py`, `config_api.py` | 替换私有访问 |
| 6 | 修复测试文件旧导入 | `test_query_builder.py`, `test_connection.py` | 改导入路径 |
| 7 | 删除死代码目录 | `factors_v2/`, `infrastructure/repository/` | 删除 |

---

## Task 1: 删除 factor_compute.py 对 ProductionEngine 的依赖

**文件:**
- Modify: `app/api/v1/production/factor_compute.py`
- Modify: `services/factor_compute_service.py`

**背景:** `factor_compute.py` 导入废弃的 `ProductionEngine` 只为取 `DEFAULT_PREPROCESS` 常量。
需要把这个常量移到 `factor_compute_service.py` 并从那里导入。

**Step 1: 在 factor_compute_service.py 顶部添加常量**

在 `services/factor_compute_service.py` 找到 `class FactorComputeService:` 之前，添加：

```python
# 默认预处理选项（从 ProductionEngine 迁移）
DEFAULT_PREPROCESS = {
    "adjust_price": "forward",
    "filter_st": True,
    "filter_new_stock": True,
    "new_stock_days": 60,
    "handle_suspension": True,
    "mark_limit": True,
}
```

确认 `engine/production/engine.py` 里 `DEFAULT_PREPROCESS` 的值与上面一致（line 84）。

**Step 2: 修改 factor_compute.py 的导入**

找到：
```python
from engine.production.engine import ProductionEngine
```
替换为：
```python
from services.factor_compute_service import DEFAULT_PREPROCESS as _DEFAULT_PREPROCESS
```

**Step 3: 修改 factor_compute.py 中使用常量的地方**

找到（约 line 273）：
```python
opts = {**ProductionEngine.DEFAULT_PREPROCESS, **preprocess_opts}
```
替换为：
```python
opts = {**_DEFAULT_PREPROCESS, **preprocess_opts}
```

**Step 4: 验证**

```bash
cd backend
grep -n "ProductionEngine" app/api/v1/production/factor_compute.py
# 期望：无输出

python -c "from app.api.v1.production.factor_compute import router; print('OK')"
# 期望：OK
```

**Step 5: Commit**

```bash
git add services/factor_compute_service.py app/api/v1/production/factor_compute.py
git commit -m "refactor: move DEFAULT_PREPROCESS to FactorComputeService, remove ProductionEngine import"
```

---

## Task 2: 消除 factor_compute_service.py 的 _session 访问

**文件:**
- Modify: `services/factor_compute_service.py`

**背景:** 两处直接访问 `self.db._session`：
1. Line ~387: 删除 `factor_run_log` 旧记录
2. Line ~650: 插入 `factor_task_run` 运行记录

**Step 1: 修复 _finish_run_record 中的 _session（line ~387）**

找到：
```python
delete_script = f"""
    t = loadTable("dfs://quant", "factor_run_log");
    delete from t where run_id = '{run_id}';
"""
self.db._session.run(delete_script)
```
替换为：
```python
self.db.execute(
    "DELETE FROM factor_run_log WHERE run_id = %s", (run_id,)
)
```

**Step 2: 修复 _insert_run_record 中的 _session（line ~650）**

这段代码用 DolphinDB 原生脚本构建并插入 `factor_task_run` 记录，原因是列顺序必须精确匹配。
改为用 Polars DataFrame + `db.append()`：

找到整个 `with self.db._lock:` 块（约 line 648-680），替换为：

```python
now = datetime.now()
record = pl.DataFrame({
    "factor_id": [factor_id],
    "mode": [mode or ""],
    "status": ["running"],
    "start_date": [start_date or ""],
    "end_date": [end_date or ""],
    "rows_affected": [0],
    "duration_seconds": [0.0],
    "filter_st": [opts.get("filter_st", True)],
    "filter_new_stock": [opts.get("filter_new_stock", True)],
    "new_stock_days": [opts.get("new_stock_days", 60)],
    "handle_suspension": [opts.get("handle_suspension", True)],
    "mark_limit": [opts.get("mark_limit", True)],
    "adjust_price": [opts.get("adjust_price", "none")],
    "preprocess": [opts_str],
    "run_id": [run_id],
    "error_message": [""],
    "created_at": [now],
})
self.db.append("factor_task_run", record)
```

**Step 3: 验证**

```bash
cd backend
grep -n "_session" services/factor_compute_service.py
# 期望：无输出

python -c "from services.factor_compute_service import FactorComputeService; print('OK')"
# 期望：OK
```

**Step 4: Commit**

```bash
git add services/factor_compute_service.py
git commit -m "refactor: replace db._session direct access in FactorComputeService"
```

---

## Task 3: 消除 factor_compute.py 的 _session 访问

**文件:**
- Modify: `app/api/v1/production/factor_compute.py`

**背景:** 测试因子时用 `db_client._session` 上传临时表、执行 `stat()`、清理临时表。
这段逻辑可以完全用 Polars 替代，不需要 DolphinDB 的 `stat()` 函数。

**Step 1: 找到 _session 使用的代码块（约 line 375-415）**

整段逻辑是：上传 DataFrame → 用 DolphinDB `stat()` 计算统计 → 清理。
已有 fallback 用 Polars 计算统计。直接删除 DolphinDB stat 路径，只保留 Polars 路径：

找到整个 `try` 块（从 `import pandas as pd` 到 `except Exception as e:`），替换为：

```python
# 用 Polars 直接计算统计（不需要 DolphinDB 临时表）
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
```

**Step 2: 验证**

```bash
cd backend
grep -n "_session" app/api/v1/production/factor_compute.py
# 期望：无输出

python -c "from app.api.v1.production.factor_compute import router; print('OK')"
# 期望：OK
```

**Step 3: Commit**

```bash
git add app/api/v1/production/factor_compute.py
git commit -m "refactor: replace DolphinDB stat() with Polars in factor test endpoint"
```

---

## Task 4: 消除 factor_config.py 的 _session 访问

**文件:**
- Modify: `app/api/v1/production/factor_config.py`

**背景:** 三处 `_session.run()`：
1. Line ~432: 删除 `index_constituents` 表中的数据
2. Line ~438: 删除 `index_metadata` 表中的数据
3. Line ~498: 查询表的 schema

**Step 1: 修复 delete_index_pool 中的两处 _session（line ~432, 438）**

找到：
```python
db_client._session.run(f"""
    constituents_table = loadTable("dfs://quant", "index_constituents");
    delete from constituents_table where index_code = "{index_code}";
""")

db_client._session.run(f"""
    metadata_table = loadTable("dfs://quant", "index_metadata");
    delete from metadata_table where index_code = "{index_code}";
""")
```
替换为：
```python
db_client.execute(
    "DELETE FROM index_constituents WHERE index_code = %s", (index_code,)
)
db_client.execute(
    "DELETE FROM index_metadata WHERE index_code = %s", (index_code,)
)
```

**Step 2: 修复 schema 查询（line ~498）**

找到：
```python
schema_result = db_client._session.run(f'schema(loadTable("dfs://quant", "{dep}"))')
col_defs = schema_result.get('colDefs')
if col_defs is not None and not col_defs.empty:
    for _, row in col_defs.iterrows():
```
替换为：
```python
schema_df = db_client.query(
    f"SELECT name, typeString FROM schema(loadTable('dfs://quant', '{dep}')).colDefs"
)
if not schema_df.is_empty():
    for row in schema_df.to_dicts():
        col_name = row["name"]
        # 原来用 row["name"] 和 row["typeString"]，保持一致
```

注意：需要检查原来循环体里用的字段名，确保替换后逻辑一致。

**Step 3: 验证**

```bash
cd backend
grep -n "_session" app/api/v1/production/factor_config.py
# 期望：无输出

python -c "from app.api.v1.production.factor_config import router; print('OK')"
# 期望：OK
```

**Step 4: Commit**

```bash
git add app/api/v1/production/factor_config.py
git commit -m "refactor: replace db._session direct access in factor_config API"
```

---

## Task 5: 消除 etl_api.py 和 config_api.py 的 _session 访问

**文件:**
- Modify: `app/api/v1/data/etl_api.py`
- Modify: `app/api/v1/data/config_api.py`

**背景:**
- `etl_api.py` 4处：2处 `existsTable`，2处 `schema()`
- `config_api.py` 1处：删除 `sync_task_config` 旧记录

**Step 1: 修复 config_api.py（line ~205）**

找到：
```python
db_path = db_client._conn.db_path
db_client._session.run(f"""
    handle = loadTable("{db_path}", "sync_task_config");
    delete from handle where task_id = "{task_id}";
""")
```
替换为：
```python
db_client.execute(
    "DELETE FROM sync_task_config WHERE task_id = %s", (task_id,)
)
```

**Step 2: 修复 etl_api.py 的 existsTable（line ~178, ~771）**

找到（两处）：
```python
db_path = db_client._db_path
exists = db_client._session.run(f"existsTable('{db_path}', '{table_name}')")
```
替换为：
```python
exists = db_client.table_exists(table_name)
```

**Step 3: 修复 etl_api.py 的 schema 查询（line ~425, ~828）**

找到（两处）：
```python
db_path = db_client._resolve_db_path(table_name)
schema_info = db_client._session.run(f"schema(loadTable('{db_path}', '{table_name}'))")
table_schema = {}
if isinstance(schema_info, dict) and "colDefs" in schema_info:
    col_defs = schema_info["colDefs"]
    if isinstance(col_defs, pd.DataFrame) and "name" in col_defs.columns:
        table_cols = col_defs["name"].tolist()
```
替换为：
```python
schema_df = db_client.query(
    f"SELECT name, typeString FROM schema(loadTable('dfs://quant', '{table_name}')).colDefs"
)
table_cols = schema_df["name"].to_list() if not schema_df.is_empty() else []
```

注意：检查原来代码里 `table_schema` 和 `table_cols` 的后续用法，确保替换后逻辑一致。

**Step 4: 验证**

```bash
cd backend
grep -n "_session\|_db_path\|_conn\." app/api/v1/data/etl_api.py
grep -n "_session\|_conn\." app/api/v1/data/config_api.py
# 期望：无输出（或只有注释）

python -c "from app.api.v1.data.etl_api import router; print('OK')"
python -c "from app.api.v1.data.config_api import router; print('OK')"
# 期望：OK
```

**Step 5: Commit**

```bash
git add app/api/v1/data/etl_api.py app/api/v1/data/config_api.py
git commit -m "refactor: replace db._session direct access in etl_api and config_api"
```

---

## Task 6: 修复测试文件的旧导入路径

**文件:**
- Modify: `tests/test_query_builder.py`
- Modify: `tests/test_connection.py`

**Step 1: 修复 test_query_builder.py**

找到：
```python
from store.dolphindb.query_builder import QueryBuilder
from store.dolphindb.connection import DolphinDBConnection
```
替换为：
```python
from infrastructure.database.query_builder import QueryBuilder
from infrastructure.database.connection import DolphinDBConnection
```

**Step 2: 修复 test_connection.py**

找到：
```python
from store.dolphindb.connection import DolphinDBConnection
```
替换为：
```python
from infrastructure.database.connection import DolphinDBConnection
```

**Step 3: 验证**

```bash
cd backend
python -c "import tests.test_query_builder; print('OK')" 2>&1 | head -5
python -c "import tests.test_connection; print('OK')" 2>&1 | head -5
```

**Step 4: Commit**

```bash
git add tests/test_query_builder.py tests/test_connection.py
git commit -m "fix: update test imports from store.dolphindb to infrastructure.database"
```

---

## Task 7: 删除死代码目录

**文件:**
- Delete: `factors_v2/` 目录（未被任何生产代码引用）
- Delete: `infrastructure/repository/` 目录（未被任何生产代码引用）

**Step 1: 确认无生产代码引用**

```bash
cd backend
grep -rn "factors_v2\|FactorDataRepository\|MarketDataRepository" --include="*.py" . \
  | grep -v "factors_v2/\|repository/\|USAGE_EXAMPLES\|scripts/migrate_factor"
# 期望：无输出
```

**Step 2: 删除目录**

```bash
cd backend
rm -rf factors_v2/
rm -rf infrastructure/repository/
```

**Step 3: 验证系统启动正常**

```bash
cd backend
python -c "from app.main import app; print('OK')"
# 期望：OK（无 ImportError）
```

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: delete unused factors_v2 and infrastructure/repository directories"
```

---

## 最终验证

所有任务完成后，运行全量检查：

```bash
cd backend

echo "=== 检查无 ProductionEngine 导入（除 engine.py 本身）==="
grep -rn "from engine.production.engine import ProductionEngine" --include="*.py" . \
  | grep -v "engine/production/engine.py\|engine/production/__init__.py\|scripts/"

echo "=== 检查无 _session 直接访问（除 infrastructure 内部）==="
grep -rn "\._session\." --include="*.py" . \
  | grep -v "infrastructure/database/\|tests/test_alphalens"

echo "=== 检查无旧 store.dolphindb 子目录导入 ==="
grep -rn "from store\.dolphindb\." --include="*.py" . \
  | grep -v "tests/test_alphalens"

echo "=== 检查系统可正常导入 ==="
python -c "from app.main import app; print('app import OK')"
```

期望：所有检查无输出，最后一行打印 `app import OK`。
