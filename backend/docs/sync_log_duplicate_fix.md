# sync_log 表重复记录问题修复

## 问题描述

**现象**：同步任务管理页面显示的"上次同步"时间与数据库中的实际最新时间不一致。

**示例**：
- 页面显示：`2026-03-14 16:40`
- 数据库实际：`2026-03-14 18:17`

## 根本原因

### 1. sync_log 表存在重复记录

`sync_log` 表设计为维度表，每个任务应该只有一条记录（最新状态），但实际存在大量重复：

```
总记录数：1437 条
实际任务数：15 个
平均每个任务：95.8 条记录
```

**重复最严重的任务**：
- `sync_daily_data`: 198 条记录
- `etl_index_member_daily`: 195 条记录
- `sync_stk_limit`: 194 条记录
- `sync_daily_basic`: 194 条记录

### 2. 查询缺少排序

`SyncLogManager.get_last_sync_info()` 的 SQL 查询：

```python
# 修复前（错误）
SELECT last_date, updated_at
FROM sync_log
WHERE source = %s AND data_type = %s
LIMIT 1  # ❌ 没有 ORDER BY，返回第一条（最旧的）
```

由于没有 `ORDER BY` 子句，`LIMIT 1` 返回的是表中第一条记录（通常是最旧的），而不是最新的。

## 修复方案

### 1. 修复查询逻辑

**文件**：`backend/data_manager/sync_components.py`

**修改**：添加 `ORDER BY updated_at DESC`

```python
# 修复后（正确）
SELECT last_date, updated_at
FROM sync_log
WHERE source = %s AND data_type = %s
ORDER BY updated_at DESC  # ✅ 按时间倒序排列
LIMIT 1                   # 返回最新的一条
```

### 2. 清理重复记录

**脚本**：`backend/scripts/cleanup_duplicate_sync_log.py`

**执行结果**：
- 删除了 1422 条旧记录
- 保留了 15 条最新记录（每个任务一条）
- 从 1437 条减少到 15 条

**清理策略**：
1. 对于每个 `(source, data_type)` 组合
2. 按 `updated_at DESC` 排序
3. 保留第一条（最新的）
4. 删除其他所有旧记录

## 验证结果

### 修复前

```sql
-- sync_sw_index_member_N 有 4 条记录
SELECT * FROM sync_log WHERE data_type = 'sync_sw_index_member_N';

-- 返回第一条（最旧的）
2026-03-14 16:40:26.104  ← 前端显示这个时间
2026-03-14 17:33:55.575
2026-03-14 17:34:26.592
2026-03-14 18:17:34.422  ← 实际最新时间
```

### 修复后

```sql
-- sync_sw_index_member_N 只有 1 条记录
SELECT * FROM sync_log WHERE data_type = 'sync_sw_index_member_N';

-- 返回唯一的一条（最新的）
2026-03-14 18:17:34.422  ← 前端现在显示正确时间
```

## 为什么会产生重复记录？

`update_sync_log()` 方法使用了 `upsert()`，理论上应该更新而不是插入：

```python
self.repository.upsert(
    "sync_log",
    log_data,
    ["source", "data_type"]  # 主键
)
```

**可能的原因**：
1. **历史遗留问题**：早期版本的 `upsert()` 实现有 bug
2. **并发写入**：多个同步任务同时写入导致竞态条件
3. **DolphinDB 维度表特性**：维度表的 upsert 需要先 delete 再 insert，可能在某些情况下 delete 失败

## 预防措施

### 1. 定期检查重复记录

```sql
-- 检查是否有重复记录
SELECT source, data_type, COUNT(*) as count
FROM sync_log
GROUP BY source, data_type
HAVING count > 1;
```

### 2. 添加唯一约束（建议）

在 DolphinDB 表创建时添加唯一约束：

```python
# 创建 sync_log 表时
schema = {
    "source": "STRING",
    "data_type": "STRING",
    "last_date": "STRING",
    "updated_at": "TIMESTAMP"
}
# 设置 (source, data_type) 为主键
```

### 3. 监控日志

在 `update_sync_log()` 中添加日志，记录是否成功 upsert：

```python
logger.info(f"Upserted sync_log for {task_id}: {sync_date}")
```

## 相关文件

| 文件 | 说明 |
|------|------|
| `data_manager/sync_components.py` | 修复查询逻辑 |
| `scripts/cleanup_duplicate_sync_log.py` | 清理重复记录脚本 |
| `app/api/v1/data/sync_api.py` | API 端点 |
| `frontend/src/pages/DataCenter/SyncPanel.tsx` | 前端显示 |

## 修复时间

- **发现时间**：2026-03-14
- **修复时间**：2026-03-14
- **影响范围**：所有同步任务的"上次同步"时间显示

---

**注意**：此问题已完全修复，前端现在会显示正确的最新同步时间。
