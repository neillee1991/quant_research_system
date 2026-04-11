# 统一任务日志系统重构 - 实施方案

**日期**: 2026-03-29
**状态**: 实施中
**风险接受**: 已知增量同步逻辑在失败场景下不会重试失败日期，与当前行为一致

---

## 一、目标

### 1.1 核心目标
- ✅ 解决代码冗余：删除双写逻辑
- ✅ 数据一致性：统一使用 `task_runs` 表
- ✅ 前端单一接口：只调用 `/tasks/*`
- ✅ 新任务类型统一框架：回测、因子分析等直接用 `task_runs`

### 1.2 非目标
- ❌ 不修复增量同步失败重试逻辑（与当前行为一致）
- ❌ 不迁移历史数据（可接受丢失）

---

## 二、数据表处理

| 表 | 处理方式 |
|----|---------|
| `sync_log` | **删除** |
| `sync_log_history` | **删除** |
| `factor_run_log` | **删除** |
| `task_runs` | **唯一执行日志表** - 所有任务类型共用 |

---

## 三、last_date 实时计算方案

### 3.1 实现逻辑

```python
# SyncLogManager.get_last_sync_date() 改为：
def get_last_sync_date(self, task_id: str) -> Optional[str]:
    """从目标表实时计算最后同步日期"""
    # 1. 从 sync_task_config 获取 table_name, date_field
    # 2. 查询 MAX(date_field) FROM table_name
    # 3. 格式化为 YYYYMMDD
```

### 3.2 风险与当前一致

- 当前逻辑：某天失败后，`last_date` 仍前进，不会重试
- 新逻辑：某天失败后，目标表无那天数据，`MAX` 不前进，**会自动重试**？

**等待确认**：需要再仔细看 upsert 逻辑是先删后写吗？

---

## 四、实施清单

### 后端
- [ ] 修改 `SyncLogManager.get_last_sync_date()` 实时计算
- [ ] 删除 `SyncLogManager.update_sync_log()` 方法
- [ ] 删除所有 `update_sync_log()` 调用
- [ ] 删除 `_etl_log_sync()` 函数及调用
- [ ] 删除 Factor 任务中写 `factor_run_log` 的代码
- [ ] 删除旧 API 端点 (`/data/sync/status*`, `/data/etl/logs`)
- [ ] 扩展 `/tasks/history` 支持 `task_type`/`task_id` 过滤

### 前端
- [ ] 修改 `useSyncTasks.ts` 改用 `taskApi`
- [ ] 修改 `useETLTasks.ts` 改用 `taskApi`
- [ ] 修改 `SyncTaskDrawer.tsx` 改用 `taskApi`
- [ ] 修改 `ETLTaskDrawer.tsx` 改用 `taskApi`
- [ ] 修改 `DataCenter/index.tsx` 改用 `taskApi`
- [ ] 删除 `api/index.ts` 中的旧方法

### 测试
- [ ] 测试 Sync 增量同步
- [ ] 测试 ETL 任务执行
- [ ] 测试 Factor 任务执行
- [ ] 测试任务监控页面
- [ ] 测试任务历史查询

---

## 五、团队分工

| 角色 | 负责人 | 任务 |
|------|--------|------|
| 后端工程师 | | 后端修改 |
| 前端工程师 | | 前端修改 |
| 数据库工程师 | | 表删除（延后） |
| QA 测试 | | 测试验证 |

---

## 六、回滚方案

用户确认：**不需要回滚**，可接受日志丢失。
