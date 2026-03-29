# 统一任务日志系统重构方案

**日期**: 2026-03-29
**状态**: 待实施
**类型**: 架构重构

---

## 一、现状分析

### 1.1 当前架构问题

| 任务类型 | 专用日志表 | 统一日志表 | 问题 |
|---------|-----------|-----------|------|
| **Sync** | `sync_log` + `sync_log_history` | ✅ `task_runs` | 双写，数据冗余 |
| **ETL** | `sync_log` + `sync_log_history` | ✅ `task_runs` | 双写，数据冗余 |
| **Factor** | `factor_run_log` | ✅ `task_runs` | 双写，数据冗余 |

### 1.2 关键依赖分析

#### `sync_log` 表的核心用途
- ❌ **增量同步锚点** (`last_date` 字段)
  - ✅ **替代方案**：从目标表 `SELECT MAX(date_field)` 实时计算

#### 各任务类型的日志写入点
| 任务类型 | 写入位置 |
|---------|---------|
| Sync | `SyncLogManager.update_sync_log()` |
| ETL | `_etl_log_sync()` (etl_api.py) |
| Factor | `factor_run_log` 表写入 |

---

## 二、重构目标

### 2.1 核心目标
- **统一日志存储**：所有任务类型只使用 `task_runs` 表
- **简化代码**：删除双写逻辑
- **last_date 实时化**：从目标表实时计算，不存储

### 2.2 非目标
- 不迁移历史数据（用户确认可接受）
- 不保留兼容层（直接修改前端）

---

## 三、实施方案

### 阶段 1：后端重构（优先）

#### 1.1 修改 Sync 任务
| 文件 | 修改内容 |
|------|---------|
| `data_manager/sync_components.py` | `SyncLogManager.get_last_sync_date()` 改为实时计算 |
| `data_manager/sync_components.py` | 删除 `SyncLogManager.update_sync_log()` 调用 |
| `data_manager/refactored_sync_engine.py` | 同上 |

#### 1.2 修改 ETL 任务
| 文件 | 修改内容 |
|------|---------|
| `app/api/v1/data/etl_api.py` | 删除 `_etl_log_sync()` 调用 |

#### 1.3 修改 Factor 任务
| 文件 | 修改内容 |
|------|---------|
| `app/services/factor_compute_service.py` | 停止写 `factor_run_log` |
| `app/api/v1/production/factor_compute.py` | 同上 |

#### 1.4 删除旧 API 端点
| 端点 | 说明 |
|------|------|
| `GET /data/sync/status` | 删除 |
| `GET /data/sync/status/{task_id}` | 删除 |
| `GET /data/sync/tasks/status-batch` | 删除 |
| `GET /data/etl/logs` | 删除 |

---

### 阶段 2：前端调整

#### 2.1 Sync 任务页面
| 文件 | 修改内容 |
|------|---------|
| `api/index.ts` | 删除 `getSyncStatus`, `getTaskStatus` (sync版), `getTaskStatusBatch` |
| `pages/DataCenter/SyncTaskDrawer.tsx` | 改用 `taskApi` |
| `pages/DataCenter/hooks/useSyncTasks.ts` | 改用 `taskApi` |
| `pages/DataCenter/index.tsx` | 改用 `taskApi` |

#### 2.2 ETL 任务页面
| 文件 | 修改内容 |
|------|---------|
| `api/index.ts` | 删除 `getEtlLogs` |
| `pages/DataCenter/ETLTaskDrawer.tsx` | 改用 `taskApi` |
| `pages/DataCenter/hooks/useETLTasks.ts` | 改用 `taskApi` |

#### 2.3 Factor 任务页面
| 文件 | 修改内容 |
|------|---------|
| 【需确认】 | 改用 `taskApi` |

---

### 阶段 3：清理（可选，延后）

| 操作 | 说明 |
|------|------|
| 删除 `sync_log` 表 | 确认无使用后 |
| 删除 `sync_log_history` 表 | 确认无使用后 |
| 删除 `factor_run_log` 表 | 确认无使用后 |

---

## 四、实施清单

### 后端
- [ ] 修改 `SyncLogManager.get_last_sync_date()` 实时计算
- [ ] 移除 Sync 任务中的 `update_sync_log()` 调用
- [ ] 移除 ETL 任务中的 `_etl_log_sync()` 调用
- [ ] 移除 Factor 任务中的 `factor_run_log` 写入
- [ ] 删除旧的同步日志 API 端点
- [ ] 删除旧的 ETL 日志 API 端点

### 前端
- [ ] 修改 Sync 任务相关页面改用 `taskApi`
- [ ] 修改 ETL 任务相关页面改用 `taskApi`
- [ ] 修改 Factor 任务相关页面改用 `taskApi`
- [ ] 删除旧的 API 调用方法

### 测试
- [ ] 测试 Sync 增量同步（验证 last_date 实时计算）
- [ ] 测试 ETL 任务执行
- [ ] 测试 Factor 任务执行
- [ ] 测试任务监控页面
- [ ] 测试任务历史查看

---

## 五、风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| last_date 实时计算性能 | 中 | 目标表有日期索引，查询很快 |
| 前端修改遗漏 | 中 | 完整的清单跟踪 |
| 增量同步出错 | 高 | 先在测试环境验证 |

---

## 六、回滚方案

用户已确认**不需要回滚**，可接受日志丢失。
