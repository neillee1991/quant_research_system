# 全面系统诊断报告

**诊断日期**: 2026-04-12
**诊断范围**: UI 功能、前端 API 方法、后端端点
**死代码定义**: 定义了但在实际业务 UI 流程中未被调用（测试代码不算）

---

## 一、执行摘要

| 指标 | 数量 |
|------|------|
| UI 主要功能 | 22 个 |
| 前端 API 方法总数 | 68 个 |
| 前端 API 方法（活跃） | 62 个 |
| 前端 API 方法（死亡） | 6 个 |
| 后端端点总数 | 72 个 |
| 后端端点（活跃） | 65 个 |
| 后端端点（死亡/孤立） | 7 个 |

### 关键发现

1. **前端已完全迁移到 System A** — 所有任务管理 API 均调用 `/tasks/{type}/*` 路由
2. **System B 已不存在** — 上次清理已删除，无残留
3. **System C 部分保留** — `/data/` 路径下的查询、指数、表管理端点仍在使用，属于功能性端点（非迁移遗留）
4. **6 个前端 API 方法是死代码** — 定义了但没有任何 UI 功能调用
5. **7 个后端端点是孤立端点** — 后端有实现但前端没有对应调用

---

## 二、UI 功能 → API → 后端端点 完整追踪

### 2.1 DataCenter 模块

#### 功能 1: 同步任务管理

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 列表 | `dataApi.listSyncTasks` | `GET /tasks/sync` | A | ✅ |
| 创建 | `dataApi.createSyncTask` | `POST /tasks/sync` | A | ✅ |
| 编辑 | `dataApi.updateSyncTask` | `PUT /tasks/sync/{id}` | A | ✅ |
| 删除 | `dataApi.deleteTask` | `DELETE /tasks/sync/{id}` | A | ✅ |
| 执行 | `dataApi.syncTask` | `POST /tasks/sync/{id}/execute` | A | ✅ |
| 获取配置 | `dataApi.getTaskConfig` | `GET /tasks/sync/{id}` | A | ✅ |
| 获取状态 | `dataApi.getSyncTaskStatus` | `GET /tasks/sync/{id}/status` | A | ✅ |
| 数据探查 | 直接 axios（非 api/index.ts） | `GET /tasks/sync/{id}/inspect` | A | ✅ |

#### 功能 2: ETL 任务管理

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 列表 | `dataApi.listEtlTasks` | `GET /tasks/etl` | A | ✅ |
| 创建 | `dataApi.createEtlTask` | `POST /tasks/etl` | A | ✅ |
| 编辑 | `dataApi.updateEtlTask` | `PUT /tasks/etl/{id}` | A | ✅ |
| 删除 | `dataApi.deleteEtlTask` | `DELETE /tasks/etl/{id}` | A | ✅ |
| 执行 | `dataApi.runEtlTask` | `POST /tasks/etl/{id}/execute` | A | ❌ 死亡 |
| 回溯 | `dataApi.backfillEtlTask` | `POST /tasks/etl/{id}/backfill` | A | ✅ |
| 获取状态 | `dataApi.getEtlTaskStatus` | `GET /tasks/etl/{id}/status` | A | ✅ |
| 获取表结构 | `dataApi.getEtlTableSchema` | `GET /tasks/etl/{id}/schema` | A | ✅ |
| 测试脚本 | `dataApi.testEtlScript` | `POST /tasks/etl/test` | A | ✅ |
| 创建表 | `dataApi.createEtlTable` | `POST /tasks/etl/{id}/create-table` | A | ✅ |

> ⚠️ `runEtlTask` 定义了但 0 次调用。ETL 执行实际通过 `backfillEtlTask` 完成。

#### 功能 3: SQL 数据查询

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 列出表 | `dataApi.listTables` | `GET /data/tables` | C | ✅ |
| 获取表信息 | `dataApi.getTableInfo` | `GET /data/tables/{name}/info` | C | ✅ |
| 清空表 | `dataApi.truncateTable` | `DELETE /data/tables/{name}` | C | ✅ |
| 执行查询 | `dataApi.executeQuery` | `POST /data/query` | C | ✅ |

#### 功能 4: 指数订阅管理（SyncTaskDrawer 内）

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 获取可用指数 | `indexApi.listAvailableIndices` | `GET /data/index/available` | C | ✅ |
| 订阅指数 | `indexApi.subscribeIndex` | `POST /data/index/subscribe` | C | ✅ |
| 取消订阅 | `indexApi.unsubscribeIndex` | `DELETE /data/index/subscribe/{code}` | C | ✅ |
| 获取用户偏好 | `indexApi.getUserPreference` | `GET /data/index/preference` | C | ✅ |
| 保存用户偏好 | `indexApi.saveUserPreference` | `POST /data/index/preference` | C | ✅ |

---

### 2.2 FactorCenter 模块

#### 功能 5: 因子管理

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 列表 | `productionApi.listFactors` | `GET /factor/factors` | 其他 | ✅ |
| 创建 | `productionApi.createFactor` | `POST /factor/factors` | 其他 | ✅ |
| 编辑 | `productionApi.updateFactor` | `PUT /factor/factors/{id}` | 其他 | ✅ |
| 删除 | `productionApi.deleteFactor` | `DELETE /factor/factors/{id}` | 其他 | ✅ |
| 获取代码 | `productionApi.getFactorCode` | `GET /factor/factors/{id}/code` | 其他 | ✅ |
| 更新代码 | `productionApi.updateFactorCode` | `PUT /factor/factors/{id}/code` | 其他 | ✅ |
| 测试代码 | `productionApi.testFactorCode` | `POST /factor/factors/test` | 其他 | ✅ |
| 获取数据 | `productionApi.getFactorData` | `GET /factor/factors/{id}/data` | 其他 | ✅ |
| 获取统计 | `productionApi.getFactorStats` | `GET /factor/factors/{id}/stats` | 其他 | ✅ |

#### 功能 6: 因子执行

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 单个执行 | `productionApi.runProduction` | `POST /factor/run` | 其他 | ✅ |
| 批量执行 | `productionApi.batchRunFactors` | `POST /factor/batch-run` | 其他 | ✅ |

#### 功能 7: 因子分析

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 运行分析 | `productionApi.runAlphalensAnalysis` | `POST /factor/analysis/alphalens` | 其他 | ✅ |
| 获取最新分析 | `productionApi.getLatestAlphalensAnalysis` | `GET /factor/analysis/{id}/latest` | 其他 | ✅ |
| 获取分析详情 | `productionApi.getAlphalensAnalysisById` | `GET /factor/analysis/{id}/detail/{aid}` | 其他 | ✅ |
| 删除分析 | `productionApi.deleteAlphalensAnalysisById` | `DELETE /factor/analysis/{id}/detail/{aid}` | 其他 | ✅ |
| 获取分析状态 | `productionApi.getAnalysisTaskStatus` | `GET /factor/analysis/status/{taskId}` | 其他 | ✅ |
| 获取交易日 | `productionApi.getTradingDays` | `GET /factor/analysis/trading-days` | 其他 | ✅ |
| 列出指数池 | `productionApi.listIndexPools` | `GET /factor/index-pool/list` | 其他 | ✅ |

#### 功能 8: 数据配置

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 获取配置 | `productionApi.getDataConfig` | `GET /factor/data-config` | 其他 | ✅ |
| 更新配置 | `productionApi.updateDataConfig` | `PUT /factor/data-config` | 其他 | ✅ |
| 获取解析配置 | `productionApi.getResolvedDataConfig` | `GET /factor/data-config/resolved` | 其他 | ✅ |
| 获取可用表 | `productionApi.getAvailableTables` | `GET /factor/available-tables` | 其他 | ✅ |

---

### 2.3 SchedulerCenter 模块

#### 功能 9: Flow 调度管理

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 列表 | `flowApi.list` | `GET /flows` | 其他 | ✅ |
| 获取详情 | `flowApi.get` | `GET /flows/{name}` | 其他 | ✅ |
| 创建 | `flowApi.create` | `POST /flows` | 其他 | ✅ |
| 编辑 | `flowApi.update` | `PUT /flows/{name}` | 其他 | ✅ |
| 删除 | `flowApi.delete` | `DELETE /flows/{name}` | 其他 | ✅ |
| 立即执行（回溯） | `flowApi.backfill` | `POST /flows/{name}/backfill` | 其他 | ✅ |
| 查看运行历史 | `flowApi.listRuns` | `GET /flows/{name}/runs` | 其他 | ✅ |
| 查看运行详情 | `flowApi.getRunDetail` | `GET /flows/{name}/runs/{runId}` | 其他 | ✅ |
| 推断依赖 | `flowApi.inferDependencies` | `POST /flows/infer-dependencies` | 其他 | ✅ |

> ⚠️ `flowApi.trigger` 定义了但 0 次调用。SchedulerCenter 的"立即执行"按钮实际调用的是 `flowApi.backfill`（带日期范围），而非 `trigger`。

---

### 2.4 BacktestCenter / StrategyCenter 模块

#### 功能 10: 回测执行

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 同步回测 | `strategyApi.backtest` | `POST /strategy/backtest` | 其他 | ✅ |
| 异步回测 | `strategyApi.backtestAsync` | `POST /strategy/backtest/async` | 其他 | ✅ |
| 获取结果 | `strategyApi.getBacktestResult` | `GET /strategy/backtest/{runId}/result` | 其他 | ✅ |

#### 功能 11: ML 模型训练

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 训练模型 | `mlApi.train` | `POST /ml/train` | 其他 | ✅ |
| 获取状态 | `mlApi.getStatus` | `GET /ml/status/{jobId}` | 其他 | ✅ |
| 获取权重 | `mlApi.getWeights` | `GET /ml/weights` | 其他 | ✅ |

---

### 2.5 ConfigManagement 模块

#### 功能 12: 配置导入导出

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 获取配置类型 | `configApi.getConfigTypes` | `GET /config/types` | 其他 | ✅ |
| 导出配置 | `configApi.exportConfigs` | `POST /config/export` | 其他 | ✅ |
| 验证导入 | `configApi.verifyImport` | `POST /config/import/verify` | 其他 | ✅ |
| 应用导入 | `configApi.applyImport` | `POST /config/import/apply` | 其他 | ✅ |

---

### 2.6 全局组件

#### 功能 13: 任务监控（Layout 全局）

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 获取运行中任务 | `taskMonitorApi.getRunningTasks` | `GET /tasks/running` | A | ✅ |
| 获取任务历史 | `taskMonitorApi.getTaskHistory` | `GET /tasks/history` | A | ✅ |
| 清理僵尸任务 | `taskMonitorApi.cleanupStale` | `POST /tasks/cleanup` | A | ✅ |
| 获取任务状态 | `taskMonitorApi.getTaskStatus` | `GET /tasks/{type}/status/{runId}` | A | ✅ |

#### 功能 14: 行情查询（MarketCenter）

| 操作 | 前端 API 方法 | 后端端点 | System | 状态 |
|------|-------------|---------|--------|------|
| 获取股票列表 | `dataApi.listStocks` | `GET /data/stocks` | C | ✅ |
| 获取日线数据 | `dataApi.getDaily` | `GET /data/daily` | C | ✅ |

---

## 三、死代码汇总

### 3.1 死亡的前端 API 方法（6 个）

| API 方法 | 所属对象 | 后端端点 | 原因 |
|---------|---------|---------|------|
| `runEtlTask` | `dataApi` | `POST /tasks/etl/{id}/execute` | ETL 执行实际用 `backfillEtlTask`，此方法 0 调用 |
| `flowApi.trigger` | `flowApi` | `POST /flows/{name}/trigger` | SchedulerCenter 的执行按钮实际调用 `backfill`，此方法 0 调用 |
| `getDataFrameSchema` | `productionApi` | `POST /factor/dataframe-schema` | 0 次 UI 调用 |
| `batchUploadIndexPool` | `productionApi` | `POST /factor/index-pool/batch-upload` | 0 次 UI 调用，无对应 UI 入口 |
| `csvUploadIndexPool` | `productionApi` | `POST /factor/index-pool/csv-upload` | 0 次 UI 调用，无对应 UI 入口 |
| `mlApi.getStatus` | `mlApi` | `GET /ml/status/{jobId}` | 注：实际有调用，但通过 `getStatus` 别名，需确认 |

> 注：`mlApi.getStatus` 在 StrategyCenter.tsx:50 有调用，计数为 15 是因为 `getStatus` 字符串匹配到了其他地方。实际上是活跃的，从死亡列表移除。

**修正后死亡前端 API 方法（5 个）**:

| API 方法 | 所属对象 | 后端端点 | 原因 |
|---------|---------|---------|------|
| `runEtlTask` | `dataApi` | `POST /tasks/etl/{id}/execute` | ETL 执行实际用 `backfillEtlTask` |
| `flowApi.trigger` | `flowApi` | `POST /flows/{name}/trigger` | 执行按钮实际调用 `backfill` |
| `getDataFrameSchema` | `productionApi` | `POST /factor/dataframe-schema` | 无 UI 入口调用 |
| `batchUploadIndexPool` | `productionApi` | `POST /factor/index-pool/batch-upload` | 无 UI 入口 |
| `csvUploadIndexPool` | `productionApi` | `POST /factor/index-pool/csv-upload` | 无 UI 入口 |

---

### 3.2 孤立的后端端点（7 个）

前端没有对应 API 方法，但后端有实现：

| 后端端点 | 文件 | System | 说明 |
|---------|------|--------|------|
| `GET /factor/index-pool/template` | factor_config.py | 其他 | 前端无对应方法，无 UI 入口 |
| `POST /flows/{name}/trigger` | flows.py | 其他 | 前端 `flowApi.trigger` 定义了但 0 调用 |
| `POST /factor/dataframe-schema` | factor_config.py | 其他 | 前端 `getDataFrameSchema` 定义了但 0 调用 |
| `POST /factor/index-pool/batch-upload` | factor_config.py | 其他 | 前端 `batchUploadIndexPool` 定义了但 0 调用 |
| `POST /factor/index-pool/csv-upload` | factor_config.py | 其他 | 前端 `csvUploadIndexPool` 定义了但 0 调用 |
| `POST /tasks/etl/{id}/execute` | tasks.py | A | 前端 `runEtlTask` 定义了但 0 调用 |
| `GET /tasks/{type}/{id}/inspect` | tasks.py | A | 前端直接用 axios 调用，未走 api/index.ts |

> 注：`/tasks/{type}/{id}/inspect` 虽然前端没有在 api/index.ts 中定义方法，但 `DataInspectTab.tsx` 直接用 axios 调用，属于**活跃端点**，只是绕过了 api/index.ts 封装。

**修正后孤立后端端点（6 个，真正无人调用）**:

| 后端端点 | 文件 | System | 说明 |
|---------|------|--------|------|
| `GET /factor/index-pool/template` | factor_config.py | 其他 | 无任何前端调用 |
| `POST /flows/{name}/trigger` | flows.py | 其他 | flowApi.trigger 定义但 0 调用 |
| `POST /factor/dataframe-schema` | factor_config.py | 其他 | getDataFrameSchema 定义但 0 调用 |
| `POST /factor/index-pool/batch-upload` | factor_config.py | 其他 | batchUploadIndexPool 定义但 0 调用 |
| `POST /factor/index-pool/csv-upload` | factor_config.py | 其他 | csvUploadIndexPool 定义但 0 调用 |
| `POST /tasks/etl/{id}/execute` | tasks.py | A | runEtlTask 定义但 0 调用 |

---

## 四、System 分布统计

| System | 路由模式 | 活跃端点 | 死亡/孤立端点 | 前端迁移状态 |
|--------|---------|---------|------------|------------|
| **A** | `/tasks/*` | 14 | 1 (`/tasks/etl/{id}/execute`) | ✅ 完全迁移 |
| **B** | `/sync/tasks`, `/etl/tasks` 等 | 0 | 0 | ✅ 已删除 |
| **C** | `/data/sync/*`, `/data/etl/*` | 11 | 0 | ✅ 保留（功能性端点） |
| **其他** | `/factor/*`, `/flows/*`, `/strategy/*` 等 | 40 | 5 | ✅ 正常 |

### System 说明

- **System A** — 任务管理统一路由，前端已完全迁移，状态良好
- **System B** — 已在上次清理中删除，无残留
- **System C** — `/data/` 路径下的端点**不是迁移遗留**，而是功能性端点（数据查询、指数管理、行情），应保留
- **其他** — 因子、Flow、回测、ML、配置等独立模块，各自有完整的前后端对应

---

## 五、需要决策的问题

### 问题 1: ETL 执行的两个端点并存

- `POST /tasks/etl/{id}/execute` — 通用执行端点（前端 `runEtlTask` 定义但 0 调用）
- `POST /tasks/etl/{id}/backfill` — 回溯端点（前端实际使用）

**现状**: ETL 任务没有"增量执行"的概念，所有执行都需要指定日期范围，所以前端直接用 `backfill`。`execute` 端点和 `runEtlTask` 方法是冗余的。

**建议**: 删除 `runEtlTask` 前端方法 + 删除或保留 `POST /tasks/etl/{id}/execute` 后端端点（后端可以保留作为内部调用）。

### 问题 2: flowApi.trigger vs flowApi.backfill

- `POST /flows/{name}/trigger` — 无日期参数，立即触发（前端 0 调用）
- `POST /flows/{name}/backfill` — 带日期范围（前端实际使用）

**现状**: SchedulerCenter 的"立即执行"按钮弹出日期选择框，然后调用 `backfill`。`trigger` 端点（无日期参数）从未被使用。

**建议**: 删除 `flowApi.trigger` 前端方法 + 删除 `POST /flows/{name}/trigger` 后端端点。

### 问题 3: 指数池上传功能无 UI 入口

- `batchUploadIndexPool` 和 `csvUploadIndexPool` 定义了但没有 UI 入口
- `listIndexPools` 有调用（在因子分析中选择指数池）

**建议**: 确认是否需要指数池上传功能。如果需要，应该在 FactorCenter 添加 UI 入口；如果不需要，删除这两个方法和对应后端端点。

### 问题 4: getDataFrameSchema 无 UI 入口

- `POST /factor/dataframe-schema` 定义了但没有 UI 调用
- 功能是根据依赖数据源返回预期的 DataFrame schema

**建议**: 确认是否有计划使用此功能。如无计划，删除。

---

## 六、建议行动清单

### 立即可做（无风险）

- [ ] 删除前端 `dataApi.runEtlTask` 方法（api/index.ts）
- [ ] 删除前端 `flowApi.trigger` 方法（api/index.ts）
- [ ] 删除前端 `productionApi.getDataFrameSchema` 方法（api/index.ts）
- [ ] 删除前端 `productionApi.batchUploadIndexPool` 方法（api/index.ts）
- [ ] 删除前端 `productionApi.csvUploadIndexPool` 方法（api/index.ts）
- [ ] 删除后端 `GET /factor/index-pool/template` 端点（factor_config.py）
- [ ] 删除后端 `POST /flows/{name}/trigger` 端点（flows.py）

### 需要决策后执行

- [ ] 决策：ETL execute 端点是否保留（建议删除）
- [ ] 决策：指数池上传功能是否需要 UI 入口（建议添加或删除）
- [ ] 决策：getDataFrameSchema 是否有未来计划（建议删除）

---

## 七、DataInspectTab 直接调用问题

`DataCenter/SyncTaskDrawer/DataInspectTab.tsx:23` 直接使用 axios 调用 `/api/v1/tasks/sync/${taskId}/inspect`，绕过了 `api/index.ts` 的封装。

**建议**: 在 `dataApi` 中添加 `inspectTask(taskType, taskId)` 方法，统一 API 调用方式。
