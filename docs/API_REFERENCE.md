# API 参考

## 概述

所有 API 端点都在 `/api/v1/` 路径下。

---

## 数据 API (`/data/*`)

### 数据查询

| 端点 | 方法 | 描述 |
|------|------|------|
| `/data/stocks` | GET | 获取股票列表 |
| `/data/daily` | GET | 获取日线数据 |
| `/data/tables` | GET | 列出所有表 |
| `/data/tables/{table_name}/info` | GET | 获取表信息 |
| `/data/query` | POST | 执行 SQL 查询 |

### 同步任务

| 端点 | 方法 | 描述 |
|------|------|------|
| `/data/sync/tasks` | GET | 列出同步任务 |
| `/data/sync/tasks` | POST | 创建同步任务 |
| `/data/sync/task/{task_id}` | POST | 运行同步任务 |
| `/data/sync/task/{task_id}/config` | GET | 获取任务配置 |
| `/data/sync/task/{task_id}/config` | PUT | 更新任务配置 |
| `/data/sync/tasks/{task_id}` | DELETE | 删除同步任务 |
| `/data/sync/status` | GET | 获取同步状态 |

### ETL 任务

| 端点 | 方法 | 描述 |
|------|------|------|
| `/data/etl/tasks` | GET | 列出 ETL 任务 |
| `/data/etl/tasks` | POST | 创建 ETL 任务 |
| `/data/etl/task/{task_id}` | PUT | 更新 ETL 任务 |
| `/data/etl/task/{task_id}` | DELETE | 删除 ETL 任务 |
| `/data/etl/task/{task_id}/run` | POST | 运行 ETL 任务 |
| `/data/etl/test` | POST | 测试 ETL 脚本 |

### 配置管理

| 端点 | 方法 | 描述 |
|------|------|------|
| `/data/config/tables` | GET | 获取表配置 |

---

## 生产因子 API (`/production/*`)

### 因子 CRUD

| 端点 | 方法 | 描述 |
|------|------|------|
| `/production/factors` | GET | 列出所有因子 |
| `/production/factors` | POST | 创建因子 |
| `/production/factors/{factor_id}` | PUT | 更新因子 |
| `/production/factors/{factor_id}` | DELETE | 删除因子 |

### 因子计算

| 端点 | 方法 | 描述 |
|------|------|------|
| `/production/run` | POST | 运行生产任务 |
| `/production/batch-run` | POST | 批量运行因子 |
| `/production/history` | GET | 获取运行历史 |
| `/production/status/{run_id}` | GET | 获取任务状态 |

### 因子测试

| 端点 | 方法 | 描述 |
|------|------|------|
| `/production/factors/test` | POST | 测试因子代码 |
| `/production/factors/{factor_id}/code` | GET | 获取因子代码 |
| `/production/factors/{factor_id}/code` | PUT | 更新因子代码 |
| `/production/dataframe-schema` | POST | 获取 DataFrame schema |

### 因子数据探查

| 端点 | 方法 | 描述 |
|------|------|------|
| `/production/factors/{factor_id}/data` | GET | 获取因子数据 |
| `/production/factors/{factor_id}/stats` | GET | 获取因子统计 |
| `/production/factors/{factor_id}/missing-dates` | GET | 获取缺失日期 |

### 数据配置

| 端点 | 方法 | 描述 |
|------|------|------|
| `/production/data-config` | GET | 获取数据配置 |
| `/production/data-config` | PUT | 更新数据配置 |
| `/production/data-config/resolved` | GET | 获取解析后的数据配置 |
| `/production/available-tables` | GET | 获取可用表列表 |

---

## 因子 API (`/factor/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/factor/compute` | POST | 计算技术指标 (单只股票) |
| `/factor/ic` | POST | 计算 IC |

**注意**: `/factor/*` 和 `/production/*` 功能不同，不是重复：
- `/factor/*` = 快速、轻量级技术指标计算（单只股票，无状态）
- `/production/*` = 完整生产因子框架（全市场，有状态，保存到 factor_values）

---

## 策略 API (`/strategy/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/strategy/backtest` | POST | 执行回测 |
| `/strategy/operators` | GET | 列出可用操作符 |

---

## AutoML API (`/ml/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/ml/train` | POST | 训练模型 |
| `/ml/status/{job_id}` | GET | 获取任务状态 |
| `/ml/weights` | GET | 获取模型权重 |

---

## 工作流 API (`/flows/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/flows` | GET | 列出工作流 |
| `/flows/{name}` | GET | 获取工作流配置 |
| `/flows` | POST | 创建工作流 |
| `/flows/{name}` | PUT | 更新工作流 |
| `/flows/{name}` | DELETE | 删除工作流 |
| `/flows/{name}/run` | POST | 运行工作流 |

---

## 任务 API (`/tasks/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/tasks` | GET | 列出所有任务 |
| `/tasks/{task_id}` | GET | 获取任务详情 |

---

## 版本 API (`/versions/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/versions` | GET | 获取版本信息 |

---

## Schema 工具 API (`/schema-tools/*`)

| 端点 | 方法 | 描述 |
|------|------|------|
| `/schema-tools/validate` | POST | 验证 schema |
