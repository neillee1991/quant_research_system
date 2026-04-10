# 自研调度系统迁移实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 完整替换 Prefect，迁移到自研 in-process 调度器

**Architecture:**
- PostgreSQL 存储元数据（flow_config、flow_run、task_run）
- DolphinDB 存储时序数据（股票数据、因子值）
- FastAPI 内置 asyncio 调度器（主循环 + DAG 执行器）

**Tech Stack:**
- FastAPI + asyncio
- asyncpg（异步 PostgreSQL）
- psycopg2-binary（同步 PostgreSQL，用于 flow_service）
- croniter（cron 表达式解析）

---

## Week 1：基础设施 + 调度器核心

### Day 1-2：PostgreSQL 基础设施

**Task 1：更新 docker-compose.yml，添加 PostgreSQL 服务**
- 文件：`docker-compose.yml`
- 添加 postgres 服务（端口 5432，数据卷 ./data/postgres）
- 环境变量：POSTGRES_DB=quantsystem, POSTGRES_USER=quant, POSTGRES_PASSWORD=quant123

**Task 2：创建数据库表 SQL**
- 创建：`backend/scripts/migrations/001_create_scheduler_tables.sql`
- 内容：flow_config、flow_run、task_run 三个表（参考设计文档）

**Task 3：创建 scheduler/db.py（asyncpg 连接池）**
- 创建：`backend/scheduler/__init__.py`
- 创建：`backend/scheduler/db.py`
- 功能：asyncpg 连接池管理

**Task 4：创建数据迁移脚本**
- 创建：`backend/scripts/migrations/migrate_flow_config.py`
- 功能：从 DolphinDB flow_config 读取，写入 PostgreSQL

**Task 5：更新 backend/app/core/config.py**
- 添加 PostgreSQL 配置项（host、port、db、user、password）

**Task 6：更新 .env**
- 添加 PostgreSQL 环境变量

### Day 3-4：调度器核心开发

**Task 7：创建 scheduler/models.py**
- 创建：`backend/scheduler/models.py`
- 枚举：FlowStatus（pending/running/success/failed/cancelled）
- 枚举：TaskStatus（pending/running/success/failed）
- 枚举：TriggerType（cron/manual/parent_flow）
- Pydantic 模型：FlowRun、TaskRun

**Task 8：创建 scheduler/repository.py**
- 创建：`backend/scheduler/repository.py`
- 功能：Async CRUD for flow_config、flow_run、task_run（用 asyncpg）

**Task 9：创建 scheduler/submitter.py**
- 创建：`backend/scheduler/submitter.py`
- 功能：HTTP 任务提交器，3 次重试，指数退避
- 调用端点：
  - sync → POST /api/v1/sync/task/{task_id}/run
  - etl → POST /api/v1/etl/task/{task_id}/run
  - factor → POST /api/v1/factors/task/{task_id}/run
  - flow → 内部触发子 Flow

**Task 10：创建 scheduler/executor.py**
- 创建：`backend/scheduler/executor.py`
- 功能：DAG 执行器
  - 拓扑排序 tasks
  - 层并行执行（asyncio.gather）
  - 支持 task_type=flow（等待子 Flow 完成）
  - 失败传播

**Task 11：创建 scheduler/core.py**
- 创建：`backend/scheduler/core.py`
- 功能：
  - 主循环（每秒检查）
  - 最小堆管理下次运行时间
  - croniter 计算下次运行
  - 10s 轮询 DB 检测 flow_config 变化

### Day 5：集成到 FastAPI

**Task 12：更新 app/models/flow_config.py**
- 修改：`backend/app/models/flow_config.py`
  - TaskInDAG 添加 `flow_name: Optional[str]`
  - FlowConfigBase 的 `cron` 改为 `Optional[str]`

**Task 13：重写 app/services/flow_service.py**
- 修改：`backend/app/services/flow_service.py`
- 改用 psycopg2 连接 PostgreSQL
- 保持相同的公共接口

**Task 14：更新 app/main.py lifespan**
- 修改：`backend/app/main.py`
- startup 时启动调度器
- shutdown 时优雅停止调度器

---

## Week 2：API 与前端适配

### Day 1-2：后端 API 更新

**Task 15：更新 app/api/v1/flows.py**
- 修改：`backend/app/api/v1/flows.py`
- 添加：`GET /flows/{name}/runs`（获取执行历史）
- 添加：`POST /flows/{name}/trigger`（手动触发）
- 更新：`POST /flows/{name}/run` 使用调度器

### Day 3-4：前端适配

**Task 16：更新 frontend/src/api/index.ts**
- 修改：`frontend/src/api/index.ts`
- 添加：FlowRun、TaskRun 接口
- 添加：flowRunApi（list、get）

**Task 17：更新 SchedulerCenter.tsx**
- 修改：`frontend/src/pages/SchedulerCenter.tsx`
- 移除："Prefect Dashboard" Tab
- 新增："执行历史" Tab（显示 flow_run 列表）

### Day 5：移除 Prefect

**Task 18：删除 Prefect 文件**
- 删除：`backend/flows/serve_with_reload.py`
- 删除：`backend/flows/serve.py`
- 删除：`backend/flows/dynamic_from_config.py`
- 删除：`backend/app/services/prefect_sync_service.py`

**Task 19：更新 docker-compose.yml**
- 修改：`docker-compose.yml`
- 移除：prefect-server 服务

**Task 20：更新 config/scripts.config.sh**
- 修改：`config/scripts.config.sh`
- 移除：PREFECT_PORT、PREFECT_CONTAINER、PREFECT_WORKER_PID、ENABLE_PREFECT_WORKER

**Task 21：更新启动停止脚本**
- 修改：`start.sh`、`stop.sh`、`check_status.sh`
- 移除：Prefect 相关操作

**Task 22：更新 requirements.txt**
- 修改：`backend/requirements.txt`
- 移除：prefect>=3.0.0
- 添加：asyncpg>=0.29.0
- 添加：psycopg2-binary>=2.9.0
- 添加：croniter>=2.0.0

---

## Week 3-4：测试 + 缓冲

**Task 23：单元测试**
- 创建：`backend/tests/unit/test_scheduler_models.py`
- 创建：`backend/tests/unit/test_scheduler_executor.py`
- 创建：`backend/tests/unit/test_scheduler_core.py`

**Task 24：集成测试**
- 创建：`backend/tests/integration/test_full_flow.py`

**Task 25：回归测试**
- 验证：现有 daily_task Flow 正常运行
