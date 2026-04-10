# 自研调度系统迁移设计文档

**日期**: 2026-04-10
**状态**: 已批准，待实施
**决策**: 完整替换 Prefect，迁移到自研调度系统

---

## 背景

当前系统使用 Prefect 3.x 作为调度引擎，存在以下问题：

- Prefect 只被用作 cron 触发器，没有使用其执行引擎，属于过度依赖
- 运维复杂度高：5+ 个服务、两层监控、自定义进程管理（`os.execv()` 重启）
- 两套数据存储（Prefect SQLite + DolphinDB），状态容易漂移
- 元数据（flow_config、任务状态）存在 DolphinDB（时序数据库），不适合频繁更新

---

## 关键决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 迁移范围 | 完整替换 Prefect | 一步到位，避免维护两套系统 |
| 时间线 | 4 周（2 开发 + 1 测试 + 1 缓冲） | 稳健型，充分测试 |
| 运行架构 | 后端进程内调度器 | 简单、共享数据库连接 |
| 数据库 | PostgreSQL（元数据）+ DolphinDB（时序数据） | 职责分离，各司其职 |
| PostgreSQL 部署 | Docker 容器 | 与现有架构一致 |
| 任务执行 | 通过后端 API（HTTP） | 复用现有 API，解耦清晰 |
| Flow 嵌套 | 支持，最多 1 层 | 灵活组合，避免无限嵌套 |
| 历史数据 | 不迁移 Prefect 历史 | 简化迁移，新系统从零开始 |

---

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                     前端 (React)                            │
│  SchedulerCenter (创建/编辑/删除 Flow, 查看执行历史)        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              后端 (FastAPI + 内置调度器)                    │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Scheduler Core (Async Event Loop)                    │  │
│  │  - Cron Parser (croniter)                             │  │
│  │  - DAG Executor (拓扑排序 + 依赖追踪)                │  │
│  │  - Schedule Heap (下次运行时间管理)                   │  │
│  └────────────────────┬──────────────────────────────────┘  │
│                       │                                      │
│  ┌────────────────────▼──────────────────────────────────┐  │
│  │  Task Submitter (HTTP Client)                         │  │
│  │  - POST /api/v1/tasks/{task_id}/run                   │  │
│  │  - 重试/超时处理（最多 3 次，指数退避）               │  │
│  └────────────────────┬──────────────────────────────────┘  │
└───────────────────────┼──────────────────────────────────────┘
                        │
         ┌──────────────┴───────────────┐
         │                              │
         ▼                              ▼
┌──────────────────┐      ┌──────────────────────────┐
│   PostgreSQL     │      │      DolphinDB           │
│  - flow_config   │      │  - daily_data            │
│  - flow_run      │      │  - factor_values         │
│  - task_run      │      │  - stock_basic           │
└──────────────────┘      │  - ... (时序数据)        │
                          └──────────────────────────┘
```

---

## 数据库设计（PostgreSQL）

### `flow_config`

```sql
CREATE TABLE flow_config (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(255) UNIQUE NOT NULL,
    description      TEXT,
    cron             VARCHAR(100),           -- 空表示只手动触发
    timezone         VARCHAR(50) DEFAULT 'Asia/Shanghai',
    tags             JSONB DEFAULT '[]',
    tasks            JSONB NOT NULL,         -- DAG 任务节点定义
    date_offset_days INTEGER DEFAULT 0,
    enabled          BOOLEAN DEFAULT TRUE,
    version          INTEGER DEFAULT 1,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);
```

### `flow_run`

```sql
CREATE TABLE flow_run (
    id                  SERIAL PRIMARY KEY,
    flow_name           VARCHAR(255) NOT NULL,
    parent_flow_run_id  INTEGER REFERENCES flow_run(id),  -- 支持嵌套
    status              VARCHAR(20) NOT NULL,  -- pending/running/success/failed/cancelled
    trigger_type        VARCHAR(20) NOT NULL,  -- cron/manual/parent_flow
    target_date         VARCHAR(8),            -- YYYYMMDD
    scheduled_at        TIMESTAMPTZ,
    started_at          TIMESTAMPTZ,
    ended_at            TIMESTAMPTZ,
    error_message       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### `task_run`

```sql
CREATE TABLE task_run (
    id            SERIAL PRIMARY KEY,
    flow_run_id   INTEGER REFERENCES flow_run(id) ON DELETE CASCADE,
    task_id       VARCHAR(255) NOT NULL,
    task_type     VARCHAR(20) NOT NULL,  -- sync/etl/factor/flow
    status        VARCHAR(20) NOT NULL,  -- pending/running/success/failed
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,
    error_message TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
```

### Flow 嵌套 Flow 示例（tasks 字段）

```json
[
  {
    "id": "daily_sync_flow",
    "type": "flow",
    "flow_name": "daily_sync",
    "depends_on": []
  },
  {
    "id": "factor_momentum",
    "type": "factor",
    "depends_on": ["daily_sync_flow"]
  }
]
```

**嵌套规则：**
- 最多 1 层嵌套（子 Flow 不能再嵌套 Flow）
- 子 Flow 失败 → 父 Flow 失败
- 子 Flow 产生独立的 `flow_run` 记录，通过 `parent_flow_run_id` 关联

---

## 调度器核心设计

### 文件结构

```
backend/
└── scheduler/
    ├── __init__.py
    ├── core.py          # 主循环 + 堆管理
    ├── executor.py      # DAG 执行器
    ├── submitter.py     # Task 提交器
    ├── models.py        # FlowRun/TaskRun 数据模型
    └── repository.py    # PostgreSQL CRUD
```

### Scheduler Core（主循环）

```
启动时：
  1. 从 PostgreSQL 加载所有 enabled flow
  2. 用 croniter 计算每个 flow 的下次运行时间
  3. 维护一个最小堆（按下次运行时间排序）

主循环（每秒检查一次）：
  while True:
      now = datetime.now()
      while heap.top().next_run <= now:
          flow = heap.pop()
          submit flow_run to executor  # 异步，不阻塞主循环
          heap.push(flow, next_run_time)
      sleep(1)

监听数据库变化（每 10 秒）：
  - 检测 flow_config.updated_at 变化
  - 动态更新堆（新增/修改/删除 flow）
```

### DAG Executor

```
收到 flow_run 请求：
  1. 拓扑排序 tasks
  2. 按层执行：
     - 同层 tasks 并发提交（asyncio.gather）
     - 等待当前层全部完成再执行下一层
  3. 遇到 task_type=flow：
     - 触发子 Flow 执行
     - 等待子 Flow 的 flow_run 完成
  4. 任何 task 失败 → 停止后续执行 → 标记 flow_run 为 failed
```

### Task Submitter

```
提交单个 task：
  1. 创建 task_run 记录（status=pending）
  2. 根据 task_type 调用对应 API：
     - sync   → POST /api/v1/data/sync/task/{task_id}
     - etl    → POST /api/v1/data/etl/task/{task_id}
     - factor → POST /api/v1/production/task/{task_id}/run
     - flow   → 内部触发子 Flow
  3. 更新 task_run 状态（running → success/failed）
  4. 失败时重试（最多 3 次，指数退避）
```

---

## 迁移计划

### Week 1：基础设施 + 调度器核心

- Day 1-2: 引入 PostgreSQL（Docker）
  - 创建 PostgreSQL 容器（docker-compose.yml）
  - 创建 flow_config/flow_run/task_run 表
  - 迁移 DolphinDB flow_config 数据到 PostgreSQL
  - 删除 DolphinDB flow_config 表

- Day 3-4: 调度器核心开发
  - `scheduler/core.py`（主循环 + 堆管理）
  - `scheduler/executor.py`（DAG 执行器）
  - `scheduler/submitter.py`（Task 提交器）
  - `scheduler/repository.py`（PostgreSQL CRUD）

- Day 5: 集成到 FastAPI
  - app startup 时启动调度器
  - app shutdown 时优雅停止

### Week 2：API 与前端适配

- Day 1-2: 后端 API 更新
  - `/api/v1/flows` → 改为读写 PostgreSQL
  - 新增 `/api/v1/flows/{name}/runs`（执行历史）
  - 新增 `/api/v1/flows/{name}/trigger`（手动触发）

- Day 3-4: 前端适配
  - SchedulerCenter 移除 Prefect Dashboard iframe
  - 新增 Flow 执行历史面板
  - 新增手动触发按钮

- Day 5: 移除 Prefect 相关代码
  - 删除 `flows/serve_with_reload.py`
  - 删除 `flows/serve.py`
  - 删除 `flows/dynamic_from_config.py`
  - `docker-compose.yml` 移除 prefect-server

### Week 3：测试

- 单元测试：cron 计算、DAG 执行、Flow 嵌套
- 集成测试：完整 Flow 执行链路
- 回归测试：现有 daily_task Flow 正常运行
- 边界测试：任务失败重试、并发执行、Flow 嵌套

### Week 4：缓冲 + 上线

- 修复测试发现的问题
- 更新文档（CLAUDE.md、FLOW_SCHEDULER_GUIDE.md）
- 更新 start.sh/stop.sh/check_status.sh
- 正式下线 Prefect

---

## 回滚计划

如果 Week 3 测试发现严重问题：
1. PostgreSQL 数据已有，可以快速回滚到 Prefect
2. 重启 Prefect Worker
3. 恢复 DolphinDB flow_config（从 PostgreSQL 反向导出）

---

## 需要删除的文件（Week 2 Day 5）

- `backend/flows/serve_with_reload.py`
- `backend/flows/serve.py`
- `backend/flows/dynamic_from_config.py`
- `backend/flows/data_sync_flow.py`（如果只用于 Prefect）

## 需要更新的文件

- `docker-compose.yml` — 添加 PostgreSQL，移除 prefect-server
- `backend/app/services/flow_service.py` — 改为读写 PostgreSQL
- `backend/app/api/v1/flows.py` — 新增 runs/trigger 端点
- `frontend/src/pages/SchedulerCenter.tsx` — 移除 Prefect iframe，新增历史面板
- `start.sh` / `stop.sh` / `check_status.sh` — 移除 Prefect Worker 相关
- `backend/requirements.txt` — 移除 prefect，添加 asyncpg/psycopg2、croniter
