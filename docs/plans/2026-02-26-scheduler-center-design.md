# 调度中心双 TAB 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 重构调度中心为双 TAB 设计，支持可视化 Flow 编辑和 Prefect Dashboard 嵌入

**Architecture:** 前端使用 Semi Tabs 分隔两个功能区，TAB1 包含 Flow 列表和 SideSheet 编辑器（内含 React Flow DAG 编辑），TAB2 保留 Prefect iframe。后端新增 Flow CRUD API，配置存储为 YAML 文件，运行时动态解析执行。

**Tech Stack:** React 18, Semi Design, React Flow 11.11.4, cronstrue, FastAPI, PyYAML, Prefect 3.x

---

## Task 1: 安装前端依赖

**Files:**
- Modify: `frontend/package.json`

**Step 1: 安装 cronstrue 库**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend && npm install cronstrue
```

---

## Task 2: 创建后端 Flow 配置目录和示例文件

**Files:**
- Create: `config/flows/daily-sync.yaml`
- Create: `config/flows/weekly-analysis.yaml`

**Step 1: 创建目录和示例配置**

`config/flows/daily-sync.yaml`:
```yaml
name: daily-sync
description: 每日数据同步流水线
cron: "0 18 * * 1-5"
tags:
  - daily
enabled: true
tasks:
  - id: daily
    type: sync
  - id: daily_basic
    type: sync
  - id: adj_factor
    type: sync
  - id: factor_ma_20
    type: factor
    depends_on:
      - daily
  - id: factor_pe_rank
    type: factor
    depends_on:
      - daily_basic
```

`config/flows/weekly-analysis.yaml`:
```yaml
name: weekly-analysis
description: 每周因子分析流水线
cron: "0 3 * * 6"
tags:
  - weekly
enabled: true
tasks:
  - id: stock_basic
    type: sync
  - id: daily
    type: sync
  - id: factor_ma_5
    type: factor
    depends_on:
      - daily
  - id: factor_rsi_14
    type: factor
    depends_on:
      - daily
```

---

## Task 3: 创建后端 Flow API

**Files:**
- Create: `backend/app/api/v1/flows.py`

**Step 1: 实现 Flow CRUD API**

```python
"""
Flow 配置管理 API
"""
import os
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import yaml

from app.core.logger import logger

router = APIRouter()

# Flow 配置目录
FLOWS_DIR = Path(__file__).parent.parent.parent.parent.parent / "config" / "flows"


class TaskConfig(BaseModel):
    id: str
    type: str  # "sync" or "factor"
    depends_on: List[str] = []


class FlowConfig(BaseModel):
    name: str
    description: str = ""
    cron: str
    tags: List[str] = []
    enabled: bool = True
    tasks: List[TaskConfig] = []


class FlowListItem(BaseModel):
    name: str
    description: str
    cron: str
    tags: List[str]
    enabled: bool
    task_count: int


def _load_flow(name: str) -> dict:
    """加载单个 Flow 配置"""
    file_path = FLOWS_DIR / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _save_flow(config: dict):
    """保存 Flow 配置"""
    FLOWS_DIR.mkdir(parents=True, exist_ok=True)
    file_path = FLOWS_DIR / f"{config['name']}.yaml"
    with open(file_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


def _delete_flow_file(name: str):
    """删除 Flow 配置文件"""
    file_path = FLOWS_DIR / f"{name}.yaml"
    if file_path.exists():
        file_path.unlink()


@router.get("/flows", response_model=List[FlowListItem])
def list_flows():
    """列出所有 Flow 配置"""
    flows = []
    if not FLOWS_DIR.exists():
        return flows

    for file_path in FLOWS_DIR.glob("*.yaml"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                flows.append(FlowListItem(
                    name=config.get("name", file_path.stem),
                    description=config.get("description", ""),
                    cron=config.get("cron", ""),
                    tags=config.get("tags", []),
                    enabled=config.get("enabled", True),
                    task_count=len(config.get("tasks", []))
                ))
        except Exception as e:
            logger.warning(f"Failed to load flow {file_path}: {e}")

    return flows


@router.get("/flows/{name}", response_model=FlowConfig)
def get_flow(name: str):
    """获取单个 Flow 配置"""
    config = _load_flow(name)
    return FlowConfig(**config)


@router.post("/flows", response_model=FlowConfig)
def create_flow(config: FlowConfig):
    """创建新 Flow"""
    file_path = FLOWS_DIR / f"{config.name}.yaml"
    if file_path.exists():
        raise HTTPException(status_code=400, detail=f"Flow '{config.name}' already exists")

    _save_flow(config.model_dump())
    logger.info(f"Created flow: {config.name}")
    return config


@router.put("/flows/{name}", response_model=FlowConfig)
def update_flow(name: str, config: FlowConfig):
    """更新 Flow 配置"""
    file_path = FLOWS_DIR / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")

    # 如果名称变更，删除旧文件
    if config.name != name:
        _delete_flow_file(name)

    _save_flow(config.model_dump())
    logger.info(f"Updated flow: {config.name}")
    return config


@router.delete("/flows/{name}")
def delete_flow(name: str):
    """删除 Flow"""
    file_path = FLOWS_DIR / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")

    _delete_flow_file(name)
    logger.info(f"Deleted flow: {name}")
    return {"status": "success", "message": f"Flow '{name}' deleted"}


@router.post("/flows/{name}/run")
async def run_flow(name: str, target_date: Optional[str] = None):
    """立即执行 Flow"""
    from flows.dynamic_flow import run_dynamic_flow

    config = _load_flow(name)

    try:
        result = await run_dynamic_flow(config, target_date)
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Flow execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
```

---

## Task 4: 创建动态 Flow 执行器

**Files:**
- Create: `backend/flows/dynamic_flow.py`

**Step 1: 实现动态 Flow 执行逻辑**

```python
"""
动态 Flow 执行器
根据 YAML 配置动态构建和执行 Prefect Flow
"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import defaultdict

from app.core.logger import logger


def _topological_sort(tasks: List[dict]) -> List[List[dict]]:
    """
    拓扑排序，返回按层分组的任务列表
    同一层的任务可以并行执行
    """
    # 构建依赖图
    task_map = {t["id"]: t for t in tasks}
    in_degree = defaultdict(int)
    dependents = defaultdict(list)

    for task in tasks:
        task_id = task["id"]
        deps = task.get("depends_on", [])
        in_degree[task_id] = len(deps)
        for dep in deps:
            dependents[dep].append(task_id)

    # BFS 分层
    layers = []
    queue = [t["id"] for t in tasks if in_degree[t["id"]] == 0]

    while queue:
        layers.append([task_map[tid] for tid in queue])
        next_queue = []
        for tid in queue:
            for dep_id in dependents[tid]:
                in_degree[dep_id] -= 1
                if in_degree[dep_id] == 0:
                    next_queue.append(dep_id)
        queue = next_queue

    # 检查是否有循环依赖
    total_tasks = sum(len(layer) for layer in layers)
    if total_tasks != len(tasks):
        raise ValueError("检测到循环依赖")

    return layers


async def _execute_sync_task(task_id: str, target_date: Optional[str]) -> dict:
    """执行同步任务"""
    from data_manager.refactored_sync_engine import sync_engine

    logger.info(f"执行同步任务: {task_id}")
    try:
        success = sync_engine.sync_task(task_id, target_date)
        return {"task_id": task_id, "type": "sync", "success": success}
    except Exception as e:
        logger.error(f"同步任务 {task_id} 失败: {e}")
        return {"task_id": task_id, "type": "sync", "success": False, "error": str(e)}


async def _execute_factor_task(task_id: str, target_date: Optional[str]) -> dict:
    """执行因子计算任务"""
    from store.dolphindb_client import db_client
    from engine.production.engine import ProductionEngine

    logger.info(f"执行因子任务: {task_id}")
    try:
        engine = ProductionEngine(db_client)
        result = engine.run_task(task_id, target_date=target_date)
        return {"task_id": task_id, "type": "factor", "success": result}
    except Exception as e:
        logger.error(f"因子任务 {task_id} 失败: {e}")
        return {"task_id": task_id, "type": "factor", "success": False, "error": str(e)}


async def _execute_task(task: dict, target_date: Optional[str]) -> dict:
    """执行单个任务"""
    task_id = task["id"]
    task_type = task["type"]

    if task_type == "sync":
        return await _execute_sync_task(task_id, target_date)
    elif task_type == "factor":
        return await _execute_factor_task(task_id, target_date)
    else:
        return {"task_id": task_id, "type": task_type, "success": False, "error": f"未知任务类型: {task_type}"}


async def run_dynamic_flow(config: dict, target_date: Optional[str] = None) -> Dict[str, Any]:
    """
    执行动态 Flow

    Args:
        config: Flow 配置字典
        target_date: 目标日期 YYYYMMDD

    Returns:
        执行结果字典
    """
    flow_name = config.get("name", "unknown")
    tasks = config.get("tasks", [])

    if not tasks:
        return {"flow": flow_name, "status": "empty", "results": []}

    if target_date is None:
        target_date = datetime.now().strftime("%Y%m%d")

    logger.info(f"开始执行 Flow: {flow_name}, 目标日期: {target_date}")

    # 拓扑排序
    try:
        layers = _topological_sort(tasks)
    except ValueError as e:
        return {"flow": flow_name, "status": "error", "error": str(e)}

    # 按层执行
    all_results = []
    for i, layer in enumerate(layers):
        logger.info(f"执行第 {i+1} 层，共 {len(layer)} 个任务")

        # 并行执行同一层的任务
        layer_tasks = [_execute_task(task, target_date) for task in layer]
        layer_results = await asyncio.gather(*layer_tasks)
        all_results.extend(layer_results)

        # 检查是否有失败的任务
        failed = [r for r in layer_results if not r.get("success")]
        if failed:
            logger.warning(f"第 {i+1} 层有 {len(failed)} 个任务失败")

    success_count = sum(1 for r in all_results if r.get("success"))
    fail_count = len(all_results) - success_count

    logger.info(f"Flow {flow_name} 执行完成: {success_count} 成功, {fail_count} 失败")

    return {
        "flow": flow_name,
        "status": "completed",
        "target_date": target_date,
        "success_count": success_count,
        "fail_count": fail_count,
        "results": all_results
    }
```

---

## Task 5: 注册 Flow API 路由

**Files:**
- Modify: `backend/app/main.py`

**Step 1: 添加 flows 路由**

在 `from app.api.v1 import` 行添加 `flows`，并注册路由。

---

## Task 6: 添加前端 Flow API

**Files:**
- Modify: `frontend/src/api/index.ts`

**Step 1: 添加 flowApi**

```typescript
export const flowApi = {
  list: () => api.get('/flows'),
  get: (name: string) => api.get(`/flows/${name}`),
  create: (config: any) => api.post('/flows', config),
  update: (name: string, config: any) => api.put(`/flows/${name}`, config),
  delete: (name: string) => api.delete(`/flows/${name}`),
  run: (name: string, targetDate?: string) =>
    longRunningApi.post(`/flows/${name}/run`, null, { params: { target_date: targetDate } }),
};
```

---

## Task 7: 创建 SchedulerFlowEditor 组件

**Files:**
- Create: `frontend/src/components/SchedulerFlowEditor/index.tsx`
- Create: `frontend/src/components/SchedulerFlowEditor/TaskSelector.tsx`
- Create: `frontend/src/components/SchedulerFlowEditor/DAGEditor.tsx`

**Step 1: 创建主编辑器组件**

包含 SideSheet、表单、任务选择器和 DAG 编辑器。

---

## Task 8: 重构 SchedulerCenter 页面

**Files:**
- Modify: `frontend/src/pages/SchedulerCenter.tsx`

**Step 1: 实现双 TAB 布局**

TAB 1: 调度管理（Flow 列表 + 编辑器）
TAB 2: Prefect（iframe 嵌入）
