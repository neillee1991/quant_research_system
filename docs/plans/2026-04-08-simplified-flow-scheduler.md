# Simplified Flow Scheduler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a simplified, DAG-only flow scheduler that auto-generates Prefect flows from database-stored configurations, with automatic dependency inference, date offset support, and local timezone configuration.

**Architecture:**
- Flow configurations stored in database table `flow_config` (DAG definitions, not code)
- Backend auto-generates Prefect flows from DAG configurations
- Frontend provides only visual DAG editor (no code exposure)
- All scheduling and execution handled by Prefect
- Support for sync/etl/factor task types with easy extensibility

**Tech Stack:** FastAPI, DolphinDB, Prefect 3.x, React + Ant Design + React Flow

---

## Prerequisites

Before starting, verify these exist:
- `/Users/lisheng/Code/quantsystem/quant_research_system/backend/store/dolphindb_client.py` - DolphinDB client
- `/Users/lisheng/Code/quantsystem/quant_research_system/backend/app/api/v1/flows.py` - Existing flows API
- `/Users/lisheng/Code/quantsystem/quant_research_system/backend/flows/data_sync_flow.py` - Prefect flow examples
- `/Users/lisheng/Code/quantsystem/quant_research_system/frontend/src/api/index.ts` - Flow API types
- `/Users/lisheng/Code/quantsystem/quant_research_system/frontend/src/components/SchedulerFlowEditor/index.tsx` - Existing flow editor

---

## Task 1: Create `flow_config` Database Table and Seed Data

**Files:**
- Modify: `backend/infrastructure/database/table_manager.py` (or similar)
- Create: `backend/infrastructure/seed/seed_flow_config.py`
- Verify: `backend/store/dolphindb_client.py` has seed method

**Step 1: Read existing seed pattern**

Read `/Users/lisheng/Code/quantsystem/quant_research_system/backend/infrastructure/seed/` to understand the seed data pattern (look for how `sync_task_config` or `etl_task_config` are seeded).

**Step 2: Create flow_config table definition**

Add table creation code in the appropriate place (likely in `infrastructure/database/table_manager.py` or similar):

```python
def create_flow_config_table(self):
    """Create flow_config table for storing flow DAG definitions"""
    sql = """
    CREATE TABLE IF NOT EXISTS flow_config (
        name STRING,
        description STRING,
        cron STRING,
        tags STRING,      -- JSON array
        enabled BOOLEAN,
        date_offset_days INT,
        tasks STRING,     -- JSON array: [{id, type, depends_on}]
        created_at DATETIME,
        updated_at DATETIME,
        version INT
    )
    """
    self.execute(sql)
    
    # Add primary key or index if needed
    # Example: CREATE INDEX IF NOT EXISTS idx_flow_config_name ON flow_config (name)
```

**Step 3: Create seed_flow_config.py**

Create `backend/infrastructure/seed/seed_flow_config.py`:

```python
"""
Seed data for flow_config table
"""
from typing import Any
from app.core.logger import logger
from store.dolphindb_client import db_client

DEFAULT_FLOWS = [
    {
        "name": "daily_data_sync",
        "description": "每日数据同步流水线: 同步行情 → 计算因子",
        "cron": "0 18 * * 1-5",
        "tags": ["data-sync", "daily"],
        "enabled": True,
        "date_offset_days": -1,
        "tasks": [
            {"id": "sync_daily", "type": "sync", "depends_on": []},
            {"id": "sync_daily_basic", "type": "sync", "depends_on": []},
            {"id": "sync_adj_factor", "type": "sync", "depends_on": []},
            {"id": "factor_ma_20", "type": "factor", "depends_on": ["sync_daily"]},
            {"id": "factor_pe_rank", "type": "factor", "depends_on": ["sync_daily_basic"]},
        ],
    },
    {
        "name": "weekly_analysis",
        "description": "每周分析流水线: 同步基础数据 → 计算技术因子",
        "cron": "0 3 * * 6",
        "tags": ["analysis", "weekly"],
        "enabled": True,
        "date_offset_days": -1,
        "tasks": [
            {"id": "sync_stock_basic", "type": "sync", "depends_on": []},
            {"id": "sync_daily", "type": "sync", "depends_on": []},
            {"id": "factor_rsi_14", "type": "factor", "depends_on": ["sync_daily"]},
        ],
    },
]

def seed_flow_config(force: bool = False) -> None:
    """
    Seed flow_config table with default flows
    
    Args:
        force: If True, delete existing data first
    """
    try:
        if force:
            logger.info("Clearing existing flow_config data")
            db_client.query("DELETE FROM flow_config")
        
        # Check if data already exists
        check_df = db_client.query("SELECT count(*) as cnt FROM flow_config")
        if not check_df.is_empty() and check_df["cnt"][0] > 0 and not force:
            logger.info("flow_config already has data, skipping seed")
            return
        
        from datetime import datetime
        import json
        
        rows = []
        for flow in DEFAULT_FLOWS:
            rows.append({
                "name": flow["name"],
                "description": flow["description"],
                "cron": flow["cron"],
                "tags": json.dumps(flow["tags"]),
                "enabled": flow["enabled"],
                "date_offset_days": flow["date_offset_days"],
                "tasks": json.dumps(flow["tasks"]),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "version": 1,
            })
        
        if rows:
            import polars as pl
            df = pl.DataFrame(rows)
            db_client.upsert("flow_config", df, ["name"])
            logger.info(f"Seeded {len(rows)} flows into flow_config")
        
    except Exception as e:
        logger.error(f"Failed to seed flow_config: {e}", exc_info=True)
        raise
```

**Step 4: Add seed method to DolphinDBClient**

In `backend/store/dolphindb_client.py` (the backward compatibility one), add:

```python
def seed_flow_config(self) -> None:
    """Seed data: flow_config"""
    try:
        from infrastructure.seed import seed_flow_config
        seed_flow_config()
    except Exception as e:
        logger.error(f"seed_flow_config failed: {e}")
```

**Step 5: Verify table creation and seeding**

Run Python and test:
```python
from store.dolphindb_client import db_client
db_client.seed_flow_config()
df = db_client.query("SELECT * FROM flow_config")
print(df)
```

Expected: 2 default flows in the table.

**Step 6: Commit**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
git add infrastructure/seed/seed_flow_config.py
git add store/dolphindb_client.py
git add infrastructure/database/table_manager.py  # if modified
git commit -m "feat: add flow_config table and seed data"
```

---

## Task 2: Backend - Flow Configuration CRUD API

**Files:**
- Modify: `backend/app/api/v1/flows.py`
- Create: `backend/app/models/flow_config.py` (Pydantic models)
- Create: `backend/app/services/flow_service.py` (CRUD service)

**Step 1: Create Pydantic models**

Create `backend/app/models/flow_config.py`:

```python
"""
Flow Configuration Pydantic Models
"""
from typing import List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime


class TaskInDAG(BaseModel):
    """Task within a DAG"""
    id: str = Field(..., description="Task ID (e.g., sync_daily, factor_ma_20)")
    type: str = Field(..., description="Task type: sync, etl, factor")
    depends_on: List[str] = Field(default_factory=list, description="Dependencies (task IDs)")


class FlowConfigBase(BaseModel):
    """Base Flow Configuration"""
    name: str = Field(..., description="Flow name (unique)")
    description: str = Field(default="", description="Flow description")
    cron: str = Field(..., description="Cron expression")
    tags: List[str] = Field(default_factory=list, description="Tags")
    enabled: bool = Field(default=True, description="Whether flow is enabled")
    date_offset_days: int = Field(default=0, description="Date offset: -1=yesterday, 0=today, 1=tomorrow")
    tasks: List[TaskInDAG] = Field(default_factory=list, description="Tasks in DAG")


class FlowConfigCreate(FlowConfigBase):
    """Create Flow Configuration"""
    pass


class FlowConfigUpdate(BaseModel):
    """Update Flow Configuration"""
    description: Optional[str] = None
    cron: Optional[str] = None
    tags: Optional[List[str]] = None
    enabled: Optional[bool] = None
    date_offset_days: Optional[int] = None
    tasks: Optional[List[TaskInDAG]] = None


class FlowConfigInDB(FlowConfigBase):
    """Flow Configuration from Database"""
    created_at: datetime
    updated_at: datetime
    version: int
    
    class Config:
        from_attributes = True


class FlowConfigListItem(BaseModel):
    """Flow Configuration for List View"""
    name: str
    description: str
    cron: str
    tags: List[str]
    enabled: bool
    date_offset_days: int
    task_count: int
    updated_at: datetime
```

**Step 2: Create FlowService**

Create `backend/app/services/flow_service.py`:

```python
"""
Flow Configuration Service
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

from app.core.logger import logger
from store.dolphindb_client import db_client
from app.models.flow_config import (
    FlowConfigCreate,
    FlowConfigUpdate,
    FlowConfigInDB,
    FlowConfigListItem,
    TaskInDAG,
)


class FlowService:
    """Flow Configuration CRUD Service"""
    
    @staticmethod
    def _parse_db_row(row: Dict[str, Any]) -> FlowConfigInDB:
        """Parse database row to FlowConfigInDB"""
        # Parse JSON fields
        tags = json.loads(row.get("tags", "[]")) if row.get("tags") else []
        tasks_data = json.loads(row.get("tasks", "[]")) if row.get("tasks") else []
        tasks = [TaskInDAG(**t) for t in tasks_data]
        
        return FlowConfigInDB(
            name=row["name"],
            description=row.get("description", ""),
            cron=row.get("cron", ""),
            tags=tags,
            enabled=bool(row.get("enabled", True)),
            date_offset_days=int(row.get("date_offset_days", 0)),
            tasks=tasks,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            version=int(row.get("version", 1)),
        )
    
    @staticmethod
    def list_flows(enabled_only: bool = False) -> List[FlowConfigListItem]:
        """List all flows"""
        try:
            where_clause = "WHERE enabled = true" if enabled_only else ""
            df = db_client.query(f"""
                SELECT name, description, cron, tags, enabled, date_offset_days, tasks, updated_at
                FROM flow_config
                {where_clause}
                ORDER BY updated_at DESC
            """)
            
            flows = []
            if not df.is_empty():
                for row in df.to_dicts():
                    tasks_data = json.loads(row.get("tasks", "[]")) if row.get("tasks") else []
                    tags = json.loads(row.get("tags", "[]")) if row.get("tags") else []
                    flows.append(FlowConfigListItem(
                        name=row["name"],
                        description=row.get("description", ""),
                        cron=row.get("cron", ""),
                        tags=tags,
                        enabled=bool(row.get("enabled", True)),
                        date_offset_days=int(row.get("date_offset_days", 0)),
                        task_count=len(tasks_data),
                        updated_at=row["updated_at"],
                    ))
            return flows
        except Exception as e:
            logger.error(f"Failed to list flows: {e}", exc_info=True)
            raise
    
    @staticmethod
    def get_flow(name: str) -> Optional[FlowConfigInDB]:
        """Get a single flow by name"""
        try:
            df = db_client.query("""
                SELECT * FROM flow_config WHERE name = %s
            """, (name,))
            
            if df.is_empty():
                return None
            
            return FlowService._parse_db_row(df.to_dicts()[0])
        except Exception as e:
            logger.error(f"Failed to get flow {name}: {e}", exc_info=True)
            raise
    
    @staticmethod
    def create_flow(config: FlowConfigCreate) -> FlowConfigInDB:
        """Create a new flow"""
        try:
            # Check if flow already exists
            existing = FlowService.get_flow(config.name)
            if existing:
                raise ValueError(f"Flow with name '{config.name}' already exists")
            
            now = datetime.now()
            row = {
                "name": config.name,
                "description": config.description,
                "cron": config.cron,
                "tags": json.dumps(config.tags),
                "enabled": config.enabled,
                "date_offset_days": config.date_offset_days,
                "tasks": json.dumps([t.model_dump() for t in config.tasks]),
                "created_at": now,
                "updated_at": now,
                "version": 1,
            }
            
            import polars as pl
            df = pl.DataFrame([row])
            db_client.upsert("flow_config", df, ["name"])
            
            logger.info(f"Created flow: {config.name}")
            return FlowService.get_flow(config.name)
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to create flow: {e}", exc_info=True)
            raise
    
    @staticmethod
    def update_flow(name: str, config: FlowConfigUpdate) -> FlowConfigInDB:
        """Update an existing flow"""
        try:
            # Get existing flow
            existing = FlowService.get_flow(name)
            if not existing:
                raise ValueError(f"Flow '{name}' not found")
            
            # Build update data
            update_data = {}
            if config.description is not None:
                update_data["description"] = config.description
            if config.cron is not None:
                update_data["cron"] = config.cron
            if config.tags is not None:
                update_data["tags"] = json.dumps(config.tags)
            if config.enabled is not None:
                update_data["enabled"] = config.enabled
            if config.date_offset_days is not None:
                update_data["date_offset_days"] = config.date_offset_days
            if config.tasks is not None:
                update_data["tasks"] = json.dumps([t.model_dump() for t in config.tasks])
            
            if not update_data:
                return existing
            
            # Update in database
            update_data["updated_at"] = datetime.now()
            update_data["version"] = existing.version + 1
            
            # Build update SQL
            set_clause = ", ".join([f"{k} = %s" for k in update_data.keys()])
            params = list(update_data.values()) + [name]
            
            db_client.query(f"""
                UPDATE flow_config SET {set_clause} WHERE name = %s
            """, tuple(params))
            
            logger.info(f"Updated flow: {name}")
            return FlowService.get_flow(name)
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to update flow {name}: {e}", exc_info=True)
            raise
    
    @staticmethod
    def delete_flow(name: str) -> bool:
        """Delete a flow (soft delete by disabling)"""
        try:
            existing = FlowService.get_flow(name)
            if not existing:
                raise ValueError(f"Flow '{name}' not found")
            
            db_client.query("""
                UPDATE flow_config SET enabled = false, updated_at = NOW() WHERE name = %s
            """, (name,))
            
            logger.info(f"Disabled (deleted) flow: {name}")
            return True
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"Failed to delete flow {name}: {e}", exc_info=True)
            raise


# Singleton instance
flow_service = FlowService()
```

**Step 3: Update flows.py API**

Replace `backend/app/api/v1/flows.py` with:

```python
"""
Flow Configuration Management API (Simplified Version)
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from datetime import datetime, timedelta
import os

from app.core.logger import logger
from app.models.flow_config import (
    FlowConfigCreate,
    FlowConfigUpdate,
    FlowConfigInDB,
    FlowConfigListItem,
    TaskInDAG,
)
from app.services.flow_service import flow_service

router = APIRouter()


# ==================== API Endpoints ====================

@router.get("/flows", response_model=List[FlowConfigListItem])
def list_flows(
    enabled_only: bool = Query(default=False, description="Only return enabled flows"),
):
    """List all flow configurations"""
    try:
        return flow_service.list_flows(enabled_only=enabled_only)
    except Exception as e:
        logger.error(f"Failed to list flows: {e}")
        raise HTTPException(status_code=500, detail="Failed to list flows")


@router.get("/flows/{name}", response_model=FlowConfigInDB)
def get_flow(name: str):
    """Get a single flow configuration"""
    try:
        flow = flow_service.get_flow(name)
        if not flow:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")
        return flow
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get flow")


@router.post("/flows", response_model=FlowConfigInDB)
def create_flow(config: FlowConfigCreate):
    """Create a new flow configuration"""
    try:
        return flow_service.create_flow(config)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create flow: {e}")
        raise HTTPException(status_code=500, detail="Failed to create flow")


@router.put("/flows/{name}", response_model=FlowConfigInDB)
def update_flow(name: str, config: FlowConfigUpdate):
    """Update a flow configuration"""
    try:
        return flow_service.update_flow(name, config)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to update flow")


@router.delete("/flows/{name}")
def delete_flow(name: str):
    """Delete (disable) a flow"""
    try:
        flow_service.delete_flow(name)
        return {"status": "success", "message": f"Flow '{name}' disabled"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete flow")


@router.post("/flows/{name}/run")
async def run_flow(
    name: str,
    target_date: Optional[str] = Query(None, description="Target date YYYYMMDD (override date offset)"),
    background_tasks: BackgroundTasks = None,
):
    """Run a flow immediately"""
    try:
        # Get flow config
        flow = flow_service.get_flow(name)
        if not flow:
            raise HTTPException(status_code=404, detail=f"Flow '{name}' not found")
        
        # Calculate target date if not provided
        if not target_date:
            base_date = datetime.now()
            offset_days = flow.date_offset_days
            target_date = (base_date + timedelta(days=offset_days)).strftime("%Y%m%d")
        
        # In simplified version, we'll just log this for now
        # Full implementation would trigger Prefect flow run
        logger.info(f"Would run flow '{name}' with target_date={target_date}")
        
        return {
            "status": "success",
            "message": f"Flow '{name}' triggered",
            "target_date": target_date,
            "note": "Full Prefect integration coming soon",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to run flow {name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to run flow")


# ==================== Dependency Inference ====================

@router.post("/flows/infer-dependencies")
def infer_dependencies(tasks: List[TaskInDAG]):
    """
    Infer dependencies for tasks automatically
    
    For ETL tasks: parses SQL to find source tables and maps to sync tasks
    For factor tasks: uses depends_on from factor config
    """
    try:
        from app.services.dependency_inference import DependencyInferenceService
        
        service = DependencyInferenceService()
        inferred_tasks = service.infer_dependencies(tasks)
        
        return {
            "status": "success",
            "tasks": inferred_tasks,
        }
    except Exception as e:
        logger.error(f"Failed to infer dependencies: {e}")
        raise HTTPException(status_code=500, detail="Failed to infer dependencies")
```

**Step 4: Add DependencyInferenceService placeholder**

Create `backend/app/services/dependency_inference.py`:

```python
"""
Dependency Inference Service
Automatically infers task dependencies based on task configurations
"""
from typing import List
from app.core.logger import logger
from app.models.flow_config import TaskInDAG


class DependencyInferenceService:
    """Service for inferring task dependencies"""
    
    def infer_dependencies(self, tasks: List[TaskInDAG]) -> List[TaskInDAG]:
        """
        Infer dependencies for tasks
        
        Args:
            tasks: List of tasks with empty depends_on
            
        Returns:
            Tasks with inferred dependencies
        """
        result = []
        
        for task in tasks:
            task_copy = TaskInDAG(
                id=task.id,
                type=task.type,
                depends_on=task.depends_on.copy() if task.depends_on else [],
            )
            
            if not task.depends_on:
                # Try to infer dependencies
                inferred = self._infer_for_task(task)
                if inferred:
                    task_copy.depends_on = inferred
                    logger.info(f"Inferred dependencies for {task.id}: {inferred}")
            
            result.append(task_copy)
        
        return result
    
    def _infer_for_task(self, task: TaskInDAG) -> List[str]:
        """Infer dependencies for a single task"""
        if task.type == "factor":
            return self._infer_for_factor(task.id)
        elif task.type == "etl":
            return self._infer_for_etl(task.id)
        elif task.type == "sync":
            # Sync tasks usually don't have dependencies
            return []
        return []
    
    def _infer_for_factor(self, factor_id: str) -> List[str]:
        """Infer dependencies for a factor task"""
        try:
            from store.dolphindb_client import db_client
            import json
            
            df = db_client.query("""
                SELECT depends_on FROM factor_metadata WHERE factor_id = %s
            """, (factor_id,))
            
            if not df.is_empty():
                depends_on_json = df["depends_on"][0]
                if depends_on_json:
                    depends_on = json.loads(depends_on_json)
                    # Filter to sync task IDs
                    sync_tasks = []
                    for dep in depends_on:
                        if dep.startswith("sync_"):
                            sync_tasks.append(dep)
                    return sync_tasks
        except Exception as e:
            logger.warning(f"Failed to infer factor dependencies for {factor_id}: {e}")
        
        return []
    
    def _infer_for_etl(self, etl_id: str) -> List[str]:
        """Infer dependencies for an ETL task"""
        try:
            from store.dolphindb_client import db_client
            import json
            
            df = db_client.query("""
                SELECT script FROM etl_task_config WHERE task_id = %s
            """, (etl_id,))
            
            if not df.is_empty():
                script = df["script"][0]
                if script:
                    # Simple parsing: look for table references
                    # In production, would use SQL parser
                    source_tables = self._extract_source_tables(script)
                    # Map tables to sync tasks
                    return self._map_tables_to_sync_tasks(source_tables)
        except Exception as e:
            logger.warning(f"Failed to infer ETL dependencies for {etl_id}: {e}")
        
        return []
    
    def _extract_source_tables(self, script: str) -> List[str]:
        """Extract source table references from ETL script (simplified)"""
        # This is a simplified version - production would use proper SQL parsing
        import re
        tables = []
        # Look for common patterns
        patterns = [
            r"FROM\s+(\w+)",
            r"from\s+(\w+)",
            r"JOIN\s+(\w+)",
            r"join\s+(\w+)",
        ]
        for pattern in patterns:
            matches = re.findall(pattern, script)
            tables.extend(matches)
        return list(set(tables))
    
    def _map_tables_to_sync_tasks(self, tables: List[str]) -> List[str]:
        """Map table names to sync task IDs"""
        try:
            from store.dolphindb_client import db_client
            
            df = db_client.query("""
                SELECT task_id, table_name FROM sync_task_config WHERE enabled = true
            """)
            
            table_to_task = {}
            if not df.is_empty():
                for row in df.to_dicts():
                    table_name = row.get("table_name")
                    task_id = row.get("task_id")
                    if table_name and task_id:
                        table_to_task[table_name] = task_id
            
            sync_tasks = []
            for table in tables:
                if table in table_to_task:
                    sync_tasks.append(table_to_task[table])
            
            return sync_tasks
        except Exception as e:
            logger.warning(f"Failed to map tables to sync tasks: {e}")
            return []


# Singleton instance
dependency_inference_service = DependencyInferenceService()
```

**Step 5: Register router in main.py**

In `backend/app/main.py`, ensure the flows router is included:

```python
from app.api.v1.flows import router as flows_router

app.include_router(flows_router, prefix="/api/v1", tags=["flows"])
```

**Step 6: Test the API**

Run backend and test:
```bash
# List flows
curl http://localhost:8000/api/v1/flows

# Create a flow
curl -X POST http://localhost:8000/api/v1/flows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-flow",
    "description": "Test flow",
    "cron": "0 0 * * *",
    "tags": ["test"],
    "enabled": true,
    "date_offset_days": 0,
    "tasks": []
  }'
```

**Step 7: Commit**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
git add app/models/flow_config.py
git add app/services/flow_service.py
git add app/services/dependency_inference.py
git add app/api/v1/flows.py
git add app/main.py  # if modified
git commit -m "feat: add flow config CRUD API and dependency inference"
```

---

## Task 3: Backend - Dynamic Prefect Flow Generator and Serve

**Files:**
- Create: `backend/flows/dynamic_from_config.py`
- Modify: `backend/flows/serve.py`
- Update: `backend/.env` (add timezone)

**Step 1: Create dynamic_from_config.py**

Create `backend/flows/dynamic_from_config.py`:

```python
"""
Dynamic Prefect Flow Generator
Generates Prefect flows from database-stored flow_config configurations
"""
from typing import Optional
from datetime import datetime, timedelta
from prefect import flow, task
from app.core.logger import logger


# Task wrappers for each task type
@task(retries=2, retry_delay_seconds=30)
def run_sync_task(task_id: str, target_date: str):
    """Run a sync task"""
    from data_manager.refactored_sync_engine import sync_engine
    logger.info(f"Running sync task: {task_id}, target_date={target_date}")
    success = sync_engine.sync_task(task_id, target_date)
    return {"task_id": task_id, "type": "sync", "success": success}


@task(retries=2, retry_delay_seconds=30)
def run_etl_task(task_id: str, target_date: str):
    """Run an ETL task"""
    from data_manager.etl_engine import etl_engine
    logger.info(f"Running ETL task: {task_id}, target_date={target_date}")
    success = etl_engine.run_etl_task(task_id, target_date)
    return {"task_id": task_id, "type": "etl", "success": success}


@task(retries=2, retry_delay_seconds=30)
def run_factor_task(task_id: str, target_date: str):
    """Run a factor task"""
    from store.dolphindb_client import db_client
    from app.services.factor_compute_service import FactorComputeService
    logger.info(f"Running factor task: {task_id}, target_date={target_date}")
    service = FactorComputeService(db_client)
    result = service.compute_factor(task_id, target_date=target_date)
    return {"task_id": task_id, "type": "factor", "success": result.success}


def create_prefect_flow_from_config(flow_config: dict):
    """
    Create a Prefect flow from a flow_config dictionary
    
    Args:
        flow_config: Dictionary from flow_config table
        
    Returns:
        Prefect flow function
    """
    from collections import defaultdict
    
    flow_name = flow_config["name"]
    tasks = flow_config.get("tasks", [])
    date_offset_days = flow_config.get("date_offset_days", 0)
    
    # Build task map and dependency graph
    task_map = {t["id"]: t for t in tasks}
    dependents = defaultdict(list)  # task_id -> [tasks that depend on it]
    
    for task in tasks:
        for dep in task.get("depends_on", []):
            dependents[dep].append(task["id"])
    
    # Topological sort
    in_degree = {t["id"]: len(t.get("depends_on", [])) for t in tasks}
    queue = [t["id"] for t in tasks if in_degree[t["id"]] == 0]
    layers = []
    
    while queue:
        layers.append(queue)
        next_queue = []
        for task_id in queue:
            for dependent in dependents[task_id]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    next_queue.append(dependent)
        queue = next_queue
    
    # Create the Prefect flow
    @flow(name=flow_name, log_prints=True)
    def generated_flow(
        scheduled_start_time: Optional[datetime] = None,
        manual_target_date: Optional[str] = None,
    ):
        # Calculate target date
        if manual_target_date:
            target_date = manual_target_date
        elif scheduled_start_time:
            target_date = (scheduled_start_time + timedelta(days=date_offset_days)).strftime("%Y%m%d")
        else:
            target_date = (datetime.now() + timedelta(days=date_offset_days)).strftime("%Y%m%d")
        
        logger.info(f"Flow {flow_name} starting, target_date={target_date}")
        
        # Execute tasks in layers
        task_futures = {}
        
        for layer in layers:
            layer_futures = []
            
            for task_id in layer:
                task_config = task_map[task_id]
                task_type = task_config["type"]
                
                # Get dependencies
                deps = task_config.get("depends_on", [])
                dep_futures = [task_futures[dep] for dep in deps if dep in task_futures]
                
                # Wait for dependencies (implicit in Prefect with .submit())
                if task_type == "sync":
                    future = run_sync_task.submit(task_id, target_date)
                elif task_type == "etl":
                    future = run_etl_task.submit(task_id, target_date)
                elif task_type == "factor":
                    future = run_factor_task.submit(task_id, target_date)
                else:
                    logger.warning(f"Unknown task type: {task_type} for task {task_id}")
                    continue
                
                task_futures[task_id] = future
                layer_futures.append(future)
            
            # Wait for layer to complete
            [f.result() for f in layer_futures]
        
        logger.info(f"Flow {flow_name} completed")
    
    return generated_flow
```

**Step 2: Update serve.py**

Replace `backend/flows/serve.py` with:

```python
"""
Prefect Flow Deployment Script (Simplified Version)
Loads flow configurations from database and creates Prefect deployments
"""
import os
import sys
from pathlib import Path

# Ensure project root is in path
backend_dir = str(Path(__file__).parent.parent)
sys.path.insert(0, backend_dir)

# Load environment variables before importing prefect
from dotenv import load_dotenv
env_path = Path(backend_dir).parent / ".env"
load_dotenv(env_path)

# Configure timezone - use local timezone, not UTC
tz = os.getenv("TZ", "Asia/Shanghai")
os.environ["TZ"] = tz
os.environ["PREFECT_API_URL"] = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# Bypass proxy for localhost
os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")
os.environ.setdefault("no_proxy", "localhost,127.0.0.1")

from prefect import serve
from app.core.logger import logger


def main():
    """Main function to serve all flows from database"""
    from flows.dynamic_from_config import create_prefect_flow_from_config
    from app.services.flow_service import flow_service
    
    prefect_url = os.environ["PREFECT_API_URL"]
    logger.info(f"Prefect API: {prefect_url}")
    logger.info(f"Timezone: {tz}")
    
    # Check Prefect Server connection
    import httpx
    try:
        resp = httpx.get(f"{prefect_url}/health", timeout=5)
        resp.raise_for_status()
        logger.info("Prefect Server connection successful")
    except Exception as e:
        logger.warning(f"Could not connect to Prefect Server ({e})")
        logger.warning("Please ensure Prefect Server is running: docker compose up prefect-server")
        sys.exit(1)
    
    # Load all enabled flows from database
    logger.info("Loading flows from database...")
    try:
        flows = flow_service.list_flows(enabled_only=True)
        logger.info(f"Found {len(flows)} enabled flows")
    except Exception as e:
        logger.error(f"Failed to load flows from database: {e}")
        sys.exit(1)
    
    # Create deployments
    deployments = []
    
    for flow_list_item in flows:
        try:
            # Get full flow config
            flow_config = flow_service.get_flow(flow_list_item.name)
            if not flow_config:
                continue
            
            # Convert to dict
            flow_dict = {
                "name": flow_config.name,
                "description": flow_config.description,
                "tasks": [t.model_dump() for t in flow_config.tasks],
                "date_offset_days": flow_config.date_offset_days,
            }
            
            # Create Prefect flow
            prefect_flow = create_prefect_flow_from_config(flow_dict)
            
            # Create deployment
            deployment = prefect_flow.to_deployment(
                name=f"{flow_config.name}-deployment",
                cron=flow_config.cron,
                cron_timezone=tz,  # Use local timezone!
                tags=flow_config.tags,
                description=flow_config.description,
            )
            
            deployments.append(deployment)
            logger.info(f"Created deployment for flow: {flow_config.name}")
            
        except Exception as e:
            logger.error(f"Failed to create deployment for flow {flow_list_item.name}: {e}", exc_info=True)
    
    if not deployments:
        logger.warning("No deployments created! Check that flows exist and are enabled.")
        sys.exit(1)
    
    # Serve all deployments
    logger.info(f"Starting to serve {len(deployments)} deployments...")
    serve(*deployments)


if __name__ == "__main__":
    main()
```

**Step 3: Update .env**

Add to `backend/.env`:

```env
# Timezone configuration
TZ=Asia/Shanghai
```

**Step 4: Test flow generation**

Run Python and test:
```python
from app.services.flow_service import flow_service
from flows.dynamic_from_config import create_prefect_flow_from_config

flow_config = flow_service.get_flow("daily_data_sync")
flow_dict = {
    "name": flow_config.name,
    "tasks": [t.model_dump() for t in flow_config.tasks],
    "date_offset_days": flow_config.date_offset_days,
}
prefect_flow = create_prefect_flow_from_config(flow_dict)
print(prefect_flow)
```

**Step 5: Commit**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
git add flows/dynamic_from_config.py
git add flows/serve.py
git add .env  # if not in gitignore
git commit -m "feat: dynamic Prefect flow generation from config"
```

---

## Task 4: Frontend - Add Date Offset Field and Remove Code Editor

**Files:**
- Modify: `frontend/src/api/index.ts` (FlowConfig types)
- Modify: `frontend/src/components/SchedulerFlowEditor/index.tsx`
- Create/Modify: `frontend/src/components/SchedulerFlowEditor/TaskSelector.tsx` (add auto-dependency button)

**Step 1: Update FlowConfig types**

In `frontend/src/api/index.ts`, update `FlowConfig` interface:

```typescript
export interface TaskConfig {
  id: string;
  type: 'sync' | 'etl' | 'factor';
  depends_on: string[];
}

export interface FlowConfig {
  name: string;
  description: string;
  cron: string;
  tags: string[];
  enabled: boolean;
  date_offset_days: number;  // NEW: date offset field
  tasks: TaskConfig[];
}

export interface FlowListItem {
  name: string;
  description: string;
  cron: string;
  tags: string[];
  enabled: boolean;
  date_offset_days: number;  // NEW
  task_count: number;
  updated_at: string;
}
```

Also add the dependency inference API:

```typescript
export const flowApi = {
  // ... existing methods ...
  
  inferDependencies: (tasks: TaskConfig[]) =>
    api.post<{ status: string; tasks: TaskConfig[] }>('/flows/infer-dependencies', tasks),
};
```

**Step 2: Update SchedulerFlowEditor - Add date offset field**

In `frontend/src/components/SchedulerFlowEditor/index.tsx`:

First, update the defaultFlow:

```typescript
const defaultFlow: FlowConfig = {
  name: '',
  description: '',
  cron: '0 18 * * 1-5',
  tags: [],
  enabled: true,
  date_offset_days: -1,  // Default: yesterday
  tasks: [],
};
```

Add the date offset form item after the "状态" section:

```tsx
<FormItem label="业务日期偏移">
  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
    <InputNumber
      min={-365}
      max={365}
      value={flow.date_offset_days}
      onChange={(v) => setFlow(prev => ({ ...prev, date_offset_days: v ?? 0 }))}
      style={{ width: 120 }}
    />
    <Text type="secondary" style={{ fontSize: 12 }}>
      天
      {flow.date_offset_days === 0 && ' (今天)'}
      {flow.date_offset_days === -1 && ' (昨天)'}
      {flow.date_offset_days === 1 && ' (明天)'}
    </Text>
  </div>
  <div style={{ marginTop: 4, fontSize: 12, color: 'var(--text-secondary)' }}>
    调度触发时，业务日期 = 触发日期 + 偏移量
  </div>
</FormItem>
```

**Step 3: Add auto-dependency button to TaskSelector**

In `frontend/src/components/SchedulerFlowEditor/TaskSelector.tsx`, add a button to infer dependencies:

```tsx
// First, import the API
import { flowApi, TaskConfig } from '../../api';

// Add state for loading
const [inferring, setInferring] = useState(false);

// Add the button near the task selection area
<div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
  <Text strong>选择任务</Text>
  <Button
    type="link"
    size="small"
    loading={inferring}
    onClick={async () => {
      if (selectedTasks.length === 0) {
        message.warning('请先选择任务');
        return;
      }
      setInferring(true);
      try {
        const res = await flowApi.inferDependencies(selectedTasks);
        onChange(res.data.tasks);
        message.success('依赖关系识别成功');
      } catch (e) {
        message.error('识别依赖关系失败');
      } finally {
        setInferring(false);
      }
    }}
  >
    自动识别依赖
  </Button>
</div>
```

**Step 4: Test frontend changes**

Start frontend and test:
- Verify date offset field appears and works
- Verify "自动识别依赖" button triggers API call
- Verify dependencies are auto-populated

**Step 5: Commit**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
git add src/api/index.ts
git add src/components/SchedulerFlowEditor/index.tsx
git add src/components/SchedulerFlowEditor/TaskSelector.tsx
git commit -m "feat: add date offset and auto-dependency inference to flow editor"
```

---

## Task 5: Integration Testing and Cleanup

**Files:**
- Test: End-to-end testing
- Cleanup: Remove unused code

**Step 1: Run full integration test**

1. Start all services:
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system
./start.sh
```

2. Seed flow_config table:
```python
from store.dolphindb_client import db_client
db_client.seed_flow_config()
```

3. Test flow CRUD via API:
```bash
# List flows
curl http://localhost:8000/api/v1/flows

# Create a test flow
curl -X POST http://localhost:8000/api/v1/flows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "integration-test-flow",
    "description": "Integration test",
    "cron": "0 0 * * *",
    "tags": ["test"],
    "enabled": true,
    "date_offset_days": -1,
    "tasks": [{"id": "sync_daily", "type": "sync", "depends_on": []}]
  }'
```

4. Test frontend UI:
- Open http://localhost:3000/scheduler
- Create/edit a flow, verify date offset field
- Test auto-dependency button

5. Test Prefect deployment:
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python flows/serve.py
```

Verify deployments appear in Prefect UI at http://localhost:4200.

**Step 2: Cleanup old dynamic_flow.py**

Remove the old file (we're not using it anymore):
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
rm flows/dynamic_flow.py
git add -u
git commit -m "chore: remove old dynamic_flow.py"
```

**Step 3: Final verification**

- [ ] All flows listed in UI
- [ ] Date offset field works
- [ ] Auto-dependency inference works
- [ ] Prefect deployments created successfully
- [ ] Cron uses local timezone
- [ ] No TypeScript errors
- [ ] No Python errors

**Step 4: Commit everything**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system
git add docs/plans/2026-04-08-simplified-flow-scheduler.md
git commit -m "docs: add simplified flow scheduler implementation plan"
```

---

## Execution Handoff

Plan complete and saved to `docs/plans/2026-04-08-simplified-flow-scheduler.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
