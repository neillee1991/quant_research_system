"""
Flow 配置管理 API
"""
import os
from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
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
async def run_flow(
    name: str,
    target_date: Optional[str] = Query(None, description="目标日期 YYYYMMDD")
):
    """立即执行 Flow"""
    from flows.dynamic_flow import run_dynamic_flow

    config = _load_flow(name)

    try:
        result = await run_dynamic_flow(config, target_date)
        return {"status": "success", "result": result}
    except Exception as e:
        logger.error(f"Flow execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
