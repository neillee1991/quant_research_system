"""
DAG 执行引擎
支持任务依赖解析、拓扑排序、按层并行执行
"""
from enum import Enum
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque
import threading
import concurrent.futures
import json
from pathlib import Path

from app.core.logger import logger
from app.core.utils import DateUtils


class TaskStatus(Enum):
    PENDING = "pending"
    WAITING = "waiting"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskNode:
    task_id: str
    task_type: str  # "sync" or "production"
    depends_on: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    rows_affected: int = 0


@dataclass
class DAGRun:
    dag_id: str
    run_id: str
    description: str
    tasks: Dict[str, TaskNode] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    target_date: Optional[str] = None
    dag_started_at: Optional[datetime] = None
    dag_finished_at: Optional[datetime] = None


class DAGConfigManager:
    """DAG 配置管理器"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = str(Path(__file__).parent / "dag_config.json")
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> dict:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Loaded DAG config from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.warning(f"DAG config not found: {self.config_path}")
            return {"dags": []}

    def get_dag(self, dag_id: str) -> Optional[dict]:
        for dag in self._config.get("dags", []):
            if dag["dag_id"] == dag_id:
                return dag
        return None

    def get_all_dags(self) -> List[dict]:
        return self._config.get("dags", [])

    def get_dags_by_schedule(self, schedule: str) -> List[dict]:
        return [d for d in self._config.get("dags", []) if d.get("schedule") == schedule]

    def add_dag(self, dag_config: dict) -> None:
        """添加新 DAG"""
        dag_id = dag_config.get("dag_id")
        if not dag_id:
            raise ValueError("dag_id is required")
        if self.get_dag(dag_id):
            raise ValueError(f"DAG {dag_id} already exists")
        self._config.setdefault("dags", []).append(dag_config)
        self._save_config()

    def update_dag(self, dag_id: str, dag_config: dict) -> None:
        """更新 DAG 配置"""
        dags = self._config.get("dags", [])
        for i, dag in enumerate(dags):
            if dag["dag_id"] == dag_id:
                dag_config["dag_id"] = dag_id  # 保持 ID 不变
                dags[i] = dag_config
                self._save_config()
                return
        raise ValueError(f"DAG {dag_id} not found")

    def delete_dag(self, dag_id: str) -> None:
        """删除 DAG"""
        dags = self._config.get("dags", [])
        before = len(dags)
        self._config["dags"] = [d for d in dags if d["dag_id"] != dag_id]
        if len(self._config["dags"]) == before:
            raise ValueError(f"DAG {dag_id} not found")
        self._save_config()

    def _save_config(self) -> None:
        """写回配置文件"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self._config, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved DAG config to {self.config_path}")

    def reload(self) -> None:
        """重新加载配置"""
        self._config = self._load_config()


class DAGExecutor:
    """DAG 执行引擎"""

    def __init__(self, sync_engine, production_engine, db_client, max_workers: int = 3):
        self.sync_engine = sync_engine
        self.production_engine = production_engine
        self.db = db_client
        self.max_workers = max_workers
        self.config_manager = DAGConfigManager()
        self._current_runs: Dict[str, DAGRun] = {}
        self._lock = threading.Lock()

    def build_dag(self, dag_config: dict) -> DAGRun:
        """从配置构建 DAGRun，校验无环"""
        dag_id = dag_config["dag_id"]
        run_id = f"{dag_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        tasks = {}
        for task_cfg in dag_config.get("tasks", []):
            tasks[task_cfg["task_id"]] = TaskNode(
                task_id=task_cfg["task_id"],
                task_type=task_cfg.get("task_type", "sync"),
                depends_on=task_cfg.get("depends_on", []),
            )

        dag_run = DAGRun(
            dag_id=dag_id, run_id=run_id,
            description=dag_config.get("description", ""),
            tasks=tasks,
        )

        if not self._validate_no_cycles(dag_run):
            raise ValueError(f"DAG {dag_id} contains cycles")

        all_ids = set(tasks.keys())
        for node in tasks.values():
            for dep in node.depends_on:
                if dep not in all_ids:
                    raise ValueError(f"Task {node.task_id} depends on unknown: {dep}")

        return dag_run

    def _validate_no_cycles(self, dag_run: DAGRun) -> bool:
        """Kahn 算法检测环"""
        in_degree = {tid: 0 for tid in dag_run.tasks}
        for node in dag_run.tasks.values():
            for dep in node.depends_on:
                if dep in in_degree:
                    in_degree[node.task_id] += 1

        queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
        visited = 0
        while queue:
            tid = queue.popleft()
            visited += 1
            for node in dag_run.tasks.values():
                if tid in node.depends_on:
                    in_degree[node.task_id] -= 1
                    if in_degree[node.task_id] == 0:
                        queue.append(node.task_id)
        return visited == len(dag_run.tasks)

    def topological_sort(self, dag_run: DAGRun) -> List[List[str]]:
        """拓扑排序，返回执行层列表（同层可并行）"""
        in_degree = {tid: 0 for tid in dag_run.tasks}
        dependents = defaultdict(list)

        for node in dag_run.tasks.values():
            for dep in node.depends_on:
                if dep in dag_run.tasks:
                    in_degree[node.task_id] += 1
                    dependents[dep].append(node.task_id)

        layers = []
        queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
        while queue:
            layer = list(queue)
            layers.append(layer)
            next_queue = deque()
            for tid in layer:
                for dependent in dependents[tid]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_queue.append(dependent)
            queue = next_queue
        return layers

    def execute_dag(self, dag_id: str, target_date: Optional[str] = None,
                    trigger_type: str = "manual",
                    run_type: str = "today",
                    backfill_id: Optional[str] = None) -> DAGRun:
        """执行 DAG"""
        dag_config = self.config_manager.get_dag(dag_id)
        if not dag_config:
            raise ValueError(f"DAG not found: {dag_id}")

        dag_run = self.build_dag(dag_config)
        dag_run.target_date = target_date
        dag_run.dag_started_at = datetime.now()
        dag_run.status = TaskStatus.RUNNING

        with self._lock:
            self._current_runs[dag_run.run_id] = dag_run
        self._save_dag_run(dag_run, trigger_type, run_type, backfill_id)

        try:
            layers = self.topological_sort(dag_run)
            logger.info(f"DAG {dag_id} plan: {layers}")

            for layer_idx, layer in enumerate(layers):
                logger.info(f"DAG {dag_id} layer {layer_idx+1}/{len(layers)}: {layer}")
                runnable = []
                for tid in layer:
                    node = dag_run.tasks[tid]
                    if self._should_skip(node, dag_run):
                        node.status = TaskStatus.SKIPPED
                        node.error_message = "Dependency failed"
                        self._save_task_log(dag_run.run_id, node)
                    else:
                        node.status = TaskStatus.WAITING
                        runnable.append(tid)

                if runnable:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                        futs = {pool.submit(self._execute_task, dag_run, t, target_date): t for t in runnable}
                        for fut in concurrent.futures.as_completed(futs):
                            try:
                                fut.result()
                            except Exception as e:
                                logger.error(f"Task {futs[fut]} exception: {e}")

            statuses = [n.status for n in dag_run.tasks.values()]
            if all(s == TaskStatus.SUCCESS for s in statuses):
                dag_run.status = TaskStatus.SUCCESS
            elif any(s == TaskStatus.FAILED for s in statuses):
                dag_run.status = TaskStatus.FAILED
            else:
                dag_run.status = TaskStatus.SUCCESS
        except Exception as e:
            dag_run.status = TaskStatus.FAILED
            logger.error(f"DAG {dag_id} failed: {e}")

        dag_run.dag_finished_at = datetime.now()
        self._update_dag_run(dag_run)
        elapsed = (dag_run.dag_finished_at - dag_run.dag_started_at).total_seconds()
        logger.info(f"DAG {dag_id}: {dag_run.status.value} in {elapsed:.1f}s")
        return dag_run

    def _should_skip(self, node: TaskNode, dag_run: DAGRun) -> bool:
        for dep_id in node.depends_on:
            dep = dag_run.tasks.get(dep_id)
            if dep and dep.status in (TaskStatus.FAILED, TaskStatus.SKIPPED):
                return True
        return False

    def _execute_task(self, dag_run: DAGRun, task_id: str,
                      target_date: Optional[str]) -> bool:
        node = dag_run.tasks[task_id]
        node.status = TaskStatus.RUNNING
        node.started_at = datetime.now()
        self._save_task_log(dag_run.run_id, node)

        # 统一日期格式为 YYYYMMDD（sync_engine / production_engine 内部使用此格式）
        normalized_date = DateUtils.normalize_date(target_date)

        try:
            if node.task_type == "sync":
                success = self.sync_engine.sync_task(task_id, normalized_date)
            elif node.task_type == "production":
                success = self.production_engine.run_task(task_id, normalized_date)
            else:
                raise ValueError(f"Unknown task type: {node.task_type}")
            node.status = TaskStatus.SUCCESS if success else TaskStatus.FAILED
        except Exception as e:
            node.status = TaskStatus.FAILED
            node.error_message = str(e)
            logger.error(f"Task {task_id} failed: {e}")

        node.finished_at = datetime.now()
        self._save_task_log(dag_run.run_id, node)
        return node.status == TaskStatus.SUCCESS

    # ==================== 持久化 ====================

    def _save_dag_run(self, dag_run: DAGRun, trigger_type: str,
                      run_type: str = "today", backfill_id: Optional[str] = None):
        try:
            self.db.execute("""
                INSERT INTO dag_run_log (dag_id, run_id, status, target_date, started_at, trigger_type, run_type, backfill_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (dag_run.dag_id, dag_run.run_id, dag_run.status.value,
                  dag_run.target_date, dag_run.dag_started_at, trigger_type, run_type, backfill_id))
        except Exception as e:
            logger.error(f"Failed to save DAG run: {e}")

    def _update_dag_run(self, dag_run: DAGRun):
        try:
            self.db.execute("""
                UPDATE dag_run_log SET status=%s, finished_at=%s WHERE run_id=%s
            """, (dag_run.status.value, dag_run.dag_finished_at, dag_run.run_id))
        except Exception as e:
            logger.error(f"Failed to update DAG run: {e}")

    def _save_task_log(self, run_id: str, node: TaskNode):
        try:
            self.db.execute("""
                INSERT INTO dag_task_log (run_id, task_id, task_type, status,
                    started_at, finished_at, rows_affected, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (run_id, node.task_id, node.task_type, node.status.value,
                  node.started_at, node.finished_at, node.rows_affected, node.error_message))
        except Exception as e:
            logger.error(f"Failed to save task log: {e}")

    # ==================== 查询 ====================

    def get_run_status(self, run_id: str) -> Optional[dict]:
        try:
            run_df = self.db.query("SELECT * FROM dag_run_log WHERE run_id=%s", (run_id,))
            if run_df.is_empty():
                return None
            tasks_df = self.db.query(
                "SELECT * FROM dag_task_log WHERE run_id=%s ORDER BY started_at", (run_id,))
            run = run_df.to_dicts()[0]
            run["tasks"] = tasks_df.to_dicts() if not tasks_df.is_empty() else []
            return run
        except Exception as e:
            logger.error(f"Failed to get run status: {e}")
            return None

    def get_dag_runs(self, dag_id: str, limit: int = 20,
                     run_type: Optional[str] = None) -> List[dict]:
        try:
            where = "WHERE dag_id=%s"
            params: list = [dag_id]
            if run_type:
                where += " AND run_type=%s"
                params.append(run_type)
            params.append(limit)
            df = self.db.query(f"""
                SELECT * FROM dag_run_log {where} ORDER BY started_at DESC LIMIT %s
            """, tuple(params))
            if df.is_empty():
                return []
            runs = df.to_dicts()
            # 批量获取所有 run_id 的任务日志
            run_ids = [r["run_id"] for r in runs]
            placeholders = ",".join(["%s"] * len(run_ids))
            task_df = self.db.query(f"""
                SELECT run_id, task_id, task_type, status, error_message,
                    started_at, finished_at, rows_affected
                FROM dag_task_log WHERE run_id IN ({placeholders})
            """, tuple(run_ids))
            task_map: dict = {}
            if not task_df.is_empty():
                for row in task_df.to_dicts():
                    rid = row["run_id"]
                    if rid not in task_map:
                        task_map[rid] = []
                    task_map[rid].append({
                        "task_id": row["task_id"],
                        "task_type": row["task_type"],
                        "status": row["status"],
                        "error_message": row.get("error_message"),
                        "started_at": str(row["started_at"]) if row.get("started_at") else None,
                        "finished_at": str(row["finished_at"]) if row.get("finished_at") else None,
                        "rows_affected": row.get("rows_affected", 0),
                    })
            for r in runs:
                r["task_results"] = task_map.get(r["run_id"], [])
            return runs
        except Exception as e:
            logger.error(f"Failed to get DAG runs: {e}")
            return []

    def get_backfill_summary(self, backfill_id: str) -> Optional[dict]:
        """获取回溯批次汇总"""
        try:
            df = self.db.query("""
                SELECT * FROM dag_run_log WHERE backfill_id=%s ORDER BY target_date
            """, (backfill_id,))
            if df.is_empty():
                return None
            runs = df.to_dicts()
            success_count = sum(1 for r in runs if r["status"] == "success")
            failed_count = sum(1 for r in runs if r["status"] == "failed")
            return {
                "backfill_id": backfill_id,
                "dag_id": runs[0]["dag_id"],
                "total_days": len(runs),
                "success_days": success_count,
                "failed_days": failed_count,
                "date_range": [runs[0]["target_date"], runs[-1]["target_date"]],
                "started_at": str(runs[0].get("started_at", "")),
                "finished_at": str(runs[-1].get("finished_at", "")),
                "runs": runs,
            }
        except Exception as e:
            logger.error(f"Failed to get backfill summary: {e}")
            return None
