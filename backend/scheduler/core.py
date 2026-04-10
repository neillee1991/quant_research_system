"""
调度器核心
主循环 + 最小堆 + cron 计算
"""
import asyncio
import heapq
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from croniter import croniter

from app.core.logger import logger
from .db import DatabasePool, init_db, close_db
from .models import FlowRun, FlowStatus, TriggerType
from .repository import FlowRepository, FlowRunRepository
from .executor import DAGExecutor


class ScheduledFlow:
    """调度堆中的 Flow 项"""

    def __init__(self, flow_config: Dict[str, Any], next_run: datetime):
        self.flow_config = flow_config
        self.next_run = next_run

    def __lt__(self, other: "ScheduledFlow") -> bool:
        return self.next_run < other.next_run


class Scheduler:
    """自研调度器核心"""

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._heap: List[ScheduledFlow] = []
        self._flow_versions: Dict[str, int] = {}  # name -> version
        self._executor = DAGExecutor()
        self._db_poll_interval = 10.0  # 10 秒轮询一次 DB
        self._check_interval = 1.0  # 1 秒检查一次堆

    async def start(self):
        """启动调度器"""
        if self._running:
            return

        logger.info("启动调度器...")

        # 初始化数据库
        await init_db()

        # 加载所有 enabled flow
        await self._load_flows()

        self._running = True
        self._task = asyncio.create_task(self._main_loop())
        logger.info("调度器已启动")

    async def stop(self):
        """停止调度器"""
        if not self._running:
            return

        logger.info("停止调度器...")
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        await close_db()
        logger.info("调度器已停止")

    async def _load_flows(self):
        """从数据库加载所有 enabled flow"""
        logger.info("加载 Flow 配置...")
        flows = await FlowRepository.list_all(enabled_only=True)

        for flow in flows:
            self._add_or_update_flow(flow)

        logger.info(f"已加载 {len(flows)} 个 Flow")

    def _add_or_update_flow(self, flow_config: Dict[str, Any]):
        """添加或更新 flow 到堆"""
        name = flow_config["name"]
        version = flow_config["version"]
        cron = flow_config.get("cron")

        # 检查是否需要更新
        if name in self._flow_versions and self._flow_versions[name] >= version:
            return

        # 计算下次运行时间
        if cron:
            try:
                now = datetime.now()
                iter = croniter(cron, now)
                next_run = iter.get_next(datetime)
                heapq.heappush(self._heap, ScheduledFlow(flow_config, next_run))
                self._flow_versions[name] = version
                logger.info(f"Flow 已加入调度: {name}, 下次运行: {next_run}")
            except Exception as e:
                logger.error(f"无效的 cron 表达式 '{cron}' for flow '{name}': {e}")
        else:
            logger.info(f"Flow 无 cron 表达式，仅支持手动触发: {name}")

    async def _main_loop(self):
        """主循环"""
        last_db_poll = datetime.now()

        while self._running:
            try:
                now = datetime.now()

                # 检查堆中到期的 flow
                while self._heap and self._heap[0].next_run <= now:
                    scheduled = heapq.heappop(self._heap)
                    asyncio.create_task(self._trigger_flow(scheduled.flow_config, TriggerType.CRON))

                    # 计算下次运行时间并重新加入堆
                    if scheduled.flow_config.get("cron"):
                        try:
                            iter = croniter(scheduled.flow_config["cron"], now)
                            next_run = iter.get_next(datetime)
                            heapq.heappush(self._heap, ScheduledFlow(scheduled.flow_config, next_run))
                        except Exception as e:
                            logger.error(f"计算下次运行时间失败: {e}")

                # 定期轮询 DB 检查变化
                if (now - last_db_poll).total_seconds() >= self._db_poll_interval:
                    await self._poll_db_changes()
                    last_db_poll = now

                await asyncio.sleep(self._check_interval)

            except Exception as e:
                logger.error(f"调度器主循环异常: {e}", exc_info=True)
                await asyncio.sleep(self._check_interval)

    async def _poll_db_changes(self):
        """轮询 DB 检查 flow 变化"""
        try:
            # 重新加载所有 enabled flow
            await self._load_flows()
        except Exception as e:
            logger.error(f"轮询 DB 失败: {e}", exc_info=True)

    async def _trigger_flow(self, flow_config: Dict[str, Any], trigger_type: TriggerType,
                          target_date: Optional[str] = None) -> Optional[int]:
        """触发 Flow 执行"""
        name = flow_config["name"]
        logger.info(f"触发 Flow: {name}, 触发类型: {trigger_type.value}")

        # 计算 target_date
        if not target_date:
            offset_days = flow_config.get("date_offset_days", 0)
            base_date = datetime.now()
            target_date = (base_date + timedelta(days=offset_days)).strftime("%Y%m%d")

        # 创建 FlowRun
        flow_run = FlowRun(
            flow_name=name,
            status=FlowStatus.PENDING,
            trigger_type=trigger_type,
            target_date=target_date,
            scheduled_at=datetime.now(),
        )
        flow_run_id = await FlowRunRepository.create(flow_run)
        flow_run.id = flow_run_id

        # 异步执行 Flow
        tasks = flow_config.get("tasks", [])
        asyncio.create_task(self._executor.execute_flow(flow_run, tasks))

        return flow_run_id

    async def trigger_flow_manual(self, flow_name: str,
                                 target_date: Optional[str] = None) -> Optional[int]:
        """手动触发 Flow（外部 API 调用）"""
        flow = await FlowRepository.get_by_name(flow_name)
        if not flow:
            logger.error(f"Flow 不存在: {flow_name}")
            return None

        return await self._trigger_flow(flow, TriggerType.MANUAL, target_date)


# 全局调度器实例
_scheduler: Optional[Scheduler] = None


def get_scheduler() -> Scheduler:
    """获取全局调度器实例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = Scheduler()
    return _scheduler
