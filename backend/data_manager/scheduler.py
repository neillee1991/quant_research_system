"""
数据同步调度器
使用 APScheduler 实现定时任务调度
"""
from typing import Dict, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

from app.core.logger import logger
from data_manager.refactored_sync_engine import sync_engine


class SyncScheduler:
    """同步任务调度器"""

    def __init__(self):
        # 配置 APScheduler
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': ThreadPoolExecutor(max_workers=5)
        }
        job_defaults = {
            'coalesce': True,  # 合并错过的任务
            'max_instances': 1,  # 每个任务最多同时运行1个实例
            'misfire_grace_time': 3600  # 错过任务的宽限时间（秒）
        }

        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='Asia/Shanghai'
        )
        self._running = False

    def start(self):
        """启动调度器"""
        if not self._running:
            self.scheduler.start()
            self._running = True
            logger.info("Sync scheduler started")

    def shutdown(self):
        """关闭调度器"""
        if self._running:
            self.scheduler.shutdown(wait=False)
            self._running = False
            logger.info("Sync scheduler shutdown")

    def add_task_schedule(
        self,
        task_id: str,
        schedule: str,
        cron_expression: Optional[str] = None
    ) -> bool:
        """
        添加任务调度

        Args:
            task_id: 任务ID
            schedule: 调度类型 (daily/weekly/monthly/custom)
            cron_expression: 自定义 cron 表达式（当 schedule='custom' 时使用）

        Returns:
            是否添加成功
        """
        try:
            # 如果任务已存在，先移除
            self.remove_task_schedule(task_id)

            # 根据调度类型创建触发器
            if schedule == "daily":
                # 每天凌晨 2:00 执行
                trigger = CronTrigger(hour=2, minute=0)
            elif schedule == "weekly":
                # 每周一凌晨 3:00 执行
                trigger = CronTrigger(day_of_week='mon', hour=3, minute=0)
            elif schedule == "monthly":
                # 每月1号凌晨 4:00 执行
                trigger = CronTrigger(day=1, hour=4, minute=0)
            elif schedule == "custom" and cron_expression:
                # 自定义 cron 表达式
                trigger = CronTrigger.from_crontab(cron_expression)
            else:
                logger.warning(f"Invalid schedule type: {schedule}")
                return False

            # 添加任务
            self.scheduler.add_job(
                func=self._execute_sync_task,
                trigger=trigger,
                args=[task_id],
                id=task_id,
                name=f"Sync task: {task_id}",
                replace_existing=True
            )

            logger.info(f"Added schedule for task {task_id}: {schedule}")
            return True

        except Exception as e:
            logger.error(f"Failed to add schedule for task {task_id}: {e}")
            return False

    def remove_task_schedule(self, task_id: str) -> bool:
        """移除任务调度"""
        try:
            if self.scheduler.get_job(task_id):
                self.scheduler.remove_job(task_id)
                logger.info(f"Removed schedule for task {task_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to remove schedule for task {task_id}: {e}")
            return False

    def get_task_schedule_info(self, task_id: str) -> Optional[Dict]:
        """获取任务调度信息"""
        try:
            job = self.scheduler.get_job(task_id)
            if job:
                return {
                    "task_id": job.id,
                    "name": job.name,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger)
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get schedule info for task {task_id}: {e}")
            return None

    def get_all_schedules(self) -> Dict[str, Dict]:
        """获取所有调度任务信息"""
        schedules = {}
        for job in self.scheduler.get_jobs():
            schedules[job.id] = {
                "task_id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            }
        return schedules

    def _execute_sync_task(self, task_id: str):
        """执行同步任务（内部方法）"""
        try:
            logger.info(f"Scheduled sync started for task: {task_id}")
            success = sync_engine.sync_task(task_id)
            if success:
                logger.info(f"Scheduled sync completed successfully for task: {task_id}")
            else:
                logger.error(f"Scheduled sync failed for task: {task_id}")
        except Exception as e:
            logger.error(f"Error in scheduled sync for task {task_id}: {e}")

    def load_schedules_from_config(self):
        """从配置文件加载所有启用的任务调度"""
        try:
            tasks = sync_engine.get_all_tasks()
            loaded_count = 0

            for task in tasks:
                task_id = task.get("task_id")
                enabled = task.get("enabled", True)
                schedule = task.get("schedule", "")

                if enabled and schedule and schedule != "manual":
                    if self.add_task_schedule(task_id, schedule):
                        loaded_count += 1

            logger.info(f"Loaded {loaded_count} task schedules from config")
            return loaded_count

        except Exception as e:
            logger.error(f"Failed to load schedules from config: {e}")
            return 0


# 全局调度器实例
sync_scheduler = SyncScheduler()
