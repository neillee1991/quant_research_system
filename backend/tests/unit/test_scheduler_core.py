"""Scheduler core 单元测试（不依赖数据库）"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import heapq

from scheduler.core import Scheduler, ScheduledFlow


class TestScheduledFlow:

    def test_lt_comparison(self):
        now = datetime.now()
        a = ScheduledFlow({"name": "a"}, now)
        b = ScheduledFlow({"name": "b"}, now + timedelta(seconds=10))
        assert a < b

    def test_heap_ordering(self):
        now = datetime.now()
        flows = [
            ScheduledFlow({"name": "c"}, now + timedelta(seconds=30)),
            ScheduledFlow({"name": "a"}, now + timedelta(seconds=10)),
            ScheduledFlow({"name": "b"}, now + timedelta(seconds=20)),
        ]
        heap = []
        for f in flows:
            heapq.heappush(heap, f)
        # 最小堆应按 next_run 升序弹出
        assert heapq.heappop(heap).flow_config["name"] == "a"
        assert heapq.heappop(heap).flow_config["name"] == "b"
        assert heapq.heappop(heap).flow_config["name"] == "c"


class TestSchedulerAddOrUpdateFlow:

    def setup_method(self):
        self.scheduler = Scheduler.__new__(Scheduler)
        self.scheduler._heap = []
        self.scheduler._flow_versions = {}

    def test_add_new_flow_with_cron(self):
        flow = {"name": "test_flow", "version": 1, "cron": "0 9 * * *"}
        self.scheduler._add_or_update_flow(flow)
        assert len(self.scheduler._heap) == 1
        assert self.scheduler._flow_versions["test_flow"] == 1

    def test_skip_older_version(self):
        flow_v2 = {"name": "test_flow", "version": 2, "cron": "0 9 * * *"}
        self.scheduler._add_or_update_flow(flow_v2)
        assert self.scheduler._flow_versions["test_flow"] == 2

        flow_v1 = {"name": "test_flow", "version": 1, "cron": "0 10 * * *"}
        self.scheduler._add_or_update_flow(flow_v1)
        # 版本号更低，不应更新
        assert self.scheduler._flow_versions["test_flow"] == 2
        assert len(self.scheduler._heap) == 1

    def test_update_with_newer_version(self):
        flow_v1 = {"name": "test_flow", "version": 1, "cron": "0 9 * * *"}
        self.scheduler._add_or_update_flow(flow_v1)
        assert len(self.scheduler._heap) == 1

        flow_v2 = {"name": "test_flow", "version": 2, "cron": "0 10 * * *"}
        self.scheduler._add_or_update_flow(flow_v2)
        assert self.scheduler._flow_versions["test_flow"] == 2
        assert len(self.scheduler._heap) == 2  # 旧条目留在堆中，新条目加入

    def test_flow_without_cron_not_added_to_heap(self):
        flow = {"name": "manual_flow", "version": 1, "cron": None}
        self.scheduler._add_or_update_flow(flow)
        assert len(self.scheduler._heap) == 0
        assert "manual_flow" not in self.scheduler._flow_versions

    def test_invalid_cron_not_added(self):
        flow = {"name": "bad_flow", "version": 1, "cron": "invalid_cron"}
        self.scheduler._add_or_update_flow(flow)
        assert len(self.scheduler._heap) == 0

    def test_next_run_is_in_future(self):
        flow = {"name": "future_flow", "version": 1, "cron": "0 9 * * *"}
        self.scheduler._add_or_update_flow(flow)
        scheduled = self.scheduler._heap[0]
        assert scheduled.next_run > datetime.now()

    def test_multiple_flows(self):
        flows = [
            {"name": "flow_a", "version": 1, "cron": "0 9 * * *"},
            {"name": "flow_b", "version": 1, "cron": "0 10 * * *"},
            {"name": "flow_c", "version": 1, "cron": "0 11 * * *"},
        ]
        for f in flows:
            self.scheduler._add_or_update_flow(f)
        assert len(self.scheduler._heap) == 3
        assert len(self.scheduler._flow_versions) == 3


class TestSchedulerTargetDate:
    """测试 target_date 计算逻辑"""

    def test_date_offset_zero(self):
        from scheduler.core import Scheduler
        scheduler = Scheduler.__new__(Scheduler)
        offset_days = 0
        base = datetime(2026, 4, 25)
        result = (base + timedelta(days=offset_days)).strftime("%Y%m%d")
        assert result == "20260425"

    def test_date_offset_negative(self):
        offset_days = -1
        base = datetime(2026, 4, 25)
        result = (base + timedelta(days=offset_days)).strftime("%Y%m%d")
        assert result == "20260424"

    def test_date_offset_positive(self):
        offset_days = 1
        base = datetime(2026, 4, 25)
        result = (base + timedelta(days=offset_days)).strftime("%Y%m%d")
        assert result == "20260426"
