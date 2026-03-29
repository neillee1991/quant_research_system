"""
API tests for stock pool routes.
"""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.stock_pool import router
from stock_pool.models.pool import (
    ApiResponse,
    ListResponse,
    PoolDetail,
    PoolMetadata,
    PoolStatus,
    PoolSyncStatus,
    PoolType,
    SubscribeResult,
    SyncStatus,
    WeightMethod,
)

app = FastAPI()
app.include_router(router)
client = TestClient(app)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_metadata(**kwargs) -> PoolMetadata:
    defaults = dict(
        pool_id="TEST_POOL",
        pool_type=PoolType.STATIC,
        pool_name="Test Pool",
        description="",
        status=PoolStatus.DRAFT,
        version=1,
        weight_method=WeightMethod.EQUAL,
        created_at=datetime(2024, 1, 1),
        updated_at=datetime(2024, 1, 1),
    )
    defaults.update(kwargs)
    return PoolMetadata(**defaults)


def _make_list_response(items=None) -> ListResponse:
    return ListResponse(
        items=items or [_make_metadata()],
        total=1,
        page=1,
        limit=20,
        has_more=False,
    )


# ---------------------------------------------------------------------------
# Pool Management
# ---------------------------------------------------------------------------

def test_list_pools_success():
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.list_pools.return_value = _make_list_response()
        resp = client.get("/stock-pool/pools")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["total"] == 1
    assert len(body["data"]["items"]) == 1


def test_list_pools_with_filters():
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.list_pools.return_value = _make_list_response()
        resp = client.get("/stock-pool/pools?pool_type=static&status=active&page=2&limit=10")
    assert resp.status_code == 200
    mock_svc.list_pools.assert_called_once_with(
        pool_type="static", status="active", page=2, limit=10
    )


def test_get_pool_success():
    detail = PoolDetail(metadata=_make_metadata(), constituents=[], sync_status=None)
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.get_pool.return_value = detail
        resp = client.get("/stock-pool/pools/TEST_POOL")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["metadata"]["pool_id"] == "TEST_POOL"


def test_get_pool_not_found():
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.get_pool.side_effect = ValueError("股票池不存在: NONEXISTENT")
        resp = client.get("/stock-pool/pools/NONEXISTENT")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "NOT_FOUND"


def test_create_pool_success():
    meta = _make_metadata(pool_id="NEW_POOL", pool_name="New Pool")
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.create_pool.return_value = meta
        resp = client.post("/stock-pool/pools", json={
            "pool_id": "NEW_POOL",
            "pool_type": "static",
            "pool_name": "New Pool",
        })
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["pool_id"] == "NEW_POOL"


def test_create_pool_invalid_id():
    resp = client.post("/stock-pool/pools", json={
        "pool_id": "invalid id!",
        "pool_type": "static",
        "pool_name": "Bad Pool",
    })
    assert resp.status_code == 422


def test_update_pool_success():
    meta = _make_metadata(pool_name="Updated")
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.update_pool.return_value = meta
        resp = client.put("/stock-pool/pools/TEST_POOL", json={"pool_name": "Updated"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True


def test_change_status_success():
    meta = _make_metadata(status=PoolStatus.ACTIVE)
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.change_status.return_value = meta
        resp = client.post("/stock-pool/pools/TEST_POOL/status", json={
            "status": "active",
            "reason": "ready",
        })
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["status"] == "active"


def test_change_status_invalid_transition():
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.change_status.side_effect = ValueError("非法状态转换")
        resp = client.post("/stock-pool/pools/TEST_POOL/status", json={"status": "active"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "INVALID_TRANSITION"


def test_archive_pool_success():
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.archive_pool.return_value = _make_metadata(status=PoolStatus.ARCHIVED)
        resp = client.delete("/stock-pool/pools/TEST_POOL")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["archived"] is True


# ---------------------------------------------------------------------------
# Index Discovery & Subscription
# ---------------------------------------------------------------------------

def test_get_available_indexes():
    from stock_pool.models.pool import AvailableIndex
    index_item = AvailableIndex(ts_code="000300.SH", name="沪深300", is_subscribed=False)
    list_resp = ListResponse(items=[index_item], total=1, page=1, limit=20, has_more=False)
    with patch("app.api.v1.stock_pool._index_sync_service") as mock_svc:
        mock_svc.get_available_indexes.return_value = list_resp
        resp = client.get("/stock-pool/index/available")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["total"] == 1


def test_get_available_indexes_with_search():
    from stock_pool.models.pool import AvailableIndex
    list_resp = ListResponse(items=[], total=0, page=1, limit=20, has_more=False)
    with patch("app.api.v1.stock_pool._index_sync_service") as mock_svc:
        mock_svc.get_available_indexes.return_value = list_resp
        resp = client.get("/stock-pool/index/available?search=沪深&page=1&limit=10")
    assert resp.status_code == 200
    mock_svc.get_available_indexes.assert_called_once_with(search="沪深", page=1, limit=10)


def test_subscribe_index_success():
    result = SubscribeResult(
        pool_id="POOL_000300_SH",
        pool_type=PoolType.INDEX,
        pool_name="沪深300",
        status=PoolStatus.ACTIVE,
        index_code="000300.SH",
        created_at=datetime(2024, 1, 1),
    )
    with patch("app.api.v1.stock_pool._index_sync_service") as mock_svc:
        mock_svc.subscribe_index.return_value = result
        resp = client.post("/stock-pool/pools/index-subscribe", json={
            "index_code": "000300.SH",
        })
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["pool_id"] == "POOL_000300_SH"
    assert body["data"]["index_code"] == "000300.SH"


def test_subscribe_index_duplicate():
    with patch("app.api.v1.stock_pool._index_sync_service") as mock_svc:
        mock_svc.subscribe_index.side_effect = ValueError("指数已订阅: 000300.SH")
        resp = client.post("/stock-pool/pools/index-subscribe", json={
            "index_code": "000300.SH",
        })
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "DUPLICATE_OR_NOT_FOUND"


def test_sync_pool_success():
    with patch("app.api.v1.stock_pool._index_sync_service") as mock_svc:
        mock_svc.trigger_sync.return_value = {"synced": True, "records": 300}
        resp = client.post("/stock-pool/pools/POOL_000300_SH/sync", json={"trade_date": "20240328"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["synced"] is True


def test_sync_pool_not_found():
    with patch("app.api.v1.stock_pool._index_sync_service") as mock_svc:
        mock_svc.trigger_sync.side_effect = ValueError("股票池不存在")
        resp = client.post("/stock-pool/pools/NONEXISTENT/sync", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "NOT_FOUND"


def test_get_pool_constituents():
    constituents = [
        {"trade_date": "20240328", "ts_code": "600519.SH", "weight": 0.05, "rank": 1}
    ]
    with patch("app.api.v1.stock_pool._pool_service") as mock_svc:
        mock_svc.get_constituents.return_value = constituents
        resp = client.get("/stock-pool/pools/TEST_POOL/constituents?trade_date=20240328")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert len(body["data"]) == 1
    assert body["data"][0]["ts_code"] == "600519.SH"
