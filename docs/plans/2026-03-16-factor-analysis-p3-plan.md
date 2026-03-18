# Factor Analysis P3: Data Cache + Interactive Re-run

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现分析数据缓存（TTL + LRU + 主动释放），支持用户调整参数后快速重跑（跳过数据加载），前端展示缓存状态和重跑入口。

**Architecture:** 新建 `AnalysisDataCache` 单例（`data_cache.py`），以 `task_id` 为 key 缓存原始 factor_df 和 price_df；`_analyze_with_alphalens()` 优先从缓存取数据；API 新增 `cache_key` 参数和缓存管理端点；前端在 warnings 卡片上提供"调整参数并重跑"按钮。

**Tech Stack:** Python threading, Polars, FastAPI

**依赖:** P0、P1、P2 计划已完成

---

## Task 1: 新建 AnalysisDataCache

**Files:**
- Create: `backend/engine/analysis/data_cache.py`

**Step 1: 创建文件**

```python
"""
分析数据缓存
以 task_id 为 key 缓存原始 factor_df 和 price_df，避免重跑时重复加载数据库。

释放策略（三重保障）：
1. TTL 自动过期（30 分钟未访问）
2. 用户主动释放（DELETE /analysis/cache/{task_id}）
3. 内存压力保护（总缓存超 500MB 时 LRU 淘汰）
"""
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional
import polars as pl
from app.core.logger import logger

_TTL_SECONDS = 1800          # 30 分钟
_MAX_TOTAL_BYTES = 500 * 1024 * 1024  # 500 MB
_SCAN_INTERVAL = 300         # 每 5 分钟扫描一次


@dataclass
class CacheEntry:
    factor_df: pl.DataFrame
    price_df: pl.DataFrame
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)

    @property
    def size_bytes(self) -> int:
        """估算内存占用（行数 × 列数 × 8 字节）"""
        f_size = self.factor_df.shape[0] * self.factor_df.shape[1] * 8
        p_size = self.price_df.shape[0] * self.price_df.shape[1] * 8
        return f_size + p_size

    def touch(self):
        self.last_accessed = time.time()


class AnalysisDataCache:
    """线程安全的分析数据缓存，单例模式"""

    _instance: Optional["AnalysisDataCache"] = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._store: Dict[str, CacheEntry] = {}
                    cls._instance._store_lock = threading.Lock()
                    cls._instance._start_background_cleanup()
        return cls._instance

    def put(self, task_id: str, factor_df: pl.DataFrame, price_df: pl.DataFrame):
        """存入缓存，若内存超限则先 LRU 淘汰"""
        entry = CacheEntry(factor_df=factor_df, price_df=price_df)
        with self._store_lock:
            self._evict_if_needed(entry.size_bytes)
            self._store[task_id] = entry
            logger.info(f"Cache put: task_id={task_id}, size={entry.size_bytes // 1024}KB, "
                        f"total_entries={len(self._store)}")

    def get(self, task_id: str) -> Optional[CacheEntry]:
        """取出缓存，更新 last_accessed"""
        with self._store_lock:
            entry = self._store.get(task_id)
            if entry is None:
                return None
            # 检查 TTL
            if time.time() - entry.last_accessed > _TTL_SECONDS:
                del self._store[task_id]
                logger.info(f"Cache expired on access: task_id={task_id}")
                return None
            entry.touch()
            return entry

    def delete(self, task_id: str) -> bool:
        """主动释放缓存"""
        with self._store_lock:
            if task_id in self._store:
                del self._store[task_id]
                logger.info(f"Cache deleted: task_id={task_id}")
                return True
            return False

    def stats(self) -> dict:
        """返回缓存状态（调试用）"""
        with self._store_lock:
            total_bytes = sum(e.size_bytes for e in self._store.values())
            return {
                "entries": len(self._store),
                "total_mb": round(total_bytes / 1024 / 1024, 2),
                "keys": list(self._store.keys()),
            }

    def _evict_if_needed(self, new_entry_bytes: int):
        """若总内存超限，按 LRU 淘汰（调用时已持有锁）"""
        total = sum(e.size_bytes for e in self._store.values()) + new_entry_bytes
        if total <= _MAX_TOTAL_BYTES:
            return
        # 按 last_accessed 升序排列，淘汰最久未访问的
        sorted_keys = sorted(self._store.keys(), key=lambda k: self._store[k].last_accessed)
        for key in sorted_keys:
            if total <= _MAX_TOTAL_BYTES:
                break
            total -= self._store[key].size_bytes
            del self._store[key]
            logger.info(f"Cache LRU evicted: task_id={key}")

    def _cleanup_expired(self):
        """清除过期条目"""
        now = time.time()
        with self._store_lock:
            expired = [k for k, e in self._store.items()
                       if now - e.last_accessed > _TTL_SECONDS]
            for k in expired:
                del self._store[k]
                logger.info(f"Cache TTL expired: task_id={k}")

    def _start_background_cleanup(self):
        """启动后台清理线程"""
        def _loop():
            while True:
                time.sleep(_SCAN_INTERVAL)
                try:
                    self._cleanup_expired()
                except Exception as e:
                    logger.error(f"Cache cleanup error: {e}")

        t = threading.Thread(target=_loop, daemon=True)
        t.start()


# 全局单例
analysis_cache = AnalysisDataCache()
```

**Step 2: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "
from engine.analysis.data_cache import analysis_cache
stats = analysis_cache.stats()
assert stats['entries'] == 0
print('Cache OK:', stats)
"
```

Expected: `Cache OK: {'entries': 0, 'total_mb': 0.0, 'keys': []}`

**Step 3: Commit**

```bash
git add backend/engine/analysis/data_cache.py
git commit -m "feat: 新增 AnalysisDataCache，TTL+LRU+主动释放三重保障"
```

---

## Task 2: 将缓存接入 analyzer.py

**Files:**
- Modify: `backend/engine/analysis/analyzer.py` — `_analyze_with_alphalens()` 方法
- Modify: `backend/app/api/v1/production/factor_analysis.py` — `AlphalensAnalysisRequest`

**Step 1: 在 `AlphalensAnalysisRequest` 中新增 `cache_key` 参数**

```python
cache_key: Optional[str] = None  # 传入上次的 task_id，复用数据缓存
```

**Step 2: 在 `analyze()` 和 `_analyze_with_alphalens()` 签名中新增 `cache_key` 参数**

**Step 3: 在 `_analyze_with_alphalens()` 数据加载部分，优先从缓存取数据**

在加载 factor_df 之前：

```python
from engine.analysis.data_cache import analysis_cache

# 尝试从缓存取数据
cached = None
if cache_key:
    cached = analysis_cache.get(cache_key)
    if cached:
        logger.info(f"Cache hit: cache_key={cache_key}, skipping DB load")

if cached:
    factor_df = cached.factor_df
    price_df = cached.price_df
    # 重新应用股票池过滤（缓存的是原始数据）
    if index_pool:
        constituents = self._get_index_constituents(index_pool, start_date, end_date)
        if constituents is not None and not constituents.is_empty():
            factor_df = factor_df.join(
                constituents.select(["ts_code", "trade_date"]),
                on=["ts_code", "trade_date"], how="inner"
            )
else:
    # 正常从 DB 加载
    factor_df = self._load_factor_data(factor_id, start_date, end_date)
    ...
    price_df = self._load_price_data(factor_df, start_date, end_date, max(periods))
    ...
    # 存入缓存（用当前 task_id，由调用方传入）
    # task_id 在 API 层生成，通过参数传入
```

**Step 4: 在 `_analyze_with_alphalens()` 末尾，将数据存入缓存**

```python
# 存入缓存（供后续重跑使用）
if task_id:
    analysis_cache.put(str(task_id), original_factor_df, original_price_df)
    results['cache_key'] = str(task_id)
```

注意：缓存的是**原始**数据（winsorize 和中性化之前），`original_factor_df` 需要在 winsorize 步骤之前保存一份引用。

**Step 5: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.analyzer import FactorAnalyzer; print('OK')"
```

Expected: `OK`

**Step 6: Commit**

```bash
git add backend/engine/analysis/analyzer.py \
        backend/app/api/v1/production/factor_analysis.py
git commit -m "feat: 分析数据缓存接入，重跑时跳过 DB 加载"
```

---

## Task 3: 新增缓存管理 API 端点

**Files:**
- Modify: `backend/app/api/v1/production/factor_analysis.py`

**Step 1: 新增两个端点**

```python
from engine.analysis.data_cache import analysis_cache

@router.delete("/analysis/cache/{task_id}")
async def delete_analysis_cache(task_id: str):
    """主动释放分析数据缓存"""
    deleted = analysis_cache.delete(task_id)
    return {
        "status": "success",
        "data": {"deleted": deleted, "task_id": task_id}
    }


@router.get("/analysis/cache/stats")
async def get_cache_stats():
    """查看缓存状态（调试用）"""
    return {"status": "success", "data": analysis_cache.stats()}
```

**Step 2: 在 `submit_analysis` 端点中，将 `task_id` 透传给 `_run_analysis_background`**

（P0 异步重构计划中已有 `task_id`，此处确保它被传入 `analyzer.analyze()`）

**Step 3: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from app.api.v1.production.factor_analysis import router; print('OK')"
```

Expected: `OK`

**Step 4: Commit**

```bash
git add backend/app/api/v1/production/factor_analysis.py
git commit -m "feat: 新增缓存管理 API（DELETE /analysis/cache/{task_id}）"
```

---

## Task 4: 前端支持带缓存的重跑

**Files:**
- Modify: `frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts`
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`
- Modify: `frontend/src/api/index.ts`

**Step 1: 更新 `api/index.ts`，新增缓存相关 API**

```typescript
deleteAnalysisCache: (taskId: string) =>
  api.delete(`/analysis/cache/${taskId}`),
getAnalysisCacheStats: () =>
  api.get('/analysis/cache/stats'),
```

同时在 `runAlphalensAnalysis` 类型中新增 `cache_key?: string`。

**Step 2: 在 `useFactorAnalysis.ts` 中新增缓存 key 状态**

```typescript
const [cacheKey, setCacheKey] = useState<string | null>(null);
```

在 `runAnalysis` 成功后，从返回结果中提取 `cache_key`：

```typescript
// 分析完成后（轮询到 completed 状态，加载结果时）
const resultRes = await productionApi.getLatestAlphalensAnalysis(selectedFactor);
const result = resultRes.data?.data;
setAnalysisResult(result);
// 保存 cache_key 供重跑使用
if (result?.cache_key) {
  setCacheKey(result.cache_key);
}
```

新增 `rerunWithParams` 函数（带缓存重跑）：

```typescript
const rerunWithParams = async (overrides: Partial<AnalysisParams>) => {
  if (!cacheKey) {
    Toast.warning('无缓存数据，将重新加载');
  }
  setRunLoading(true);
  setTaskStatus('pending');
  try {
    const res = await productionApi.runAlphalensAnalysis({
      factor_id: selectedFactor,
      start_date: startDate,
      end_date: endDate,
      periods,
      quantiles,
      ...overrides,
      cache_key: cacheKey || undefined,
    });
    const id = res.data?.data?.task_id;
    setTaskId(id);
    startPolling(id);
  } catch (error: any) {
    setRunLoading(false);
    setTaskStatus('failed');
    Toast.error(error.response?.data?.detail || '提交失败');
  }
};
```

在组件卸载或切换因子时，主动释放缓存：

```typescript
useEffect(() => {
  return () => {
    if (cacheKey) {
      productionApi.deleteAnalysisCache(cacheKey).catch(() => {});
    }
  };
}, [cacheKey]);
```

**Step 3: 在 `AnalysisPanel.tsx` 的 warnings 卡片中，用 `rerunWithParams` 替代普通重跑**

```tsx
{w.key === 'extreme_values' && (
  <Button
    size="small"
    onClick={() => rerunWithParams({
      winsorize: true,
      winsorize_lower: w.suggested_params.winsorize_lower,
      winsorize_upper: w.suggested_params.winsorize_upper,
    })}
  >
    启用 Winsorize 并重跑（使用缓存）
  </Button>
)}
{w.key === 'size_bias' && (
  <Button
    size="small"
    onClick={() => rerunWithParams({ neutralize_size: true })}
  >
    启用市值中性化并重跑（使用缓存）
  </Button>
)}
{w.key === 'industry_concentration' && (
  <Button
    size="small"
    onClick={() => rerunWithParams({ neutralize_industry: true })}
  >
    启用行业中性化并重跑（使用缓存）
  </Button>
)}
```

**Step 4: 验证前端编译**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm run build 2>&1 | grep -E "ERROR|error TS" | head -20
```

Expected: 无输出

**Step 5: Commit**

```bash
git add frontend/src/pages/FactorCenter/AnalysisPanel.tsx \
        frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts \
        frontend/src/api/index.ts
git commit -m "feat: 前端支持带缓存的参数调整重跑，组件卸载时自动释放缓存"
```

---

## 注意事项

1. **缓存的是原始数据**：`original_factor_df` 必须在 winsorize 和中性化之前保存，否则重跑时无法用不同参数重新处理。

2. **`task_id` 的传递链路**：`task_id` 在 API 层（`factor_analysis.py`）生成，需要通过 `analyzer.analyze()` 的参数传入 `_analyze_with_alphalens()`，再传给 `analysis_cache.put()`。当前 `analyze()` 签名没有 `task_id` 参数，需要新增。

3. **缓存 key 的返回**：`cache_key` 需要出现在 `factor_analysis_extended` 表的结果中（或直接在 API 响应的 `data` 字段里），前端才能在轮询完成后拿到它。最简单的方式是在 `_save_alphalens_analysis()` 时把 `task_id` 存入 `config` JSON 字段。

4. **并发安全**：`AnalysisDataCache` 使用 `threading.Lock` 保护 `_store`，但 Polars DataFrame 本身是不可变的，读取时不需要额外锁。
