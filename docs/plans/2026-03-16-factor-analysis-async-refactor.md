# Factor Analysis Async Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 删除 legacy 分析路径，统一走 Alphalens，并将分析任务改为异步（提交返回 task_id，前端轮询状态）。

**Architecture:** 后端用 FastAPI `BackgroundTasks` 在后台执行 Alphalens 分析，任务状态写入已有的 `factor_analysis_extended` 表（`task_status` 字段）。前端提交后立即拿到 `task_id`，轮询 `/analysis/alphalens/{factor_id}/status/{task_id}` 直到完成。保留一个 `run_prefect_flow` 钩子函数供后续接入 Prefect。

**Tech Stack:** Python FastAPI BackgroundTasks, Polars, Alphalens, React + TypeScript, Semi UI

---

## 改动范围总览

| 文件 | 操作 |
|------|------|
| `backend/engine/analysis/analyzer.py` | 删除 `_analyze_legacy` 及相关私有方法，简化 `analyze()` |
| `backend/app/api/v1/production/factor_analysis.py` | 重写：删除 legacy 端点，`/analysis/alphalens` 改为异步提交，新增状态查询端点 |
| `frontend/src/api/index.ts` | 删除 `runAnalysis`/`getAnalysis`/`getAnalysisHistory`，新增 `getAnalysisStatus` |
| `frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts` | 删除 `useAlphalens` 分支，改为轮询逻辑 |
| `frontend/src/pages/FactorCenter/AnalysisPanel.tsx` | 删除 Alphalens 切换复选框及 legacy 渲染分支 |

---

## Task 1: 简化 analyzer.py，删除 legacy 路径

**Files:**
- Modify: `backend/engine/analysis/analyzer.py`

**Step 1: 删除 legacy 相关方法**

删除以下方法（全部在 `FactorAnalyzer` 类中）：
- `_analyze_legacy()`（约 L79-134）
- `_calc_ic_series()`（约 L186-242）
- `_calc_quantile_returns()`（约 L246-283）
- `_calc_turnover()`（约 L287-354）
- `_build_summary()`（约 L358-429）
- `_save_analysis()`（约 L433-467）
- `get_latest_analysis()`（约 L723-741）
- `get_analysis_history()`（约 L743-756）

**Step 2: 简化 `analyze()` 方法**

将 `analyze()` 改为直接调用 `_analyze_with_alphalens()`，去掉 `use_alphalens` 参数：

```python
def analyze(
    self,
    factor_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    periods: List[int] = None,
    quantiles: int = 5,
    index_pool: Optional[str] = None,
    groupby_field: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """执行因子分析（Alphalens）"""
    if periods is None:
        periods = [1, 5, 10, 20]
    return self._analyze_with_alphalens(
        factor_id, start_date, end_date, periods, quantiles, index_pool, groupby_field
    )
```

**Step 3: 新增 Prefect 钩子（空实现，供后续接入）**

在 `FactorAnalyzer` 类末尾添加：

```python
def run_prefect_flow(
    self,
    factor_id: str,
    start_date: Optional[str],
    end_date: Optional[str],
    periods: List[int],
    quantiles: int,
    index_pool: Optional[str],
    groupby_field: Optional[str],
) -> Optional[str]:
    """
    通过 Prefect 调度分析任务（可选）。
    返回 Prefect flow run ID，或 None（表示未配置 Prefect）。
    用户可在此处实现 Prefect flow 提交逻辑：
        from prefect import flow
        # flow_run = your_analysis_flow.submit(...)
        # return str(flow_run.id)
    """
    return None
```

**Step 4: 新增任务状态查询方法**

```python
def get_task_status(self, task_id: int) -> Optional[Dict]:
    """查询分析任务状态"""
    try:
        df = self.db.query("""
            SELECT id, factor_id, task_status, error_message, analysis_date
            FROM factor_analysis_extended
            WHERE id = %s
        """, (task_id,))
        if df.is_empty():
            return None
        return df.to_dicts()[0]
    except Exception as e:
        logger.error(f"Failed to get task status: {e}")
        return None
```

**Step 5: 验证文件无语法错误**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from engine.analysis.analyzer import FactorAnalyzer; print('OK')"
```

Expected: `OK`

**Step 6: Commit**

```bash
git add backend/engine/analysis/analyzer.py
git commit -m "refactor: 删除 legacy 分析路径，保留 Alphalens + Prefect 钩子"
```

---

## Task 2: 重写 factor_analysis.py API 层

**Files:**
- Modify: `backend/app/api/v1/production/factor_analysis.py`

**Step 1: 更新 Pydantic 模型**

删除 `AnalyzeRequest`（legacy 用），保留并重命名 `AlphalensAnalysisRequest` → `AnalysisRequest`：

```python
class AnalysisRequest(BaseModel):
    """因子分析请求"""
    factor_id: str
    start_date: str
    end_date: str
    periods: List[int] = [1, 5, 10]
    quantiles: int = 5
    index_pool: Optional[str] = None
    groupby_field: Optional[str] = None


class AnalysisTaskResponse(BaseModel):
    """异步任务提交响应"""
    task_id: int
    factor_id: str
    status: str  # "pending" | "running" | "completed" | "failed"
```

**Step 2: 删除旧端点，新增异步提交端点**

删除：
- `POST /analysis/run`
- `GET /analysis/{factor_id}`
- `GET /analysis/{factor_id}/history`
- `_format_analysis_summary()`
- `_format_db_analysis()`

新增 `POST /analysis/alphalens`（替换原有同名端点，改为异步）：

```python
@router.post("/analysis/alphalens", response_model=dict)
async def submit_analysis(req: AnalysisRequest, background_tasks: BackgroundTasks):
    """提交因子分析任务（异步）。立即返回 task_id，后台执行分析。"""
    import time
    task_id = int(time.time() * 1000)

    # 先在 DB 写入 pending 状态占位
    _create_pending_task(task_id, req)

    # 后台执行
    background_tasks.add_task(
        _run_analysis_background,
        task_id=task_id,
        req=req,
    )

    return {
        "status": "success",
        "data": {"task_id": task_id, "factor_id": req.factor_id, "status": "pending"}
    }
```

**Step 3: 实现后台任务函数**

```python
def _create_pending_task(task_id: int, req: AnalysisRequest):
    """在 DB 写入 pending 占位记录"""
    import json
    from datetime import datetime as dt
    record = {
        "id": task_id,
        "factor_id": req.factor_id,
        "analysis_date": dt.now(),
        "start_date": dt.strptime(req.start_date, "%Y%m%d").date(),
        "end_date": dt.strptime(req.end_date, "%Y%m%d").date(),
        "config": json.dumps({
            "periods": req.periods,
            "quantiles": req.quantiles,
            "index_pool": req.index_pool,
            "groupby_field": req.groupby_field,
        }),
        "task_status": "pending",
        "task_id": str(task_id),
        "error_message": None,
        "ic_summary": None, "ic_by_period": None, "ic_ts": None,
        "quantile_returns": None, "cumulative_returns": None,
        "ic_by_group": None, "returns_by_group": None,
        "turnover": None, "decay_analysis": None, "charts_data": None,
    }
    import polars as pl
    df = pl.DataFrame([record])
    db_client.upsert("factor_analysis_extended", df, key_columns=["id"])


def _run_analysis_background(task_id: int, req: AnalysisRequest):
    """后台执行分析，更新 task_status"""
    import polars as pl
    try:
        # 更新状态为 running
        _update_task_status(task_id, "running")

        results = analyzer.analyze(
            factor_id=req.factor_id,
            start_date=req.start_date,
            end_date=req.end_date,
            periods=req.periods,
            quantiles=req.quantiles,
            index_pool=req.index_pool,
            groupby_field=req.groupby_field,
        )
        if results is None:
            _update_task_status(task_id, "failed", error="分析返回空结果")
        else:
            _update_task_status(task_id, "completed")
    except Exception as e:
        logger.error(f"Background analysis failed for task {task_id}: {e}")
        _update_task_status(task_id, "failed", error=str(e))


def _update_task_status(task_id: int, status: str, error: Optional[str] = None):
    """更新任务状态"""
    try:
        db_client.execute("""
            UPDATE factor_analysis_extended
            SET task_status = %s, error_message = %s
            WHERE id = %s
        """, (status, error, task_id))
    except Exception as e:
        logger.error(f"Failed to update task status {task_id}: {e}")
```

**Step 4: 新增状态查询端点**

```python
@router.get("/analysis/alphalens/status/{task_id}")
async def get_analysis_status(task_id: int):
    """查询分析任务状态"""
    status = analyzer.get_task_status(task_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"任务 {task_id} 不存在")
    return {"status": "success", "data": status}
```

保留已有的：
- `GET /analysis/alphalens/{factor_id}/latest`
- `GET /analysis/alphalens/{factor_id}/history`

**Step 5: 更新 FastAPI imports**

```python
from fastapi import APIRouter, HTTPException, BackgroundTasks
```

**Step 6: 验证**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "from app.api.v1.production.factor_analysis import router; print('OK')"
```

Expected: `OK`

**Step 7: Commit**

```bash
git add backend/app/api/v1/production/factor_analysis.py
git commit -m "feat: 因子分析改为异步任务，删除 legacy 端点"
```

---

## Task 3: 更新前端 API 层

**Files:**
- Modify: `frontend/src/api/index.ts`

**Step 1: 删除 legacy API 方法**

删除以下三个方法：
```typescript
runAnalysis: ...       // L163-164
getAnalysis: ...       // L165
getAnalysisHistory: ...// L166-167
```

**Step 2: 更新 `runAlphalensAnalysis` 的返回类型注释**

将 `runAlphalensAnalysis` 的注释更新，说明返回 `{ task_id, factor_id, status }`。

**Step 3: 新增状态查询方法**

在 `getLatestAlphalensAnalysis` 后面添加：

```typescript
getAnalysisTaskStatus: (taskId: number) =>
  api.get(`/analysis/alphalens/status/${taskId}`),
```

**Step 4: Commit**

```bash
git add frontend/src/api/index.ts
git commit -m "refactor: 删除 legacy 分析 API，新增任务状态查询"
```

---

## Task 4: 重写 useFactorAnalysis.ts Hook

**Files:**
- Modify: `frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts`

**Step 1: 删除 legacy 状态和分支**

删除：
- `useAlphalens` state 和 `setUseAlphalens`
- `runAnalysis` 中的 `if (useAlphalens)` 分支，只保留 Alphalens 路径
- `loadAnalysis` 中的 `if (useAlphalens)` 分支，只保留 Alphalens 路径

**Step 2: 新增轮询逻辑**

```typescript
const [taskId, setTaskId] = useState<number | null>(null);
const [taskStatus, setTaskStatus] = useState<'idle' | 'pending' | 'running' | 'completed' | 'failed'>('idle');
const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

const stopPolling = () => {
  if (pollingRef.current) {
    clearInterval(pollingRef.current);
    pollingRef.current = null;
  }
};

const startPolling = (id: number) => {
  stopPolling();
  pollingRef.current = setInterval(async () => {
    try {
      const res = await productionApi.getAnalysisTaskStatus(id);
      const status = res.data?.data?.status;
      setTaskStatus(status);
      if (status === 'completed') {
        stopPolling();
        setRunLoading(false);
        // 加载最新结果
        const resultRes = await productionApi.getLatestAlphalensAnalysis(selectedFactor);
        setAnalysisResult(resultRes.data?.data);
        await loadHistory(selectedFactor);
        Toast.success('分析完成');
      } else if (status === 'failed') {
        stopPolling();
        setRunLoading(false);
        const error = res.data?.data?.error_message || '分析失败';
        Toast.error(error);
      }
    } catch (e) {
      stopPolling();
      setRunLoading(false);
    }
  }, 2000); // 每 2 秒轮询
};

// 组件卸载时清理
useEffect(() => () => stopPolling(), []);
```

**Step 3: 更新 `runAnalysis`**

```typescript
const runAnalysis = async () => {
  if (!selectedFactor) { Toast.warning('请选择因子'); return; }
  setRunLoading(true);
  setTaskStatus('pending');
  try {
    const res = await productionApi.runAlphalensAnalysis({
      factor_id: selectedFactor,
      start_date: startDate,
      end_date: endDate,
      periods,
      quantiles,
      index_pool: indexPool || undefined,
      groupby_field: groupbyField || undefined,
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

**Step 4: 更新 return 对象**

删除 `useAlphalens`/`setUseAlphalens`，新增 `taskId`/`taskStatus`。

**Step 5: Commit**

```bash
git add frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts
git commit -m "feat: 分析 hook 改为异步轮询模式"
```

---

## Task 5: 更新 AnalysisPanel.tsx UI

**Files:**
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`

**Step 1: 删除 Alphalens 切换复选框**

找到并删除类似以下的 UI 代码：
```tsx
<Checkbox checked={useAlphalens} onChange={...}>使用 Alphalens</Checkbox>
```

**Step 2: 删除 legacy 渲染分支**

`getICChartOption()` 等函数中有 `if (useAlphalens) { ... } else { ... }` 分支，删除 `else` 分支，只保留 Alphalens 路径。

**Step 3: 新增任务状态展示**

在"运行分析"按钮旁边，根据 `taskStatus` 显示状态标签：

```tsx
{taskStatus === 'pending' && <Tag color="orange">等待中...</Tag>}
{taskStatus === 'running' && <Tag color="blue">分析中...</Tag>}
{taskStatus === 'completed' && <Tag color="green">已完成</Tag>}
{taskStatus === 'failed' && <Tag color="red">失败</Tag>}
```

**Step 4: 更新 hook 解构，删除 `useAlphalens`/`setUseAlphalens`，新增 `taskId`/`taskStatus`**

**Step 5: 验证前端编译**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm run build 2>&1 | tail -20
```

Expected: 无 TypeScript 错误

**Step 6: Commit**

```bash
git add frontend/src/pages/FactorCenter/AnalysisPanel.tsx
git commit -m "feat: 分析面板删除 legacy 切换，新增任务状态展示"
```

---

## Task 6: 验证端到端流程

**Step 1: 启动后端，验证路由注册**

```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python -c "
from app.main import app
routes = [r.path for r in app.routes]
assert any('/analysis/alphalens' in r for r in routes), 'alphalens route missing'
assert not any(r == '/api/v1/analysis/run' for r in routes), 'legacy route still exists'
print('Routes OK')
"
```

**Step 2: 验证 `_analyze_legacy` 已不存在**

```bash
grep -r "_analyze_legacy\|use_alphalens\|AnalyzeRequest\|runAnalysis\|getAnalysis[^H]" \
  backend/engine/analysis/analyzer.py \
  backend/app/api/v1/production/factor_analysis.py \
  frontend/src/api/index.ts \
  frontend/src/pages/FactorCenter/hooks/useFactorAnalysis.ts
```

Expected: 无输出（或只有注释中的引用）

**Step 3: 最终 commit**

```bash
git add -A
git commit -m "chore: 验证 legacy 路径已完全清除"
```

---

## 注意事项

1. **DolphinDB UPDATE 语法**：`_update_task_status` 中用了 `UPDATE ... SET ... WHERE`，需确认 DolphinDB 是否支持。如果不支持，改为 upsert 整行（先查出记录，更新字段，再 upsert）。

2. **`factor_analysis_extended` 表的 `task_id` 字段类型**：当前代码中 `task_id` 存的是字符串，但 `id` 是 int。轮询时用 `id` 字段查询，不用 `task_id`。

3. **Prefect 接入**：`analyzer.run_prefect_flow()` 是空钩子，用户可在其中实现 Prefect flow 提交，然后在 `_run_analysis_background` 中调用它替代直接调用 `analyzer.analyze()`。
