# Index Filter Config Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在配置指数基础信息表时，允许用户选择哪些字段作为指数列表页的筛选项并设置默认值，配置持久化后动态渲染筛选栏。

**Architecture:** `user_sync_preference` 表新增 `filter_config` JSON 字段存储筛选配置；后端 preference API 扩展读写该字段；`list_available_indices` 接口改为接受动态 `filters` 参数；前端配置阶段增加字段选择 UI，指数列表筛选栏改为动态渲染。

**Tech Stack:** FastAPI, Polars, DolphinDB, React, TypeScript, Ant Design

---

### Task 1: 后端 — 扩展 Pydantic 模型

**Files:**
- Modify: `backend/app/api/v1/data/index_api.py`

**Step 1: 更新 Pydantic 模型，增加 filter_config 字段**

在 `index_api.py` 中修改以下模型：

```python
from typing import List, Optional, Any

class FilterFieldConfig(BaseModel):
    """单个筛选字段配置"""
    field: str = Field(..., description="字段名")
    label: str = Field(..., description="显示标签")
    enabled: bool = Field(default=True, description="是否启用为筛选项")
    default_value: Optional[str] = Field(default=None, description="默认筛选值")

class UserSyncPreference(BaseModel):
    """用户同步偏好配置"""
    index_basic_table: str = Field(..., description="指数基础信息表名")
    filter_config: Optional[List[FilterFieldConfig]] = Field(default=None, description="筛选字段配置")

class UserSyncPreferenceResponse(BaseModel):
    """用户同步偏好响应"""
    user_id: str
    index_basic_table: str
    filter_config: Optional[List[FilterFieldConfig]] = None
```

**Step 2: 手动测试后端启动无报错**

```bash
cd backend && python -c "from app.api.v1.data.index_api import UserSyncPreference; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/app/api/v1/data/index_api.py
git commit -m "feat: extend UserSyncPreference model with filter_config"
```

---

### Task 2: 后端 — 更新 get_user_preference 读取 filter_config

**Files:**
- Modify: `backend/app/api/v1/data/index_api.py` (get_user_preference 函数, ~line 422)

**Step 1: 更新 SQL 查询，读取 filter_config 字段**

```python
@router.get("/data/index/preference", response_model=UserSyncPreferenceResponse)
async def get_user_preference():
    try:
        df = db_client.query(
            "SELECT user_id, index_table, filter_config FROM user_sync_preference WHERE user_id = %s",
            ("default",)
        )

        if df.is_empty():
            return UserSyncPreferenceResponse(
                user_id="default",
                index_basic_table="sync_index_basic",
                filter_config=None
            )

        row = df.to_dicts()[0]
        filter_config = None
        raw_filter = row.get("filter_config")
        if raw_filter:
            try:
                parsed = json.loads(raw_filter) if isinstance(raw_filter, str) else raw_filter
                filter_config = [FilterFieldConfig(**f) for f in parsed]
            except Exception:
                filter_config = None

        return UserSyncPreferenceResponse(
            user_id=row.get("user_id", "default"),
            index_basic_table=row.get("index_table", "sync_index_basic"),
            filter_config=filter_config
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get user preference: {e}")
        raise HTTPException(status_code=500, detail=f"获取用户偏好失败: {str(e)}")
```

注意：DolphinDB 的 `user_sync_preference` 表可能还没有 `filter_config` 列。如果查询报错，需要先 ALTER TABLE 或重建表。查看 `store/dolphindb_client.py` 中 `user_sync_preference` 的建表语句，确认是否需要迁移。

**Step 2: 检查建表语句**

```bash
grep -n "user_sync_preference" backend/store/dolphindb_client.py
```

如果建表语句里没有 `filter_config`，在建表语句中加上：
```
filter_config STRING
```

**Step 3: Commit**

```bash
git add backend/app/api/v1/data/index_api.py backend/store/dolphindb_client.py
git commit -m "feat: read filter_config from user_sync_preference"
```

---

### Task 3: 后端 — 更新 save_user_preference 写入 filter_config

**Files:**
- Modify: `backend/app/api/v1/data/index_api.py` (save_user_preference 函数, ~line 456)

**Step 1: 更新保存逻辑，写入 filter_config**

```python
@router.post("/data/index/preference", response_model=UserSyncPreferenceResponse)
async def save_user_preference(request: UserSyncPreference):
    try:
        index_table = request.index_basic_table.strip()
        if not index_table or len(index_table) > 100:
            raise HTTPException(status_code=400, detail="表名不能为空且长度不能超过100个字符")
        if not index_table.replace("_", "").isalnum():
            raise HTTPException(status_code=400, detail="表名只能包含字母、数字和下划线")

        filter_config_json = None
        if request.filter_config is not None:
            filter_config_json = json.dumps(
                [f.model_dump() for f in request.filter_config],
                ensure_ascii=False
            )

        now = datetime.now()
        data = {
            "user_id": "default",
            "index_table": index_table,
            "filter_config": filter_config_json or "",
            "created_at": now,
            "updated_at": now,
        }
        db_client.upsert("user_sync_preference", pl.DataFrame([data]), ["user_id"])
        logger.info(f"Saved user preference: index_table={index_table}, filter_config={filter_config_json}")

        return UserSyncPreferenceResponse(
            user_id="default",
            index_basic_table=index_table,
            filter_config=request.filter_config
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save user preference: {e}")
        raise HTTPException(status_code=500, detail=f"保存用户偏好失败: {str(e)}")
```

**Step 2: 手动测试 API**

```bash
curl -X POST http://localhost:8000/api/v1/data/index/preference \
  -H "Content-Type: application/json" \
  -d '{"index_basic_table":"sync_index_basic","filter_config":[{"field":"market","label":"市场","enabled":true,"default_value":null}]}'
```

Expected: 返回包含 `filter_config` 的 JSON

**Step 3: Commit**

```bash
git add backend/app/api/v1/data/index_api.py
git commit -m "feat: save filter_config to user_sync_preference"
```

---

### Task 4: 后端 — list_available_indices 支持动态 filters

**Files:**
- Modify: `backend/app/api/v1/data/index_api.py` (list_available_indices 函数, ~line 144)

**Step 1: 替换固定 market/publisher 参数为动态 filters**

```python
@router.get("/data/index/available", response_model=IndexListResponse)
async def list_available_indices(
    search: Optional[str] = Query(None),
    filters: Optional[str] = Query(None, description="JSON格式筛选条件，如 {\"market\":\"SSE\"}"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    show_subscribed_only: bool = Query(False),
):
    # 获取表的合法字段名（白名单）
    try:
        allowed_columns = set(db_client.get_table_columns("sync_index_basic"))
    except Exception:
        allowed_columns = {"market", "publisher", "ts_code", "name", "list_date"}

    conditions = []
    params = []

    if search:
        conditions.append("(name LIKE %s OR ts_code LIKE %s)")
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern])

    if filters:
        try:
            filter_dict = json.loads(filters)
            for field, value in filter_dict.items():
                # 白名单校验，防止 SQL 注入
                if field in allowed_columns and value:
                    conditions.append(f"{field} = %s")
                    params.append(value)
        except json.JSONDecodeError:
            pass  # 忽略非法 filters

    # 其余逻辑不变（where_clause, count, query, sort, paginate）
    ...
```

注意：保留原有的 `market`/`publisher` query 参数作为向后兼容，或直接删除（前端同步改掉）。推荐直接删除，前端一起改。

**Step 2: Commit**

```bash
git add backend/app/api/v1/data/index_api.py
git commit -m "feat: dynamic filters in list_available_indices with whitelist validation"
```

---

### Task 5: 前端 — 更新 TypeScript 类型

**Files:**
- Modify: `frontend/src/types/indexSubscribe.ts`
- Modify: `frontend/src/api/index.ts`

**Step 1: 更新类型定义**

在 `indexSubscribe.ts` 中：

```typescript
export interface FilterFieldConfig {
  field: string;
  label: string;
  enabled: boolean;
  default_value: string | null;
}

// 更新 UserPreference
export interface UserPreference {
  index_basic_table: string;
  filter_config?: FilterFieldConfig[];
}

// 删除旧的 FilterOptions（不再需要）
// export interface FilterOptions { ... }  ← 删除
```

**Step 2: 更新 API 调用**

在 `api/index.ts` 中更新 `saveUserPreference` 和 `listAvailableIndices`：

```typescript
saveUserPreference: (data: { index_basic_table: string; filter_config?: FilterFieldConfig[] }) =>
  api.post('/data/index/preference', data),

listAvailableIndices: (params?: {
  page?: number;
  limit?: number;
  search?: string;
  filters?: Record<string, string>;  // 替换原来的 market/publisher
  show_subscribed_only?: boolean;
}) => {
  const { filters, ...rest } = params || {};
  return api.get('/data/index/available', {
    params: {
      ...rest,
      filters: filters ? JSON.stringify(filters) : undefined,
    },
  });
},
```

**Step 3: Commit**

```bash
git add frontend/src/types/indexSubscribe.ts frontend/src/api/index.ts
git commit -m "feat: update types and API for dynamic filter_config"
```

---

### Task 6: 前端 — 配置阶段增加字段选择 UI

**Files:**
- Modify: `frontend/src/pages/DataCenter/IndexSubscribeDrawer.tsx`

**Step 1: 增加状态和加载字段逻辑**

在组件 state 区域增加：

```typescript
const [tableColumns, setTableColumns] = useState<string[]>([]);
const [filterConfig, setFilterConfig] = useState<FilterFieldConfig[]>([]);
const [loadingColumns, setLoadingColumns] = useState(false);
```

增加加载字段函数（选表后触发）：

```typescript
const loadTableColumns = useCallback(async (tableName: string) => {
  if (!tableName) return;
  setLoadingColumns(true);
  try {
    const res = await dataApi.getTableInfo(tableName);
    const cols: string[] = res.data.columns || [];
    setTableColumns(cols);
    // 如果已有 filter_config，保留；否则用表字段初始化（全部 disabled）
    setFilterConfig(prev => {
      if (prev.length > 0) return prev;
      return cols.map(col => ({
        field: col,
        label: col,
        enabled: false,
        default_value: null,
      }));
    });
  } catch (e) {
    console.error('Failed to load table columns:', e);
  } finally {
    setLoadingColumns(false);
  }
}, []);
```

在 `loadUserPreference` 成功后，如果有 `filter_config` 则 `setFilterConfig`；在 `selectedTable` 变化时调 `loadTableColumns`。

**Step 2: 更新 renderConfigStage，增加字段配置 UI**

在"保存配置"按钮上方增加字段配置区域：

```tsx
{tableColumns.length > 0 && (
  <div style={{ marginBottom: '24px' }}>
    <div style={{ marginBottom: '8px', fontSize: '14px', fontWeight: 500 }}>
      筛选字段配置
    </div>
    <Table
      size="small"
      dataSource={filterConfig}
      rowKey="field"
      pagination={false}
      loading={loadingColumns}
      columns={[
        {
          title: '字段名',
          dataIndex: 'field',
          width: 150,
        },
        {
          title: '显示标签',
          dataIndex: 'label',
          width: 150,
          render: (label: string, record: FilterFieldConfig, index: number) => (
            <Input
              size="small"
              value={label}
              onChange={e => {
                const updated = filterConfig.map((f, i) =>
                  i === index ? { ...f, label: e.target.value } : f
                );
                setFilterConfig(updated);
              }}
            />
          ),
        },
        {
          title: '作为筛选项',
          dataIndex: 'enabled',
          width: 100,
          render: (enabled: boolean, record: FilterFieldConfig, index: number) => (
            <Switch
              size="small"
              checked={enabled}
              onChange={val => {
                const updated = filterConfig.map((f, i) =>
                  i === index ? { ...f, enabled: val } : f
                );
                setFilterConfig(updated);
              }}
            />
          ),
        },
        {
          title: '默认值',
          dataIndex: 'default_value',
          render: (val: string | null, record: FilterFieldConfig, index: number) => (
            <Input
              size="small"
              placeholder="留空表示无默认值"
              value={val || ''}
              disabled={!record.enabled}
              onChange={e => {
                const updated = filterConfig.map((f, i) =>
                  i === index ? { ...f, default_value: e.target.value || null } : f
                );
                setFilterConfig(updated);
              }}
            />
          ),
        },
      ]}
    />
  </div>
)}
```

需要在 antd import 中加 `Switch`。

**Step 3: 更新 handleSavePreference，带上 filter_config**

```typescript
const handleSavePreference = async () => {
  if (!selectedTable) {
    message.warning('请选择指数基础信息表');
    return;
  }
  setSavingPreference(true);
  try {
    await indexApi.saveUserPreference({
      index_basic_table: selectedTable,
      filter_config: filterConfig.length > 0 ? filterConfig : undefined,
    });
    message.success('配置已保存');
    setCurrentStep(1);
  } catch (error: any) {
    message.error(`保存配置失败: ${error.response?.data?.detail || error.message}`);
  } finally {
    setSavingPreference(false);
  }
};
```

**Step 4: Commit**

```bash
git add frontend/src/pages/DataCenter/IndexSubscribeDrawer.tsx
git commit -m "feat: add filter field config UI in preference stage"
```

---

### Task 7: 前端 — 指数列表筛选栏改为动态渲染

**Files:**
- Modify: `frontend/src/pages/DataCenter/IndexSubscribeDrawer.tsx`

**Step 1: 替换硬编码筛选状态**

删除：
```typescript
const [selectedMarket, setSelectedMarket] = useState<string>();
// filterOptions 相关状态和 loadFilterOptions 函数
```

新增：
```typescript
const [activeFilters, setActiveFilters] = useState<Record<string, string>>({});
```

在 `loadUserPreference` 成功后，用 `filter_config` 中 `enabled=true` 且有 `default_value` 的字段初始化 `activeFilters`：

```typescript
const initialFilters: Record<string, string> = {};
(pref.filter_config || [])
  .filter(f => f.enabled && f.default_value)
  .forEach(f => { initialFilters[f.field] = f.default_value!; });
setActiveFilters(initialFilters);
```

**Step 2: 更新 loadIndices 调用**

```typescript
const loadIndices = useCallback(async (
  currentPage = 1,
  currentPageSize = 20,
  search = '',
  filters: Record<string, string> = {}
) => {
  setLoading(true);
  try {
    const res = await indexApi.listAvailableIndices({
      page: currentPage,
      limit: currentPageSize,
      search: search || undefined,
      filters: Object.keys(filters).length > 0 ? filters : undefined,
    });
    // ... 其余不变
  }
}, [message]);
```

**Step 3: 更新 renderIndexListStage 筛选栏**

删除硬编码的 market/publisher Select，改为根据 `filterConfig` 动态渲染：

```tsx
{filterConfig.filter(f => f.enabled).map(f => (
  <Select
    key={f.field}
    placeholder={f.label}
    value={activeFilters[f.field]}
    onChange={val => setActiveFilters(prev => ({ ...prev, [f.field]: val }))}
    style={{ width: 150 }}
    allowClear
    onClear={() => setActiveFilters(prev => {
      const next = { ...prev };
      delete next[f.field];
      return next;
    })}
    // 选项从后端 distinct 值获取，或直接让用户输入
    // 简单方案：用 Input 替代 Select，后续可优化为 Select+动态选项
  />
))}
```

注意：动态 Select 的选项来源需要考虑。最简单的方案是改为 `Input` 输入框，用户手动输入筛选值。如果要保留下拉选项，需要后端提供 `GET /data/index/filter-options?table=xxx&field=yyy` 接口返回 distinct 值。**先用 Input 实现，后续再优化为 Select。**

**Step 4: 更新 handleSearch 和 handleReset**

```typescript
const handleSearch = () => {
  setPage(1);
  loadIndices(1, pageSize, searchText, activeFilters);
};

const handleReset = () => {
  setSearchText('');
  // 重置为默认值
  const defaultFilters: Record<string, string> = {};
  filterConfig.filter(f => f.enabled && f.default_value)
    .forEach(f => { defaultFilters[f.field] = f.default_value!; });
  setActiveFilters(defaultFilters);
  setPage(1);
  loadIndices(1, pageSize, '', defaultFilters);
};
```

**Step 5: Commit**

```bash
git add frontend/src/pages/DataCenter/IndexSubscribeDrawer.tsx
git commit -m "feat: dynamic filter bar based on filter_config"
```

---

### Task 8: 清理 — 删除不再使用的代码

**Files:**
- Modify: `frontend/src/pages/DataCenter/IndexSubscribeDrawer.tsx`
- Modify: `frontend/src/types/indexSubscribe.ts`
- Modify: `backend/app/api/v1/data/index_api.py`

**Step 1: 前端删除**
- `FilterOptions` 类型（如果没有其他地方用）
- `loadFilterOptions` 函数
- `filterOptions` state
- 旧的 `selectedMarket` state

**Step 2: 后端删除**
- `GET /data/index/filter-options` 接口（如果前端不再调用）
- `FilterOptionsResponse` 模型

先用 grep 确认没有其他地方引用：
```bash
grep -r "filter-options\|FilterOptions\|loadFilterOptions" frontend/src/
grep -r "filter.options\|FilterOptionsResponse" backend/
```

**Step 3: Commit**

```bash
git add -A
git commit -m "chore: remove unused filter-options code"
```
