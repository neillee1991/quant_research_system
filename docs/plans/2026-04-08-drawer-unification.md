# Drawer Unification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Unify all task drawer history tabs to use `TaskLogTable` + `useTaskLogs`, delete legacy `UniversalHistoryTab` and `UniversalStatusTab`, and migrate `FactorDrawer` to use `BaseTaskDrawer`.

**Architecture:** All task drawers (Sync, ETL, Factor) share `BaseTaskDrawer` as the shell. History tabs use `TaskLogTable` with `useTaskLogs` hook for consistent filtering. Legacy tab components are deleted after migration.

**Tech Stack:** React, TypeScript, Ant Design, `useTaskLogs` hook, `TaskLogTable` component, `BaseTaskDrawer`

---

### Task 1: ETLTaskDrawer — delete status tab, replace UniversalHistoryTab with TaskLogTable

**Files:**
- Modify: `frontend/src/pages/DataCenter/ETLTaskDrawer/index.tsx`

**Context:**
- `ETLTaskDrawer` already uses `BaseTaskDrawer` as shell
- Currently has tabs: 配置 | 脚本测试 | JSON编辑 | 状态 | 历史记录
- Target tabs: 配置 | 脚本测试 | JSON编辑 | 历史记录
- `UniversalStatusTab` is imported but its data (`taskStatus`) is already shown in the inline status bar above the tabs — the tab is redundant
- `UniversalHistoryTab` needs to be replaced with `TaskLogTable` + `useTaskLogs('etl', 50)`

**Step 1: Remove status tab and UniversalStatusTab/UniversalHistoryTab imports**

In `ETLTaskDrawer/index.tsx`:
- Remove import: `UniversalStatusTab` from `../../../components/TaskDrawer/tabs/UniversalStatusTab`
- Remove import: `UniversalHistoryTab` from `../../../components/TaskDrawer/tabs/UniversalHistoryTab`
- Add imports: `useTaskLogs` from `../../../hooks/useTaskLogs`, `TaskLogTable` from `../../../components/TaskLogTable`

**Step 2: Add useTaskLogs hook**

Add inside the component (after existing state declarations):
```typescript
const { logs: etlLogs, loading: etlLogsLoading, loadLogs: loadEtlLogs } = useTaskLogs('etl', 50);
```

**Step 3: Remove taskStatus state and loadTaskStatus**

- Remove: `const [taskStatus, setTaskStatus] = useState<any>(null);`
- Remove: `loadTaskStatus()` call in `useEffect`
- Remove: `const loadTaskStatus = async () => { ... }` function
- Remove: `setTaskStatus(null)` in the isNew branch of useEffect

**Step 4: Replace history tab, delete status tab**

Replace the `tabItems` array. Remove the `status` tab entirely. Replace the `history` tab:
```typescript
...(isNew ? [] : [{
  key: 'history',
  label: '历史记录',
  children: (
    <TaskLogTable
      logs={etlLogs}
      loading={etlLogsLoading}
      taskIdLabel="任务ID"
      onFilter={(f) => loadEtlLogs({ ...f, taskId: f.taskId || config.task_id })}
    />
  ),
}]),
```

**Step 5: Load logs when history tab is activated**

Add a `useEffect` that triggers when `activeTab === 'history'` and `config.task_id` is set:
```typescript
useEffect(() => {
  if (activeTab === 'history' && config.task_id && !isNew) {
    loadEtlLogs({ taskId: config.task_id });
  }
}, [activeTab, config.task_id, isNew]);
```

**Step 6: Verify TypeScript compiles without errors**

Check that no references to `taskStatus`, `loadTaskStatus`, `UniversalStatusTab`, `UniversalHistoryTab` remain.

---

### Task 2: SyncTaskDrawer — replace UniversalHistoryTab with TaskLogTable

**Files:**
- Modify: `frontend/src/pages/DataCenter/SyncTaskDrawer/index.tsx`

**Context:**
- `SyncTaskDrawer` already uses `BaseTaskDrawer` as shell (from previous session)
- Status tab was already deleted in previous session
- Currently has tabs: 可视化编辑 | JSON 编辑 | 历史与数据
- The "历史与数据" tab uses `UniversalHistoryTab` + `SyncDataInspectTab`
- Replace `UniversalHistoryTab` with `TaskLogTable` + `useTaskLogs('sync', 50)`

**Step 1: Update imports**

In `SyncTaskDrawer/index.tsx`:
- Remove import: `UniversalHistoryTab` from `../../../components/TaskDrawer/tabs/UniversalHistoryTab`
- Add imports: `useTaskLogs` from `../../../hooks/useTaskLogs`, `TaskLogTable` from `../../../components/TaskLogTable`

**Step 2: Add useTaskLogs hook**

Add inside the component:
```typescript
const { logs: syncLogs, loading: syncLogsLoading, loadLogs: loadSyncLogs } = useTaskLogs('sync', 50);
```

**Step 3: Replace UniversalHistoryTab in the history tab**

The "历史与数据" tab currently renders:
```tsx
<UniversalHistoryTab taskType="sync" taskId={task.task_id} />
```

Replace with:
```tsx
<TaskLogTable
  logs={syncLogs}
  loading={syncLogsLoading}
  taskIdLabel="任务ID"
  onFilter={(f) => loadSyncLogs({ ...f, taskId: f.taskId || task.task_id })}
/>
```

**Step 4: Load logs when history tab is activated**

Add a `useEffect`:
```typescript
useEffect(() => {
  if (activeTab === 'history' && task?.task_id && !isNew) {
    loadSyncLogs({ taskId: task.task_id });
  }
}, [activeTab, task?.task_id, isNew]);
```

**Step 5: Verify TypeScript compiles without errors**

Check that no references to `UniversalHistoryTab` remain in this file.

---

### Task 3: FactorDrawer — adopt BaseTaskDrawer

**Files:**
- Modify: `frontend/src/pages/FactorCenter/FactorDrawer.tsx`

**Context:**
- `FactorDrawer` currently uses raw `<Drawer>` from antd
- The save button is rendered inline at the bottom of the "编辑" tab content
- `BaseTaskDrawer` provides: unified shell, title, footer with cancel + save buttons
- The "编辑" tab has TWO save actions: `handleSave` (basic info + preprocess) and `handleSaveCode` (code)
- Only `handleSave` should go in the footer; `handleSaveCode` stays inline in the code section
- `FactorDrawer` has no `saveLoading` prop on `BaseTaskDrawer` — use `editSaving`

**Step 1: Add BaseTaskDrawer import**

Add import: `BaseTaskDrawer` from `../../components/TaskDrawer/BaseTaskDrawer`

**Step 2: Remove raw Drawer import**

Remove `Drawer` from the antd imports list (keep all other antd imports).

**Step 3: Replace <Drawer> with <BaseTaskDrawer>**

Replace:
```tsx
<Drawer
  title={
    <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
      <span style={{ color: 'var(--color-primary)' }}>{factorId}</span>
    </div>
  }
  open={open} onClose={onClose} width={780}
>
  ...
</Drawer>
```

With:
```tsx
<BaseTaskDrawer
  visible={open}
  title={factorId || ''}
  onClose={onClose}
  onSave={handleSave}
  saveLoading={editSaving}
  saveText="保存"
  width={780}
>
  ...
</BaseTaskDrawer>
```

**Step 4: Remove inline save button from edit tab**

Remove the `<div style={{ marginTop: 12, textAlign: 'right' }}>` block containing the save Button at the bottom of the 'edit' tab children. The save action is now in the BaseTaskDrawer footer.

**Step 5: Verify TypeScript compiles without errors**

Check that `Drawer` is no longer imported and `BaseTaskDrawer` is used correctly.

---

### Task 4: Delete legacy components

**Files:**
- Delete: `frontend/src/components/TaskDrawer/tabs/UniversalHistoryTab.tsx`
- Delete: `frontend/src/components/TaskDrawer/tabs/UniversalStatusTab.tsx`

**Context:**
- After Tasks 1-3, neither file should have any remaining imports
- Verify with grep before deleting

**Step 1: Verify no remaining imports**

Run grep to confirm no files import these components:
```bash
grep -r "UniversalHistoryTab\|UniversalStatusTab" frontend/src --include="*.tsx" --include="*.ts"
```

Expected: no results (or only the files themselves).

**Step 2: Delete the files**

```bash
rm frontend/src/components/TaskDrawer/tabs/UniversalHistoryTab.tsx
rm frontend/src/components/TaskDrawer/tabs/UniversalStatusTab.tsx
```

**Step 3: Check if tabs/ directory has other files**

```bash
ls frontend/src/components/TaskDrawer/tabs/
```

If only `UniversalJsonEditorTab.tsx` remains, that's correct — keep it.

**Step 4: Verify TypeScript compiles without errors**

The build should have no missing module errors.
