import { create } from 'zustand';
import type { RunningTask } from '../api';

interface BacktestState {
  result: any | null;
  loading: boolean;
  setResult: (r: any) => void;
  setLoading: (v: boolean) => void;
}

export const useBacktestStore = create<BacktestState>((set) => ({
  result: null,
  loading: false,
  setResult: (result) => set({ result }),
  setLoading: (loading) => set({ loading }),
}));

type ThemeMode = 'dark' | 'light';

interface ThemeState {
  mode: ThemeMode;
  toggle: () => void;
  setMode: (mode: ThemeMode) => void;
}

export const useThemeStore = create<ThemeState>((set) => ({
  mode: (localStorage.getItem('theme-mode') as ThemeMode) || 'dark',
  toggle: () =>
    set((s) => {
      const next = s.mode === 'dark' ? 'light' : 'dark';
      localStorage.setItem('theme-mode', next);
      document.body.setAttribute('theme-mode', next);
      return { mode: next };
    }),
  setMode: (mode) => {
    localStorage.setItem('theme-mode', mode);
    document.body.setAttribute('theme-mode', mode);
    set({ mode });
  },
}));

interface TaskMonitorState {
  runningTasks: RunningTask[];
  isLoading: boolean;
  lastFetched: number;
  setRunningTasks: (tasks: RunningTask[]) => void;
  addTask: (task: RunningTask) => void;
  updateTask: (runId: string, updates: Partial<RunningTask>) => void;
  removeTask: (runId: string) => void;
  setLoading: (loading: boolean) => void;
  setLastFetched: (time: number) => void;
}

export const useTaskMonitorStore = create<TaskMonitorState>((set) => ({
  runningTasks: [],
  isLoading: false,
  lastFetched: 0,
  setRunningTasks: (tasks) => set({ runningTasks: tasks }),
  addTask: (task) => set((s) => ({ runningTasks: [...s.runningTasks, task] })),
  updateTask: (runId, updates) =>
    set((s) => ({
      runningTasks: s.runningTasks.map((t) =>
        t.run_id === runId ? { ...t, ...updates } : t
      ),
    })),
  removeTask: (runId) =>
    set((s) => ({
      runningTasks: s.runningTasks.filter((t) => t.run_id !== runId),
    })),
  setLoading: (loading) => set({ isLoading: loading }),
  setLastFetched: (time) => set({ lastFetched: time }),
}));

interface NavState {
  dataTab: '1' | '2' | '3';
  setDataTab: (tab: '1' | '2' | '3') => void;
}

export const useNavStore = create<NavState>((set) => ({
  dataTab: '1',
  setDataTab: (dataTab) => set({ dataTab }),
}));
