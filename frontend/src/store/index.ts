import { create } from 'zustand';
import { Node, Edge } from 'reactflow';

interface FlowState {
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[]) => void;
  setEdges: (edges: Edge[]) => void;
  addNode: (node: Node) => void;
  reset: () => void;
}

export const useFlowStore = create<FlowState>((set) => ({
  nodes: [],
  edges: [],
  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set({ edges }),
  addNode: (node) => set((s) => ({ nodes: [...s.nodes, node] })),
  reset: () => set({ nodes: [], edges: [] }),
}));

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
