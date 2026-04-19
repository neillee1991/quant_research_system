import { create } from 'zustand';
import type {
  StrategyScriptValidateResponse,
  StrategyScriptCompileResponse,
} from '../types';

export type ScriptRunStatus =
  | 'idle'
  | 'validating'
  | 'compiling'
  | 'submitting'
  | 'running'
  | 'success'
  | 'failed';

interface StrategyScriptState {
  code: string;
  validationResult: StrategyScriptValidateResponse | null;
  compileResult: StrategyScriptCompileResponse | null;
  runId: string | null;
  runStatus: ScriptRunStatus;
  runError: string | null;

  setCode: (code: string) => void;
  setValidationResult: (result: StrategyScriptValidateResponse | null) => void;
  setCompileResult: (result: StrategyScriptCompileResponse | null) => void;
  setRunId: (runId: string | null) => void;
  setRunStatus: (status: ScriptRunStatus) => void;
  setRunError: (error: string | null) => void;
  resetRun: () => void;
}

const DEFAULT_CODE = [
  'def build_strategy():',
  '    return {',
  '        "ts_code": "000001.SZ",',
  '        "start_date": "20230101",',
  '        "end_date": "20241231",',
  '        "capital": 1000000,',
  '        "signals": [',
  '            {"type": "indicator", "op": "sma", "params": {"window": 20}, "output_col": "sma20"},',
  '            {"type": "condition", "expr": "close > sma20", "output_col": "signal"},',
  '        ],',
  '    }',
].join('\n');

export const useStrategyScriptStore = create<StrategyScriptState>((set) => ({
  code: DEFAULT_CODE,
  validationResult: null,
  compileResult: null,
  runId: null,
  runStatus: 'idle',
  runError: null,

  setCode: (code) => set({ code }),
  setValidationResult: (validationResult) => set({ validationResult }),
  setCompileResult: (compileResult) => set({ compileResult }),
  setRunId: (runId) => set({ runId }),
  setRunStatus: (runStatus) => set({ runStatus }),
  setRunError: (runError) => set({ runError }),
  resetRun: () => set({ runId: null, runStatus: 'idle', runError: null }),
}));
