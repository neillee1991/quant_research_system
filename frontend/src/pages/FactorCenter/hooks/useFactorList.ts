/**
 * 因子列表管理 Hook
 */

import { useState, useCallback, useEffect } from 'react';
import { notify, extractApiError } from '../../../utils/notify';
import { productionApi } from '../../../api';
import { useTaskLogs } from '../../../hooks/useTaskLogs';
import type { FactorDefinition } from '../../../types';

export const useFactorList = () => {
  const [factors, setFactors] = useState<FactorDefinition[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [runLoading, setRunLoading] = useState<string | null>(null);
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);

  const { logs: history, loading: historyLoading, loadLogs: loadHistory } = useTaskLogs('factor', 100);

  const loadFactors = useCallback(async (): Promise<void> => {
    setLoading(true);
    try {
      const res = await productionApi.listFactors();
      setFactors(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load factors:', error);
    } finally {
      setLoading(false);
    }
  }, []);

  const runFactor = async (
    factorId: string,
    runMode: string,
    startDate?: string,
    endDate?: string
  ): Promise<void> => {
    setRunLoading(factorId);
    try {
      const factor = factors.find(f => f.factor_id === factorId);
      const params = factor?.params as any;
      const pp = params?.preprocess || undefined;
      await productionApi.runProduction(factorId, runMode, undefined, startDate, endDate, pp);
      notify.success(`因子 ${factorId} ${runMode === 'incremental' ? '增量' : '全量'}计算完成`);
      await loadFactors();
      await loadHistory({ taskId: selectedFactor || undefined });
    } catch (error: any) {
      const errorMessage = extractApiError(error.response?.data?.detail, '执行失败');
      notify.error(errorMessage);
      throw error;
    } finally {
      setRunLoading(null);
    }
  };

  const deleteFactor = async (factorId: string): Promise<void> => {
    try {
      await productionApi.deleteFactor(factorId);
      notify.success('因子已删除');
      await loadFactors();
      await loadHistory();
    } catch (error: any) {
      const errorMessage = extractApiError(error.response?.data?.detail, '删除失败');
      notify.error(errorMessage);
      throw error;
    }
  };

  useEffect(() => {
    loadFactors();
    loadHistory();
  }, [loadFactors, loadHistory]);

  return {
    factors,
    history,
    loading,
    historyLoading,
    runLoading,
    selectedFactor,
    setSelectedFactor,
    loadFactors,
    loadHistory,
    runFactor,
    deleteFactor,
  };
};
