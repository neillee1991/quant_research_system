/**
 * 因子列表管理 Hook
 */

import { useState, useCallback, useEffect } from 'react';
import { message } from 'antd';
import { productionApi } from '../../../api';
import type { FactorDefinition, FactorRunRecord } from '../../../types';

export const useFactorList = () => {
  const [factors, setFactors] = useState<FactorDefinition[]>([]);
  const [history, setHistory] = useState<FactorRunRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [runLoading, setRunLoading] = useState<string | null>(null);
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);

  const loadFactors = useCallback(async (): Promise<void> => {
    setLoading(true);
    try {
      const res = await productionApi.listFactors();
      setFactors(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load factors:', error);
      message.error('加载因子列表失败');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadHistory = useCallback(async (factorId?: string): Promise<void> => {
    try {
      const res = await productionApi.getProductionHistory(factorId, 30);
      setHistory(res.data?.data || []);
    } catch (error) {
      console.error('Failed to load history:', error);
      message.error('加载历史记录失败');
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
      message.success(`因子 ${factorId} ${runMode === 'incremental' ? '增量' : '全量'}计算完成`);
      await loadFactors();
      await loadHistory(selectedFactor || undefined);
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || '执行失败';
      message.error(errorMessage);
      throw error;
    } finally {
      setRunLoading(null);
    }
  };

  const deleteFactor = async (factorId: string): Promise<void> => {
    try {
      await productionApi.deleteFactor(factorId);
      message.success('因子已删除');
      await loadFactors();
      await loadHistory();
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || '删除失败';
      message.error(errorMessage);
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
    runLoading,
    selectedFactor,
    setSelectedFactor,
    loadFactors,
    loadHistory,
    runFactor,
    deleteFactor,
  };
};
