/**
 * 因子分析 Hook
 */

import { useState, useEffect } from 'react';
import { Toast } from '@douyinfe/semi-ui';
import { productionApi } from '../../../api';

export const useFactorAnalysis = () => {
  const [factors, setFactors] = useState<any[]>([]);
  const [indexPools, setIndexPools] = useState<any[]>([]);
  const [selectedFactor, setSelectedFactor] = useState<string>('');
  const [periods, setPeriods] = useState<number[]>([1, 5, 10]);
  const [quantiles, setQuantiles] = useState(5);
  const [startDate, setStartDate] = useState<string>('');
  const [endDate, setEndDate] = useState<string>('');
  const [indexPool, setIndexPool] = useState<string>('');
  const [groupbyField, setGroupbyField] = useState<string>('');
  const [useAlphalens, setUseAlphalens] = useState(true);
  const [analysisResult, setAnalysisResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);
  const [analysisHistory, setAnalysisHistory] = useState<any[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  useEffect(() => {
    productionApi.listFactors()
      .then(res => setFactors(res.data?.data || []))
      .catch(error => {
        console.error('Failed to load factors:', error);
      });
    productionApi.listIndexPools()
      .then(res => setIndexPools(res.data?.data || []))
      .catch(error => {
        console.error('Failed to load index pools:', error);
      });
  }, []);

  const runAnalysis = async () => {
    if (!selectedFactor) {
      Toast.warning('请选择因子');
      return;
    }

    setRunLoading(true);
    try {
      let res;
      if (useAlphalens) {
        res = await productionApi.runAlphalensAnalysis({
          factor_id: selectedFactor,
          start_date: startDate || undefined as any,
          end_date: endDate || undefined as any,
          periods,
          quantiles,
          index_pool: indexPool || undefined,
          groupby_field: groupbyField || undefined,
        });
        Toast.success('Alphalens 分析完成');
        setAnalysisResult(res.data?.data);
      } else {
        res = await productionApi.runAnalysis(
          selectedFactor,
          startDate || undefined,
          endDate || undefined,
          periods,
          quantiles
        );
        Toast.success('分析完成');
        setAnalysisResult(res.data?.data);
      }

      if (selectedFactor) {
        await loadHistory(selectedFactor);
      }
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || '分析失败';
      Toast.error(errorMessage);
      throw error;
    } finally {
      setRunLoading(false);
    }
  };

  const loadAnalysis = async (factorId: string) => {
    setLoading(true);
    try {
      if (useAlphalens) {
        const res = await productionApi.getLatestAlphalensAnalysis(factorId);
        setAnalysisResult(res.data?.data);
      } else {
        const res = await productionApi.getAnalysis(factorId);
        setAnalysisResult(res.data?.data);
      }
    } catch (error) {
      console.error('Failed to load analysis:', error);
      setAnalysisResult(null);
    } finally {
      setLoading(false);
    }
  };

  const loadHistory = async (factorId: string) => {
    setHistoryLoading(true);
    try {
      const res = await productionApi.getAlphalensAnalysisHistory(factorId, 10);
      setAnalysisHistory(res.data?.data?.records || []);
    } catch (error) {
      console.error('Failed to load analysis history:', error);
      setAnalysisHistory([]);
    } finally {
      setHistoryLoading(false);
    }
  };

  return {
    factors,
    indexPools,
    selectedFactor,
    setSelectedFactor,
    periods,
    setPeriods,
    quantiles,
    setQuantiles,
    startDate,
    setStartDate,
    endDate,
    setEndDate,
    indexPool,
    setIndexPool,
    groupbyField,
    setGroupbyField,
    useAlphalens,
    setUseAlphalens,
    analysisResult,
    loading,
    runLoading,
    analysisHistory,
    historyLoading,
    runAnalysis,
    loadAnalysis,
    loadHistory,
  };
};
