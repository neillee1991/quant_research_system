/**
 * 因子分析 Hook
 */

import { useState, useEffect, useRef } from 'react';
import { message } from 'antd';
import dayjs from 'dayjs';
import { productionApi } from '../../../api';

const defaultEndDate = dayjs().format('YYYYMMDD');
const defaultStartDate = dayjs().subtract(1, 'year').format('YYYYMMDD');

export const useFactorAnalysis = () => {
  const [factors, setFactors] = useState<any[]>([]);
  const [indexPools, setIndexPools] = useState<any[]>([]);
  const [selectedFactor, setSelectedFactor] = useState<string>('');
  const [periods, setPeriods] = useState<number[]>([5, 10, 20]);
  const [quantiles, setQuantiles] = useState(5);
  const [startDate, setStartDate] = useState<string>(defaultStartDate);
  const [endDate, setEndDate] = useState<string>(defaultEndDate);
  const [indexPool, setIndexPool] = useState<string>('');
  const [groupbyField, setGroupbyField] = useState<string>('');
  const [nextDayEntry, setNextDayEntry] = useState(true);
  const [entryPrice, setEntryPrice] = useState<string>('open');
  const [neutralize, setNeutralize] = useState(true);
  const [neutralizeControls, setNeutralizeControls] = useState<string[]>(['market', 'industry', 'size']);
  const [industryLevel, setIndustryLevel] = useState<'industry_l1' | 'industry_l2'>('industry_l1');
  const [winsorize, setWinsorize] = useState(false);
  const [winsorizeLower, setWinsorizeLower] = useState(0.01);
  const [winsorizeUpper, setWinsorizeUpper] = useState(0.99);
  const [resolvedConfig, setResolvedConfig] = useState<Record<string, { source_label: string }>>({});
  const [analysisResult, setAnalysisResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<'idle' | 'pending' | 'running' | 'completed' | 'failed'>('idle');
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    productionApi.listFactors()
      .then(res => setFactors(res.data?.data || []))
      .catch(error => { console.error('Failed to load factors:', error); });
    productionApi.listIndexPools()
      .then(res => setIndexPools(res.data?.data || []))
      .catch(error => { console.error('Failed to load index pools:', error); });
    productionApi.getResolvedDataConfig()
      .then(res => setResolvedConfig(res.data?.data || {}))
      .catch(error => { console.error('Failed to load resolved config:', error); });
  }, []);

  const stopPolling = () => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  };

  const startPolling = (id: string) => {
    stopPolling();
    pollingRef.current = setInterval(async () => {
      try {
        const res = await productionApi.getAnalysisTaskStatus(id);
        const status = res.data?.data?.task_status;
        setTaskStatus(status);
        if (status === 'completed') {
          stopPolling();
          setRunLoading(false);
          const resultRes = await productionApi.getLatestAlphalensAnalysis(selectedFactor);
          setAnalysisResult(resultRes.data?.data);
          message.success('分析完成');
        } else if (status === 'failed') {
          stopPolling();
          setRunLoading(false);
          const error = res.data?.data?.error_message || '分析失败';
          message.error(error);
        }
      } catch (e) {
        stopPolling();
        setRunLoading(false);
      }
    }, 2000);
  };

  useEffect(() => () => stopPolling(), []);

  const runAnalysis = async () => {
    if (!selectedFactor) {
      message.warning('请选择因子');
      return;
    }
    setRunLoading(true);
    setTaskStatus('pending');
    try {
      const res = await productionApi.runAlphalensAnalysis({
        factor_id: selectedFactor,
        start_date: startDate || undefined as any,
        end_date: endDate || undefined as any,
        periods,
        quantiles,
        index_pool: indexPool || undefined,
        groupby_field: groupbyField || undefined,
        next_day_entry: nextDayEntry,
        entry_price: entryPrice,
        neutralize,
        neutralize_controls: neutralize ? neutralizeControls : undefined,
        industry_level: neutralize && neutralizeControls.includes('industry') ? industryLevel : undefined,
        winsorize,
        winsorize_lower: winsorize ? winsorizeLower : undefined,
        winsorize_upper: winsorize ? winsorizeUpper : undefined,
      });
      const id = res.data?.data?.task_id;
      setTaskId(id);
      startPolling(id);
    } catch (error: any) {
      setRunLoading(false);
      setTaskStatus('failed');
      message.error(error.response?.data?.detail || '提交失败');
    }
  };

  const loadAnalysis = async (factorId: string) => {
    setLoading(true);
    try {
      const res = await productionApi.getLatestAlphalensAnalysis(factorId);
      setAnalysisResult(res.data?.data);
    } catch (error) {
      console.error('Failed to load analysis:', error);
      setAnalysisResult(null);
    } finally {
      setLoading(false);
    }
  };

  const loadAnalysisById = async (factorId: string, analysisId: string) => {
    setLoading(true);
    try {
      const res = await productionApi.getAlphalensAnalysisById(factorId, analysisId);
      setAnalysisResult(res.data?.data);
    } catch (error) {
      console.error('Failed to load analysis by id:', error);
      setAnalysisResult(null);
    } finally {
      setLoading(false);
    }
  };

  const deleteAnalysis = async (analysisId: string) => {
    if (!selectedFactor) return false;
    try {
      await productionApi.deleteAlphalensAnalysisById(selectedFactor, analysisId);
      return true;
    } catch (error) {
      console.error('Failed to delete analysis:', error);
      return false;
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
    nextDayEntry,
    setNextDayEntry,
    entryPrice,
    setEntryPrice,
    neutralize,
    setNeutralize,
    neutralizeControls,
    setNeutralizeControls,
    industryLevel,
    setIndustryLevel,
    winsorize,
    setWinsorize,
    winsorizeLower,
    setWinsorizeLower,
    winsorizeUpper,
    setWinsorizeUpper,
    resolvedConfig,
    taskId,
    taskStatus,
    analysisResult,
    setAnalysisResult,
    loading,
    runLoading,
    runAnalysis,
    loadAnalysis,
    loadAnalysisById,
    deleteAnalysis,
  };
};
