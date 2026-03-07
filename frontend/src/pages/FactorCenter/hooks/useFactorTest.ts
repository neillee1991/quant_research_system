/**
 * 因子测试 Hook
 */

import { useState } from 'react';
import { Toast } from '@douyinfe/semi-ui';
import { productionApi } from '../../../api';
import type { PreprocessOptions } from '../../../types';
import type { TestResult, TestLog } from '../types';

export const useFactorTest = () => {
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [testLogs, setTestLogs] = useState<TestLog[]>([]);
  const [testLoading, setTestLoading] = useState<boolean>(false);
  const [testError, setTestError] = useState<string>('');

  const runTest = async (
    code: string,
    dependsOn?: string[],
    preprocess?: PreprocessOptions,
    startDate?: string,
    endDate?: string
  ): Promise<void> => {
    setTestLoading(true);
    setTestError('');
    setTestLogs([]);
    setTestResult(null);

    try {
      const res = await productionApi.testFactorCode({
        code,
        start_date: startDate || '20240101',
        end_date: endDate || '20241231',
        depends_on: dependsOn || [],
        preprocess: preprocess || undefined,
      });

      const data = res.data?.data;
      if (!data) {
        throw new Error('测试返回数据为空');
      }

      setTestResult(data);
      setTestLogs(data.logs || []);

      if (data.error) {
        setTestError(data.error);
        Toast.error('测试失败');
      } else {
        Toast.success('测试成功');
      }
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || error.message || '测试失败';
      setTestError(errorMessage);
      Toast.error(errorMessage);
      throw error;
    } finally {
      setTestLoading(false);
    }
  };

  const clearTest = () => {
    setTestResult(null);
    setTestLogs([]);
    setTestError('');
  };

  return {
    testResult,
    testLogs,
    testLoading,
    testError,
    runTest,
    clearTest,
  };
};
