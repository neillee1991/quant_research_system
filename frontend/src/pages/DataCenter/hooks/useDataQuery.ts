/**
 * 数据查询管理 Hook
 */
import { useState, useCallback } from 'react';
import { message } from 'antd';
import { dataApi } from '../../../api';
import type { TableInfo, DailyData } from '../../../types';

export const useDataQuery = () => {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [sqlQuery, setSqlQuery] = useState<string>('SELECT * FROM sync_daily_basic LIMIT 10');
  const [queryResult, setQueryResult] = useState<Record<string, unknown>[]>([]);
  const [queryColumns, setQueryColumns] = useState<string[]>([]);
  const [queryLoading, setQueryLoading] = useState<boolean>(false);
  const [dailyData, setDailyData] = useState<DailyData[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  const loadTables = useCallback(async () => {
    try {
      const res = await dataApi.listTables();
      setTables(res.data.tables || []);
    } catch (error) {
      console.error('Failed to load tables:', error);
      message.error('加载数据表失败');
    }
  }, []);

  const executeQuery = useCallback(async (sql?: string) => {
    const queryToExecute = sql || sqlQuery;
    if (!queryToExecute.trim()) {
      message.warning('请输入 SQL 查询语句');
      return;
    }

    setQueryLoading(true);
    try {
      const res = await dataApi.executeQuery(queryToExecute);
      setQueryResult(res.data.data || []);
      setQueryColumns(res.data.columns || []);
      message.success(`查询返回 ${res.data.count} 行数据`);
    } catch (error: any) {
      message.error(error.response?.data?.detail || '查询失败');
      throw error;
    } finally {
      setQueryLoading(false);
    }
  }, [sqlQuery]);

  const truncateTable = useCallback(async (tableName: string) => {
    try {
      await dataApi.truncateTable(tableName);
      message.success(`表 ${tableName} 已清空`);
      await loadTables();
    } catch (error: any) {
      message.error(error.response?.data?.detail || '清空表失败');
      throw error;
    }
  }, [loadTables]);

  const loadDaily = useCallback(async (code: string) => {
    setLoading(true);
    try {
      const r = await dataApi.getDaily(code, '20230101', undefined, 500);
      setDailyData(r.data.data || []);
    } catch (error) {
      console.error('Failed to load daily data:', error);
      message.error('加载日线数据失败');
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    tables,
    sqlQuery,
    queryResult,
    queryColumns,
    queryLoading,
    dailyData,
    loading,
    setSqlQuery,
    loadTables,
    executeQuery,
    truncateTable,
    loadDaily,
  };
};
