/**
 * 数据配置管理 Hook
 */

import { useState, useCallback, useEffect } from 'react';
import { Toast } from '@douyinfe/semi-ui';
import { productionApi, dataApi } from '../../../api';
import type { DataFieldMapping } from '../../../types';

export const useDataConfig = () => {
  const [mappings, setMappings] = useState<DataFieldMapping[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<Record<string, string[]>>({});
  const [changed, setChanged] = useState(false);

  const loadConfig = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.getDataConfig();
      setMappings(res.data?.data || []);
      setChanged(false);
    } catch (error) {
      console.error('Failed to load data config:', error);
      Toast.error('加载数据配置失败');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadTables = useCallback(async () => {
    try {
      const res = await dataApi.listTables();
      const list: string[] = (res.data?.tables || []).map((t: any) => t.table_name || t.name || t);
      setTables(list);
    } catch (error) {
      console.error('Failed to load tables:', error);
    }
  }, []);

  const loadColumnsForTable = async (tableName: string) => {
    if (!tableName || tableColumns[tableName]) return;
    try {
      const res = await dataApi.getTableInfo(tableName);
      const cols: string[] = (res.data?.columns || []).map((c: any) => c.name || c);
      setTableColumns(prev => ({ ...prev, [tableName]: cols }));
    } catch (error) {
      console.error(`Failed to load columns for table ${tableName}:`, error);
    }
  };

  const updateMapping = (idx: number, field: Partial<DataFieldMapping>) => {
    setMappings(prev => prev.map((m, i) => i === idx ? { ...m, ...field } as DataFieldMapping : m));
    setChanged(true);
  };

  const saveConfig = async () => {
    setSaving(true);
    try {
      await productionApi.updateDataConfig(mappings as any);
      Toast.success('配置已保存');
      setChanged(false);
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || '保存失败';
      Toast.error(errorMessage);
      throw error;
    } finally {
      setSaving(false);
    }
  };

  useEffect(() => {
    loadConfig();
    loadTables();
  }, [loadConfig, loadTables]);

  return {
    mappings,
    loading,
    saving,
    tables,
    tableColumns,
    changed,
    loadConfig,
    loadColumnsForTable,
    updateMapping,
    saveConfig,
  };
};
