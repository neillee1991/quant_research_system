/**
 * 数据配置管理 Hook
 */

import { useState, useCallback, useEffect } from 'react';
import { notify } from '../../../utils/notify';
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
    if (!tableName) return;

    // 检查缓存，但如果是空数组则重新加载
    const cached = tableColumns[tableName];
    if (cached && cached.length > 0) return;

    try {
      const res = await dataApi.getTableInfo(tableName);
      // 后端返回的 columns 已经是字符串数组，不需要 map
      const cols: string[] = res.data?.columns || [];
      console.log(`Loaded columns for ${tableName}:`, cols);
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
      notify.success('配置已保存');
      setChanged(false);
    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || '保存失败';
      notify.error(errorMessage);
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
