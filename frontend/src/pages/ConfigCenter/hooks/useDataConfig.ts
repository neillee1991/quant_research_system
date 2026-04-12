/**
 * 数据配置管理 Hook
 */

import { useState, useCallback, useEffect } from 'react';
import { notify } from '../../../utils/notify';
import { productionApi } from '../../../api';
import type { DataFieldMapping } from '../../../types';

// 预定义字段（与后端引擎支持的字段保持一致）
const PRESET_FIELDS: DataFieldMapping[] = [
  { field_key: 'adj_factor',  description: '复权因子',   table_name: '', column_name: '', extra_config: '{}' },
  { field_key: 'industry_l1', description: '一级行业',   table_name: '', column_name: '', extra_config: '{}' },
  { field_key: 'industry_l2', description: '二级行业',   table_name: '', column_name: '', extra_config: '{}' },
  { field_key: 'is_limit',    description: '涨跌停标记', table_name: '', column_name: '', extra_config: '{}' },
  { field_key: 'is_st',       description: 'ST标记',     table_name: '', column_name: '', extra_config: '{}' },
  { field_key: 'list_date',   description: '上市日期',   table_name: '', column_name: '', extra_config: '{}' },
  { field_key: 'market_cap',  description: '市值',       table_name: '', column_name: '', extra_config: '{}' },
];

export const useDataConfig = () => {
  const [mappings, setMappings] = useState<DataFieldMapping[]>(PRESET_FIELDS);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<Record<string, string[]>>({});
  const [changed, setChanged] = useState(false);

  const loadConfig = useCallback(async () => {
    setLoading(true);
    try {
      const res = await productionApi.getDataConfig();
      const dbRows: DataFieldMapping[] = res.data?.data || [];
      // 以预设字段为基础，用数据库中已保存的值覆盖
      const dbMap = Object.fromEntries(dbRows.map(r => [r.field_key, r]));
      setMappings(PRESET_FIELDS.map(f => dbMap[f.field_key] ? { ...f, ...dbMap[f.field_key] } : f));
      setChanged(false);
    } catch (error) {
      console.error('Failed to load data config:', error);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadTables = useCallback(async () => {
    try {
      const res = await productionApi.getAvailableTables();
      const list: string[] = (res.data?.data || []).map((t: any) => t.value || t);
      setTables(list);
    } catch (error) {
      console.error('Failed to load tables:', error);
    }
  }, []);

  const loadColumnsForTable = async (tableName: string) => {
    if (!tableName) return;
    const cached = tableColumns[tableName];
    if (cached && cached.length > 0) return;
    try {
      const res = await productionApi.getTableColumns(tableName);
      const cols: string[] = res.data?.columns || [];
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
