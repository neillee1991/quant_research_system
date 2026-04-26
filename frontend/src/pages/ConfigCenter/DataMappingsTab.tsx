/**
 * 统一数据配置 Tab
 * 同时管理因子分析和回测配置
 */
import React from 'react';
import { useDataConfig } from './hooks/useDataConfig';
import { FieldMappingTable } from './components/FieldMappingTable';

const DataMappingsTab: React.FC = () => {
  const {
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
  } = useDataConfig();

  return (
    <FieldMappingTable
      title="数据配置"
      description="统一管理因子分析和回测引擎所需的数据字段映射，配置时请注意字段被哪个模块使用。"
      mappings={mappings}
      loading={loading}
      saving={saving}
      tables={tables}
      tableColumns={tableColumns}
      changed={changed}
      onLoadConfig={loadConfig}
      onLoadColumnsForTable={loadColumnsForTable}
      onUpdateMapping={updateMapping}
      onSaveConfig={saveConfig}
    />
  );
};

export default DataMappingsTab;
