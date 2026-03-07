/**
 * Version Control Integration Example
 *
 * This file demonstrates how to integrate version control components
 * into DataCenter and FactorCenter pages.
 */

import React, { useState, useEffect } from 'react';
import { Button, Card, Descriptions, Toast } from '@douyinfe/semi-ui';
import { IconHistory } from '@douyinfe/semi-icons';
import { VersionHistory, VersionRecord } from './index';
import { VersionBadge } from './VersionBadge';
import { VersionSelector } from './VersionSelector';
import { versionApi } from '../../api';

/**
 * Example 1: Add version badge to task detail view
 *
 * Usage in DataCenter.tsx or FactorCenter.tsx:
 * - Show current version in task configuration drawer
 * - Click badge to open version history
 */
export const TaskDetailWithVersion: React.FC<{
  taskId: string;
  taskType: 'sync' | 'factor' | 'etl';
  currentVersion: number;
  changedBy?: string;
  changeReason?: string;
  createdAt?: string;
}> = ({ taskId, taskType, currentVersion, changedBy, changeReason, createdAt }) => {
  const [historyVisible, setHistoryVisible] = useState(false);

  return (
    <div>
      <Descriptions
        data={[
          { key: '任务ID', value: taskId },
          { key: '任务类型', value: taskType },
          {
            key: '当前版本',
            value: (
              <VersionBadge
                version={currentVersion}
                changedBy={changedBy}
                changeReason={changeReason}
                createdAt={createdAt}
                showIcon
                onClick={() => setHistoryVisible(true)}
              />
            ),
          },
        ]}
      />

      <Button
        icon={<IconHistory />}
        onClick={() => setHistoryVisible(true)}
        style={{ marginTop: 16 }}
      >
        查看版本历史
      </Button>

      <VersionHistory
        visible={historyVisible}
        onClose={() => setHistoryVisible(false)}
        taskId={taskId}
        taskType={taskType}
        currentVersion={currentVersion}
        onRollback={(version) => {
          Toast.success(`已回滚到版本 ${version}`);
          // Reload task configuration
        }}
      />
    </div>
  );
};

/**
 * Example 2: Add version selector to task configuration page
 *
 * Usage in task edit drawer:
 * - Allow users to select and view different versions
 * - Load configuration for selected version
 */
export const TaskConfigWithVersionSelector: React.FC<{
  taskId: string;
  taskType: 'sync' | 'factor' | 'etl';
}> = ({ taskId, taskType }) => {
  const [currentVersion, setCurrentVersion] = useState(1);
  const [versions, setVersions] = useState<VersionRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [config, setConfig] = useState<any>(null);

  useEffect(() => {
    loadVersions();
  }, [taskId]);

  const loadVersions = async () => {
    setLoading(true);
    try {
      // TODO: Replace with actual API call when backend is ready
      // const response = await versionApi.getHistory(taskType, taskId);
      // setVersions(response.data.versions);
      // setCurrentVersion(response.data.current_version);

      // Mock data
      const mockVersions: VersionRecord[] = [
        {
          version: 3,
          changed_by: 'admin',
          change_reason: '调整同步频率',
          created_at: '2026-03-07 10:30:00',
        },
        {
          version: 2,
          changed_by: 'user1',
          change_reason: '修改数据源',
          created_at: '2026-03-05 14:20:00',
        },
        {
          version: 1,
          changed_by: 'admin',
          change_reason: '初始创建',
          created_at: '2026-03-01 09:00:00',
        },
      ];
      setVersions(mockVersions);
      setCurrentVersion(3);
    } catch (error: any) {
      Toast.error(`加载版本失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleVersionChange = async (version: number) => {
    setLoading(true);
    try {
      // TODO: Replace with actual API call
      // const response = await versionApi.getVersion(taskType, taskId, version);
      // setConfig(response.data.config);
      setCurrentVersion(version);
      Toast.success(`已切换到版本 ${version}`);
    } catch (error: any) {
      Toast.error(`加载版本配置失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card
      title="任务配置"
      headerExtraContent={
        <VersionSelector
          currentVersion={currentVersion}
          versions={versions}
          onChange={handleVersionChange}
          loading={loading}
        />
      }
    >
      {/* Task configuration form here */}
      <div>配置内容: {JSON.stringify(config)}</div>
    </Card>
  );
};

/**
 * Example 3: Add version column to task list table
 *
 * Usage in DataCenter.tsx task table:
 * - Show version badge in table column
 * - Click to view version history
 */
export const getVersionColumn = () => ({
  title: '版本',
  dataIndex: 'version',
  width: 100,
  render: (version: number, record: any) => (
    <VersionBadge
      version={version}
      changedBy={record.changed_by}
      changeReason={record.change_reason}
      createdAt={record.updated_at}
      size="small"
    />
  ),
});

/**
 * Example 4: Integration steps for DataCenter.tsx
 *
 * Step 1: Import components
 * ```typescript
 * import { VersionHistory } from '../components/VersionHistory';
 * import { VersionBadge } from '../components/VersionHistory/VersionBadge';
 * import { VersionSelector } from '../components/VersionHistory/VersionSelector';
 * ```
 *
 * Step 2: Add state for version history
 * ```typescript
 * const [versionHistoryVisible, setVersionHistoryVisible] = useState(false);
 * const [selectedTaskForVersion, setSelectedTaskForVersion] = useState<string | null>(null);
 * const [selectedTaskType, setSelectedTaskType] = useState<'sync' | 'etl'>('sync');
 * ```
 *
 * Step 3: Add version column to task table
 * ```typescript
 * const columns = [
 *   // ... existing columns
 *   {
 *     title: '版本',
 *     dataIndex: 'version',
 *     width: 100,
 *     render: (version: number, record: any) => (
 *       <VersionBadge
 *         version={version || 1}
 *         changedBy={record.changed_by}
 *         changeReason={record.change_reason}
 *         createdAt={record.updated_at}
 *         size="small"
 *         onClick={() => {
 *           setSelectedTaskForVersion(record.task_id);
 *           setSelectedTaskType('sync');
 *           setVersionHistoryVisible(true);
 *         }}
 *       />
 *     ),
 *   },
 *   // ... other columns
 * ];
 * ```
 *
 * Step 4: Add version history button to task drawer
 * ```typescript
 * <SideSheet
 *   title={
 *     <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
 *       <span>任务配置 - {taskDrawerTask?.task_id}</span>
 *       <VersionBadge
 *         version={taskDrawerConfig?.version || 1}
 *         changedBy={taskDrawerConfig?.changed_by}
 *         changeReason={taskDrawerConfig?.change_reason}
 *         createdAt={taskDrawerConfig?.updated_at}
 *         showIcon
 *         onClick={() => {
 *           setSelectedTaskForVersion(taskDrawerTask?.task_id || '');
 *           setSelectedTaskType('sync');
 *           setVersionHistoryVisible(true);
 *         }}
 *       />
 *     </div>
 *   }
 *   // ... other props
 * >
 *   {/* Task configuration content *\/}
 * </SideSheet>
 * ```
 *
 * Step 5: Add VersionHistory component at the end of component
 * ```typescript
 * <VersionHistory
 *   visible={versionHistoryVisible}
 *   onClose={() => setVersionHistoryVisible(false)}
 *   taskId={selectedTaskForVersion || ''}
 *   taskType={selectedTaskType}
 *   currentVersion={taskDrawerConfig?.version || 1}
 *   onRollback={(version) => {
 *     Toast.success(`已回滚到版本 ${version}`);
 *     loadInitialData();
 *     setVersionHistoryVisible(false);
 *   }}
 * />
 * ```
 */

/**
 * Example 5: Integration steps for FactorCenter.tsx
 *
 * Similar to DataCenter, but with taskType='factor'
 *
 * Step 1: Add version info to factor metadata display
 * ```typescript
 * <Descriptions
 *   data={[
 *     { key: '因子ID', value: factorMetadata.factor_id },
 *     { key: '因子名称', value: factorMetadata.name },
 *     {
 *       key: '版本',
 *       value: (
 *         <VersionBadge
 *           version={factorMetadata.version || 1}
 *           changedBy={factorMetadata.changed_by}
 *           changeReason={factorMetadata.change_reason}
 *           createdAt={factorMetadata.updated_at}
 *           showIcon
 *           onClick={() => openVersionHistory(factorMetadata.factor_id)}
 *         />
 *       ),
 *     },
 *   ]}
 * />
 * ```
 *
 * Step 2: Add version selector to factor query interface
 * ```typescript
 * <Card title="因子查询">
 *   <Form>
 *     <Form.Select field="factor_id" label="因子" />
 *     <Form.DatePicker field="start_date" label="开始日期" />
 *     <Form.DatePicker field="end_date" label="结束日期" />
 *     <Form.Item label="版本">
 *       <VersionSelector
 *         currentVersion={selectedVersion}
 *         versions={factorVersions}
 *         onChange={setSelectedVersion}
 *       />
 *     </Form.Item>
 *   </Form>
 * </Card>
 * ```
 */

export default {
  TaskDetailWithVersion,
  TaskConfigWithVersionSelector,
  getVersionColumn,
};
