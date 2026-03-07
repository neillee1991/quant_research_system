import React, { useState, useEffect } from 'react';
import {
  SideSheet,
  Table,
  Button,
  Tag,
  Tooltip,
  Modal,
  Input,
  Toast,
  Empty,
  Spin,
  Popconfirm,
  Descriptions,
  Timeline,
} from '@douyinfe/semi-ui';
import {
  IconHistory,
  IconRefresh,
  IconInfoCircle,
  IconCode,
  IconClose,
} from '@douyinfe/semi-icons';
import dayjs from 'dayjs';
import Editor from '@monaco-editor/react';
import { useThemeStore } from '../../store';
import type { VersionRecord, TaskType } from './types';

export type { VersionRecord, TaskType } from './types';

interface VersionHistoryProps {
  visible: boolean;
  onClose: () => void;
  taskId: string;
  taskType: TaskType;
  currentVersion?: number;
  onRollback?: (version: number) => void;
}

export const VersionHistory: React.FC<VersionHistoryProps> = ({
  visible,
  onClose,
  taskId,
  taskType,
  currentVersion: propCurrentVersion,
  onRollback,
}) => {
  const { mode } = useThemeStore();
  const [versions, setVersions] = useState<VersionRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [diffModalVisible, setDiffModalVisible] = useState(false);
  const [selectedVersions, setSelectedVersions] = useState<[number, number] | null>(null);
  const [currentVersion, setCurrentVersion] = useState<number | undefined>(propCurrentVersion);

  useEffect(() => {
    if (visible) {
      loadVersionHistory();
    }
  }, [visible, taskId]);

  const loadVersionHistory = async () => {
    setLoading(true);
    try {
      const response = await fetch(`/api/v1/tasks/${taskType}/${taskId}/versions`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data = await response.json();

      // 转换后端数据格式为前端格式
      const formattedVersions: VersionRecord[] = data.versions.map((v: any) => ({
        version: v.version_number,
        changed_by: v.changed_by || 'system',
        change_reason: v.change_reason || '无说明',
        created_at: v.created_at ? dayjs(v.created_at).format('YYYY-MM-DD HH:mm:ss') : '',
        config: v, // 保存完整配置用于对比
      }));

      setVersions(formattedVersions);

      // 从 API 响应中提取当前版本号
      const current = data.versions.find((v: any) => v.is_current);
      if (current) {
        setCurrentVersion(current.version_number);
      }
    } catch (error: any) {
      Toast.error(`加载版本历史失败: ${error.message}`);
      setVersions([]);
    } finally {
      setLoading(false);
    }
  };

  const handleRollback = async (version: number) => {
    try {
      const response = await fetch(`/api/v1/tasks/${taskType}/${taskId}/rollback/${version}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ changed_by: 'user', change_reason: '手动回滚' }),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      Toast.success(`已回滚到版本 ${version}`);
      onRollback?.(version);
      loadVersionHistory(); // 重新加载版本历史
      onClose();
    } catch (error: any) {
      Toast.error(`回滚失败: ${error.message}`);
    }
  };

  const handleViewDiff = (v1: number, v2: number) => {
    setSelectedVersions([v1, v2]);
    setDiffModalVisible(true);
  };

  const columns = [
    {
      title: '版本',
      dataIndex: 'version',
      width: 80,
      render: (version: number) => (
        <Tag color={version === currentVersion ? 'blue' : 'grey'} size="large">
          v{version}
          {version === currentVersion && ' (当前)'}
        </Tag>
      ),
    },
    {
      title: '修改人',
      dataIndex: 'changed_by',
      width: 100,
    },
    {
      title: '修改原因',
      dataIndex: 'change_reason',
      render: (text: string) => (
        <Tooltip content={text}>
          <div style={{ maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {text}
          </div>
        </Tooltip>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      width: 180,
      render: (text: string) => dayjs(text).format('YYYY-MM-DD HH:mm:ss'),
    },
    {
      title: '操作',
      width: 200,
      render: (_: any, record: VersionRecord) => (
        <div style={{ display: 'flex', gap: 8 }}>
          <Tooltip content="查看配置">
            <Button
              icon={<IconInfoCircle />}
              size="small"
              onClick={() => {
                Modal.info({
                  title: `版本 ${record.version} 配置详情`,
                  content: (
                    <Editor
                      height="400px"
                      language="json"
                      value={JSON.stringify(record.config, null, 2)}
                      theme={mode === 'dark' ? 'vs-dark' : 'light'}
                      options={{ readOnly: true, minimap: { enabled: false } }}
                    />
                  ),
                  width: 700,
                });
              }}
            />
          </Tooltip>
          {record.version !== currentVersion && (
            <>
              <Tooltip content="对比差异">
                <Button
                  icon={<IconCode />}
                  size="small"
                  onClick={() => handleViewDiff(currentVersion || 1, record.version)}
                />
              </Tooltip>
              <Popconfirm
                title="确认回滚"
                content={`确定要回滚到版本 ${record.version} 吗？`}
                onConfirm={() => handleRollback(record.version)}
              >
                <Button type="danger" size="small">
                  回滚
                </Button>
              </Popconfirm>
            </>
          )}
        </div>
      ),
    },
  ];

  return (
    <>
      <SideSheet
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <IconHistory />
            <span>版本历史 - {taskId}</span>
          </div>
        }
        visible={visible}
        onCancel={onClose}
        width={900}
        headerStyle={{ borderBottom: '1px solid var(--semi-color-border)' }}
      >
        <div style={{ padding: '16px 0' }}>
          <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <Tag color="blue" size="large">
                当前版本: v{currentVersion || 1}
              </Tag>
              <span style={{ marginLeft: 12, color: 'var(--semi-color-text-2)' }}>
                共 {versions.length} 个版本
              </span>
            </div>
            <Button icon={<IconRefresh />} onClick={loadVersionHistory}>
              刷新
            </Button>
          </div>

          <Spin spinning={loading}>
            {versions.length === 0 && !loading ? (
              <Empty description="暂无版本历史" />
            ) : (
              <Table
                columns={columns}
                dataSource={versions}
                pagination={false}
                rowKey="version"
              />
            )}
          </Spin>
        </div>
      </SideSheet>

      <Modal
        title="版本差异对比"
        visible={diffModalVisible}
        onCancel={() => setDiffModalVisible(false)}
        footer={null}
        width={1000}
      >
        {selectedVersions && (
          <div>
            <div style={{ marginBottom: 16, display: 'flex', gap: 16 }}>
              <Tag color="green" size="large">v{selectedVersions[0]} (当前)</Tag>
              <span>vs</span>
              <Tag color="orange" size="large">v{selectedVersions[1]}</Tag>
            </div>
            <Editor
              height="500px"
              language="json"
              value={`// TODO: Implement diff view\n// Version ${selectedVersions[0]} vs Version ${selectedVersions[1]}`}
              theme={mode === 'dark' ? 'vs-dark' : 'light'}
              options={{ readOnly: true, minimap: { enabled: false } }}
            />
          </div>
        )}
      </Modal>
    </>
  );
};

export default VersionHistory;
