/**
 * Version Control Demo Component
 *
 * This component demonstrates all version control features with mock data.
 * Use this as a reference for integration into actual pages.
 */

import React, { useState } from 'react';
import {
  Card,
  Button,
  Table,
  Space,
  Toast,
  Descriptions,
  Tabs,
  TabPane,
} from '@douyinfe/semi-ui';
import { IconHistory, IconPlay } from '@douyinfe/semi-icons';
import { VersionHistory } from './index';
import { VersionBadge } from './VersionBadge';
import { VersionSelector } from './VersionSelector';
import type { VersionRecord, TaskWithVersion } from './types';

const VersionControlDemo: React.FC = () => {
  const [historyVisible, setHistoryVisible] = useState(false);
  const [selectedTask, setSelectedTask] = useState<TaskWithVersion | null>(null);
  const [selectedVersion, setSelectedVersion] = useState(3);

  // Mock task data with version info
  const mockTasks: TaskWithVersion[] = [
    {
      task_id: 'daily_basic',
      version: 3,
      changed_by: 'admin',
      change_reason: '调整同步频率为每日',
      updated_at: '2026-03-07 10:30:00',
      config: { enabled: true, schedule: 'daily' },
    },
    {
      task_id: 'stock_daily',
      version: 2,
      changed_by: 'user1',
      change_reason: '修改数据源配置',
      updated_at: '2026-03-05 14:20:00',
      config: { enabled: true, schedule: 'weekly' },
    },
    {
      task_id: 'adj_factor',
      version: 1,
      changed_by: 'admin',
      change_reason: '初始创建',
      updated_at: '2026-03-01 09:00:00',
      config: { enabled: false, schedule: 'manual' },
    },
  ];

  // Mock version history
  const mockVersions: VersionRecord[] = [
    {
      version: 3,
      changed_by: 'admin',
      change_reason: '调整同步频率为每日',
      created_at: '2026-03-07 10:30:00',
    },
    {
      version: 2,
      changed_by: 'user1',
      change_reason: '修改数据源配置',
      created_at: '2026-03-05 14:20:00',
    },
    {
      version: 1,
      changed_by: 'admin',
      change_reason: '初始创建',
      created_at: '2026-03-01 09:00:00',
    },
  ];

  const handleOpenHistory = (task: TaskWithVersion) => {
    setSelectedTask(task);
    setHistoryVisible(true);
  };

  const handleVersionChange = (version: number) => {
    setSelectedVersion(version);
    Toast.success(`已切换到版本 ${version}`);
  };

  const handleRollback = (version: number) => {
    Toast.success(`任务 ${selectedTask?.task_id} 已回滚到版本 ${version}`);
    setHistoryVisible(false);
  };

  const columns = [
    {
      title: '任务ID',
      dataIndex: 'task_id',
      width: 150,
    },
    {
      title: '版本',
      dataIndex: 'version',
      width: 120,
      render: (version: number, record: TaskWithVersion) => (
        <VersionBadge
          version={version}
          changedBy={record.changed_by}
          changeReason={record.change_reason}
          createdAt={record.updated_at}
          size="small"
          onClick={() => handleOpenHistory(record)}
        />
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
      width: 200,
    },
    {
      title: '更新时间',
      dataIndex: 'updated_at',
      width: 180,
    },
    {
      title: '操作',
      width: 150,
      render: (_: any, record: TaskWithVersion) => (
        <Space>
          <Button
            icon={<IconHistory />}
            size="small"
            onClick={() => handleOpenHistory(record)}
          >
            版本历史
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Tabs type="line">
        <TabPane tab="示例1: 任务列表" itemKey="1">
          <Card
            title="数据同步任务列表"
            headerExtraContent={
              <Button type="primary" icon={<IconPlay />}>
                新建任务
              </Button>
            }
          >
            <Table
              columns={columns}
              dataSource={mockTasks}
              pagination={false}
              rowKey="task_id"
            />
          </Card>
        </TabPane>

        <TabPane tab="示例2: 任务详情" itemKey="2">
          <Card title="任务详情 - daily_basic">
            <Descriptions
              data={[
                { key: '任务ID', value: 'daily_basic' },
                { key: '任务类型', value: '数据同步' },
                { key: '数据源', value: 'Tushare' },
                {
                  key: '当前版本',
                  value: (
                    <VersionBadge
                      version={3}
                      changedBy="admin"
                      changeReason="调整同步频率为每日"
                      createdAt="2026-03-07 10:30:00"
                      showIcon
                      onClick={() =>
                        handleOpenHistory({
                          task_id: 'daily_basic',
                          version: 3,
                          changed_by: 'admin',
                          change_reason: '调整同步频率为每日',
                          updated_at: '2026-03-07 10:30:00',
                        })
                      }
                    />
                  ),
                },
                { key: '状态', value: '已启用' },
                { key: '同步频率', value: '每日' },
              ]}
            />

            <div style={{ marginTop: 24 }}>
              <Button
                icon={<IconHistory />}
                onClick={() =>
                  handleOpenHistory({
                    task_id: 'daily_basic',
                    version: 3,
                    changed_by: 'admin',
                    change_reason: '调整同步频率为每日',
                    updated_at: '2026-03-07 10:30:00',
                  })
                }
              >
                查看完整版本历史
              </Button>
            </div>
          </Card>
        </TabPane>

        <TabPane tab="示例3: 版本选择器" itemKey="3">
          <Card
            title="任务配置编辑"
            headerExtraContent={
              <VersionSelector
                currentVersion={selectedVersion}
                versions={mockVersions}
                onChange={handleVersionChange}
              />
            }
          >
            <div style={{ padding: 16 }}>
              <p>当前正在编辑版本: v{selectedVersion}</p>
              <p style={{ color: 'var(--semi-color-text-2)', marginTop: 8 }}>
                使用版本选择器可以查看和编辑不同版本的配置。
              </p>
              <div
                style={{
                  marginTop: 16,
                  padding: 16,
                  background: 'var(--semi-color-fill-0)',
                  borderRadius: 4,
                }}
              >
                <pre>{JSON.stringify({ version: selectedVersion, config: '...' }, null, 2)}</pre>
              </div>
            </div>
          </Card>
        </TabPane>

        <TabPane tab="示例4: 组件说明" itemKey="4">
          <Card title="版本控制组件使用说明">
            <div style={{ lineHeight: 1.8 }}>
              <h3>组件列表</h3>
              <ul>
                <li>
                  <strong>VersionHistory</strong>: 完整的版本历史侧边栏，支持查看、对比、回滚
                </li>
                <li>
                  <strong>VersionBadge</strong>: 紧凑的版本徽章，显示版本号和元数据
                </li>
                <li>
                  <strong>VersionSelector</strong>: 版本下拉选择器，用于切换版本
                </li>
              </ul>

              <h3 style={{ marginTop: 24 }}>集成步骤</h3>
              <ol>
                <li>导入所需组件</li>
                <li>添加版本相关的状态管理</li>
                <li>在表格或详情页中使用VersionBadge显示版本信息</li>
                <li>添加VersionHistory组件用于查看完整历史</li>
                <li>在编辑页面使用VersionSelector切换版本</li>
              </ol>

              <h3 style={{ marginTop: 24 }}>API要求</h3>
              <p>
                前端组件已完成，需要后端实现以下API端点：
              </p>
              <ul>
                <li>GET /api/v1/&#123;taskType&#125;/tasks/&#123;taskId&#125;/versions - 获取版本历史</li>
                <li>GET /api/v1/&#123;taskType&#125;/tasks/&#123;taskId&#125;/versions/&#123;version&#125; - 获取特定版本</li>
                <li>POST /api/v1/&#123;taskType&#125;/tasks/&#123;taskId&#125;/versions/&#123;version&#125;/rollback - 回滚</li>
                <li>GET /api/v1/&#123;taskType&#125;/tasks/&#123;taskId&#125;/versions/diff - 对比版本</li>
              </ul>

              <h3 style={{ marginTop: 24 }}>更多信息</h3>
              <p>
                详细的使用文档和集成示例请参考：
                <code style={{ margin: '0 8px' }}>
                  src/components/VersionHistory/README.md
                </code>
              </p>
            </div>
          </Card>
        </TabPane>
      </Tabs>

      <VersionHistory
        visible={historyVisible}
        onClose={() => setHistoryVisible(false)}
        taskId={selectedTask?.task_id || ''}
        taskType="sync"
        currentVersion={selectedTask?.version || 1}
        onRollback={handleRollback}
      />
    </div>
  );
};

export default VersionControlDemo;
