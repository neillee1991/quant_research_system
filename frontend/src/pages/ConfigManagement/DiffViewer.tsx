import React from 'react';
import {
  Collapse,
  Checkbox,
  Tag,
  Table,
  Space,
  Typography,
  Row,
  Col,
} from 'antd';
import {
  ConfigType,
  ConfigTypeDiff,
  ConfigItemDiff,
} from './types';

const { Text } = Typography;
const { Panel } = Collapse;

interface DiffViewerProps {
  diffs: ConfigTypeDiff[];
  selectedItems: Record<ConfigType, string[]>;
  onToggleItem: (configType: ConfigType, itemId: string, checked: boolean) => void;
  onToggleSelectAll: (configType: ConfigType, items: ConfigItemDiff[], checked: boolean) => void;
  getConfigTypeLabel: (value: ConfigType) => string;
}

const statusColors: Record<string, string> = {
  new: 'green',
  modified: 'orange',
  unchanged: 'default',
  deleted: 'red',
};

const statusLabels: Record<string, string> = {
  new: '新增',
  modified: '修改',
  unchanged: '未变化',
  deleted: '删除',
};

const DiffViewer: React.FC<DiffViewerProps> = ({
  diffs,
  selectedItems,
  onToggleItem,
  onToggleSelectAll,
  getConfigTypeLabel,
}) => {
  const getItemColumns = (configType: ConfigType) => [
    {
      title: (
        <Checkbox
          onChange={(e) => {
            const items = diffs.find(d => d.config_type === configType)?.items || [];
            onToggleSelectAll(configType, items, e.target.checked);
          }}
        />
      ),
      key: 'select',
      width: 50,
      render: (_: any, record: ConfigItemDiff) => {
        if (record.status === 'unchanged') {
          return null;
        }
        const selected = selectedItems[configType]?.includes(record.item_id) || false;
        return (
          <Checkbox
            checked={selected}
            onChange={(e) => onToggleItem(configType, record.item_id, e.target.checked)}
          />
        );
      },
    },
    {
      title: '状态',
      key: 'status',
      width: 100,
      render: (_: any, record: ConfigItemDiff) => (
        <Tag color={statusColors[record.status]}>
          {statusLabels[record.status]}
        </Tag>
      ),
    },
    {
      title: 'ID',
      dataIndex: 'item_id',
      key: 'item_id',
    },
  ];

  const renderItemContent = (record: ConfigItemDiff) => {
    if (record.status === 'new') {
      return (
        <div>
          <Text type="secondary">新增配置：</Text>
          <pre style={{ background: '#f6ffed', padding: 8, marginTop: 8 }}>
            {JSON.stringify(record.imported, null, 2)}
          </pre>
        </div>
      );
    }
    if (record.status === 'modified') {
      return (
        <Row gutter={16}>
          <Col span={12}>
            <Text type="secondary">当前：</Text>
            <pre style={{ background: '#fff1f0', padding: 8, marginTop: 8 }}>
              {JSON.stringify(record.current, null, 2)}
            </pre>
          </Col>
          <Col span={12}>
            <Text type="secondary">导入：</Text>
            <pre style={{ background: '#fff7e6', padding: 8, marginTop: 8 }}>
              {JSON.stringify(record.imported, null, 2)}
            </pre>
          </Col>
        </Row>
      );
    }
    if (record.status === 'deleted') {
      return (
        <div>
          <Text type="secondary">将被删除：</Text>
          <pre style={{ background: '#fff1f0', padding: 8, marginTop: 8 }}>
            {JSON.stringify(record.current, null, 2)}
          </pre>
        </div>
      );
    }
    return null;
  };

  return (
    <Collapse defaultActiveKey={diffs.map(d => d.config_type)}>
      {diffs.map((diff) => (
        <Panel
          key={diff.config_type}
          header={
            <Space>
              <span>{getConfigTypeLabel(diff.config_type)}</span>
              {diff.summary.new > 0 && (
                <Tag color="green">+{diff.summary.new} 新增</Tag>
              )}
              {diff.summary.modified > 0 && (
                <Tag color="orange">~{diff.summary.modified} 修改</Tag>
              )}
              {diff.summary.deleted > 0 && (
                <Tag color="red">-{diff.summary.deleted} 删除</Tag>
              )}
              <Tag color="default">{diff.summary.unchanged} 未变化</Tag>
            </Space>
          }
        >
          <Table
            columns={getItemColumns(diff.config_type)}
            dataSource={diff.items}
            rowKey="item_id"
            pagination={false}
            expandable={{
              expandedRowRender: renderItemContent,
              rowExpandable: (record) => record.status !== 'unchanged',
            }}
          />
        </Panel>
      ))}
    </Collapse>
  );
};

export default DiffViewer;
