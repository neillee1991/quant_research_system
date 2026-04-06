import React from 'react';
import { Card, Table, Tag, Typography, Alert } from 'antd';
import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { ConfigType, ImportApplyResponse } from './types';

const { Title } = Typography;

interface ImportResultProps {
  result: ImportApplyResponse;
  getConfigTypeLabel: (value: ConfigType) => string;
}

const ImportResult: React.FC<ImportResultProps> = ({ result, getConfigTypeLabel }) => {
  const columns = [
    {
      title: '配置类型',
      key: 'config_type',
      render: (_: any, record: { config_type: ConfigType }) => (
        getConfigTypeLabel(record.config_type)
      ),
    },
    {
      title: '新增',
      key: 'created',
      render: (_: any, record: { created: number }) => (
        record.created > 0 ? <Tag color="green">+{record.created}</Tag> : '-'
      ),
    },
    {
      title: '更新',
      key: 'updated',
      render: (_: any, record: { updated: number }) => (
        record.updated > 0 ? <Tag color="orange">~{record.updated}</Tag> : '-'
      ),
    },
    {
      title: '跳过',
      key: 'skipped',
      render: (_: any, record: { skipped: number }) => (
        record.skipped > 0 ? <Tag color="default">{record.skipped}</Tag> : '-'
      ),
    },
  ];

  const data = Object.entries(result.summary).map(([config_type, summary]) => ({
    config_type: config_type as ConfigType,
    ...summary,
  }));

  return (
    <Card title="导入结果">
      {result.success ? (
        <Alert
          icon={<CheckCircleOutlined />}
          message="导入成功"
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
        />
      ) : (
        <Alert
          icon={<CloseCircleOutlined />}
          message="导入失败"
          description={
            <ul>
              {result.errors.map((err, idx) => (
                <li key={idx}>{err}</li>
              ))}
            </ul>
          }
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

      <Table
        columns={columns}
        dataSource={data}
        rowKey="config_type"
        pagination={false}
      />
    </Card>
  );
};

export default ImportResult;
