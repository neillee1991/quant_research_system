import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Input, Form } from 'antd';

const DataInputNode: React.FC<NodeProps> = ({ data }) => (
  <Card size="small" title="Data Input" style={{ minWidth: 200, background: '#1a2a1a', borderColor: '#52c41a' }}>
    <Form layout="vertical" size="small">
      <Form.Item label="Stock Code">
        <Input defaultValue={data.ts_code} placeholder="000001.SZ" />
      </Form.Item>
      <Form.Item label="Start Date">
        <Input defaultValue={data.start} placeholder="20200101" />
      </Form.Item>
      <Form.Item label="End Date">
        <Input defaultValue={data.end} placeholder="20241231" />
      </Form.Item>
    </Form>
    <Handle type="source" position={Position.Right} />
  </Card>
);

export default DataInputNode;
