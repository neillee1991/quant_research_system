import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, InputNumber, Form, Tag } from 'antd';

const OperatorNode: React.FC<NodeProps> = ({ data }) => (
  <Card size="small" title={<><Tag color="blue">{data.op?.toUpperCase()}</Tag>Operator</>}
    style={{ minWidth: 180, background: '#1a1a2a', borderColor: '#1677ff' }}>
    <Handle type="target" position={Position.Left} />
    <Form layout="vertical" size="small">
      <Form.Item label="Window">
        <InputNumber defaultValue={data.window || 20} min={1} max={200} style={{ width: '100%' }} />
      </Form.Item>
      <Form.Item label="Output Column">
        <span style={{ color: '#aaa', fontSize: 12 }}>{data.output_col || data.op}</span>
      </Form.Item>
    </Form>
    <Handle type="source" position={Position.Right} />
  </Card>
);

export default OperatorNode;
