import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Input, Form } from 'antd';

const SignalNode: React.FC<NodeProps> = ({ data }) => (
  <Card size="small" title="Signal" style={{ minWidth: 200, background: '#2a1a1a', borderColor: '#ff4d4f' }}>
    <Handle type="target" position={Position.Left} />
    <Form layout="vertical" size="small">
      <Form.Item label="Condition">
        <Input defaultValue={data.condition} placeholder="close > sma20" />
      </Form.Item>
      <Form.Item label="Signal Column">
        <Input defaultValue={data.signal_col || 'signal'} />
      </Form.Item>
    </Form>
    <Handle type="source" position={Position.Right} />
  </Card>
);

export default SignalNode;
