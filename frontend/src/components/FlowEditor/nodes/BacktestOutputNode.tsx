import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, InputNumber, Form } from 'antd';

const BacktestOutputNode: React.FC<NodeProps> = ({ data }) => (
  <Card size="small" title="Backtest Output" style={{ minWidth: 200, background: '#1a2a2a', borderColor: '#faad14' }}>
    <Handle type="target" position={Position.Left} />
    <Form layout="vertical" size="small">
      <Form.Item label="Initial Capital">
        <InputNumber defaultValue={data.config?.initial_capital || 1000000} style={{ width: '100%' }} />
      </Form.Item>
      <Form.Item label="Commission (%)">
        <InputNumber defaultValue={(data.config?.commission_rate || 0.0003) * 100} step={0.01} style={{ width: '100%' }} />
      </Form.Item>
    </Form>
  </Card>
);

export default BacktestOutputNode;
