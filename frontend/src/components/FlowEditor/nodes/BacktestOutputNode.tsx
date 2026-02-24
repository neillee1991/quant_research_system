import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Form, InputNumber } from '@douyinfe/semi-ui';

const BacktestOutputNode: React.FC<NodeProps> = ({ data }) => (
  <Card
    title="Backtest Output"
    style={{ minWidth: 200, background: 'var(--bg-node-output)', border: '1px solid var(--color-warning)' }}
    headerStyle={{ padding: '8px 12px' }}
    bodyStyle={{ padding: '8px 12px' }}
  >
    <Handle type="target" position={Position.Left} />
    <Form layout="vertical" labelPosition="top">
      <Form.InputNumber
        field="initial_capital"
        label="Initial Capital"
        initValue={data.config?.initial_capital || 1000000}
        size="small"
        style={{ width: '100%' }}
      />
      <Form.InputNumber
        field="commission_rate"
        label="Commission (%)"
        initValue={(data.config?.commission_rate || 0.0003) * 100}
        step={0.01}
        size="small"
        style={{ width: '100%' }}
      />
    </Form>
  </Card>
);

export default BacktestOutputNode;
