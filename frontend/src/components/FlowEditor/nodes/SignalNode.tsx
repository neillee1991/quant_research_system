import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Form } from '@douyinfe/semi-ui';

const SignalNode: React.FC<NodeProps> = ({ data }) => (
  <Card
    title="Signal"
    style={{ minWidth: 200, background: 'var(--bg-node-signal)', border: '1px solid var(--color-loss)' }}
    headerStyle={{ padding: '8px 12px' }}
    bodyStyle={{ padding: '8px 12px' }}
  >
    <Handle type="target" position={Position.Left} />
    <Form layout="vertical" labelPosition="top">
      <Form.Input
        field="condition"
        label="Condition"
        initValue={data.condition}
        placeholder="close > sma20"
        size="small"
      />
      <Form.Input
        field="signal_col"
        label="Signal Column"
        initValue={data.signal_col || 'signal'}
        size="small"
      />
    </Form>
    <Handle type="source" position={Position.Right} />
  </Card>
);

export default SignalNode;
