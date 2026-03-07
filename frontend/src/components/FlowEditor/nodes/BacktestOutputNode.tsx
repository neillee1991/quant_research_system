import React from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Card, Form } from '@douyinfe/semi-ui';

const BacktestOutputNode: React.FC<NodeProps> = ({ id, data }) => {
  const { setNodes } = useReactFlow();

  const handleValueChange = (values: Record<string, any>) => {
    setNodes(nodes => nodes.map(n =>
      n.id === id ? { ...n, data: { ...n.data, config: { ...n.data.config, ...values } } } : n
    ));
  };

  return (
    <Card
      title="Backtest Output"
      style={{ minWidth: 200, background: 'var(--bg-node-output)', border: '1px solid var(--color-warning)' }}
      headerStyle={{ padding: '8px 12px' }}
      bodyStyle={{ padding: '8px 12px' }}
    >
      <Handle type="target" position={Position.Left} />
      <Form layout="vertical" labelPosition="top" onValueChange={handleValueChange}>
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
};

export default BacktestOutputNode;
