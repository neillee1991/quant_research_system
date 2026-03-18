import React from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Card, InputNumber } from 'antd';

const BacktestOutputNode: React.FC<NodeProps> = ({ id, data }) => {
  const { setNodes } = useReactFlow();

  const handleConfigChange = (field: string, value: number | null) => {
    setNodes(nodes => nodes.map(n =>
      n.id === id ? { ...n, data: { ...n.data, config: { ...n.data.config, [field]: value } } } : n
    ));
  };

  return (
    <Card
      title="Backtest Output"
      size="small"
      style={{ minWidth: 200, background: 'var(--bg-node-output)', border: '1px solid var(--color-warning)' }}
      styles={{ header: { padding: '8px 12px' }, body: { padding: '8px 12px' } }}
    >
      <Handle type="target" position={Position.Left} />
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Initial Capital</div>
        <InputNumber
          size="small"
          defaultValue={data.config?.initial_capital || 1000000}
          style={{ width: '100%' }}
          onChange={(v) => handleConfigChange('initial_capital', v)}
        />
      </div>
      <div>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Commission (%)</div>
        <InputNumber
          size="small"
          defaultValue={(data.config?.commission_rate || 0.0003) * 100}
          step={0.01}
          style={{ width: '100%' }}
          onChange={(v) => handleConfigChange('commission_rate', v !== null ? v / 100 : null)}
        />
      </div>
    </Card>
  );
};

export default BacktestOutputNode;
