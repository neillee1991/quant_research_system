import React from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Card, Tag, InputNumber } from 'antd';

const OperatorNode: React.FC<NodeProps> = ({ id, data }) => {
  const { setNodes } = useReactFlow();

  const handleWindowChange = (value: number | null) => {
    setNodes(nodes => nodes.map(n =>
      n.id === id ? { ...n, data: { ...n.data, window: value ?? 20 } } : n
    ));
  };

  return (
    <Card
      title={<><Tag color="blue">{data.op?.toUpperCase()}</Tag> Operator</>}
      size="small"
      style={{ minWidth: 180, background: 'var(--bg-node-operator)', border: '1px solid var(--color-accent)' }}
      styles={{ header: { padding: '8px 12px' }, body: { padding: '8px 12px' } }}
    >
      <Handle type="target" position={Position.Left} />
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Window</div>
        <InputNumber
          size="small"
          defaultValue={data.window || 20}
          min={1}
          max={200}
          style={{ width: '100%' }}
          onChange={handleWindowChange}
        />
      </div>
      <div style={{ marginTop: 8 }}>
        <span style={{ color: 'var(--text-tertiary)', fontSize: 12 }}>Output Column</span>
        <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{data.output_col || data.op}</div>
      </div>
      <Handle type="source" position={Position.Right} />
    </Card>
  );
};

export default OperatorNode;
