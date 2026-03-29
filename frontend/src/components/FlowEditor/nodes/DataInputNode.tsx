import React from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Card, Input } from 'antd';

const DataInputNode: React.FC<NodeProps> = ({ id, data }) => {
  const { setNodes } = useReactFlow();

  const handleChange = (field: string, value: string) => {
    setNodes(nodes => nodes.map(n =>
      n.id === id ? { ...n, data: { ...n.data, [field]: value } } : n
    ));
  };

  return (
    <Card
      title="Data Input"
      size="middle"
      style={{ minWidth: 200, background: 'var(--bg-node-data)', border: '1px solid var(--color-gain)' }}
      styles={{ header: { padding: '8px 12px' }, body: { padding: '8px 12px' } }}
    >
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Stock Code</div>
        <Input
          size="middle"
          defaultValue={data.ts_code}
          placeholder="000001.SZ"
          onChange={(e) => handleChange('ts_code', e.target.value)}
        />
      </div>
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Start Date</div>
        <Input
          size="middle"
          defaultValue={data.start}
          placeholder="20200101"
          onChange={(e) => handleChange('start', e.target.value)}
        />
      </div>
      <div>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>End Date</div>
        <Input
          size="middle"
          defaultValue={data.end}
          placeholder="20241231"
          onChange={(e) => handleChange('end', e.target.value)}
        />
      </div>
      <Handle type="source" position={Position.Right} />
    </Card>
  );
};

export default DataInputNode;
