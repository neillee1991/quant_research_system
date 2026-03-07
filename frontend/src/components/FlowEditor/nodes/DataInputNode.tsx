import React from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Card, Form } from '@douyinfe/semi-ui';

const DataInputNode: React.FC<NodeProps> = ({ id, data }) => {
  const { setNodes } = useReactFlow();

  const handleValueChange = (values: Record<string, any>) => {
    setNodes(nodes => nodes.map(n =>
      n.id === id ? { ...n, data: { ...n.data, ...values } } : n
    ));
  };

  return (
    <Card
      title="Data Input"
      style={{ minWidth: 200, background: 'var(--bg-node-data)', border: '1px solid var(--color-gain)' }}
      headerStyle={{ padding: '8px 12px' }}
      bodyStyle={{ padding: '8px 12px' }}
    >
      <Form layout="vertical" labelPosition="top" onValueChange={handleValueChange}>
        <Form.Input
          field="ts_code"
          label="Stock Code"
          initValue={data.ts_code}
          placeholder="000001.SZ"
          noLabel={false}
          size="small"
        />
        <Form.Input
          field="start"
          label="Start Date"
          initValue={data.start}
          placeholder="20200101"
          size="small"
        />
        <Form.Input
          field="end"
          label="End Date"
          initValue={data.end}
          placeholder="20241231"
          size="small"
        />
      </Form>
      <Handle type="source" position={Position.Right} />
    </Card>
  );
};

export default DataInputNode;
