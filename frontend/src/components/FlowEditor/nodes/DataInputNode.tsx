import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Input, Form } from '@douyinfe/semi-ui';

const DataInputNode: React.FC<NodeProps> = ({ data }) => (
  <Card
    title="Data Input"
    style={{ minWidth: 200, background: 'var(--bg-node-data)', border: '1px solid var(--color-gain)' }}
    headerStyle={{ padding: '8px 12px' }}
    bodyStyle={{ padding: '8px 12px' }}
  >
    <Form layout="vertical" labelPosition="top">
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

export default DataInputNode;
