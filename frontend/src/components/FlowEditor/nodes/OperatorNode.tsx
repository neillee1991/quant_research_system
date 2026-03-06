import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Tag, Form, InputNumber } from '@douyinfe/semi-ui';

const OperatorNode: React.FC<NodeProps> = ({ data }) => (
  <Card
    title={<><Tag color="blue" size="small">{data.op?.toUpperCase()}</Tag> Operator</>}
    style={{ minWidth: 180, background: 'var(--bg-node-operator)', border: '1px solid var(--color-accent)' }}
    headerStyle={{ padding: '8px 12px' }}
    bodyStyle={{ padding: '8px 12px' }}
  >
    <Handle type="target" position={Position.Left} />
    <Form layout="vertical" labelPosition="top">
      <Form.InputNumber
        field="window"
        label="Window"
        initValue={data.window || 20}
        min={1}
        max={200}
        size="small"
        style={{ width: '100%' }}
      />
      <div style={{ marginTop: 8 }}>
        <span style={{ color: 'var(--text-tertiary)', fontSize: 12 }}>Output Column</span>
        <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{data.output_col || data.op}</div>
      </div>
    </Form>
    <Handle type="source" position={Position.Right} />
  </Card>
);

export default OperatorNode;
