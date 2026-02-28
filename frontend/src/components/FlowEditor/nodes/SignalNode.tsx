import React from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Card, Form } from '@douyinfe/semi-ui';
import Editor from '@monaco-editor/react';
import { useThemeStore } from '../../../store';

const SignalNode: React.FC<NodeProps> = ({ data }) => {
  const { mode } = useThemeStore();
  return (
    <Card
      title="Signal"
      style={{ minWidth: 240, background: 'var(--bg-node-signal)', border: '1px solid var(--color-loss)' }}
      headerStyle={{ padding: '8px 12px' }}
      bodyStyle={{ padding: '8px 12px' }}
    >
      <Handle type="target" position={Position.Left} />
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Condition</div>
        <div style={{ border: '1px solid var(--border-color)', borderRadius: 4, overflow: 'hidden' }}>
          <Editor
            height="60px"
            language="python"
            theme={mode === 'dark' ? 'vs-dark' : 'vs-light'}
            defaultValue={data.condition || 'close > sma20'}
            options={{
              minimap: { enabled: false },
              fontSize: 12,
              scrollBeyondLastLine: false,
              automaticLayout: true,
              lineNumbers: 'off',
              glyphMargin: false,
              folding: false,
              lineDecorationsWidth: 4,
              lineNumbersMinChars: 0,
              overviewRulerLanes: 0,
              scrollbar: { vertical: 'hidden', horizontal: 'auto' },
              wordWrap: 'on',
            }}
          />
        </div>
      </div>
      <Form layout="vertical" labelPosition="top">
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
};

export default SignalNode;
