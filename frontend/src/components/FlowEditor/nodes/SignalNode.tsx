import React, { useRef } from 'react';
import { Handle, Position, NodeProps, useReactFlow } from 'reactflow';
import { Card, Input } from 'antd';
import Editor from '@monaco-editor/react';
import { useThemeStore } from '../../../store';
import { formatCode } from '../../../utils/codeFormatter';
import { notify } from '../../../utils/notify';

const SignalNode: React.FC<NodeProps> = ({ id, data }) => {
  const { mode } = useThemeStore();
  const editorRef = useRef<any>(null);
  const { setNodes } = useReactFlow();

  const handleFormat = async () => {
    if (!editorRef.current) return;
    try {
      const currentValue = editorRef.current.getValue();
      const formatted = await formatCode(currentValue, 'python');
      editorRef.current.setValue(formatted);
    } catch (error: any) {
      notify.error(error.message || '格式化失败');
    }
  };

  const handleValueChange = (values: Record<string, any>) => {
    setNodes(nodes => nodes.map(n =>
      n.id === id ? { ...n, data: { ...n.data, ...values } } : n
    ));
  };

  return (
    <Card
      title="Signal"
      size="middle"
      style={{ minWidth: 240, background: 'var(--bg-node-signal)', border: '1px solid var(--color-loss)' }}
      styles={{ header: { padding: '8px 12px' }, body: { padding: '8px 12px' } }}
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
            onMount={(editor, monaco) => {
              editorRef.current = editor;
              editor.addAction({
                id: 'format-signal-code',
                label: 'Format Code',
                keybindings: [monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF],
                run: () => handleFormat(),
              });
              editor.onDidChangeModelContent(() => {
                setNodes(nodes => nodes.map(n =>
                  n.id === id ? { ...n, data: { ...n.data, condition: editor.getValue() } } : n
                ));
              });
            }}
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
      <div style={{ marginTop: 8 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>Signal Column</div>
        <Input
          size="middle"
          defaultValue={data.signal_col || 'signal'}
          onChange={(e) => handleValueChange({ signal_col: e.target.value })}
        />
      </div>
      <Handle type="source" position={Position.Right} />
    </Card>
  );
};

export default SignalNode;
