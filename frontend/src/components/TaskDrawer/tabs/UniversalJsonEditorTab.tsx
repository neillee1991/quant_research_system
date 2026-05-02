/**
 * 通用 JSON 编辑器 Tab
 */
import React from 'react';
import { Card } from 'antd';
import Editor from '@monaco-editor/react';
import { useThemeStore } from '../../../store';

interface UniversalJsonEditorTabProps {
  value: any;
  onChange?: (value: string) => void;
  readOnly?: boolean;
  height?: string | number;
}

export const UniversalJsonEditorTab: React.FC<UniversalJsonEditorTabProps> = ({
  value,
  onChange,
  readOnly = false,
  height = '500px',
}) => {
  const { mode } = useThemeStore();

  const jsonText = typeof value === 'string' ? value : JSON.stringify(value, null, 2);

  return (
    <Card style={{ height: '100%', border: 'none', padding: 0 }}>
      <Editor
        height={height}
        language="json"
        value={jsonText}
        onChange={(value) => onChange?.(value || '')}
        theme={mode === 'dark' ? 'vs-dark' : 'light'}
        options={{
          readOnly,
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          fontSize: 13,
          lineNumbers: 'on',
          folding: true,
          wordWrap: 'on',
        }}
      />
    </Card>
  );
};
