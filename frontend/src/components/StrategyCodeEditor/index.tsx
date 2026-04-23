import React from 'react';
import Editor from '@monaco-editor/react';
import { Button, Space, Tag, Collapse, Alert, Select } from 'antd';
import {
  CheckCircleOutlined,
  WarningOutlined,
  LoadingOutlined,
  CodeOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import type { StrategyScriptValidateResponse, StrategyScriptCompileResponse, ScriptRunStatus } from '../../types';

interface StrategyCodeEditorProps {
  value: string;
  runStatus: ScriptRunStatus;
  validationResult: StrategyScriptValidateResponse | null;
  compileResult: StrategyScriptCompileResponse | null;
  runError: string | null;
  onChange: (value: string) => void;
  onValidate: () => void;
  onCompile: () => void;
  onRun: () => void;
}

// 策略模板配置
const STRATEGY_TEMPLATES = [
  {
    value: 'sma',
    label: 'SMA 均线策略',
    code: `def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "capital": 1000000,
        "signals": [
            {"type": "indicator", "op": "sma", "params": {"window": 5}, "output_col": "sma5"},
            {"type": "indicator", "op": "sma", "params": {"window": 20}, "output_col": "sma20"},
            {"type": "condition", "expr": "sma5 > sma20", "output_col": "signal"},
        ],
    }
`,
  },
  {
    value: 'rsi',
    label: 'RSI 超买超卖',
    code: `def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "capital": 1000000,
        "signals": [
            {"type": "indicator", "op": "rsi", "params": {"window": 14}, "output_col": "rsi14"},
            {"type": "condition", "expr": "rsi14 < 30", "output_col": "buy_signal"},
            {"type": "condition", "expr": "rsi14 > 70", "output_col": "sell_signal"},
        ],
    }
`,
  },
  {
    value: 'macd',
    label: 'MACD 金叉死叉',
    code: `def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "capital": 1000000,
        "signals": [
            {"type": "indicator", "op": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}, "output_col": "macd"},
            {"type": "condition", "expr": "macd_macd > macd_signal", "output_col": "signal"},
        ],
    }
`,
  },
  {
    value: 'bollinger',
    label: '布林带突破',
    code: `def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "capital": 1000000,
        "signals": [
            {"type": "indicator", "op": "bollinger", "params": {"window": 20, "num_std": 2.0}, "output_col": "bollinger"},
            {"type": "condition", "expr": "close > bollinger_upper", "output_col": "sell_signal"},
            {"type": "condition", "expr": "close < bollinger_lower", "output_col": "buy_signal"},
        ],
    }
`,
  },
];

const StrategyCodeEditor: React.FC<StrategyCodeEditorProps> = ({
  value,
  runStatus,
  validationResult,
  compileResult,
  runError,
  onChange,
  onValidate,
  onCompile,
  onRun,
}) => {
  const isValidating = runStatus === 'validating';
  const isCompiling = runStatus === 'compiling';
  const isRunning = runStatus === 'running' || runStatus === 'submitting';

  // 处理模板选择
  const handleTemplateChange = (templateValue: string) => {
    const template = STRATEGY_TEMPLATES.find(t => t.value === templateValue);
    if (template) {
      onChange(template.code);
    }
  };

  const statusIcon = () => {
    if (isValidating || isCompiling || isRunning) return <LoadingOutlined spin />;
    if (runStatus === 'success') return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
    if (runStatus === 'failed') return <WarningOutlined style={{ color: '#ff4d4f' }} />;
    return <CodeOutlined />;
  };

  const statusText: Record<ScriptRunStatus, string> = {
    idle: '就绪',
    validating: '校验中...',
    compiling: '编译中...',
    submitting: '提交中...',
    running: '回测运行中...',
    success: '回测完成',
    failed: '回测失败',
  };

  const collapseItems = [];
  if (validationResult) {
    const type = validationResult.errors.length > 0 ? 'error' : validationResult.warnings.length > 0 ? 'warning' : 'success';
    const messages = [
      ...validationResult.errors.map((e) => `错误: ${e}`),
      ...validationResult.warnings.map((w) => `警告: ${w}`),
    ];
    if (messages.length === 0) messages.push('校验通过');
    collapseItems.push({
      key: 'validate',
      label: (
        <span>
          {type === 'success' ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <WarningOutlined style={{ color: type === 'error' ? '#ff4d4f' : '#faad14' }} />}
          {' '}校验结果 {validationResult.script_hash ? `(${validationResult.script_hash.slice(0, 8)})` : ''}
        </span>
      ),
      children: (
        <Alert
          type={type}
          showIcon
          message={messages.map((m, i) => <div key={i}>{m}</div>)}
        />
      ),
    });
  }

  if (compileResult) {
    const isFailure = 'errors' in compileResult;
    const hasError = isFailure && compileResult.errors && compileResult.errors.length > 0;
    const ir = 'ir' in compileResult ? compileResult.ir : undefined;
    const pipeline = ir?.pipeline || [];
    const dataSource = ir?.data_source;
    collapseItems.push({
      key: 'compile',
      label: (
        <span>
          {hasError ? <WarningOutlined style={{ color: '#ff4d4f' }} /> : <CheckCircleOutlined style={{ color: '#52c41a' }} />}
          {' '}编译结果
        </span>
      ),
      children: hasError ? (
        <Alert type="error" showIcon message={isFailure ? compileResult.errors.map((e, i) => <div key={i}>{e}</div>) : undefined} />
      ) : (
        <div style={{ fontSize: 12, lineHeight: 1.8 }}>
          {dataSource && (
            <div>数据源: {dataSource.ts_code} ({dataSource.start_date} ~ {dataSource.end_date})</div>
          )}
          <div>算子链: {pipeline.map((s: { type: string; op?: string; output_col?: string }, i: number) => (
            <Tag key={i} style={{ margin: '2px' }}>{s.op || s.type}{s.output_col ? ` → ${s.output_col}` : ''}</Tag>
          ))}</div>
          <div style={{ color: 'var(--text-muted)', marginTop: 4 }}>
            IR v{ir?.version || '?'} | entry: {ir?.entry_point || '?'}
          </div>
        </div>
      ),
    });
  }

  if (runError) {
    collapseItems.push({
      key: 'error',
      label: <span><WarningOutlined style={{ color: '#ff4d4f' }} /> 执行错误</span>,
      children: <Alert type="error" showIcon message={runError} />,
    });
  }

  return (
    <div style={{
      background: 'var(--bg-card)',
      borderRadius: 12,
      border: '1px solid var(--border-color)',
      overflow: 'hidden',
      boxShadow: 'var(--shadow-sm)',
    }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '16px 20px',
        borderBottom: '1px solid var(--border-color)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{
            display: 'inline-block',
            width: 4,
            height: 16,
            background: 'var(--gradient-primary)',
            borderRadius: 2,
          }} />
          <span style={{ color: 'var(--color-primary)', fontSize: 16, fontWeight: 600 }}>
            代码策略编辑器
          </span>
          <Tag color="blue">Phase 2</Tag>
          <span style={{ fontSize: 12, color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 4 }}>
            {statusIcon()} {statusText[runStatus]}
          </span>
        </div>
        <Space>
          <Select
            placeholder="选择策略模板"
            style={{ width: 180 }}
            size="small"
            onChange={handleTemplateChange}
            options={STRATEGY_TEMPLATES}
            prefix={<FileTextOutlined />}
          />
          <Button onClick={onValidate} loading={isValidating} size="small">校验</Button>
          <Button onClick={onCompile} loading={isCompiling} size="small">编译</Button>
          <Button type="primary" onClick={onRun} loading={isRunning} size="small">运行回测</Button>
        </Space>
      </div>
      <Editor
        height="440px"
        defaultLanguage="python"
        theme="vs-dark"
        value={value}
        onChange={(next) => onChange(next ?? '')}
        options={{
          minimap: { enabled: false },
          fontSize: 14,
          automaticLayout: true,
          tabSize: 4,
        }}
      />
      {collapseItems.length > 0 && (
        <div style={{ borderTop: '1px solid var(--border-color)' }}>
          <Collapse items={collapseItems} ghost style={{ padding: '0 16px' }} />
        </div>
      )}
    </div>
  );
};

export default StrategyCodeEditor;
