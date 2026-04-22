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
    code: `# SMA 均线策略
# 当短期均线上穿长期均线时买入，下穿时卖出

def initialize(context):
    # 策略参数
    context.params = {
        'short_period': 5,    # 短期均线周期
        'long_period': 20,    # 长期均线周期
        'capital': 100000     # 初始资金
    }

def handle_data(context, data):
    # 获取收盘价数据
    close = data['close']

    # 计算均线
    short_ma = close.rolling(context.params['short_period']).mean()
    long_ma = close.rolling(context.params['long_period']).mean()

    # 生成交易信号
    # 买入信号：短期均线上穿长期均线
    buy_signal = (short_ma > long_ma) & (short_ma.shift(1) <= long_ma.shift(1))
    # 卖出信号：短期均线下穿长期均线
    sell_signal = (short_ma < long_ma) & (short_ma.shift(1) >= long_ma.shift(1))

    return {
        'buy_signal': buy_signal,
        'sell_signal': sell_signal
    }

def risk_management(context, positions):
    # 止损逻辑：亏损10%止损
    stop_loss_pct = 0.10

    return {
        'stop_loss_pct': stop_loss_pct
    }
`,
  },
  {
    value: 'rsi',
    label: 'RSI 超买超卖',
    code: `# RSI 超买超卖策略
# RSI < 30 买入，RSI > 70 卖出

def initialize(context):
    # 策略参数
    context.params = {
        'rsi_period': 14,     # RSI周期
        'oversold': 30,       # 超卖阈值
        'overbought': 70,     # 超买阈值
        'capital': 100000     # 初始资金
    }

def handle_data(context, data):
    # 获取收盘价数据
    close = data['close']

    # 计算RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(context.params['rsi_period']).mean()
    avg_loss = loss.rolling(context.params['rsi_period']).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    # 生成交易信号
    buy_signal = rsi < context.params['oversold']
    sell_signal = rsi > context.params['overbought']

    return {
        'buy_signal': buy_signal,
        'sell_signal': sell_signal
    }

def risk_management(context, positions):
    # 止损逻辑：亏损8%止损
    stop_loss_pct = 0.08

    return {
        'stop_loss_pct': stop_loss_pct
    }
`,
  },
  {
    value: 'macd',
    label: 'MACD 金叉死叉',
    code: `# MACD 金叉死叉策略
# MACD线穿过信号线时产生交易信号

def initialize(context):
    # 策略参数
    context.params = {
        'fast_period': 12,    # 快速EMA周期
        'slow_period': 26,    # 慢速EMA周期
        'signal_period': 9,   # 信号线EMA周期
        'capital': 100000     # 初始资金
    }

def handle_data(context, data):
    # 获取收盘价数据
    close = data['close']

    # 计算MACD
    ema_fast = close.ewm(span=context.params['fast_period']).mean()
    ema_slow = close.ewm(span=context.params['slow_period']).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=context.params['signal_period']).mean()

    # 生成交易信号
    buy_signal = (macd_line > signal_line) & (macd_line.shift(1) <= signal_line.shift(1))
    sell_signal = (macd_line < signal_line) & (macd_line.shift(1) >= signal_line.shift(1))

    return {
        'buy_signal': buy_signal,
        'sell_signal': sell_signal
    }

def risk_management(context, positions):
    # 止损逻辑：亏损12%止损
    stop_loss_pct = 0.12

    return {
        'stop_loss_pct': stop_loss_pct
    }
`,
  },
  {
    value: 'bollinger',
    label: '布林带突破',
    code: `# 布林带突破策略
# 价格突破上轨买入，突破下轨卖出

def initialize(context):
    # 策略参数
    context.params = {
        'bb_period': 20,      # 布林带周期
        'bb_std': 2,          # 标准差倍数
        'capital': 100000     # 初始资金
    }

def handle_data(context, data):
    # 获取收盘价数据
    close = data['close']

    # 计算布林带
    middle_band = close.rolling(context.params['bb_period']).mean()
    std_dev = close.rolling(context.params['bb_period']).std()
    upper_band = middle_band + (context.params['bb_std'] * std_dev)
    lower_band = middle_band - (context.params['bb_std'] * std_dev)

    # 生成交易信号
    buy_signal = close > upper_band
    sell_signal = close < lower_band

    return {
        'buy_signal': buy_signal,
        'sell_signal': sell_signal
    }

def risk_management(context, positions):
    # 止损逻辑：亏损15%止损
    stop_loss_pct = 0.15

    return {
        'stop_loss_pct': stop_loss_pct
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
