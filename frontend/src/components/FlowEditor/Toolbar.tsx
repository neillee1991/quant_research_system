import React from 'react';
import { Button, Tooltip } from '@douyinfe/semi-ui';
import { Node } from 'reactflow';

interface Props {
  onAddNode: (node: Node) => void;
}

let idCounter = 100;
const uid = () => String(++idCounter);

const Toolbar: React.FC<Props> = ({ onAddNode }) => {
  const addDataInput = () =>
    onAddNode({
      id: uid(), type: 'data_input',
      position: { x: 50, y: 50 },
      data: { ts_code: '000001.SZ', start: '20200101', end: '20241231' },
    });

  const addOperator = (op: string) =>
    onAddNode({
      id: uid(), type: 'operator',
      position: { x: 250, y: 50 },
      data: { op, window: 20, output_col: op },
    });

  const addSignal = () =>
    onAddNode({
      id: uid(), type: 'signal',
      position: { x: 450, y: 50 },
      data: { condition: 'close > sma20', signal_col: 'signal' },
    });

  const addOutput = () =>
    onAddNode({
      id: uid(), type: 'backtest_output',
      position: { x: 650, y: 50 },
      data: { config: {} },
    });

  return (
    <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)', background: 'var(--bg-secondary)' }}>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
        <Tooltip content="Add data source node"><Button size="small" onClick={addDataInput}>+ Data Input</Button></Tooltip>
        <Tooltip content="SMA operator"><Button size="small" onClick={() => addOperator('sma')}>+ SMA</Button></Tooltip>
        <Tooltip content="EMA operator"><Button size="small" onClick={() => addOperator('ema')}>+ EMA</Button></Tooltip>
        <Tooltip content="RSI operator"><Button size="small" onClick={() => addOperator('rsi')}>+ RSI</Button></Tooltip>
        <Tooltip content="MACD operator"><Button size="small" onClick={() => addOperator('macd')}>+ MACD</Button></Tooltip>
        <Tooltip content="Bollinger Bands"><Button size="small" onClick={() => addOperator('bollinger')}>+ Bollinger</Button></Tooltip>
        <Tooltip content="Signal node"><Button size="small" onClick={addSignal}>+ Signal</Button></Tooltip>
        <Tooltip content="Backtest output"><Button size="small" onClick={addOutput}>+ Output</Button></Tooltip>
      </div>
    </div>
  );
};

export default Toolbar;
