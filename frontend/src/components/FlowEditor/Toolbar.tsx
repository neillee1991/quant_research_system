import React from 'react';
import { Button, Space, Tooltip } from 'antd';
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
    <div style={{ padding: '8px 12px', borderBottom: '1px solid #303030', background: '#141414' }}>
      <Space wrap>
        <Tooltip title="Add data source node"><Button size="small" onClick={addDataInput}>+ Data Input</Button></Tooltip>
        <Tooltip title="SMA operator"><Button size="small" onClick={() => addOperator('sma')}>+ SMA</Button></Tooltip>
        <Tooltip title="EMA operator"><Button size="small" onClick={() => addOperator('ema')}>+ EMA</Button></Tooltip>
        <Tooltip title="RSI operator"><Button size="small" onClick={() => addOperator('rsi')}>+ RSI</Button></Tooltip>
        <Tooltip title="MACD operator"><Button size="small" onClick={() => addOperator('macd')}>+ MACD</Button></Tooltip>
        <Tooltip title="Bollinger Bands"><Button size="small" onClick={() => addOperator('bollinger')}>+ Bollinger</Button></Tooltip>
        <Tooltip title="Signal node"><Button size="small" onClick={addSignal}>+ Signal</Button></Tooltip>
        <Tooltip title="Backtest output"><Button size="small" onClick={addOutput}>+ Output</Button></Tooltip>
      </Space>
    </div>
  );
};

export default Toolbar;
