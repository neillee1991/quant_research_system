import React, { useCallback } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
  Connection,
  Edge,
  useNodesState,
  useEdgesState,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Button, Toast } from '@douyinfe/semi-ui';
import { useFlowStore } from '../../store';
import DataInputNode from './nodes/DataInputNode';
import OperatorNode from './nodes/OperatorNode';
import SignalNode from './nodes/SignalNode';
import BacktestOutputNode from './nodes/BacktestOutputNode';
import Toolbar from './Toolbar';
import { strategyApi } from '../../api';
import { useBacktestStore } from '../../store';

const nodeTypes = {
  data_input: DataInputNode,
  operator: OperatorNode,
  signal: SignalNode,
  backtest_output: BacktestOutputNode,
};

const FlowEditor: React.FC = () => {
  const { nodes, edges, setNodes, setEdges } = useFlowStore();
  const [localNodes, setLocalNodes, onNodesChange] = useNodesState(nodes);
  const [localEdges, setLocalEdges, onEdgesChange] = useEdgesState(edges);
  const { setResult, setLoading } = useBacktestStore();

  const onConnect = useCallback(
    (params: Connection | Edge) => setLocalEdges((eds) => addEdge(params, eds)),
    [setLocalEdges]
  );

  const handleRunBacktest = async () => {
    setLoading(true);
    try {
      const graph = {
        nodes: localNodes.map((n) => ({ id: n.id, type: n.type, data: n.data })),
        edges: localEdges.map((e) => ({ source: e.source, target: e.target })),
      };
      const res = await strategyApi.backtest(graph);
      setResult(res.data);
      Toast.success({ content: '回测完成' });
    } catch (e: any) {
      Toast.error({ content: e?.response?.data?.detail || '回测失败' });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ width: '100%', height: '70vh', border: '1px solid var(--border-color)', borderRadius: 8 }}>
      <Toolbar onAddNode={(node) => setLocalNodes((ns) => [...ns, node])} />
      <ReactFlow
        nodes={localNodes}
        edges={localEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        fitView
      >
        <Background />
        <Controls />
        <MiniMap />
      </ReactFlow>
      <div style={{ padding: 8, textAlign: 'right' }}>
        <Button theme="solid" onClick={handleRunBacktest}>
          运行回测
        </Button>
      </div>
    </div>
  );
};

export default FlowEditor;
