import React, { useCallback, useMemo } from 'react';
import ReactFlow, {
  Background,
  Controls,
  addEdge,
  Connection,
  Edge,
  Node,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { TaskConfig } from '../../api';

interface DAGEditorProps {
  tasks: TaskConfig[];
  onChange: (tasks: TaskConfig[]) => void;
}

// 节点样式
const nodeStyles = {
  sync: {
    background: 'linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)',
    color: 'white',
    border: 'none',
    borderRadius: 8,
    padding: '8px 16px',
    fontSize: 12,
    fontWeight: 500,
  },
  factor: {
    background: 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
    color: 'white',
    border: 'none',
    borderRadius: 8,
    padding: '8px 16px',
    fontSize: 12,
    fontWeight: 500,
  },
};

const DAGEditor: React.FC<DAGEditorProps> = ({ tasks, onChange }) => {
  // 将 tasks 转换为 React Flow 节点和边
  const { initialNodes, initialEdges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const taskMap = new Map<string, number>();

    // 计算每个任务的层级（用于布局）
    const levels = new Map<string, number>();
    const calculateLevel = (taskId: string, visited = new Set<string>()): number => {
      if (visited.has(taskId)) return 0;
      if (levels.has(taskId)) return levels.get(taskId)!;

      visited.add(taskId);
      const task = tasks.find(t => t.id === taskId);
      if (!task || !task.depends_on || task.depends_on.length === 0) {
        levels.set(taskId, 0);
        return 0;
      }

      const maxDepLevel = Math.max(...task.depends_on.map(dep => calculateLevel(dep, visited)));
      const level = maxDepLevel + 1;
      levels.set(taskId, level);
      return level;
    };

    tasks.forEach(t => calculateLevel(t.id));

    // 按层级分组
    const levelGroups = new Map<number, TaskConfig[]>();
    tasks.forEach(task => {
      const level = levels.get(task.id) || 0;
      if (!levelGroups.has(level)) {
        levelGroups.set(level, []);
      }
      levelGroups.get(level)!.push(task);
    });

    // 创建节点
    tasks.forEach((task, index) => {
      taskMap.set(task.id, index);
      const level = levels.get(task.id) || 0;
      const levelTasks = levelGroups.get(level) || [];
      const indexInLevel = levelTasks.findIndex(t => t.id === task.id);

      nodes.push({
        id: task.id,
        data: { label: task.id },
        position: {
          x: level * 200 + 50,
          y: indexInLevel * 80 + 50,
        },
        style: nodeStyles[task.type] || nodeStyles.sync,
      });
    });

    // 创建边
    tasks.forEach(task => {
      if (task.depends_on) {
        task.depends_on.forEach(dep => {
          edges.push({
            id: `${dep}-${task.id}`,
            source: dep,
            target: task.id,
            markerEnd: { type: MarkerType.ArrowClosed },
            style: { stroke: 'var(--semi-color-text-2)' },
          });
        });
      }
    });

    return { initialNodes: nodes, initialEdges: edges };
  }, [tasks]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const onConnect = useCallback(
    (params: Connection) => {
      // 添加新边
      setEdges(eds => addEdge({
        ...params,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { stroke: 'var(--semi-color-text-2)' },
      }, eds));

      // 更新 tasks 的依赖关系
      if (params.source && params.target) {
        const newTasks = tasks.map(task => {
          if (task.id === params.target) {
            const deps = task.depends_on || [];
            if (!deps.includes(params.source!)) {
              return { ...task, depends_on: [...deps, params.source!] };
            }
          }
          return task;
        });
        onChange(newTasks);
      }
    },
    [setEdges, tasks, onChange]
  );

  const onEdgesDelete = useCallback(
    (deletedEdges: Edge[]) => {
      // 更新 tasks 的依赖关系
      const newTasks = tasks.map(task => {
        const taskEdges = deletedEdges.filter(e => e.target === task.id);
        if (taskEdges.length > 0) {
          const removedDeps = taskEdges.map(e => e.source);
          const newDeps = (task.depends_on || []).filter(d => !removedDeps.includes(d));
          return { ...task, depends_on: newDeps };
        }
        return task;
      });
      onChange(newTasks);
    },
    [tasks, onChange]
  );

  if (tasks.length === 0) {
    return (
      <div style={{
        height: 200,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--semi-color-text-2)',
        border: '1px dashed var(--semi-color-border)',
        borderRadius: 8,
      }}>
        请先选择任务
      </div>
    );
  }

  return (
    <div style={{ height: 300, border: '1px solid var(--semi-color-border)', borderRadius: 8 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onEdgesDelete={onEdgesDelete}
        fitView
        style={{ background: 'var(--semi-color-bg-0)' }}
      >
        <Background color="var(--semi-color-border)" gap={16} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  );
};

export default DAGEditor;
