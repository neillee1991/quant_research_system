import React, { useCallback, useEffect, useMemo } from 'react';
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
  Position,
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
  etl: {
    background: 'linear-gradient(135deg, #f59e0b 0%, #d97706 100%)',
    color: 'white',
    border: 'none',
    borderRadius: 8,
    padding: '8px 16px',
    fontSize: 12,
    fontWeight: 500,
  },
  flow: {
    background: 'linear-gradient(135deg, #8b5cf6 0%, #6d28d9 100%)',
    color: 'white',
    border: 'none',
    borderRadius: 8,
    padding: '8px 16px',
    fontSize: 12,
    fontWeight: 500,
  },
} as Record<string, any>;

// 计算层级的辅助函数
const calculateLevels = (taskList: TaskConfig[]) => {
  const levels = new Map<string, number>();
  const taskMap = new Map<string, TaskConfig>();
  taskList.forEach(t => taskMap.set(t.id, t));

  const calculateLevel = (taskId: string, visited = new Set<string>()): number => {
    if (visited.has(taskId)) return 0;
    if (levels.has(taskId)) return levels.get(taskId)!;

    visited.add(taskId);
    const task = taskMap.get(taskId);
    if (!task || !task.depends_on || task.depends_on.length === 0) {
      levels.set(taskId, 0);
      return 0;
    }

    const depLevels = task.depends_on.map(dep => {
      if (taskMap.has(dep)) {
        return calculateLevel(dep, new Set(visited));
      }
      return -1;
    }).filter(l => l >= 0);

    const level = depLevels.length > 0 ? Math.max(...depLevels) + 1 : 0;
    levels.set(taskId, level);
    return level;
  };

  taskList.forEach(t => calculateLevel(t.id));
  return levels;
};

// 计算两层之间的边交叉数
const countCrossings = (
  upperLevel: string[],
  lowerLevel: string[],
  upperToLower: Map<string, string[]>
): number => {
  let crossings = 0;
  const lowerPos = new Map(lowerLevel.map((id, i) => [id, i]));

  // 收集所有边（upperIdx, lowerIdx）
  const edges: Array<[number, number]> = [];
  upperLevel.forEach((upperId, upperIdx) => {
    const lowerIds = upperToLower.get(upperId) || [];
    lowerIds.forEach(lowerId => {
      const lowerIdx = lowerPos.get(lowerId);
      if (lowerIdx !== undefined) {
        edges.push([upperIdx, lowerIdx]);
      }
    });
  });

  // 计算交叉
  for (let i = 0; i < edges.length; i++) {
    for (let j = i + 1; j < edges.length; j++) {
      const [u1, l1] = edges[i];
      const [u2, l2] = edges[j];
      if ((u1 < u2 && l1 > l2) || (u1 > u2 && l1 < l2)) {
        crossings++;
      }
    }
  }

  return crossings;
};

//  barycenter 方法计算权重
const getBarycenter = (
  nodeId: string,
  adjacentLevel: string[],
  adjacencyMap: Map<string, string[]>
): number => {
  const adjacentIds = adjacencyMap.get(nodeId) || [];
  const positions = adjacentIds
    .map(id => adjacentLevel.indexOf(id))
    .filter(idx => idx >= 0);

  if (positions.length === 0) {
    return adjacentLevel.length / 2;
  }
  return positions.reduce((a, b) => a + b, 0) / positions.length;
};

// 中位数方法计算权重
const getMedian = (
  nodeId: string,
  adjacentLevel: string[],
  adjacencyMap: Map<string, string[]>
): number => {
  const adjacentIds = adjacencyMap.get(nodeId) || [];
  const positions = adjacentIds
    .map(id => adjacentLevel.indexOf(id))
    .filter(idx => idx >= 0)
    .sort((a, b) => a - b);

  if (positions.length === 0) {
    return adjacentLevel.length / 2;
  }
  if (positions.length % 2 === 1) {
    return positions[Math.floor(positions.length / 2)];
  }
  const mid = positions.length / 2;
  return (positions[mid - 1] + positions[mid]) / 2;
};

// 一层排序
const sortLevel = (
  level: string[],
  adjacentLevel: string[],
  adjacencyMap: Map<string, string[]>,
  useMedian = true
): string[] => {
  const withWeight = level.map(id => ({
    id,
    weight: useMedian
      ? getMedian(id, adjacentLevel, adjacencyMap)
      : getBarycenter(id, adjacentLevel, adjacencyMap)
  }));
  withWeight.sort((a, b) => a.weight - b.weight);
  return withWeight.map(x => x.id);
};

// 尝试交换相邻节点减少交叉
const minimizeCrossingsBySwapping = (
  level: string[],
  upperLevel: string[],
  upperToLower: Map<string, string[]>
): string[] => {
  let best = [...level];
  let bestCrossings = countCrossings(upperLevel, best, upperToLower);
  let improved = true;
  let iterations = 0;
  const maxIterations = 20;

  while (improved && iterations < maxIterations) {
    improved = false;
    iterations++;

    for (let i = 0; i < best.length - 1; i++) {
      const candidate = [...best];
      [candidate[i], candidate[i + 1]] = [candidate[i + 1], candidate[i]];
      const candidateCrossings = countCrossings(upperLevel, candidate, upperToLower);

      if (candidateCrossings < bestCrossings) {
        bestCrossings = candidateCrossings;
        best = candidate;
        improved = true;
      }
    }
  }

  return best;
};

// 多层扫描写入减少交叉
const reduceCrossings = (
  levelGroups: Map<number, string[]>,
  upperToLower: Map<string, string[]>,
  lowerToUpper: Map<string, string[]>
): Map<number, string[]> => {
  const sorted = new Map(levelGroups);
  const maxLevel = Math.max(...sorted.keys());

  // 多次扫描写入
  for (let pass = 0; pass < 3; pass++) {
    // 从左到右
    for (let level = 1; level <= maxLevel; level++) {
      const prevLevel = sorted.get(level - 1)!;
      const currLevel = sorted.get(level)!;
      const useMedian = pass % 2 === 0;
      let sortedLevel = sortLevel(currLevel, prevLevel, lowerToUpper, useMedian);
      sortedLevel = minimizeCrossingsBySwapping(sortedLevel, prevLevel, upperToLower);
      sorted.set(level, sortedLevel);
    }

    // 从右到左
    for (let level = maxLevel - 1; level >= 0; level--) {
      const nextLevel = sorted.get(level + 1)!;
      const currLevel = sorted.get(level)!;
      const useMedian = pass % 2 === 0;
      let sortedLevel = sortLevel(currLevel, nextLevel, upperToLower, useMedian);
      sortedLevel = minimizeCrossingsBySwapping(sortedLevel, nextLevel, lowerToUpper);
      sorted.set(level, sortedLevel);
    }
  }

  return sorted;
};

// 坐标分配：让父子节点对齐
const assignCoordinates = (
  sortedLevelGroups: Map<number, string[]>,
  lowerToUpper: Map<string, string[]>,
  nodeHeight: number,
  nodeSpacing: number,
  canvasHeight: number
): Map<string, number> => {
  const yPositions = new Map<string, number>();
  const maxLevel = Math.max(...sortedLevelGroups.keys());

  // 第一遍：初始分配
  sortedLevelGroups.forEach((levelTasks, level) => {
    const levelHeight = levelTasks.length * nodeHeight + (levelTasks.length - 1) * nodeSpacing;
    const startY = Math.max(0, (canvasHeight - levelHeight) / 2);

    levelTasks.forEach((taskId, indexInLevel) => {
      const y = startY + indexInLevel * (nodeHeight + nodeSpacing);
      yPositions.set(taskId, y);
    });
  });

  // 第二遍：从右到左，让父节点与子节点对齐
  for (let level = maxLevel - 1; level >= 0; level--) {
    const levelTasks = sortedLevelGroups.get(level)!;
    const nextLevelTasks = sortedLevelGroups.get(level + 1)!;

    // 计算每个节点的对齐位置
    const alignmentY = new Map<string, number>();
    levelTasks.forEach(taskId => {
      const children = lowerToUpper.get(taskId) || [];
      const childPositions = children
        .map(id => yPositions.get(id))
        .filter((y): y is number => y !== undefined);

      if (childPositions.length > 0) {
        // 与子节点的平均位置对齐
        const avgY = childPositions.reduce((a, b) => a + b, 0) / childPositions.length;
        alignmentY.set(taskId, avgY);
      }
    });

    // 按对齐位置重新排列
    const withAlignment = levelTasks.map(id => ({
      id,
      align: alignmentY.get(id) ?? yPositions.get(id)!
    }));
    withAlignment.sort((a, b) => a.align - b.align);

    // 重新分配位置
    const levelHeight = levelTasks.length * nodeHeight + (levelTasks.length - 1) * nodeSpacing;
    const startY = Math.max(0, (canvasHeight - levelHeight) / 2);

    withAlignment.forEach(({ id }, indexInLevel) => {
      const y = startY + indexInLevel * (nodeHeight + nodeSpacing);
      yPositions.set(id, y);
    });
  }

  // 第三遍：从左到右，微调让子节点与父节点对齐
  for (let level = 1; level <= maxLevel; level++) {
    const levelTasks = sortedLevelGroups.get(level)!;

    levelTasks.forEach(taskId => {
      const parents = lowerToUpper.get(taskId)!; // 这个节点的依赖
      const parentPositions = parents
        .map(id => yPositions.get(id))
        .filter((y): y is number => y !== undefined);

      if (parentPositions.length > 0) {
        const avgParentY = parentPositions.reduce((a, b) => a + b, 0) / parentPositions.length;
        const currentY = yPositions.get(taskId)!;
        // 向父节点位置微调，最多移动 nodeSpacing/2
        const maxMove = nodeSpacing / 2;
        const diff = avgParentY - currentY;
        const move = Math.max(-maxMove, Math.min(maxMove, diff));
        yPositions.set(taskId, currentY + move);
      }
    });
  }

  return yPositions;
};

const DAGEditor: React.FC<DAGEditorProps> = ({ tasks, onChange }) => {
  // 计算布局数据
  const layoutData = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const taskMap = new Map<string, TaskConfig>();

    // 创建任务映射
    tasks.forEach(t => taskMap.set(t.id, t));

    // 计算层级
    const levels = calculateLevels(tasks);
    const maxLevel = Math.max(...Array.from(levels.values()), 0);

    // 按层级分组
    const levelGroups = new Map<number, string[]>();
    for (let i = 0; i <= maxLevel; i++) {
      levelGroups.set(i, []);
    }
    tasks.forEach(task => {
      const level = levels.get(task.id) || 0;
      levelGroups.get(level)!.push(task.id);
    });

    // 第一层初始排序：按出度 + ID
    const firstLevel = levelGroups.get(0)!;
    const upperToLower = new Map<string, string[]>(); // 节点 -> 依赖它的节点
    const lowerToUpper = new Map<string, string[]>(); // 节点 -> 它依赖的节点

    // 初始化邻接表
    tasks.forEach(task => {
      lowerToUpper.set(task.id, task.depends_on || []);
      upperToLower.set(task.id, []);
    });
    tasks.forEach(task => {
      (task.depends_on || []).forEach(dep => {
        const existing = upperToLower.get(dep) || [];
        upperToLower.set(dep, [...existing, task.id]);
      });
    });

    // 第一层排序：出度降序，ID 升序
    const firstLevelWithWeight = firstLevel.map(id => ({
      id,
      outDegree: (upperToLower.get(id) || []).length
    }));
    firstLevelWithWeight.sort((a, b) => {
      if (b.outDegree !== a.outDegree) {
        return b.outDegree - a.outDegree;
      }
      return a.id.localeCompare(b.id);
    });
    levelGroups.set(0, firstLevelWithWeight.map(x => x.id));

    // 多层扫描写入减少交叉
    const sortedLevelGroups = reduceCrossings(levelGroups, upperToLower, lowerToUpper);

    // 计算每层最多节点数
    const maxTasksPerLevel = Math.max(...Array.from(sortedLevelGroups.values()).map(g => g.length), 1);

    // 布局参数
    const nodeHeight = 40;
    const levelSpacing = 280;
    const nodeSpacing = 85;
    const minCanvasHeight = 450;
    const maxCanvasHeight = 850;
    const canvasHeight = Math.max(minCanvasHeight, Math.min(maxCanvasHeight, maxTasksPerLevel * 125 + 160));

    // 坐标分配：让父子节点对齐
    const yPositions = assignCoordinates(
      sortedLevelGroups,
      lowerToUpper,
      nodeHeight,
      nodeSpacing,
      canvasHeight
    );

    // 创建节点
    sortedLevelGroups.forEach((levelTasks, level) => {
      levelTasks.forEach((taskId) => {
        const task = taskMap.get(taskId)!;
        const nodeWidth = Math.max(125, task.id.length * 10 + 55);
        nodes.push({
          id: task.id,
          data: { label: task.id },
          position: {
            x: level * levelSpacing + 60,
            y: yPositions.get(task.id)!,
          },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
          style: {
            ...nodeStyles[task.type] || nodeStyles.sync,
            width: nodeWidth,
          },
        });
      });
    });

    // 创建边 - 使用平滑曲线
    tasks.forEach(task => {
      if (task.depends_on && task.depends_on.length > 0) {
        task.depends_on.forEach(dep => {
          if (taskMap.has(dep)) {
            edges.push({
              id: `${dep}-${task.id}`,
              source: dep,
              target: task.id,
              markerEnd: { type: MarkerType.ArrowClosed },
              style: { stroke: 'var(--text-secondary)' },
              type: 'smoothstep',
            });
          }
        });
      }
    });

    return { initialNodes: nodes, initialEdges: edges, maxTasksPerLevel, canvasHeight };
  }, [tasks]);

  const { initialNodes, initialEdges, maxTasksPerLevel, canvasHeight } = layoutData;

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  const onConnect = useCallback(
    (params: Connection) => {
      // 添加新边
      setEdges(eds => addEdge({
        ...params,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { stroke: 'var(--text-secondary)' },
        type: 'smoothstep',
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
        color: 'var(--text-secondary)',
        border: '1px dashed var(--border-color)',
        borderRadius: 8,
      }}>
        请先选择任务
      </div>
    );
  }

  return (
    <div style={{ height: canvasHeight, border: '1px solid var(--border-color)', borderRadius: 8 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onEdgesDelete={onEdgesDelete}
        fitView
        fitViewOptions={{ padding: 0.12 }}
        style={{ background: 'var(--bg-surface)' }}
      >
        <Background color="var(--border-color)" gap={16} size={1} />
        <Controls />
      </ReactFlow>
    </div>
  );
};

export default DAGEditor;
