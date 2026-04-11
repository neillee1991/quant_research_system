import React, { useEffect, useState } from 'react';
import { Checkbox, Spin, Button, Typography } from 'antd';
import { notify } from '../../utils/notify';
import { dataApi, productionApi, TaskConfig, flowApi } from '../../api';

const { Text } = Typography;

interface TaskSelectorProps {
  selectedTasks: TaskConfig[];
  onChange: (tasks: TaskConfig[]) => void;
}

interface SyncTask {
  task_id: string;
  description: string;
}

interface FactorTask {
  factor_id: string;
  description: string;
}

interface EtlTask {
  task_id: string;
  description: string;
}

const TaskSelector: React.FC<TaskSelectorProps> = ({ selectedTasks, onChange }) => {
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [factorTasks, setFactorTasks] = useState<FactorTask[]>([]);
  const [etlTasks, setEtlTasks] = useState<EtlTask[]>([]);
  const [loading, setLoading] = useState(true);
  const [inferring, setInferring] = useState(false);

  useEffect(() => {
    const fetchTasks = async () => {
      setLoading(true);
      try {
        const [syncRes, factorRes, etlRes] = await Promise.all([
          dataApi.listSyncTasks(),
          productionApi.listFactors(),
          dataApi.listEtlTasks(),
        ]);
        setSyncTasks(syncRes.data.tasks || []);
        setFactorTasks(factorRes.data?.data || []);
        setEtlTasks(etlRes.data.tasks || []);
      } catch (e) {
        console.error('Failed to fetch tasks:', e);
      } finally {
        setLoading(false);
      }
    };
    fetchTasks();
  }, []);

  const selectedSyncIds = selectedTasks.filter(t => t.type === 'sync').map(t => t.id);
  const selectedFactorIds = selectedTasks.filter(t => t.type === 'factor').map(t => t.id);
  const selectedEtlIds = selectedTasks.filter(t => t.type === 'etl').map(t => t.id);

  const handleSyncChange = (values: string[]) => {
    const newSyncTasks: TaskConfig[] = values.map(id => ({ id, type: 'sync', depends_on: [] }));
    const currentFactorTasks = selectedTasks.filter(t => t.type === 'factor');
    const currentEtlTasks = selectedTasks.filter(t => t.type === 'etl');
    onChange([...newSyncTasks, ...currentFactorTasks, ...currentEtlTasks]);
  };

  const handleFactorChange = (values: string[]) => {
    const newFactorTasks: TaskConfig[] = values.map(id => ({ id, type: 'factor', depends_on: [] }));
    const currentSyncTasks = selectedTasks.filter(t => t.type === 'sync');
    const currentEtlTasks = selectedTasks.filter(t => t.type === 'etl');
    onChange([...currentSyncTasks, ...newFactorTasks, ...currentEtlTasks]);
  };

  const handleEtlChange = (values: string[]) => {
    const newEtlTasks: TaskConfig[] = values.map(id => ({ id, type: 'etl', depends_on: [] }));
    const currentSyncTasks = selectedTasks.filter(t => t.type === 'sync');
    const currentFactorTasks = selectedTasks.filter(t => t.type === 'factor');
    onChange([...currentSyncTasks, ...currentFactorTasks, ...newEtlTasks]);
  };

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: 20 }}>
        <Spin />
      </div>
    );
  }

  return (
    <div>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Text strong>选择任务</Text>
        <Button
          type="link"
          size="small"
          loading={inferring}
          onClick={async () => {
            if (selectedTasks.length === 0) {
              notify.warning('请先选择任务');
              return;
            }
            setInferring(true);
            try {
              console.log('Sending tasks to infer:', selectedTasks);
              const res = await flowApi.inferDependencies(selectedTasks);
              console.log('Full API Response:', res);
              console.log('Response data:', res.data);
              console.log('Inferred tasks:', res.data.tasks);
              console.log('Inferred tasks with deps:', res.data.tasks.map((t: any) => ({ id: t.id, type: t.type, depends_on: t.depends_on })));
              onChange(res.data.tasks);
              notify.success('依赖关系识别成功');
            } catch (e) {
              console.error('Failed to infer dependencies:', e);
              notify.error('识别依赖关系失败');
            } finally {
              setInferring(false);
            }
          }}
        >
          自动识别依赖
        </Button>
      </div>

      <div style={{ marginBottom: 16 }}>
        <div style={{ marginBottom: 8, fontWeight: 600, fontSize: 14, color: '#3b82f6' }}>
          同步任务
        </div>
        <Checkbox.Group
          value={selectedSyncIds}
          onChange={handleSyncChange as any}
          style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}
        >
          {syncTasks.map(task => (
            <Checkbox key={task.task_id} value={task.task_id}>
              {task.task_id}
            </Checkbox>
          ))}
        </Checkbox.Group>
        {syncTasks.length === 0 && (
          <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
            暂无同步任务
          </div>
        )}
      </div>

      <div>
        <div style={{ marginBottom: 8, fontWeight: 600, fontSize: 14, color: '#10b981' }}>
          因子任务
        </div>
        <Checkbox.Group
          value={selectedFactorIds}
          onChange={handleFactorChange as any}
          style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}
        >
          {factorTasks.map(task => (
            <Checkbox key={task.factor_id} value={task.factor_id}>
              {task.factor_id}
            </Checkbox>
          ))}
        </Checkbox.Group>
        {factorTasks.length === 0 && (
          <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
            暂无因子任务
          </div>
        )}
      </div>

      <div style={{ marginTop: 16 }}>
        <div style={{ marginBottom: 8, fontWeight: 600, fontSize: 14, color: '#f59e0b' }}>
          ETL 任务
        </div>
        <Checkbox.Group
          value={selectedEtlIds}
          onChange={handleEtlChange as any}
          style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}
        >
          {etlTasks.map(task => (
            <Checkbox key={task.task_id} value={task.task_id}>
              {task.task_id}
            </Checkbox>
          ))}
        </Checkbox.Group>
        {etlTasks.length === 0 && (
          <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
            暂无 ETL 任务
          </div>
        )}
      </div>
    </div>
  );
};

export default TaskSelector;
