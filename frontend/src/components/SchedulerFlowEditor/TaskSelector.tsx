import React, { useEffect, useState } from 'react';
import { Checkbox, CheckboxGroup, Spin, Typography } from '@douyinfe/semi-ui';
import { dataApi, productionApi, TaskConfig } from '../../api';

const { Title } = Typography;

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

const TaskSelector: React.FC<TaskSelectorProps> = ({ selectedTasks, onChange }) => {
  const [syncTasks, setSyncTasks] = useState<SyncTask[]>([]);
  const [factorTasks, setFactorTasks] = useState<FactorTask[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchTasks = async () => {
      setLoading(true);
      try {
        const [syncRes, factorRes] = await Promise.all([
          dataApi.listSyncTasks(),
          productionApi.listFactors(),
        ]);
        setSyncTasks(syncRes.data.tasks || []);
        setFactorTasks(factorRes.data || []);
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

  const handleSyncChange = (values: string[]) => {
    const newSyncTasks: TaskConfig[] = values.map(id => ({ id, type: 'sync' }));
    const factorTasks = selectedTasks.filter(t => t.type === 'factor');
    onChange([...newSyncTasks, ...factorTasks]);
  };

  const handleFactorChange = (values: string[]) => {
    const newFactorTasks: TaskConfig[] = values.map(id => ({ id, type: 'factor' }));
    const syncTasks = selectedTasks.filter(t => t.type === 'sync');
    onChange([...syncTasks, ...newFactorTasks]);
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
      <div style={{ marginBottom: 16 }}>
        <Title heading={6} style={{ marginBottom: 8, color: '#3b82f6' }}>
          同步任务
        </Title>
        <CheckboxGroup
          value={selectedSyncIds}
          onChange={handleSyncChange as any}
          direction="horizontal"
          style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}
        >
          {syncTasks.map(task => (
            <Checkbox key={task.task_id} value={task.task_id}>
              {task.task_id}
            </Checkbox>
          ))}
        </CheckboxGroup>
        {syncTasks.length === 0 && (
          <div style={{ color: 'var(--semi-color-text-2)', fontSize: 12 }}>
            暂无同步任务
          </div>
        )}
      </div>

      <div>
        <Title heading={6} style={{ marginBottom: 8, color: '#10b981' }}>
          因子任务
        </Title>
        <CheckboxGroup
          value={selectedFactorIds}
          onChange={handleFactorChange as any}
          direction="horizontal"
          style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}
        >
          {factorTasks.map(task => (
            <Checkbox key={task.factor_id} value={task.factor_id}>
              {task.factor_id}
            </Checkbox>
          ))}
        </CheckboxGroup>
        {factorTasks.length === 0 && (
          <div style={{ color: 'var(--semi-color-text-2)', fontSize: 12 }}>
            暂无因子任务
          </div>
        )}
      </div>
    </div>
  );
};

export default TaskSelector;
