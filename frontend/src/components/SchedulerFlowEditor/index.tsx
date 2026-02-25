import React, { useState, useEffect, useMemo } from 'react';
import {
  SideSheet,
  Input,
  Button,
  TagInput,
  Radio,
  RadioGroup,
  Typography,
  Toast,
  Divider,
} from '@douyinfe/semi-ui';
import cronstrue from 'cronstrue/i18n';
import { FlowConfig, TaskConfig, flowApi } from '../../api';
import TaskSelector from './TaskSelector';
import DAGEditor from './DAGEditor';

const { Text, Title } = Typography;

interface FlowEditorProps {
  visible: boolean;
  flowName?: string; // 编辑时传入，新建时为空
  onClose: () => void;
  onSaved: () => void;
}

const defaultFlow: FlowConfig = {
  name: '',
  description: '',
  cron: '0 18 * * 1-5',
  tags: [],
  enabled: true,
  tasks: [],
};

const FormItem: React.FC<{ label: string; children: React.ReactNode; extra?: React.ReactNode }> = ({
  label,
  children,
  extra,
}) => (
  <div style={{ marginBottom: 16 }}>
    <div style={{ marginBottom: 4, fontWeight: 500, fontSize: 14 }}>{label}</div>
    {children}
    {extra && <div style={{ marginTop: 4 }}>{extra}</div>}
  </div>
);

const FlowEditor: React.FC<FlowEditorProps> = ({ visible, flowName, onClose, onSaved }) => {
  const [flow, setFlow] = useState<FlowConfig>(defaultFlow);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);

  const isEdit = !!flowName;

  // 加载 Flow 配置
  useEffect(() => {
    if (visible && flowName) {
      setLoading(true);
      flowApi.get(flowName)
        .then(res => {
          setFlow(res.data);
        })
        .catch(e => {
          Toast.error({ content: '加载 Flow 失败' });
          console.error(e);
        })
        .finally(() => setLoading(false));
    } else if (visible) {
      setFlow(defaultFlow);
    }
  }, [visible, flowName]);

  // Cron 表达式可读描述
  const cronDescription = useMemo(() => {
    try {
      return cronstrue.toString(flow.cron, { locale: 'zh_CN' });
    } catch {
      return '无效的 Cron 表达式';
    }
  }, [flow.cron]);

  const handleSave = async () => {
    if (!flow.name.trim()) {
      Toast.warning({ content: '请输入 Flow 名称' });
      return;
    }
    if (!flow.cron.trim()) {
      Toast.warning({ content: '请输入 Cron 表达式' });
      return;
    }

    setSaving(true);
    try {
      if (isEdit) {
        await flowApi.update(flowName!, flow);
        Toast.success({ content: 'Flow 更新成功' });
      } else {
        await flowApi.create(flow);
        Toast.success({ content: 'Flow 创建成功' });
      }
      onSaved();
      onClose();
    } catch (e: any) {
      Toast.error({ content: e?.response?.data?.detail || '保存失败' });
    } finally {
      setSaving(false);
    }
  };

  const handleTasksChange = (tasks: TaskConfig[]) => {
    setFlow(prev => ({ ...prev, tasks }));
  };

  return (
    <SideSheet
      title={isEdit ? `编辑 Flow: ${flowName}` : '新建 Flow'}
      visible={visible}
      onCancel={onClose}
      width={600}
      footer={
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <Button onClick={onClose}>取消</Button>
          <Button theme="solid" onClick={handleSave} loading={saving}>
            保存
          </Button>
        </div>
      }
    >
      {loading ? (
        <div style={{ textAlign: 'center', padding: 40 }}>加载中...</div>
      ) : (
        <div style={{ padding: '0 4px' }}>
          <FormItem label="名称">
            <Input
              placeholder="如 daily-sync"
              value={flow.name}
              onChange={v => setFlow(prev => ({ ...prev, name: v }))}
              disabled={isEdit}
            />
          </FormItem>

          <FormItem label="描述">
            <Input
              placeholder="Flow 描述"
              value={flow.description}
              onChange={v => setFlow(prev => ({ ...prev, description: v }))}
            />
          </FormItem>

          <FormItem
            label="Cron 表达式"
            extra={
              <Text type="tertiary" size="small">
                {cronDescription}
              </Text>
            }
          >
            <Input
              placeholder="0 18 * * 1-5"
              value={flow.cron}
              onChange={v => setFlow(prev => ({ ...prev, cron: v }))}
            />
          </FormItem>

          <FormItem label="标签">
            <TagInput
              value={flow.tags}
              onChange={v => setFlow(prev => ({ ...prev, tags: v as string[] }))}
              placeholder="输入标签后按回车"
            />
          </FormItem>

          <FormItem label="状态">
            <RadioGroup
              value={flow.enabled ? 'enabled' : 'disabled'}
              onChange={e => setFlow(prev => ({ ...prev, enabled: e.target.value === 'enabled' }))}
              direction="horizontal"
            >
              <Radio value="enabled">启用</Radio>
              <Radio value="disabled">禁用</Radio>
            </RadioGroup>
          </FormItem>

          <Divider margin={16} />

          <Title heading={6} style={{ marginBottom: 12 }}>
            任务选择
          </Title>
          <TaskSelector selectedTasks={flow.tasks} onChange={handleTasksChange} />

          <Divider margin={16} />

          <Title heading={6} style={{ marginBottom: 12 }}>
            依赖关系 (DAG)
          </Title>
          <Text type="tertiary" size="small" style={{ display: 'block', marginBottom: 8 }}>
            拖拽连线设置任务依赖，箭头指向下游任务
          </Text>
          <DAGEditor tasks={flow.tasks} onChange={handleTasksChange} />
        </div>
      )}
    </SideSheet>
  );
};

export default FlowEditor;
