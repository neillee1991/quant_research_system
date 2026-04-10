import React, { useState, useEffect, useMemo } from 'react';
import {
  Modal,
  Input,
  Button,
  Select,
  Radio,
  Typography,
  Divider,
  InputNumber,
  Space,
} from 'antd';
import cronstrue from 'cronstrue/i18n';
import { FlowConfig, TaskConfig, flowApi } from '../../api';
import { useMessage } from '../../hooks/useMessage';
import TaskSelector from './TaskSelector';
import DAGEditor from './DAGEditor';

const { Text, Title } = Typography;

interface FlowEditorProps {
  visible: boolean;
  flowName?: string;
  onClose: () => void;
  onSaved: () => void;
}

const defaultFlow: FlowConfig = {
  name: '',
  description: '',
  cron: '0 18 * * 1-5',
  tags: [],
  enabled: true,
  date_offset_days: -1,
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
  const message = useMessage();

  const isEdit = !!flowName;

  useEffect(() => {
    if (visible && flowName) {
      setLoading(true);
      flowApi.get(flowName)
        .then(res => {
          setFlow(res.data);
        })
        .catch(e => {
          message.error('加载 Flow 失败');
          console.error(e);
        })
        .finally(() => setLoading(false));
    } else if (visible) {
      setFlow(defaultFlow);
    }
  }, [visible, flowName]);

  const cronDescription = useMemo(() => {
    try {
      return cronstrue.toString(flow.cron, { locale: 'zh_CN' });
    } catch {
      return '无效的 Cron 表达式';
    }
  }, [flow.cron]);

  const handleSave = async () => {
    if (!flow.name.trim()) {
      message.warning('请输入 Flow 名称');
      return;
    }
    if (!flow.cron.trim()) {
      message.warning('请输入 Cron 表达式');
      return;
    }

    setSaving(true);
    try {
      if (isEdit) {
        await flowApi.update(flowName!, flow);
        message.success('Flow 更新成功');
      } else {
        await flowApi.create(flow);
        message.success('Flow 创建成功');
      }
      onSaved();
      onClose();
    } catch (e: any) {
      message.error(e?.response?.data?.detail || '保存失败');
    } finally {
      setSaving(false);
    }
  };

  const handleTasksChange = (tasks: TaskConfig[]) => {
    setFlow(prev => ({ ...prev, tasks }));
  };

  return (
    <Modal
      title={isEdit ? `编辑 Flow: ${flowName}` : '新建 Flow'}
      open={visible}
      onCancel={onClose}
      width={900}
      style={{ top: 20 }}
      footer={
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <Button onClick={onClose}>取消</Button>
          <Button type="primary" onClick={handleSave} loading={saving}>
            保存
          </Button>
        </div>
      }
    >
      {loading ? (
        <div style={{ textAlign: 'center', padding: 60 }}>加载中...</div>
      ) : (
        <div style={{ padding: '0 4px' }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
            <div>
              <FormItem label="名称">
                <Input
                  placeholder="如 daily-sync"
                  value={flow.name}
                  onChange={e => setFlow(prev => ({ ...prev, name: e.target.value }))}
                  disabled={isEdit}
                />
              </FormItem>

              <FormItem label="描述">
                <Input
                  placeholder="Flow 描述"
                  value={flow.description}
                  onChange={e => setFlow(prev => ({ ...prev, description: e.target.value }))}
                />
              </FormItem>

              <FormItem
                label="Cron 表达式"
                extra={
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {cronDescription}
                  </Text>
                }
              >
                <Input
                  placeholder="0 18 * * 1-5"
                  value={flow.cron}
                  onChange={e => setFlow(prev => ({ ...prev, cron: e.target.value }))}
                />
              </FormItem>

              <FormItem label="标签">
                <Select
                  mode="tags"
                  value={flow.tags}
                  onChange={v => setFlow(prev => ({ ...prev, tags: v as string[] }))}
                  placeholder="输入标签后按回车"
                  style={{ width: '100%' }}
                  open={false}
                />
              </FormItem>

              <FormItem label="状态">
                <Radio.Group
                  value={flow.enabled ? 'enabled' : 'disabled'}
                  onChange={e => setFlow(prev => ({ ...prev, enabled: e.target.value === 'enabled' }))}
                >
                  <Radio value="enabled">启用</Radio>
                  <Radio value="disabled">禁用</Radio>
                </Radio.Group>
              </FormItem>

              <FormItem label="业务日期偏移">
                <Space>
                  <InputNumber
                    min={-365}
                    max={365}
                    value={flow.date_offset_days}
                    onChange={(v) => setFlow(prev => ({ ...prev, date_offset_days: v ?? 0 }))}
                    style={{ width: 120 }}
                  />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    天
                    {flow.date_offset_days === 0 && ' (今天)'}
                    {flow.date_offset_days === -1 && ' (昨天)'}
                    {flow.date_offset_days === 1 && ' (明天)'}
                  </Text>
                </Space>
                <div style={{ marginTop: 4, fontSize: 12, color: 'var(--text-secondary)' }}>
                  调度触发时，业务日期 = 触发日期 + 偏移量
                </div>
              </FormItem>
            </div>

            <div>
              <Title level={5} style={{ marginBottom: 12, marginTop: 0 }}>
                任务选择
              </Title>
              <TaskSelector selectedTasks={flow.tasks} onChange={handleTasksChange} />
            </div>
          </div>

          <Divider style={{ margin: '20px 0' }} />

          <Title level={5} style={{ marginBottom: 12 }}>
            依赖关系 (DAG)
          </Title>
          <Text type="secondary" style={{ display: 'block', marginBottom: 8, fontSize: 12 }}>
            拖拽连线设置任务依赖，箭头指向下游任务
          </Text>
          <DAGEditor tasks={flow.tasks} onChange={handleTasksChange} />
        </div>
      )}
    </Modal>
  );
};

export default FlowEditor;
