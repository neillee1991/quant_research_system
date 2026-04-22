import React, { useState, useCallback, useMemo } from 'react';
import { Button, Input, Space, Tooltip, Tag, Select, Collapse, Alert, Typography } from 'antd';
import { PlusOutlined, MinusCircleOutlined, SettingOutlined } from '@ant-design/icons';
import type { StrategyScriptCompileResponse } from '../../types';

const { Text } = Typography;
const { Option } = Select;

interface ParamValue {
  id: string;
  value: string;
}

interface ParamConfig {
  name: string;
  values: ParamValue[];
  type: 'number' | 'string' | 'boolean';
}

interface ScriptParamsPanelProps {
  compileResult?: StrategyScriptCompileResponse | null;
  onParamsChange?: (paramGrid: Record<string, unknown[]>) => void;
  onBatchRun?: () => void;
  disabled?: boolean;
}

// 默认预定义参数（从常见策略配置中提取）
const DEFAULT_PARAMS: string[] = ['window', 'threshold', 'stop_loss', 'take_profit', 'lookback', 'period'];

export const ScriptParamsPanel: React.FC<ScriptParamsPanelProps> = ({
  compileResult,
  onParamsChange,
  onBatchRun,
  disabled,
}) => {
  const [params, setParams] = useState<ParamConfig[]>([]);
  const [tsCodes, setTsCodes] = useState<string[]>([]);
  const [showAddParam, setShowAddParam] = useState(false);
  const [newParamName, setNewParamName] = useState('');
  const [newParamType, setNewParamType] = useState<'number' | 'string' | 'boolean'>('number');

  // 从编译结果推断参数
  React.useEffect(() => {
    const inferredParams: Record<string, ParamConfig> = {};
    const pipeline = compileResult && 'ir' in compileResult ? compileResult.ir?.pipeline || [] : [];

    // 从 pipeline 中提取参数名
    for (const step of pipeline) {
      if (step.params) {
        for (const key of Object.keys(step.params)) {
          if (!inferredParams[key]) {
            const paramValue = step.params[key];
            let type: 'number' | 'string' | 'boolean' = 'number';
            if (typeof paramValue === 'boolean') type = 'boolean';
            else if (typeof paramValue === 'string') type = 'string';

            inferredParams[key] = {
              name: key,
              type,
              values: [{ id: `new-${Date.now()}-0`, value: String(paramValue) }],
            };
          }
        }
      }
    }

    // 如果没有推断出参数，使用默认参数
    if (Object.keys(inferredParams).length === 0) {
      for (const defaultParam of DEFAULT_PARAMS.slice(0, 3)) {
        inferredParams[defaultParam] = {
          name: defaultParam,
          type: 'number',
          values: [{ id: `new-${Date.now()}-${defaultParam}-0`, value: defaultParam === 'window' ? '20' : '0.5' }],
        };
      }
    }

    setParams(Object.values(inferredParams));
  }, [compileResult]);

  // 从编译结果提取 ts_code
  React.useEffect(() => {
    const tsCode = compileResult && 'ir' in compileResult ? compileResult.ir?.data_source?.ts_code : undefined;
    if (tsCode) {
      setTsCodes([tsCode]);
    }
  }, [compileResult]);

  // 生成参数网格
  const paramGrid = useMemo(() => {
    const grid: Record<string, unknown[]> = {};
    for (const param of params) {
      grid[param.name] = param.values.map((v) => {
        if (param.type === 'number') return Number(v.value);
        if (param.type === 'boolean') return v.value.toLowerCase() === 'true';
        return v.value;
      });
    }
    return grid;
  }, [params]);

  // 计算组合总数
  const totalCombinations = useMemo(() => {
    let total = 1;
    for (const param of params) {
      total *= param.values.length;
    }
    return total * Math.max(1, tsCodes.length);
  }, [params, tsCodes]);

  // 添加参数值
  const addParamValue = useCallback((paramName: string) => {
    setParams((prev) =>
      prev.map((p) => {
        if (p.name !== paramName) return p;
        const defaultValue = p.type === 'boolean' ? 'true' : p.type === 'number' ? '0' : '';
        return {
          ...p,
          values: [...p.values, { id: `new-${Date.now()}`, value: defaultValue }],
        };
      })
    );
  }, []);

  // 移除参数值
  const removeParamValue = useCallback((paramName: string, valueId: string) => {
    setParams((prev) =>
      prev.map((p) => {
        if (p.name !== paramName) return p;
        const newValues = p.values.filter((v) => v.id !== valueId);
        if (newValues.length === 0) {
          // 移除空参数
          return null;
        }
        return { ...p, values: newValues };
      }).filter(Boolean) as ParamConfig[]
    );
  }, []);

  // 更新参数值
  const updateParamValue = useCallback((paramName: string, valueId: string, newValue: string) => {
    setParams((prev) =>
      prev.map((p) => {
        if (p.name !== paramName) return p;
        return {
          ...p,
          values: p.values.map((v) => (v.id === valueId ? { ...v, value: newValue } : v)),
        };
      })
    );
  }, []);

  // 添加新参数
  const addNewParam = useCallback(() => {
    if (!newParamName.trim()) return;

    setParams((prev) => [
      ...prev,
      {
        name: newParamName.trim(),
        type: newParamType,
        values: [{ id: `new-${Date.now()}`, value: newParamType === 'boolean' ? 'true' : newParamType === 'number' ? '0' : '' }],
      },
    ]);
    setNewParamName('');
    setShowAddParam(false);
  }, [newParamName, newParamType]);

  // 添加股票代码
  const addTsCode = useCallback(() => {
    setTsCodes((prev) => [...prev, '']);
  }, []);

  // 更新股票代码
  const updateTsCode = useCallback((index: number, value: string) => {
    setTsCodes((prev) => {
      const newCodes = [...prev];
      newCodes[index] = value;
      return newCodes;
    });
  }, []);

  // 移除股票代码
  const removeTsCode = useCallback((index: number) => {
    setTsCodes((prev) => prev.filter((_, i) => i !== index));
  }, []);

  // 参数变化时通知父组件
  React.useEffect(() => {
    onParamsChange?.(paramGrid);
  }, [paramGrid, onParamsChange]);

  // 折叠面板内容
  const collapseItems = [
    {
      key: 'params',
      label: (
        <span>
          <SettingOutlined /> 参数配置 <Tag color="blue">{totalCombinations} 组</Tag>
        </span>
      ),
      children: (
        <div style={{ padding: '8px 0' }}>
          {/* 参数配置 */}
          {params.length === 0 ? (
            <Alert type="info" message="点击下方按钮添加参数" showIcon />
          ) : (
            <div style={{ marginBottom: 16 }}>
              {params.map((param) => (
                <div key={param.name} style={{ marginBottom: 12 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                    <Text strong style={{ minWidth: 100 }}>{param.name}</Text>
                    <Tag color="grey">{param.type}</Tag>
                    {param.values.length > 1 && (
                      <Tooltip title="移除最后一个值">
                        <MinusCircleOutlined
                          style={{ color: '#ff4d4f', cursor: 'pointer' }}
                          onClick={() => removeParamValue(param.name, param.values[param.values.length - 1].id)}
                        />
                      </Tooltip>
                    )}
                    <Tooltip title="添加更多值">
                      <PlusOutlined
                        style={{ color: '#52c41a', cursor: 'pointer' }}
                        onClick={() => addParamValue(param.name)}
                      />
                    </Tooltip>
                  </div>
                  <Space wrap>
                    {param.values.map((v, idx) => (
                      <Input
                        key={v.id}
                        size="small"
                        value={v.value}
                        onChange={(e) => updateParamValue(param.name, v.id, e.target.value)}
                        style={{ width: 120 }}
                        placeholder={`值 ${idx + 1}`}
                      />
                    ))}
                  </Space>
                </div>
              ))}
            </div>
          )}

          {/* 股票代码配置 */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
              <Text strong style={{ minWidth: 100 }}>股票代码</Text>
              <Tooltip title="添加更多股票代码">
                <PlusOutlined
                  style={{ color: '#52c41a', cursor: 'pointer' }}
                  onClick={addTsCode}
                />
              </Tooltip>
            </div>
            <Space wrap>
              {tsCodes.map((code, idx) => (
                <Input
                  key={`ts-${idx}`}
                  size="small"
                  value={code}
                  onChange={(e) => updateTsCode(idx, e.target.value)}
                  style={{ width: 140 }}
                  suffix={
                    tsCodes.length > 1 && (
                      <MinusCircleOutlined
                        style={{ color: '#ff4d4f', cursor: 'pointer', fontSize: 12 }}
                        onClick={() => removeTsCode(idx)}
                      />
                    )
                  }
                />
              ))}
            </Space>
          </div>

          {/* 添加新参数 */}
          {showAddParam ? (
            <div style={{ padding: 12, background: '#fafafa', borderRadius: 8, marginBottom: 12 }}>
              <Space wrap>
                <Input
                  size="small"
                  placeholder="参数名"
                  value={newParamName}
                  onChange={(e) => setNewParamName(e.target.value)}
                  style={{ width: 120 }}
                />
                <Select
                  size="small"
                  value={newParamType}
                  onChange={(v) => setNewParamType(v as 'number' | 'string' | 'boolean')}
                  style={{ width: 100 }}
                >
                  <Option value="number">数字</Option>
                  <Option value="string">字符串</Option>
                  <Option value="boolean">布尔</Option>
                </Select>
                <Button size="small" type="primary" onClick={addNewParam}>添加</Button>
                <Button size="small" onClick={() => setShowAddParam(false)}>取消</Button>
              </Space>
            </div>
          ) : (
            <Button type="dashed" size="small" icon={<PlusOutlined />} onClick={() => setShowAddParam(true)}>添加自定义参数</Button>
          )}

          {/* 批量运行按钮 */}
          <div style={{ marginTop: 16, paddingTop: 16, borderTop: '1px solid var(--border-color)' }}>
            <Space>
              <Button
                type="primary"
                onClick={onBatchRun}
                disabled={disabled || totalCombinations === 0}
              >
                参数扫描回测
              </Button>
              <Text type="secondary" style={{ fontSize: 12 }}>
                共 {totalCombinations} 组组合
              </Text>
            </Space>
          </div>
        </div>
      ),
    },
  ];

  return (
    <Collapse
      items={collapseItems}
      ghost
      defaultActiveKey={['params']}
    />
  );
};

export default ScriptParamsPanel;
