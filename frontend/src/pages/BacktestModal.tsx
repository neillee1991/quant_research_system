import React, { useState, useEffect } from 'react';
import { Modal, Form, Input, Select, DatePicker, Button } from 'antd';
import type { FormInstance } from 'antd/es/form';
import dayjs from 'dayjs';

const { Option } = Select;

interface BacktestModalProps {
  visible: boolean;
  onCancel: () => void;
  onOk: (values: any) => void;
}

interface Benchmark {
  code: string;
  name: string;
}

interface Stock {
  ts_code: string;
  name: string;
}

interface FormValues {
  [key: string]: any;
  start_date?: string;
  end_date?: string;
}

export const BacktestModal: React.FC<BacktestModalProps> = ({
  visible,
  onCancel,
  onOk,
}) => {
  const [form] = Form.useForm<FormValues>();
  const [factors, setFactors] = useState<string[]>([]);
  const [benchmarks, setBenchmarks] = useState<Benchmark[]>([]);
  const [stocks, setStocks] = useState<Stock[]>([]);

  useEffect(() => {
    if (visible) {
      loadFactors();
      loadBenchmarks();
      loadStocks();
    }
  }, [visible]);

  const loadFactors = async () => {
    try {
      const response = await fetch('/api/v1/backtest/factors');
      if (response.ok) {
        const data = await response.json();
        setFactors(data);
      }
    } catch (error) {
      console.error('加载因子失败:', error);
    }
  };

  const loadBenchmarks = async () => {
    try {
      const response = await fetch('/api/v1/backtest/benchmarks');
      if (response.ok) {
        const data = await response.json();
        setBenchmarks(data);
      }
    } catch (error) {
      console.error('加载基准指数失败:', error);
    }
  };

  const loadStocks = async () => {
    try {
      const response = await fetch('/api/v1/data/stocks');
      if (response.ok) {
        const data = await response.json();
        // 后端返回格式: { stocks: [...], total: ... }
        if (data && Array.isArray(data.stocks)) {
          setStocks(data.stocks);
        } else {
          console.error('股票列表数据格式错误:', data);
          setStocks([]);
        }
      } else {
        // 响应失败时设置空数组
        setStocks([]);
      }
    } catch (error) {
      console.error('加载股票列表失败:', error);
      setStocks([]);
    }
  };

  const handleOk = () => {
    form.validateFields()
      .then(values => {
        // 现在 Form.Item 已经自动处理成 YYYYMMDD 格式
        onOk(values);
      })
      .catch(info => {
        console.log('Validate Failed:', info);
      });
  };

  return (
    <Modal
      title="新建回测"
      open={visible}
      onCancel={onCancel}
      onOk={handleOk}
      width={700}
    >
      <Form form={form} layout="vertical" initialValues={{
        initial_capital: 1000000,
        fees: 0.0003,
        slippage: 0.001,
        engine_mode: 'vectorbt',
        start_date: '20100101',
        end_date: '20240101',
        strategy_code: `
from typing import Dict, Any, List
from backend.engine.backtest.core.base_strategy import BaseStrategy, TradingSignal

class MyStrategy(BaseStrategy):
    """我的策略"""

    def generate_signals(self, prices, factors):
        # VectorBT 模式：生成交易信号
        return []

    def on_bar(self, context, bar_data):
        # RQAlpha 模式：处理每个 tick/bar
        pass
        `.trim()
      }}>
        <Form.Item
          name="engine_mode"
          label="引擎模式"
          rules={[{ required: true, message: '请选择引擎模式' }]}
        >
          <Select>
            <Option value="vectorbt">VectorBT (向量化计算)</Option>
            <Option value="rqalpha">RQAlpha (事件驱动)</Option>
          </Select>
        </Form.Item>

        <Form.Item
          name="initial_capital"
          label="初始资金"
          rules={[{ required: true, message: '请输入初始资金' }]}
        >
          <Input
            placeholder="请输入初始资金"
            addonAfter="元"
            type="number"
          />
        </Form.Item>

        <Form.Item
          name="start_date"
          label="开始日期"
          rules={[{ required: true, message: '请选择开始日期' }]}
          getValueProps={(value) => ({ value: value ? dayjs(value, 'YYYYMMDD') : undefined })}
          getValueFromEvent={(date) => (date ? date.format('YYYYMMDD') : undefined)}
        >
          <DatePicker style={{ width: '100%' }} format="YYYYMMDD" />
        </Form.Item>

        <Form.Item
          name="end_date"
          label="结束日期"
          rules={[{ required: true, message: '请选择结束日期' }]}
          getValueProps={(value) => ({ value: value ? dayjs(value, 'YYYYMMDD') : undefined })}
          getValueFromEvent={(date) => (date ? date.format('YYYYMMDD') : undefined)}
        >
          <DatePicker style={{ width: '100%' }} format="YYYYMMDD" />
        </Form.Item>

        <Form.Item
          name="factors"
          label="使用因子"
          rules={[{ required: true, message: '请选择使用的因子' }]}
        >
          <Select
            mode="multiple"
            placeholder="请选择因子"
            maxTagCount="responsive"
          >
            {factors.map(factor => (
              <Option key={factor} value={factor}>
                {factor}
              </Option>
            ))}
          </Select>
        </Form.Item>

        <Form.Item
          name="benchmark"
          label="基准指数"
          rules={[{ required: true, message: '请选择基准指数' }]}
        >
          <Select placeholder="请选择基准指数">
            {benchmarks.map(benchmark => (
              <Option key={benchmark.code} value={benchmark.code}>
                {benchmark.name}
              </Option>
            ))}
          </Select>
        </Form.Item>

        <Form.Item
          name="stocks"
          label="股票池（最多100只）"
          rules={[
            { required: true, message: '请选择股票' },
            { max: 100, message: '最多选择100只股票' }
          ]}
        >
          <Select
            mode="multiple"
            placeholder="请选择股票"
            maxTagCount="responsive"
            showSearch
            filterOption={(input, option) => {
              const label = option?.label as string;
              return label?.toLowerCase().includes(input.toLowerCase()) ?? false;
            }}
          >
            {stocks.map(stock => (
              <Option key={stock.ts_code} value={stock.ts_code} label={`${stock.ts_code} - ${stock.name}`}>
                {stock.ts_code} - {stock.name}
              </Option>
            ))}
          </Select>
        </Form.Item>

        <Form.Item
          name="strategy_code"
          label="策略代码"
          rules={[{ required: true, message: '请输入策略代码' }]}
        >
          <Input.TextArea
            rows={12}
            placeholder="请输入策略代码"
            style={{ fontFamily: 'monospace' }}
          />
        </Form.Item>

        <Form.Item
          name="fees"
          label="手续费"
          rules={[{ required: true, message: '请输入手续费' }]}
        >
          <Input
            placeholder="请输入手续费"
            addonAfter="%"
            type="number"
            step="0.0001"
          />
        </Form.Item>

        <Form.Item
          name="slippage"
          label="滑点"
          rules={[{ required: true, message: '请输入滑点' }]}
        >
          <Input
            placeholder="请输入滑点"
            addonAfter="%"
            type="number"
            step="0.0001"
          />
        </Form.Item>
      </Form>
    </Modal>
  );
};
