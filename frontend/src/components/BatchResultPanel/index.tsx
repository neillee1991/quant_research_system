import React, { useState, useMemo } from 'react';
import { Table, Tag, Tooltip, Typography, Progress, Statistic, Row, Col, Empty, Spin } from 'antd';
import { TrophyOutlined, LineChartOutlined, RiseOutlined, FallOutlined } from '@ant-design/icons';
import type { ScriptBatchResult, ScriptBatchAggregatedResult } from '../../api';
import type { BacktestMetrics } from '../../types';

const { Text, Paragraph } = Typography;

interface BatchResultPanelProps {
  result: ScriptBatchAggregatedResult | null;
  loading?: boolean;
  onResultSelect?: (runId: string) => void;
}

interface ResultRow extends ScriptBatchResult {
  paramsText: string;
  sharpe: number;
  return: number;
  maxDrawdown: number;
  winRate: number;
  trades: number;
}

export const BatchResultPanel: React.FC<BatchResultPanelProps> = ({
  result,
  loading,
  onResultSelect,
}) => {
  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  // 表格列配置
  const columns = [
    {
      title: '参数组合',
      dataIndex: 'paramsText',
      key: 'params',
      width: 200,
      render: (text: string, record: ResultRow) => (
        <Tooltip title={text} placement="topLeft">
          <Text strong style={{ fontSize: 12 }} ellipsis={{ tooltip: text }}>
            {text}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: '夏普比率',
      dataIndex: 'sharpe',
      key: 'sharpe',
      width: 120,
      render: (value: number, record: ResultRow) => {
        const color = value >= 1.0 ? '#52c41a' : value >= 0.5 ? '#faad14' : '#ff4d4f';
        return (
          <span style={{ color }}>
            {value.toFixed(4)}
            {record.run_id === result?.best_run?.run_id && (
              <TrophyOutlined style={{ marginLeft: 4, color: '#faad14' }} />
            )}
          </span>
        );
      },
      sorter: (a: ResultRow, b: ResultRow) => (a.sharpe || 0) - (b.sharpe || 0),
    },
    {
      title: '年化收益',
      dataIndex: 'return',
      key: 'return',
      width: 120,
      render: (value: number) => {
        const color = value > 0 ? '#52c41a' : '#ff4d4f';
        const sign = value > 0 ? '+' : '';
        return <Tag color={color}>{sign}{(value * 100).toFixed(2)}%</Tag>;
      },
    },
    {
      title: '最大回撤',
      dataIndex: 'maxDrawdown',
      key: 'maxDrawdown',
      width: 120,
      render: (value: number) => {
        const color = value < 0.1 ? '#52c41a' : value < 0.2 ? '#faad14' : '#ff4d4f';
        return <Tag color={color}>{(value * 100).toFixed(2)}%</Tag>;
      },
    },
    {
      title: '胜率',
      dataIndex: 'winRate',
      key: 'winRate',
      width: 120,
      render: (value: number) => (
        <Progress
          percent={Math.round(value * 100)}
          size="small"
          strokeColor={value >= 0.5 ? '#52c41a' : value >= 0.4 ? '#faad14' : '#ff4d4f'}
        />
      ),
    },
    {
      title: '交易次数',
      dataIndex: 'trades',
      key: 'trades',
      width: 100,
      render: (value: number) => <Text type="secondary">{value}</Text>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 80,
      render: (status: string) => {
        let color: 'success' | 'processing' | 'error' | 'default' = 'default';
        if (status === 'completed') color = 'success';
        else if (status === 'running') color = 'processing';
        else if (status === 'failed') color = 'error';
        return <Tag color={color}>{status}</Tag>;
      },
    },
  ];

  // 格式化数据
  const dataSource: ResultRow[] = useMemo(() => {
    if (!result) return [];

    return result.results.map((r) => {
      // 参数格式化
      const paramsText = Object.entries(r.params || {}).map(([k, v]) => {
        const valueText = typeof v === 'number' ? v.toFixed(1) : String(v);
        return `${k}=${valueText}`;
      }).join(', ');

      // 指标提取
      const metrics = r.metrics || {};

      return {
        ...r,
        key: r.run_id,
        paramsText,
        sharpe: Number(metrics.sharpe_ratio || 0),
        return: Number(metrics.annualized_return || 0),
        maxDrawdown: Number(metrics.max_drawdown || 0),
        winRate: Number(metrics.win_rate || 0),
        trades: Number(metrics.n_trades || 0),
      };
    });
  }, [result]);

  // 最佳结果信息（顶部统计卡片）
  const bestResultCard = useMemo(() => {
    if (!result?.best_run) return null;

    const bestMetrics = result.best_run.metrics;
    const bestParams = result.best_run.params;

    const paramsText = Object.entries(bestParams || {}).map(([k, v]) => `${k}=${v}`).join(', ');

    return (
      <div style={{
        background: 'var(--bg-card)',
        borderRadius: 12,
        border: '1px solid var(--border-color)',
        padding: '16px 20px',
        marginBottom: 16,
        boxShadow: 'var(--shadow-sm)',
        position: 'relative',
        overflow: 'hidden',
      }}>
        <div style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          height: 3,
          background: 'var(--gradient-accent)',
          opacity: 0.6,
        }} />
        <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 12 }}>
          <TrophyOutlined style={{ fontSize: 24, color: '#faad14' }} />
          <div style={{ flex: 1 }}>
            <h3 style={{ fontSize: 16, margin: 0, fontWeight: 600 }}>
              最佳参数组合（夏普最高）
            </h3>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {paramsText}
            </Text>
          </div>
        </div>
        <Row gutter={16} style={{ marginTop: 12 }}>
          <Col span={6}>
            <Statistic
              title="夏普比率"
              value={bestMetrics?.sharpe_ratio}
              precision={4}
              valueStyle={{ color: '#52c41a' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="年化收益"
              value={(bestMetrics?.annualized_return || 0) * 100}
              precision={2}
              suffix="%"
              valueStyle={{ color: '#f5222d' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="最大回撤"
              value={(bestMetrics?.max_drawdown || 0) * 100}
              precision={2}
              suffix="%"
              valueStyle={{ color: '#faad14' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="胜率"
              value={(bestMetrics?.win_rate || 0) * 100}
              precision={1}
              suffix="%"
              valueStyle={{ color: '#1890ff' }}
            />
          </Col>
        </Row>
      </div>
    );
  }, [result?.best_run]);

  // 聚合统计信息
  const summaryCard = useMemo(() => {
    if (!result) return null;

    const { summary } = result;

    return (
      <div style={{
        background: 'var(--bg-card)',
        borderRadius: 12,
        border: '1px solid var(--border-color)',
        padding: 16,
        marginBottom: 16,
      }}>
        <Row gutter={16}>
          <Col span={4}>
            <Statistic
              title="总组数"
              value={summary.total_runs}
              prefix={<LineChartOutlined />}
            />
          </Col>
          <Col span={4}>
            <Statistic
              title="成功"
              value={summary.completed_runs}
              prefix={<RiseOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Col>
          <Col span={4}>
            <Statistic
              title="失败"
              value={summary.failed_runs}
              prefix={<FallOutlined />}
              valueStyle={{ color: '#ff4d4f' }}
            />
          </Col>
          <Col span={4}>
            <Statistic
              title="运行中"
              value={summary.running_runs}
              valueStyle={{ color: '#1890ff' }}
            />
          </Col>
          <Col span={4}>
            <Statistic
              title="平均夏普"
              value={summary.avg_sharpe_ratio || 0}
              precision={4}
            />
          </Col>
          <Col span={4}>
            <Statistic
              title="最佳收益"
              value={(summary.max_total_return || 0) * 100}
              precision={2}
              suffix="%"
              valueStyle={{ color: '#f5222d' }}
            />
          </Col>
        </Row>
      </div>
    );
  }, [result]);

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px 0' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16, color: 'var(--text-secondary)' }}>
          正在查询回测结果...
        </div>
      </div>
    );
  }

  if (!result) {
    return null;
  }

  return (
    <div style={{ marginTop: 16 }}>
      {bestResultCard}
      {summaryCard}

      <div style={{
        background: 'var(--bg-card)',
        borderRadius: 12,
        border: '1px solid var(--border-color)',
        overflow: 'hidden',
        boxShadow: 'var(--shadow-sm)',
      }}>
        <Table
          dataSource={dataSource}
          columns={columns}
          rowKey="run_id"
          loading={loading}
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            pageSizeOptions: ['10', '20', '50'],
          }}
          onRow={(record) => ({
            onClick: () => {
              if (onResultSelect && record.status === 'completed') {
                onResultSelect(record.run_id);
              }
            },
            style: { cursor: onResultSelect && record.status === 'completed' ? 'pointer' : 'default' },
          })}
          expandable={{
            expandedRowRender: (record) => (
              <div style={{ padding: '12px 16px', background: 'var(--bg-surface)' }}>
                <h4 style={{ marginBottom: 8 }}>详细指标</h4>
                <Row gutter={16}>
                  <Col span={6}>
                    <Text strong>盈亏比: </Text>
                    <Text type="secondary">{Number(record.metrics?.profit_factor || 0).toFixed(2)}</Text>
                  </Col>
                  <Col span={6}>
                    <Text strong>最大连续盈利: </Text>
                    <Text type="secondary">{Number(record.metrics?.max_runup || 0).toFixed(2)}</Text>
                  </Col>
                  <Col span={6}>
                    <Text strong>最大连续亏损: </Text>
                    <Text type="secondary">{Number(record.metrics?.max_drawdown || 0).toFixed(2)}</Text>
                  </Col>
                  <Col span={6}>
                    <Text strong>年化波动率: </Text>
                    <Text type="secondary">{(Number(record.metrics?.volatility || 0) * 100).toFixed(2)}%</Text>
                  </Col>
                </Row>
              </div>
            ),
            expandedRowKeys: expandedRow ? [expandedRow] : [],
            onExpand: (expanded, record) => {
              setExpandedRow(expanded ? record.run_id : null);
            },
          }}
        />
      </div>

      {result.results.length === 0 && (
        <Empty
          description={
            <Paragraph style={{ fontSize: 14, color: 'var(--text-secondary)' }}>
              该批次暂无回测结果
            </Paragraph>
          }
        />
      )}
    </div>
  );
};

export default BatchResultPanel;
