import React, { useState, useEffect, useRef } from 'react';
import { Card, Button, Space, Tag, Progress, Spin, Alert, Typography, Divider } from 'antd';
import { SwapOutlined, TrophyOutlined, WarningOutlined, CheckCircleOutlined } from '@ant-design/icons';
import { strategyApi } from '../../api';
import type { ScriptCrossValidateRequest, ScriptCrossValidateResponse } from '../../api';

const { Text } = Typography;
const { Meta } = Card;

interface GraphScriptCrossValidatePanelProps {
  script?: string;
  graph?: any;
  disabled?: boolean;
}

export const GraphScriptCrossValidatePanel: React.FC<GraphScriptCrossValidatePanelProps> = ({
  script,
  graph,
  disabled,
}) => {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ScriptCrossValidateResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [polling, setPolling] = useState(false);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  const startCrossValidation = async () => {
    if (!script || !graph) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const request: ScriptCrossValidateRequest = {
        script,
        graph,
      };

      const response = await strategyApi.crossValidateScript(request);
      const taskId = response.data.cross_validate_id;
      setResult(response.data);
      setPolling(true);
      startPolling(taskId);
    } catch (err: any) {
      console.error('Cross validate failed:', err);
      setError(err.response?.data?.detail || '对账任务启动失败');
    } finally {
      setLoading(false);
    }
  };

  const startPolling = (taskId: string) => {
    const interval = 3000;
    pollRef.current = setInterval(async () => {
      try {
        const response = await strategyApi.getCrossValidateResult(taskId);
        const data = response.data;
        setResult(data);

        if (data.status === 'completed' || data.status === 'failed') {
          setPolling(false);
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } catch (err) {
        console.error('Polling failed:', err);
      }
    }, interval);
  };

  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const getStatusColor = (status: string): string => {
    switch (status) {
      case 'completed':
        return '#52c41a';
      case 'running':
        return '#1890ff';
      case 'failed':
        return '#ff4d4f';
      default:
        return '#d9d9d9';
    }
  };

  const renderMetricsComparison = () => {
    if (!result?.script_metrics || !result?.graph_metrics) return null;

    const sm = result.script_metrics;
    const gm = result.graph_metrics;

    // 核心指标对比
    const metricsToCompare = [
      { name: '夏普比率', key: 'sharpe_ratio', format: (v: number) => v.toFixed(4) },
      { name: '年化收益', key: 'annualized_return', format: (v: number) => `${(v * 100).toFixed(2)}%` },
      { name: '最大回撤', key: 'max_drawdown', format: (v: number) => `${(v * 100).toFixed(2)}%` },
      { name: '胜率', key: 'win_rate', format: (v: number) => `${(v * 100).toFixed(1)}%` },
      { name: '盈亏比', key: 'profit_factor', format: (v: number) => v.toFixed(2) },
      { name: '交易次数', key: 'n_trades', format: (v: number) => v.toString() },
    ];

    return (
      <div style={{ marginTop: 16 }}>
        <h4 style={{ marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
          <SwapOutlined /> 指标对比
        </h4>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 16 }}>
          {metricsToCompare.map((metric) => {
            const sVal = sm[metric.key];
            const gVal = gm[metric.key];
            return (
              <div key={metric.key} style={{ padding: '8px 12px', background: 'var(--bg-surface)', borderRadius: 8 }}>
                <Text type="secondary" style={{ fontSize: 12, display: 'block' }}>
                  {metric.name}
                </Text>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 4 }}>
                  <Text strong style={{ fontSize: 14 }}>
                    {metric.format(sVal)}{' '}
                    {result.diff && metric.key === 'sharpe_ratio' && Math.abs(result.diff.sharpe_delta) > 0.05 && (
                      <Tag color="warning" style={{ fontSize: 10 }}>⚠️</Tag>
                    )}
                  </Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>vs</Text>
                  <Text strong style={{ fontSize: 14 }}>
                    {metric.format(gVal)}
                  </Text>
                </div>
                {result.diff && (
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
                    差异:
                    {metric.key === 'sharpe_ratio' && (
                      <span style={{ marginLeft: 4, color: result.diff.sharpe_delta > 0.01 ? '#faad14' : '#52c41a' }}>
                        {result.diff.sharpe_pct_diff.toFixed(1)}%
                      </span>
                    )}
                    {metric.key === 'annualized_return' && (
                      <span style={{ marginLeft: 4, color: result.diff.return_delta > 0.005 ? '#faad14' : '#52c41a' }}>
                        {result.diff.return_pct_diff.toFixed(1)}%
                      </span>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  const renderMatchSummary = () => {
    if (!result?.diff) return null;

    const { match, sharpe_pct_diff, return_pct_diff, max_drawdown_delta, win_rate_delta } = result.diff;

    return (
      <div style={{ marginTop: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
          {match ? (
            <CheckCircleOutlined style={{ fontSize: 24, color: '#52c41a' }} />
          ) : (
            <WarningOutlined style={{ fontSize: 24, color: '#faad14' }} />
          )}
          <h4 style={{ margin: 0 }}>
            {match ? '两组回测结果高度一致' : '回测结果存在显著差异'}
          </h4>
          {match && <TrophyOutlined style={{ color: '#faad14' }} />}
        </div>

        <div style={{ padding: 12, background: 'var(--bg-surface)', borderRadius: 8 }}>
          <div style={{ marginBottom: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text type="secondary">整体一致性:</Text>
              <Text strong style={{ fontSize: 16, color: match ? '#52c41a' : '#ff4d4f' }}>
                {match ? '匹配' : '不匹配'}
              </Text>
            </div>
          </div>
          <div style={{ marginBottom: 8 }}>
            <Text type="secondary">指标相似度:</Text>
            <Progress
              percent={match ? 100 : 50}
              strokeColor={match ? '#52c41a' : '#faad14'}
              status={match ? 'success' : 'exception'}
              style={{ marginTop: 4 }}
            />
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
            {match ? '所有指标均在 1% 容差范围内' : '某些指标差异超过 1% 容差'}
          </div>
        </div>

        {!match && (
          <Alert
            type="warning"
            message="注意"
            description="指标差异较大，可能是脚本与图模式的实现逻辑不一致"
            style={{ marginTop: 12 }}
          />
        )}
      </div>
    );
  };

  return (
    <div style={{ width: '100%' }}>
      <Card
        title={
          <span style={{ fontSize: 16, fontWeight: 600 }}>
            图与脚本模式对账验证
          </span>
        }
        style={{ width: '100%', borderRadius: 12 }}
        actions={[
          <Button
            type="primary"
            onClick={startCrossValidation}
            loading={loading || polling}
            disabled={disabled || !script || !graph}
            icon={<SwapOutlined />}
            size="large"
          >
            开始对账
          </Button>,
        ]}
      >
        <Meta
          description="同时运行图模式和脚本模式的回测，对比指标差异，验证策略实现一致性"
        />
      </Card>

      {error && (
        <Alert
          type="error"
          message="对账任务失败"
          description={error}
          showIcon
          style={{ marginTop: 16 }}
          action={
            <Button size="small" onClick={startCrossValidation}>
              重试
            </Button>
          }
        />
      )}

      {result && (
        <Card
          style={{ marginTop: 16 }}
          bordered={true}
          size="small"
        >
          <Meta
            title={
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span>对账任务信息</span>
                <Tag color={result.status === 'failed' ? 'error' : result.status === 'running' ? 'processing' : 'success'}>
                  {result.status === 'running' ? '运行中' : result.status === 'completed' ? '已完成' : result.status}
                </Tag>
              </div>
            }
            description={
              <div style={{ fontSize: 12 }}>
                <Text type="secondary">任务ID: </Text>
                <Text code>{result.cross_validate_id}</Text>
                <br />
                <Text type="secondary">开始时间: </Text>
                <Text>{result.started_at}</Text>
                {result.finished_at && (
                  <>
                    <br />
                    <Text type="secondary">结束时间: </Text>
                    <Text>{result.finished_at}</Text>
                  </>
                )}
              </div>
            }
          />
        </Card>
      )}

      {result?.status === 'completed' && (
        <div style={{ marginTop: 16 }}>
          {renderMatchSummary()}
          {renderMetricsComparison()}
        </div>
      )}

      {result?.status === 'failed' && (
        <Alert
          type="error"
          message="对账任务失败"
          description={result.error}
          showIcon
          style={{ marginTop: 16 }}
        />
      )}
    </div>
  );
};

export default GraphScriptCrossValidatePanel;
