import React, { useState, useEffect } from 'react';
import { Tabs, TabPane, Select, Button, Tag, Spin, Progress, Toast } from '@douyinfe/semi-ui';
import FlowEditor from '../components/FlowEditor';
import EquityCurveChart from '../components/Charts/EquityCurveChart';
import { useBacktestStore } from '../store';
import { mlApi } from '../api';

const StrategyCenter: React.FC = () => {
  // 回测状态
  const { result, loading } = useBacktestStore();
  const metrics = result?.metrics;
  const equity = result?.equity_curve || [];

  // ML 状态
  const [tsCode, setTsCode] = useState('000001.SZ');
  const [task, setTask] = useState('full');
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<any>(null);
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [polling, setPolling] = useState(false);

  useEffect(() => {
    mlApi.getWeights().then((r) => setWeights(r.data.weights || {}));
  }, []);

  useEffect(() => {
    if (!jobId || !polling) return;

    let isCancelled = false;

    const pollStatus = async () => {
      if (isCancelled) return;

      try {
        const r = await mlApi.getStatus(jobId);
        if (isCancelled) return;

        setStatus(r.data);

        if (r.data.status === 'done' || r.data.status === 'failed') {
          setPolling(false);
          if (r.data.status === 'done') {
            Toast.success('模型训练完成');
            mlApi.getWeights().then((wr) => setWeights(wr.data.weights || {}));
          } else {
            Toast.error('模型训练失败');
          }
        } else {
          setTimeout(pollStatus, 5000);
        }
      } catch (error) {
        if (!isCancelled) {
          console.error('Failed to poll status:', error);
          setTimeout(pollStatus, 10000);
        }
      }
    };

    pollStatus();

    return () => {
      isCancelled = true;
    };
  }, [jobId, polling]);

  const handleStartTraining = async () => {
    try {
      const r = await mlApi.train({ ts_code: tsCode, task });
      setJobId(r.data.job_id);
      setPolling(true);
      setStatus({ status: 'queued' });
      Toast.info(`训练任务 ${r.data.job_id} 已启动`);
    } catch {
      Toast.error('启动训练失败');
    }
  };

  const getStatusColor = (s: string) => {
    switch (s) {
      case 'done': return 'var(--color-gain)';
      case 'failed': return 'var(--color-loss)';
      case 'running': return 'var(--color-primary)';
      default: return 'var(--text-muted)';
    }
  };

  const getStatusText = (s: string) => {
    switch (s) {
      case 'done': return '完成';
      case 'failed': return '失败';
      case 'running': return '运行中';
      case 'queued': return '排队中';
      default: return s;
    }
  };

  const getStatusTagColor = (s: string) => {
    switch (s) {
      case 'done': return 'green' as const;
      case 'failed': return 'red' as const;
      case 'running': return 'blue' as const;
      default: return 'grey' as const;
    }
  };

  return (
    <div style={{ padding: 16, maxWidth: 1600, margin: '0 auto' }}>
      <div style={{ marginBottom: 16 }}>
        <h1 style={{ color: 'var(--text-primary)', fontSize: 24, fontWeight: 700, margin: 0 }}>
          策略中心
        </h1>
        <p style={{ color: 'var(--text-secondary)', margin: '4px 0 0', fontSize: 12 }}>
          可视化策略构建、回测与模型训练
        </p>
      </div>

      <Tabs defaultActiveKey="1">
        <TabPane tab="策略回测" itemKey="1">
          <div style={{
            background: 'var(--bg-card)', borderRadius: 8,
            border: '1px solid var(--border-color)', padding: 16, marginBottom: 12,
          }}>
            <h3 style={{ color: 'var(--color-primary)', fontSize: 16, fontWeight: 600, margin: '0 0 12px' }}>
              可视化策略编辑器
            </h3>
            <FlowEditor />
          </div>

          {loading && (
            <div style={{ textAlign: 'center', padding: 48 }}>
              <Spin size="large" />
              <div style={{ color: 'var(--color-primary)', marginTop: 12, fontSize: 14 }}>
                回测运行中...
              </div>
            </div>
          )}

          {metrics && (
            <>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, marginTop: 12 }}>
                {[
                  { title: '夏普比率', value: metrics.sharpe_ratio, precision: 4, color: 'var(--color-primary)' },
                  { title: '最大回撤', value: (metrics.max_drawdown * 100).toFixed(2) + '%', color: 'var(--color-loss)' },
                  { title: '年化收益', value: (metrics.annualized_return * 100).toFixed(2) + '%', color: 'var(--color-gain)' },
                  { title: '胜率', value: (metrics.win_rate * 100).toFixed(1) + '%', color: 'var(--color-primary)' },
                  { title: '盈亏比', value: metrics.profit_factor, precision: 2, color: 'var(--color-primary)' },
                  { title: '交易次数', value: metrics.n_trades, color: 'var(--text-secondary)' },
                ].map((m) => (
                  <div key={m.title} className="stat-card" style={{ flex: '1 1 140px', minWidth: 140 }}>
                    <div className="stat-value" style={{ color: m.color }}>
                      {typeof m.value === 'number' && m.precision ? m.value.toFixed(m.precision) : m.value}
                    </div>
                    <div className="stat-label">{m.title}</div>
                  </div>
                ))}
              </div>
              {equity.length > 0 && (
                <div style={{
                  marginTop: 12, padding: 16, background: 'var(--bg-card)',
                  borderRadius: 8, border: '1px solid var(--border-color)',
                }}>
                  <h3 style={{ color: 'var(--color-primary)', fontSize: 16, fontWeight: 600, marginBottom: 12 }}>
                    权益曲线
                  </h3>
                  <EquityCurveChart data={equity} />
                </div>
              )}
            </>
          )}
        </TabPane>

        <TabPane tab="模型训练" itemKey="2">
          <div style={{
            background: 'var(--bg-card)', borderRadius: 8,
            border: '1px solid var(--border-color)', padding: 16, marginBottom: 12,
          }}>
            <h3 style={{ color: 'var(--color-primary)', fontSize: 16, fontWeight: 600, margin: '0 0 12px' }}>
              模型训练
            </h3>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, marginBottom: 12, alignItems: 'flex-end' }}>
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: 11, marginBottom: 4 }}>股票代码</div>
                <Select value={tsCode} onChange={(v) => setTsCode(v as string)} style={{ width: 160 }} size="small"
                  optionList={[
                    { label: '000001.SZ', value: '000001.SZ' },
                    { label: '600000.SH', value: '600000.SH' },
                  ]}
                />
              </div>
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: 11, marginBottom: 4 }}>任务类型</div>
                <Select value={task} onChange={(v) => setTask(v as string)} style={{ width: 180 }} size="small"
                  optionList={[
                    { label: '完整流水线', value: 'full' },
                    { label: 'AutoML', value: 'automl' },
                    { label: '仅优化', value: 'optimize' },
                  ]}
                />
              </div>
              <Button theme="solid" type="primary" onClick={handleStartTraining} loading={polling} size="small">
                开始训练
              </Button>
            </div>

            {status && (
              <div style={{
                padding: 12, marginBottom: 12,
                background: 'var(--bg-card)', borderRadius: 6,
                border: '1px solid var(--border-color)',
              }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  {status.status === 'running' && <Spin size="small" />}
                  <span style={{ color: getStatusColor(status.status), fontWeight: 600 }}>
                    {getStatusText(status.status)}
                  </span>
                  {jobId && <Tag color={getStatusTagColor(status.status)} size="small">{jobId}</Tag>}
                </div>
                {status.status === 'running' && (
                  <Progress percent={50} showInfo={false}
                    stroke="var(--color-primary)"
                    style={{ marginTop: 8 }}
                  />
                )}
              </div>
            )}
          </div>

          <div style={{
            background: 'var(--bg-card)', borderRadius: 8,
            border: '1px solid var(--border-color)', padding: 16,
          }}>
            <h3 style={{ color: 'var(--color-primary)', fontSize: 16, fontWeight: 600, margin: '0 0 12px' }}>
              因子权重
            </h3>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
              {Object.entries(weights).map(([k, v]) => (
                <div key={k} className="stat-card" style={{ flex: '1 1 140px', minWidth: 140 }}>
                  <div className="stat-label" style={{ marginBottom: 8 }}>{k}</div>
                  <div className="stat-value">{Number(v).toFixed(4)}</div>
                  <Progress percent={Math.abs(Number(v)) * 100} showInfo={false}
                    stroke="var(--color-primary)"
                    style={{ marginTop: 8 }}
                  />
                </div>
              ))}
            </div>
            {Object.keys(weights).length === 0 && (
              <div style={{ textAlign: 'center', padding: 32, color: 'var(--text-muted)', fontSize: 13 }}>
                暂无权重数据，请先启动训练任务生成因子权重。
              </div>
            )}
          </div>
        </TabPane>
      </Tabs>
    </div>
  );
};

export default StrategyCenter;
