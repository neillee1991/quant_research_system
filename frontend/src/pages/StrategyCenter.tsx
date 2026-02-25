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
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{ marginBottom: '16px' }}>
        <h1 style={{
          color: 'var(--color-primary)',
          fontSize: '24px',
          fontWeight: 700,
          margin: 0,
          letterSpacing: '1px'
        }}>
          策略
        </h1>
        <p style={{
          color: 'var(--text-secondary)',
          margin: '4px 0 0 0',
          fontSize: '12px'
        }}>
          可视化策略构建、回测与模型训练
        </p>
      </div>

      <Tabs defaultActiveKey="1" style={{
        '--semi-color-primary': 'var(--color-primary)',
        '--semi-color-primary-hover': 'var(--color-primary-hover)'
      } as React.CSSProperties}>
        <TabPane tab="策略回测" itemKey="1">
          <div style={{
            background: 'var(--bg-card)',
            borderRadius: 12,
            border: '1px solid var(--border-color)',
            padding: 20,
            marginBottom: 16,
            boxShadow: 'var(--shadow-sm)',
            transition: 'all 280ms cubic-bezier(0.4, 0, 0.2, 1)'
          }}>
            <h3 style={{
              color: 'var(--color-primary)',
              fontSize: 16,
              fontWeight: 600,
              margin: '0 0 16px',
              display: 'flex',
              alignItems: 'center',
              gap: 8
            }}>
              <span style={{
                display: 'inline-block',
                width: 4,
                height: 16,
                background: 'var(--gradient-primary)',
                borderRadius: 2
              }}></span>
              可视化策略编辑器
            </h3>
            <FlowEditor />
          </div>

          {loading && (
            <div style={{
              textAlign: 'center',
              padding: 64,
              background: 'var(--bg-card)',
              borderRadius: 12,
              border: '1px solid var(--border-color)'
            }}>
              <Spin size="large" />
              <div style={{
                color: 'var(--color-primary)',
                marginTop: 16,
                fontSize: 14,
                fontWeight: 500
              }}>
                回测运行中...
              </div>
            </div>
          )}

          {metrics && (
            <>
              <div style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: 16,
                marginTop: 16
              }}>
                {[
                  { title: '夏普比率', value: metrics.sharpe_ratio, precision: 4, color: 'var(--color-primary)', gradient: 'var(--gradient-primary)' },
                  { title: '最大回撤', value: (metrics.max_drawdown * 100).toFixed(2) + '%', color: 'var(--color-loss)', gradient: 'var(--gradient-loss)' },
                  { title: '年化收益', value: (metrics.annualized_return * 100).toFixed(2) + '%', color: 'var(--color-gain)', gradient: 'var(--gradient-gain)' },
                  { title: '胜率', value: (metrics.win_rate * 100).toFixed(1) + '%', color: 'var(--color-accent)', gradient: 'var(--gradient-accent)' },
                  { title: '盈亏比', value: metrics.profit_factor, precision: 2, color: 'var(--color-primary)', gradient: 'var(--gradient-primary)' },
                  { title: '交易次数', value: metrics.n_trades, color: 'var(--text-secondary)', gradient: 'var(--gradient-primary)' },
                ].map((m, idx) => (
                  <div
                    key={m.title}
                    className="stat-card animate-fade-in-up"
                    style={{
                      flex: '1 1 160px',
                      minWidth: 160,
                      animationDelay: `${idx * 0.1}s`,
                      opacity: 0
                    }}
                  >
                    <div className="stat-value" style={{
                      color: m.color,
                      background: m.gradient,
                      WebkitBackgroundClip: 'text',
                      WebkitTextFillColor: 'transparent',
                      backgroundClip: 'text'
                    }}>
                      {typeof m.value === 'number' && m.precision ? m.value.toFixed(m.precision) : m.value}
                    </div>
                    <div className="stat-label">{m.title}</div>
                  </div>
                ))}
              </div>
              {equity.length > 0 && (
                <div style={{
                  marginTop: 16,
                  padding: 24,
                  background: 'var(--bg-card)',
                  borderRadius: 12,
                  border: '1px solid var(--border-color)',
                  boxShadow: 'var(--shadow-sm)',
                  position: 'relative',
                  overflow: 'hidden'
                }}>
                  <div style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    right: 0,
                    height: 3,
                    background: 'var(--gradient-accent)',
                    opacity: 0.6
                  }}></div>
                  <h3 style={{
                    color: 'var(--color-primary)',
                    fontSize: 16,
                    fontWeight: 600,
                    marginBottom: 20,
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8
                  }}>
                    <span style={{
                      display: 'inline-block',
                      width: 4,
                      height: 16,
                      background: 'var(--gradient-accent)',
                      borderRadius: 2
                    }}></span>
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
            background: 'var(--bg-card)',
            borderRadius: 12,
            border: '1px solid var(--border-color)',
            padding: 24,
            marginBottom: 16,
            boxShadow: 'var(--shadow-sm)'
          }}>
            <h3 style={{
              color: 'var(--color-primary)',
              fontSize: 16,
              fontWeight: 600,
              margin: '0 0 20px',
              display: 'flex',
              alignItems: 'center',
              gap: 8
            }}>
              <span style={{
                display: 'inline-block',
                width: 4,
                height: 16,
                background: 'var(--gradient-primary)',
                borderRadius: 2
              }}></span>
              模型训练
            </h3>
            <div style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: 16,
              marginBottom: 16,
              alignItems: 'flex-end'
            }}>
              <div>
                <div style={{
                  color: 'var(--text-secondary)',
                  fontSize: 11,
                  marginBottom: 6,
                  fontWeight: 500,
                  textTransform: 'uppercase',
                  letterSpacing: 'var(--letter-spacing-wide)'
                }}>股票代码</div>
                <Select
                  value={tsCode}
                  onChange={(v) => setTsCode(v as string)}
                  style={{ width: 160 }}
                  size="small"
                  optionList={[
                    { label: '000001.SZ', value: '000001.SZ' },
                    { label: '600000.SH', value: '600000.SH' },
                  ]}
                />
              </div>
              <div>
                <div style={{
                  color: 'var(--text-secondary)',
                  fontSize: 11,
                  marginBottom: 6,
                  fontWeight: 500,
                  textTransform: 'uppercase',
                  letterSpacing: 'var(--letter-spacing-wide)'
                }}>任务类型</div>
                <Select
                  value={task}
                  onChange={(v) => setTask(v as string)}
                  style={{ width: 180 }}
                  size="small"
                  optionList={[
                    { label: '完整流水线', value: 'full' },
                    { label: 'AutoML', value: 'automl' },
                    { label: '仅优化', value: 'optimize' },
                  ]}
                />
              </div>
              <Button
                theme="solid"
                type="primary"
                onClick={handleStartTraining}
                loading={polling}
                size="small"
                style={{
                  background: 'var(--gradient-primary)',
                  border: 'none',
                  fontWeight: 500
                }}
              >
                开始训练
              </Button>
            </div>

            {status && (
              <div style={{
                padding: 16,
                marginBottom: 16,
                background: 'var(--bg-surface)',
                borderRadius: 8,
                border: '1px solid var(--border-color)',
                position: 'relative',
                overflow: 'hidden'
              }}>
                {status.status === 'running' && (
                  <div style={{
                    position: 'absolute',
                    top: 0,
                    left: 0,
                    right: 0,
                    height: 2,
                    background: 'var(--gradient-primary)',
                    animation: 'shimmer 2s infinite linear',
                    backgroundSize: '200% 100%'
                  }}></div>
                )}
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  {status.status === 'running' && <Spin size="small" />}
                  <span style={{
                    color: getStatusColor(status.status),
                    fontWeight: 600,
                    fontSize: 14
                  }}>
                    {getStatusText(status.status)}
                  </span>
                  {jobId && <Tag color={getStatusTagColor(status.status)} size="small">{jobId}</Tag>}
                </div>
                {status.status === 'running' && (
                  <Progress
                    percent={50}
                    showInfo={false}
                    stroke="var(--color-primary)"
                    style={{ marginTop: 12 }}
                  />
                )}
              </div>
            )}
          </div>

          <div style={{
            background: 'var(--bg-card)',
            borderRadius: 12,
            border: '1px solid var(--border-color)',
            padding: 24,
            boxShadow: 'var(--shadow-sm)',
            position: 'relative',
            overflow: 'hidden'
          }}>
            <div style={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              height: 3,
              background: 'var(--gradient-accent)',
              opacity: 0.6
            }}></div>
            <h3 style={{
              color: 'var(--color-primary)',
              fontSize: 16,
              fontWeight: 600,
              margin: '0 0 20px',
              display: 'flex',
              alignItems: 'center',
              gap: 8
            }}>
              <span style={{
                display: 'inline-block',
                width: 4,
                height: 16,
                background: 'var(--gradient-accent)',
                borderRadius: 2
              }}></span>
              因子权重
            </h3>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
              {Object.entries(weights).map(([k, v], idx) => (
                <div
                  key={k}
                  className="stat-card animate-fade-in-up"
                  style={{
                    flex: '1 1 160px',
                    minWidth: 160,
                    animationDelay: `${idx * 0.08}s`,
                    opacity: 0
                  }}
                >
                  <div className="stat-label" style={{ marginBottom: 12 }}>{k}</div>
                  <div className="stat-value" style={{
                    background: 'var(--gradient-primary)',
                    WebkitBackgroundClip: 'text',
                    WebkitTextFillColor: 'transparent',
                    backgroundClip: 'text'
                  }}>
                    {Number(v).toFixed(4)}
                  </div>
                  <Progress
                    percent={Math.abs(Number(v)) * 100}
                    showInfo={false}
                    stroke="var(--color-primary)"
                    style={{ marginTop: 12 }}
                  />
                </div>
              ))}
            </div>
            {Object.keys(weights).length === 0 && (
              <div style={{
                textAlign: 'center',
                padding: 48,
                color: 'var(--text-muted)',
                fontSize: 13,
                fontWeight: 500
              }}>
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
