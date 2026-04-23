import { notify } from '../utils/notify';
import React, { useState, useEffect, useRef } from 'react';
import { Tabs, Select, Button, Tag, Spin, Progress } from 'antd';
import StrategyCodeEditor from '../components/StrategyCodeEditor';
import EquityCurveChart from '../components/Charts/EquityCurveChart';
import ScriptParamsPanel from '../components/ScriptParamsPanel';
import BatchResultPanel from '../components/BatchResultPanel';
import { useBacktestStore } from '../store';
import { useStrategyScriptStore } from '../store/strategyScriptStore';
import { mlApi, strategyApi, taskMonitorApi } from '../api';
import { useTaskLogs } from '../hooks/useTaskLogs';
import { TaskLogTable } from '../components/TaskLogTable';
import type { MLJobStatus, MLWeights, EquityPoint, BacktestMetrics } from '../types';
import type { ScriptBatchAggregatedResult } from '../api';

const POLL_INTERVAL = 3000;

const StrategyCenter: React.FC = () => {
  // 回测结果状态
  const { result, loading, setLoading, setResult } = useBacktestStore();
  const metrics: BacktestMetrics | undefined = result?.metrics;
  const equity: EquityPoint[] = result?.equity_curve || [];

  // 脚本编辑器状态（store）
  const {
    code: scriptCode,
    runStatus: scriptRunStatus,
    validationResult,
    compileResult,
    runError,
    setCode: setScriptCode,
    setValidationResult,
    setCompileResult,
    setRunId,
    setRunStatus,
    setRunError,
    resetRun,
  } = useStrategyScriptStore();

  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // 新增状态管理
  const [batchResult, setBatchResult] = useState<ScriptBatchAggregatedResult | null>(null);
  const [batchLoading, setBatchLoading] = useState<boolean>(false);
  const [paramGrid, setParamGrid] = useState<Record<string, unknown[]>>({});

  // ML 状态
  const [tsCode, setTsCode] = useState<string>('000001.SZ');
  const [task, setTask] = useState<'full' | 'incremental'>('full');
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<MLJobStatus | null>(null);
  const [weights, setWeights] = useState<MLWeights>({});
  const [polling, setPolling] = useState<boolean>(false);

  const { logs: backtestLogs, loading: backtestLogsLoading, loadLogs: loadBacktestLogs } = useTaskLogs('backtest', 50);

  useEffect(() => {
    loadBacktestLogs();
  }, [loadBacktestLogs]);

  // 清理轮询
  useEffect(() => {
    return () => {
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, []);

  useEffect(() => {
    mlApi.getWeights().then((r) => {
      const weightsData = r.data.weights || {};
      setWeights(weightsData);
    }).catch((error) => {
      console.error('Failed to load weights:', error);
    });
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
            notify.success('模型训练完成');
            mlApi.getWeights().then((wr) => setWeights(wr.data.weights || {}));
          } else {
            notify.error('模型训练失败');
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

  const handleStartTraining = async (): Promise<void> => {
    try {
      const r = await mlApi.train({ ts_code: tsCode, task });
      setJobId(r.data.job_id);
      setPolling(true);
      setStatus({
        job_id: r.data.job_id,
        status: 'queued',
        created_at: new Date().toISOString()
      });
      notify.info(`训练任务 ${r.data.job_id} 已启动`);
    } catch (error) {
      console.error('Failed to start training:', error);
      notify.error('启动训练失败');
    }
  };

  const getStatusColor = (s: string): string => {
    switch (s) {
      case 'done': return 'var(--color-gain)';
      case 'failed': return 'var(--color-loss)';
      case 'running': return 'var(--color-primary)';
      default: return 'var(--text-muted)';
    }
  };

  const getStatusText = (s: string): string => {
    switch (s) {
      case 'done': return '完成';
      case 'failed': return '失败';
      case 'running': return '运行中';
      case 'queued': return '排队中';
      default: return s;
    }
  };

  const getStatusTagColor = (s: string): 'green' | 'red' | 'blue' | 'grey' => {
    switch (s) {
      case 'done': return 'green';
      case 'failed': return 'red';
      case 'running': return 'blue';
      default: return 'grey';
    }
  };

  // ── 脚本模式：校验 ──────────────────────────────────
  const handleValidateScript = async (): Promise<void> => {
    setRunStatus('validating');
    try {
      const response = await strategyApi.validateScript(scriptCode);
      const data = response.data;
      setValidationResult(data);
      if (data.valid) {
        notify.success(`校验通过 (${data.script_hash.slice(0, 8)})`);
      } else {
        notify.error(data.errors?.[0] || '校验失败');
      }
    } catch (error) {
      console.error('Failed to validate script:', error);
      notify.error('校验请求失败');
    } finally {
      setRunStatus('idle');
    }
  };

  // ── 脚本模式：编译 ──────────────────────────────────
  const handleCompileScript = async (): Promise<void> => {
    setRunStatus('compiling');
    try {
      const response = await strategyApi.compileScript(scriptCode);
      const data = response.data;
      setCompileResult(data);
      if (data.status === 'compiled') {
        notify.success('编译成功');
      } else {
        notify.error(data.errors?.[0] || '编译失败');
      }
    } catch (error) {
      console.error('Failed to compile script:', error);
      notify.error('编译请求失败');
    } finally {
      setRunStatus('idle');
    }
  };

  // ── 脚本模式：运行回测 + 异步轮询闭环 ──────────────
  const pollScriptRunResult = (runId: string) => {
    const poll = async () => {
      try {
        const r = await strategyApi.getBacktestRun(runId);
        const data = r.data;

        if (data.status === 'running') {
          pollRef.current = setTimeout(poll, POLL_INTERVAL);
          return;
        }

        if (data.status === 'failed') {
          setRunStatus('failed');
          setRunError(data.error || '回测执行失败');
          setLoading(false);
          notify.error(data.error || '回测执行失败');
          return;
        }

        // 成功：写入 backtestStore，自动展示 metrics + 权益曲线
        setRunStatus('success');
        setLoading(false);
        setResult({
          metrics: data.metrics,
          equity_curve: data.equity_curve || [],
          trades: data.trades_sample || [],
          start_date: '',
          end_date: '',
          initial_capital: 1_000_000,
        });
        notify.success('脚本回测完成');
        loadBacktestLogs();
      } catch (error) {
        console.error('Failed to poll script result:', error);
        setRunStatus('failed');
        setRunError('结果查询失败');
        setLoading(false);
      }
    };
    pollRef.current = setTimeout(poll, POLL_INTERVAL);
  };

  // ── 脚本模式：批量回测 ──────────────────────────────
  const handleBatchRun = async (): Promise<void> => {
    setBatchLoading(true);
    try {
      // 从编译结果中获取 ts_code（如果有），或者使用默认值
      const tsCodes = compileResult && 'ir' in compileResult && compileResult.ir?.data_source?.ts_code
        ? [compileResult.ir.data_source.ts_code]
        : ['000001.SZ'];

      const response = await strategyApi.batchBacktestScript({
        script: scriptCode,
        name: 'batch_backtest',
        param_grid: paramGrid,
        ts_codes: tsCodes,
      });
      const data = response.data;
      notify.info(`批量回测任务已提交，共 ${data.total_runs} 组`);
      // 轮询获取批量回测结果
      pollBatchResult(data.batch_id);
    } catch (error: unknown) {
      console.error('Failed to run batch backtest:', error);
      const msg = (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail || '批量回测启动失败';
      notify.error(msg);
      setBatchLoading(false);
    }
  };

  const pollBatchResult = (batchId: string) => {
    const poll = async () => {
      try {
        const response = await strategyApi.getBatchResult(batchId);
        const data = response.data;
        setBatchResult(data);

        // 检查是否所有回测都完成
        if (data.completed_runs + data.failed_runs < data.total_runs) {
          pollRef.current = setTimeout(poll, POLL_INTERVAL);
        } else {
          setBatchLoading(false);
          notify.success('批量回测完成');
        }
      } catch (error) {
        console.error('Failed to poll batch result:', error);
        setBatchLoading(false);
      }
    };
    pollRef.current = setTimeout(poll, POLL_INTERVAL);
  };

  // ── 处理选中回测结果 ──────────────────────────────
  const handleResultSelect = async (runId: string): Promise<void> => {
    try {
      const response = await strategyApi.getBacktestRun(runId);
      const data = response.data;

      if (data.status === 'completed' && data.metrics) {
        setResult({
          metrics: data.metrics,
          equity_curve: data.equity_curve || [],
          trades: data.trades_sample || [],
          start_date: '',
          end_date: '',
          initial_capital: 1_000_000,
        });
        notify.success('回测结果已加载');
      }
    } catch (error) {
      console.error('Failed to get backtest result:', error);
      notify.error('加载回测结果失败');
    }
  };

  const handleRunScriptBacktest = async (): Promise<void> => {
    resetRun();
    setRunStatus('submitting');
    setLoading(true);
    try {
      const response = await strategyApi.backtestScript({
        script: scriptCode,
        name: 'script_backtest',
      });
      const data = response.data;
      setRunId(data.run_id);
      setRunStatus('running');
      notify.info('脚本回测任务已提交');
      pollScriptRunResult(data.run_id);
    } catch (error: unknown) {
      console.error('Failed to run script backtest:', error);
      const msg = (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail || '脚本回测启动失败';
      setRunStatus('failed');
      setRunError(msg);
      setLoading(false);
      notify.error(msg);
    }
  };

  return (
    <div style={{ padding: '8px', maxWidth: '1600px', margin: '0 auto' }}>
      <Tabs defaultActiveKey="1" items={[
        {
          key: '1',
          label: '策略回测',
          children: (
          <><div style={{
            background: 'var(--bg-card)',
            borderRadius: 12,
            border: '1px solid var(--border-color)',
            padding: 20,
            marginBottom: 16,
            boxShadow: 'var(--shadow-sm)',
            transition: 'all 280ms cubic-bezier(0.4, 0, 0.2, 1)'
          }}>
            <div style={{ marginBottom: 16 }}>
              <h3 style={{
                color: 'var(--color-primary)',
                fontSize: 16,
                fontWeight: 600,
                margin: 0,
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
                策略回测编辑器
              </h3>
            </div>
            <StrategyCodeEditor
              value={scriptCode}
              runStatus={scriptRunStatus}
              validationResult={validationResult}
              compileResult={compileResult}
              runError={runError}
              onChange={setScriptCode}
              onValidate={handleValidateScript}
              onCompile={handleCompileScript}
              onRun={handleRunScriptBacktest}
            />
            {/* 脚本参数面板 */}
            <div style={{ marginTop: 16 }}>
              <ScriptParamsPanel
                compileResult={compileResult}
                onParamsChange={setParamGrid}
                onBatchRun={handleBatchRun}
                disabled={scriptRunStatus !== 'idle'}
              />
            </div>
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
                  { title: '最大回撤', value: ((metrics.max_drawdown ?? 0) * 100).toFixed(2) + '%', color: 'var(--color-loss)', gradient: 'var(--gradient-loss)' },
                  { title: '年化收益', value: ((metrics.annualized_return ?? 0) * 100).toFixed(2) + '%', color: 'var(--color-gain)', gradient: 'var(--gradient-gain)' },
                  { title: '胜率', value: ((metrics.win_rate ?? 0) * 100).toFixed(1) + '%', color: 'var(--color-accent)', gradient: 'var(--gradient-accent)' },
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

          {/* 批量回测结果面板 */}
          <div style={{ marginTop: 16 }}>
            <BatchResultPanel
              result={batchResult}
              loading={batchLoading}
              onResultSelect={handleResultSelect}
            />
          </div>
          </>
          )
        },
        {
          key: '2',
          label: '模型训练',
          children: (
          <><div style={{
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
                  size="middle"
                  options={[
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
                  onChange={(v) => setTask(v as 'full' | 'incremental')}
                  style={{ width: 180 }}
                  size="middle"
                  options={[
                    { label: '完整流水线', value: 'full' },
                    { label: 'AutoML', value: 'automl' },
                    { label: '仅优化', value: 'optimize' },
                  ]}
                />
              </div>
              <Button
                type="primary"
                onClick={handleStartTraining}
                loading={polling}
                size="middle"
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
                  {jobId && <Tag color={getStatusTagColor(status.status)}>{jobId}</Tag>}
                </div>
                {status.status === 'running' && (
                  <Progress
                    percent={50}
                    showInfo={false}
                    strokeColor="var(--color-primary)"
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
                    strokeColor="var(--color-primary)"
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
          </>
          )
        },
        {
          key: 'history',
          label: '回测历史',
          children: <TaskLogTable logs={backtestLogs} loading={backtestLogsLoading} taskIdLabel="策略名称" onFilter={loadBacktestLogs} />,
        },
      ]} />
    </div>
  );
};

export default StrategyCenter;
