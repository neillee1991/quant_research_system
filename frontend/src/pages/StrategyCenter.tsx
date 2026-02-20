import React, { useState, useEffect } from 'react';
import {
  Button, Select, Space, Card, Row, Col, message, Spin, Progress, Tabs, Tag,
} from 'antd';
import {
  FundOutlined, RobotOutlined, ThunderboltOutlined, CheckCircleOutlined,
  CloseCircleOutlined, RiseOutlined, FallOutlined, TrophyOutlined,
} from '@ant-design/icons';
import FlowEditor from '../components/FlowEditor';
import EquityCurveChart from '../components/Charts/EquityCurveChart';
import { useBacktestStore } from '../store';
import { mlApi } from '../api';

const { TabPane } = Tabs;

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
    const timer = setInterval(async () => {
      const r = await mlApi.getStatus(jobId);
      setStatus(r.data);
      if (r.data.status === 'done' || r.data.status === 'failed') {
        setPolling(false);
        clearInterval(timer);
        if (r.data.status === 'done') {
          message.success('模型训练完成');
          mlApi.getWeights().then((wr) => setWeights(wr.data.weights || {}));
        } else {
          message.error('模型训练失败');
        }
      }
    }, 3000);
    return () => clearInterval(timer);
  }, [jobId, polling]);

  const handleStartTraining = async () => {
    try {
      const r = await mlApi.train({ ts_code: tsCode, task });
      setJobId(r.data.job_id);
      setPolling(true);
      setStatus({ status: 'queued' });
      message.info(`训练任务 ${r.data.job_id} 已启动`);
    } catch {
      message.error('启动训练失败');
    }
  };

  const getStatusColor = (s: string) => {
    switch (s) {
      case 'done': return '#10b981';
      case 'failed': return '#ef4444';
      case 'running': return '#00d4ff';
      default: return '#ffc107';
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

  return (
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{ marginBottom: '16px' }}>
        <h1 style={{
          color: '#00d4ff', fontSize: '24px', fontWeight: 700, margin: 0,
          textShadow: '0 0 20px rgba(0, 212, 255, 0.5)', letterSpacing: '1px'
        }}>
          <FundOutlined style={{ marginRight: '8px' }} />
          策略中心
        </h1>
        <p style={{ color: '#94a3b8', margin: '4px 0 0 0', fontSize: '12px' }}>
          可视化策略构建、回测与模型训练
        </p>
      </div>

      <Tabs defaultActiveKey="1" className="tech-tabs">
        <TabPane tab="📊 策略回测" key="1">
          <Card className="tech-card" style={{ marginBottom: '12px', border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>⚡ 可视化策略编辑器</span>}
          >
            <FlowEditor />
          </Card>

          {loading && (
            <div style={{ textAlign: 'center', padding: '48px' }}>
              <Spin size="large" />
              <div style={{ color: '#00d4ff', marginTop: '12px', fontSize: '14px' }}>
                回测运行中...
              </div>
            </div>
          )}

          {metrics && (
            <>
              <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
                {[
                  { title: '夏普比率', value: metrics.sharpe_ratio, precision: 4, icon: <TrophyOutlined />, color: '#00d4ff' },
                  { title: '最大回撤', value: (metrics.max_drawdown * 100).toFixed(2) + '%', icon: <FallOutlined />, color: '#ef4444' },
                  { title: '年化收益', value: (metrics.annualized_return * 100).toFixed(2) + '%', icon: <RiseOutlined />, color: '#10b981' },
                  { title: '胜率', value: (metrics.win_rate * 100).toFixed(1) + '%', icon: <TrophyOutlined />, color: '#7c3aed' },
                  { title: '盈亏比', value: metrics.profit_factor, precision: 2, icon: <RiseOutlined />, color: '#00d4ff' },
                  { title: '交易次数', value: metrics.n_trades, icon: <FundOutlined />, color: '#94a3b8' },
                ].map((m) => (
                  <Col xs={24} sm={12} md={8} lg={4} key={m.title}>
                    <div className="stat-card">
                      <div style={{ fontSize: '20px', marginBottom: '6px', color: m.color }}>{m.icon}</div>
                      <div className="stat-value">
                        {typeof m.value === 'number' && m.precision ? m.value.toFixed(m.precision) : m.value}
                      </div>
                      <div className="stat-label">{m.title}</div>
                    </div>
                  </Col>
                ))}
              </Row>
              {equity.length > 0 && (
                <div className="tech-card" style={{ marginTop: 12, padding: '16px' }}>
                  <h3 style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600, marginBottom: '12px' }}>
                    📈 权益曲线
                  </h3>
                  <EquityCurveChart data={equity} />
                </div>
              )}
            </>
          )}
        </TabPane>

        <TabPane tab={<span><RobotOutlined /> 模型训练</span>} key="2">
          <Card className="tech-card" style={{ marginBottom: '12px', border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>🤖 模型训练</span>}
          >
            <Space style={{ marginBottom: 12 }} wrap size="small">
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>股票代码</div>
                <Select value={tsCode} onChange={setTsCode} style={{ width: 160 }} size="small"
                  options={[
                    { label: '000001.SZ', value: '000001.SZ' },
                    { label: '600000.SH', value: '600000.SH' },
                  ]}
                />
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>任务类型</div>
                <Select value={task} onChange={setTask} style={{ width: 180 }} size="small"
                  options={[
                    { label: '🚀 完整流水线', value: 'full' },
                    { label: '🤖 AutoML', value: 'automl' },
                    { label: '⚡ 仅优化', value: 'optimize' },
                  ]}
                />
              </div>
              <div style={{ display: 'flex', alignItems: 'flex-end' }}>
                <Button type="primary" onClick={handleStartTraining} loading={polling}
                  size="small" className="glow-button" icon={<ThunderboltOutlined />}>
                  开始训练
                </Button>
              </div>
            </Space>

            {status && (
              <div style={{
                padding: '12px', marginBottom: 12,
                background: 'rgba(0,0,0,0.3)', borderRadius: 6,
                border: '1px solid rgba(0,212,255,0.2)'
              }}>
                <Space>
                  {status.status === 'running' ? <Spin size="small" /> :
                    status.status === 'done' ? <CheckCircleOutlined style={{ color: '#10b981' }} /> :
                    status.status === 'failed' ? <CloseCircleOutlined style={{ color: '#ef4444' }} /> :
                    <ThunderboltOutlined style={{ color: '#ffc107' }} />}
                  <span style={{ color: getStatusColor(status.status), fontWeight: 600 }}>
                    {getStatusText(status.status)}
                  </span>
                  {jobId && <Tag>{jobId}</Tag>}
                </Space>
                {status.status === 'running' && (
                  <Progress percent={50} status="active" showInfo={false}
                    strokeColor={{ '0%': '#00d4ff', '100%': '#7c3aed' }}
                    style={{ marginTop: 8 }}
                  />
                )}
              </div>
            )}
          </Card>

          <Card className="tech-card" style={{ border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>📊 因子权重</span>}
          >
            <Row gutter={[12, 12]}>
              {Object.entries(weights).map(([k, v]) => (
                <Col xs={12} sm={8} md={6} key={k}>
                  <div className="stat-card">
                    <div className="stat-label" style={{ marginBottom: 8 }}>{k}</div>
                    <div className="stat-value">{Number(v).toFixed(4)}</div>
                    <Progress percent={Math.abs(Number(v)) * 100} showInfo={false}
                      strokeColor={{ '0%': '#00d4ff', '100%': '#7c3aed' }}
                      style={{ marginTop: 8 }}
                    />
                  </div>
                </Col>
              ))}
            </Row>
            {Object.keys(weights).length === 0 && (
              <div style={{ textAlign: 'center', padding: '32px', color: '#64748b', fontSize: '13px' }}>
                暂无权重数据，请先启动训练任务生成因子权重。
              </div>
            )}
          </Card>
        </TabPane>
      </Tabs>
    </div>
  );
};

export default StrategyCenter;
