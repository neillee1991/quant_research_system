import React, { useState, useEffect } from 'react';
import { Button, Select, Space, Card, Row, Col, message, Spin, Progress } from 'antd';
import { RobotOutlined, ThunderboltOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { mlApi } from '../api';

const MLAuto: React.FC = () => {
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
          message.success('ML job completed');
          mlApi.getWeights().then((wr) => setWeights(wr.data.weights || {}));
        } else {
          message.error('ML job failed');
        }
      }
    }, 3000);
    return () => clearInterval(timer);
  }, [jobId, polling]);

  const handleStart = async () => {
    try {
      const r = await mlApi.train({ ts_code: tsCode, task });
      setJobId(r.data.job_id);
      setPolling(true);
      setStatus({ status: 'queued' });
      message.info(`Job ${r.data.job_id} started`);
    } catch {
      message.error('Failed to start ML job');
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'done': return '#10b981';
      case 'failed': return '#ef4444';
      case 'running': return '#00d4ff';
      default: return '#ffc107';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'done': return <CheckCircleOutlined />;
      case 'failed': return <CloseCircleOutlined />;
      case 'running': return <Spin size="small" />;
      default: return <ThunderboltOutlined />;
    }
  };

  return (
    <div style={{ padding: '32px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{ marginBottom: '32px' }}>
        <h1 style={{
          color: '#00d4ff',
          fontSize: '32px',
          fontWeight: 700,
          margin: 0,
          textShadow: '0 0 20px rgba(0, 212, 255, 0.5)',
          letterSpacing: '1px'
        }}>
          <RobotOutlined style={{ marginRight: '12px' }} />
          AutoML
        </h1>
        <p style={{
          color: '#94a3b8',
          margin: '8px 0 0 0',
          fontSize: '14px'
        }}>
          Automated machine learning for factor optimization
        </p>
      </div>

      <div className="tech-card" style={{ padding: '24px', marginBottom: '24px' }}>
        <div style={{ marginBottom: '16px', color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>
          🎯 Training Configuration
        </div>
        <Space style={{ marginBottom: 24 }} wrap size="large">
          <div>
            <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '8px' }}>Stock Code</div>
            <Select
              value={tsCode}
              onChange={setTsCode}
              style={{ width: 180 }}
              className="tech-input"
              options={[
                { label: '000001.SZ', value: '000001.SZ' },
                { label: '600000.SH', value: '600000.SH' }
              ]}
            />
          </div>
          <div>
            <div style={{ color: '#94a3b8', fontSize: '12px', marginBottom: '8px' }}>Task Type</div>
            <Select
              value={task}
              onChange={setTask}
              style={{ width: 180 }}
              className="tech-input"
              options={[
                { label: '🚀 Full Pipeline', value: 'full' },
                { label: '🤖 AutoML Only', value: 'automl' },
                { label: '⚡ Optimize Only', value: 'optimize' },
              ]}
            />
          </div>
          <div style={{ display: 'flex', alignItems: 'flex-end' }}>
            <Button
              type="primary"
              onClick={handleStart}
              loading={polling}
              size="large"
              className="glow-button"
              icon={<ThunderboltOutlined />}
              style={{ height: '48px', padding: '0 32px' }}
            >
              Start Training
            </Button>
          </div>
        </Space>
      </div>

      {status && (
        <div className="tech-card" style={{ padding: '24px', marginBottom: '24px' }}>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '16px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <span style={{ color: '#94a3b8', fontSize: '14px' }}>Job Status:</span>
              <span style={{
                color: getStatusColor(status.status),
                fontSize: '18px',
                fontWeight: 600,
                display: 'flex',
                alignItems: 'center',
                gap: '8px'
              }}>
                {getStatusIcon(status.status)}
                {status.status?.toUpperCase()}
              </span>
            </div>
            {polling && (
              <div style={{ color: '#00d4ff', fontSize: '14px' }}>
                <Spin size="small" style={{ marginRight: '8px' }} />
                Processing...
              </div>
            )}
          </div>
          {status.result && (
            <pre className="code-block" style={{ marginTop: '16px' }}>
              {JSON.stringify(status.result, null, 2)}
            </pre>
          )}
        </div>
      )}

      <div className="tech-card" style={{ padding: '24px' }}>
        <h3 style={{
          color: '#00d4ff',
          fontSize: '18px',
          fontWeight: 600,
          marginBottom: '24px'
        }}>
          🏆 Best Factor Weights
        </h3>
        <Row gutter={[16, 16]}>
          {Object.entries(weights).map(([k, v]) => (
            <Col xs={24} sm={12} md={8} lg={6} key={k}>
              <div className="stat-card">
                <div className="stat-label" style={{ marginBottom: '12px' }}>{k}</div>
                <div className="stat-value">{Number(v).toFixed(4)}</div>
                <Progress
                  percent={Math.abs(Number(v)) * 100}
                  showInfo={false}
                  strokeColor={{
                    '0%': '#00d4ff',
                    '100%': '#7c3aed',
                  }}
                  style={{ marginTop: '12px' }}
                />
              </div>
            </Col>
          ))}
        </Row>
        {Object.keys(weights).length === 0 && (
          <div style={{
            textAlign: 'center',
            padding: '48px',
            color: '#64748b',
            fontSize: '14px'
          }}>
            No weights available. Start a training job to generate factor weights.
          </div>
        )}
      </div>
    </div>
  );
};

export default MLAuto;
