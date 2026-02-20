import React from 'react';
import { Row, Col, Card, Statistic, Spin } from 'antd';
import { FundOutlined, RiseOutlined, FallOutlined, TrophyOutlined } from '@ant-design/icons';
import FlowEditor from '../components/FlowEditor';
import EquityCurveChart from '../components/Charts/EquityCurveChart';
import { useBacktestStore } from '../store';

const StrategyLab: React.FC = () => {
  const { result, loading } = useBacktestStore();
  const metrics = result?.metrics;
  const equity = result?.equity_curve || [];

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
          <FundOutlined style={{ marginRight: '12px' }} />
          Strategy Lab
        </h1>
        <p style={{
          color: '#94a3b8',
          margin: '8px 0 0 0',
          fontSize: '14px'
        }}>
          Visual strategy builder and backtesting engine
        </p>
      </div>

      <div className="tech-card" style={{ padding: '24px', marginBottom: '24px' }}>
        <FlowEditor />
      </div>

      {loading && (
        <div style={{ textAlign: 'center', padding: '64px' }}>
          <Spin size="large" />
          <div style={{ color: '#00d4ff', marginTop: '16px', fontSize: '16px' }}>
            Running backtest...
          </div>
        </div>
      )}

      {metrics && (
        <>
          <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
            {[
              {
                title: 'Sharpe Ratio',
                value: metrics.sharpe_ratio,
                precision: 4,
                icon: <TrophyOutlined />,
                color: '#00d4ff'
              },
              {
                title: 'Max Drawdown',
                value: (metrics.max_drawdown * 100).toFixed(2) + '%',
                icon: <FallOutlined />,
                color: '#ef4444'
              },
              {
                title: 'Ann. Return',
                value: (metrics.annualized_return * 100).toFixed(2) + '%',
                icon: <RiseOutlined />,
                color: '#10b981'
              },
              {
                title: 'Win Rate',
                value: (metrics.win_rate * 100).toFixed(1) + '%',
                icon: <TrophyOutlined />,
                color: '#7c3aed'
              },
              {
                title: 'Profit Factor',
                value: metrics.profit_factor,
                precision: 2,
                icon: <RiseOutlined />,
                color: '#00d4ff'
              },
              {
                title: 'Trades',
                value: metrics.n_trades,
                icon: <FundOutlined />,
                color: '#94a3b8'
              },
            ].map((m) => (
              <Col xs={24} sm={12} md={8} lg={4} key={m.title}>
                <div className="stat-card">
                  <div style={{ fontSize: '24px', marginBottom: '8px', color: m.color }}>
                    {m.icon}
                  </div>
                  <div className="stat-value">
                    {typeof m.value === 'number' && m.precision
                      ? m.value.toFixed(m.precision)
                      : m.value}
                  </div>
                  <div className="stat-label">{m.title}</div>
                </div>
              </Col>
            ))}
          </Row>
          {equity.length > 0 && (
            <div className="tech-card" style={{ marginTop: 24, padding: '24px' }}>
              <h3 style={{
                color: '#00d4ff',
                fontSize: '18px',
                fontWeight: 600,
                marginBottom: '16px'
              }}>
                📈 Equity Curve
              </h3>
              <EquityCurveChart data={equity} />
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default StrategyLab;
