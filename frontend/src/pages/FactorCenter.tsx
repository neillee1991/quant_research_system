import React, { useEffect, useState } from 'react';
import {
  Table, Button, Select, Space, Tag, message, Card, Tabs, Row, Col,
} from 'antd';
import {
  ExperimentOutlined, ReloadOutlined, BarChartOutlined,
} from '@ant-design/icons';
import { dataApi, factorApi } from '../api';

const { TabPane } = Tabs;

const FACTOR_OPTIONS = [
  { label: 'SMA5', value: 'sma5' },
  { label: 'SMA10', value: 'sma10' },
  { label: 'SMA20', value: 'sma20' },
  { label: 'SMA60', value: 'sma60' },
  { label: 'EMA12', value: 'ema12' },
  { label: 'EMA26', value: 'ema26' },
  { label: 'RSI6', value: 'rsi6' },
  { label: 'RSI14', value: 'rsi14' },
  { label: 'MACD', value: 'macd' },
];

const FactorCenter: React.FC = () => {
  const [stocks, setStocks] = useState<string[]>([]);
  const [tsCode, setTsCode] = useState<string>('');
  const [startDate, setStartDate] = useState('20240101');
  const [endDate, setEndDate] = useState('20241231');
  const [selectedFactors, setSelectedFactors] = useState<string[]>(['sma20', 'rsi14']);
  const [factorData, setFactorData] = useState<any[]>([]);
  const [factorColumns, setFactorColumns] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  // IC 分析
  const [icFactor, setIcFactor] = useState('sma20');
  const [icResult, setIcResult] = useState<{ ic: number; rank_ic: number } | null>(null);
  const [icLoading, setIcLoading] = useState(false);

  useEffect(() => {
    dataApi.listStocks().then((r) => {
      const list = r.data.stocks || [];
      setStocks(list);
      if (list.length > 0) setTsCode(list[0]);
    });
  }, []);

  const handleCompute = async () => {
    if (!tsCode) { message.warning('请选择股票代码'); return; }
    if (selectedFactors.length === 0) { message.warning('请选择至少一个因子'); return; }
    setLoading(true);
    try {
      const res = await factorApi.compute({
        ts_code: tsCode,
        start_date: startDate,
        end_date: endDate,
        factors: selectedFactors,
      });
      const data = res.data.data || [];
      setFactorData(data);
      if (data.length > 0) {
        const cols = Object.keys(data[0]).map((key) => ({
          title: key,
          dataIndex: key,
          key,
          width: 120,
          ellipsis: true,
          render: (v: any) => {
            if (v === null || v === undefined) return <span style={{ color: '#999' }}>-</span>;
            if (typeof v === 'number') return v.toFixed(4);
            return String(v);
          },
        }));
        setFactorColumns(cols);
      }
      message.success(`计算完成，共 ${data.length} 行数据`);
    } catch (error: any) {
      message.error(error.response?.data?.detail || '因子计算失败');
    } finally {
      setLoading(false);
    }
  };

  const handleICAnalysis = async () => {
    if (!tsCode) { message.warning('请选择股票代码'); return; }
    setIcLoading(true);
    try {
      const res = await factorApi.ic({
        ts_code: tsCode,
        start_date: startDate,
        end_date: endDate,
        factors: [icFactor],
      });
      setIcResult({ ic: res.data.ic, rank_ic: res.data.rank_ic });
      message.success('IC 分析完成');
    } catch (error: any) {
      message.error(error.response?.data?.detail || 'IC 分析失败');
    } finally {
      setIcLoading(false);
    }
  };

  return (
    <div style={{ padding: '16px', maxWidth: '1600px', margin: '0 auto' }}>
      <div style={{ marginBottom: '16px' }}>
        <h1 style={{
          color: '#00d4ff', fontSize: '24px', fontWeight: 700, margin: 0,
          textShadow: '0 0 20px rgba(0, 212, 255, 0.5)', letterSpacing: '1px'
        }}>
          <ExperimentOutlined style={{ marginRight: '8px' }} />
          因子中心
        </h1>
        <p style={{ color: '#94a3b8', margin: '4px 0 0 0', fontSize: '12px' }}>
          技术因子计算与 IC 分析
        </p>
      </div>

      <Tabs defaultActiveKey="1" className="tech-tabs">
        <TabPane tab="📊 因子计算" key="1">
          <Card className="tech-card" style={{ marginBottom: '12px', border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>⚡ 因子计算</span>}
          >
            <Space style={{ marginBottom: 12 }} wrap size="small">
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>股票代码</div>
                <Select showSearch value={tsCode} onChange={setTsCode}
                  style={{ width: 160 }} size="small" placeholder="选择股票"
                  options={stocks.map((s) => ({ label: s, value: s }))}
                />
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>开始日期</div>
                <input value={startDate} onChange={(e) => setStartDate(e.target.value)}
                  style={{ width: 120, padding: '4px 8px', background: 'rgba(0,0,0,0.3)', border: '1px solid rgba(0,212,255,0.3)', borderRadius: 4, color: '#f1f5f9', fontSize: 12 }}
                />
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>结束日期</div>
                <input value={endDate} onChange={(e) => setEndDate(e.target.value)}
                  style={{ width: 120, padding: '4px 8px', background: 'rgba(0,0,0,0.3)', border: '1px solid rgba(0,212,255,0.3)', borderRadius: 4, color: '#f1f5f9', fontSize: 12 }}
                />
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>选择因子</div>
                <Select mode="multiple" value={selectedFactors} onChange={setSelectedFactors}
                  style={{ width: 320 }} size="small" placeholder="选择因子"
                  options={FACTOR_OPTIONS}
                />
              </div>
              <div style={{ display: 'flex', alignItems: 'flex-end' }}>
                <Button type="primary" onClick={handleCompute} loading={loading}
                  size="small" className="glow-button" icon={<BarChartOutlined />}>
                  计算
                </Button>
              </div>
            </Space>

            {factorData.length > 0 && (
              <Table dataSource={factorData} columns={factorColumns}
                rowKey={(_, i) => (i ?? 0).toString()} size="small"
                pagination={{ pageSize: 20 }} scroll={{ x: 'max-content' }}
                className="tech-table"
              />
            )}
          </Card>
        </TabPane>

        <TabPane tab="📈 IC 分析" key="2">
          <Card className="tech-card" style={{ border: 'none' }}
            title={<span style={{ color: '#00d4ff', fontSize: '16px', fontWeight: 600 }}>📈 因子 IC 分析</span>}
          >
            <Space style={{ marginBottom: 12 }} wrap size="small">
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>股票代码</div>
                <Select showSearch value={tsCode} onChange={setTsCode}
                  style={{ width: 160 }} size="small"
                  options={stocks.map((s) => ({ label: s, value: s }))}
                />
              </div>
              <div>
                <div style={{ color: '#94a3b8', fontSize: '11px', marginBottom: 4 }}>因子</div>
                <Select value={icFactor} onChange={setIcFactor}
                  style={{ width: 160 }} size="small"
                  options={FACTOR_OPTIONS}
                />
              </div>
              <div style={{ display: 'flex', alignItems: 'flex-end' }}>
                <Button type="primary" onClick={handleICAnalysis} loading={icLoading}
                  size="small" className="glow-button">
                  分析
                </Button>
              </div>
            </Space>

            {icResult && (
              <Row gutter={[16, 16]} style={{ marginTop: 12 }}>
                <Col xs={12} sm={8} md={6}>
                  <div className="stat-card">
                    <div className="stat-label" style={{ marginBottom: 8 }}>IC 值</div>
                    <div style={{
                      color: icResult.ic >= 0 ? '#10b981' : '#ef4444',
                      fontSize: '24px', fontWeight: 700
                    }}>
                      {icResult.ic.toFixed(4)}
                    </div>
                  </div>
                </Col>
                <Col xs={12} sm={8} md={6}>
                  <div className="stat-card">
                    <div className="stat-label" style={{ marginBottom: 8 }}>Rank IC</div>
                    <div style={{
                      color: icResult.rank_ic >= 0 ? '#10b981' : '#ef4444',
                      fontSize: '24px', fontWeight: 700
                    }}>
                      {icResult.rank_ic.toFixed(4)}
                    </div>
                  </div>
                </Col>
                <Col xs={12} sm={8} md={6}>
                  <div className="stat-card">
                    <div className="stat-label" style={{ marginBottom: 8 }}>IC 绝对值</div>
                    <div style={{ color: '#00d4ff', fontSize: '24px', fontWeight: 700 }}>
                      {Math.abs(icResult.ic).toFixed(4)}
                    </div>
                    <Tag color={Math.abs(icResult.ic) > 0.05 ? 'green' : Math.abs(icResult.ic) > 0.02 ? 'blue' : 'default'}
                      style={{ marginTop: 8 }}>
                      {Math.abs(icResult.ic) > 0.05 ? '强' : Math.abs(icResult.ic) > 0.02 ? '中等' : '弱'}
                    </Tag>
                  </div>
                </Col>
              </Row>
            )}
          </Card>
        </TabPane>
      </Tabs>
    </div>
  );
};

export default FactorCenter;
