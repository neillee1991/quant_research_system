import React, { useState } from 'react';
import { Card, Button, Statistic, Row, Col, Divider, Radio } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';

interface BacktestResultProps {
  result: any;
  onClose: () => void;
}

export const BacktestResult: React.FC<BacktestResultProps> = ({
  result,
  onClose,
}) => {
  const [chartType, setChartType] = useState<'linear' | 'log'>('linear');

  if (!result) {
    return null;
  }

  const equityCurveData = result.equity_curve?.values || [];
  const dates = result.equity_curve?.dates || [];

  const getEquityOption = (type: 'linear' | 'log') => {
    const baseOption: any = {
      title: {
        text: '净值曲线',
        left: 'center',
      },
      tooltip: {
        trigger: 'axis',
        formatter: '{b}<br/>净值: {c}',
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: {
          formatter: (value: string, index: number) => {
            if (index % 30 === 0) {
              return value;
            }
            return '';
          },
        },
      },
      yAxis: {
        type: 'value',
        scale: true,
      },
      series: [
        {
          name: '净值',
          type: 'line',
          data: equityCurveData,
          smooth: true,
          areaStyle: {
            color: {
              type: 'linear',
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [
                { offset: 0, color: 'rgba(54, 162, 235, 0.8)' },
                { offset: 1, color: 'rgba(54, 162, 235, 0.1)' },
              ],
            },
          },
        },
      ],
    };

    if (type === 'log') {
      baseOption.yAxis = {
        type: 'log',
        name: '净值（对数刻度）',
        minorTick: {
          show: true,
        },
        minorSplitLine: {
          show: true,
        },
      };
    } else {
      baseOption.yAxis = {
        type: 'value',
        name: '净值（线性刻度）',
        scale: true,
      };
    }

    return baseOption;
  };

  const performanceMetrics = [
    {
      title: '总收益率',
      value: result.performance?.total_return || 0,
      unit: '%',
      isPositive: true,
    },
    {
      title: '年化收益率',
      value: result.performance?.annual_return || 0,
      unit: '%',
      isPositive: true,
    },
    {
      title: '最大回撤',
      value: result.risk?.max_drawdown || 0,
      unit: '%',
      isPositive: false,
    },
    {
      title: '夏普比率',
      value: result.performance?.sharpe_ratio || 0,
      unit: '',
      isPositive: true,
    },
    {
      title: '胜率',
      value: result.trades?.win_rate || 0,
      unit: '%',
      isPositive: true,
    },
    {
      title: '盈亏比',
      value: result.performance?.profit_factor || 0,
      unit: '',
      isPositive: true,
    },
  ];

  const riskMetrics = [
    {
      title: '波动率',
      value: result.risk?.volatility || 0,
      unit: '%',
      isPositive: false,
    },
    {
      title: 'VaR (95%)',
      value: result.risk?.var_95 || 0,
      unit: '%',
      isPositive: false,
    },
    {
      title: '总交易数',
      value: result.trades?.total_trades || 0,
      unit: '次',
      isPositive: true,
    },
    {
      title: '平均盈亏',
      value: result.trades?.avg_win || 0,
      unit: '%',
      isPositive: true,
    },
  ];

  return (
    <Card title="回测结果" className="result-card">
      <div style={{ marginBottom: 16, textAlign: 'right' }}>
        <Button type="primary" onClick={onClose}>
          关闭
        </Button>
      </div>

      <div style={{ marginBottom: 24 }}>
        <Row gutter={[16, 16]}>
          {performanceMetrics.map((metric, index) => (
            <Col xs={24} sm={12} md={8} lg={6} key={index}>
              <Card>
                <Statistic
                  title={metric.title}
                  value={metric.value}
                  precision={2}
                  suffix={metric.unit}
                  prefix={
                    metric.isPositive
                      ? (metric.value > 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />)
                      : (metric.value > 0 ? <ArrowDownOutlined /> : <ArrowUpOutlined />)
                  }
                  valueStyle={{
                    color: metric.isPositive
                      ? (metric.value > 0 ? '#52c41a' : '#ff4d4f')
                      : (metric.value > 0 ? '#ff4d4f' : '#52c41a'),
                  }}
                />
              </Card>
            </Col>
          ))}
        </Row>
      </div>

      <Divider />

      <div style={{ marginBottom: 24 }}>
        <Row gutter={[16, 16]}>
          {riskMetrics.map((metric, index) => (
            <Col xs={24} sm={12} md={8} lg={6} key={index}>
              <Card>
                <Statistic
                  title={metric.title}
                  value={metric.value}
                  precision={metric.title.includes('率') ? 2 : 0}
                  suffix={metric.unit}
                  prefix={
                    metric.isPositive
                      ? (metric.value > 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />)
                      : (metric.value > 0 ? <ArrowDownOutlined /> : <ArrowUpOutlined />)
                  }
                  valueStyle={{
                    color: metric.isPositive
                      ? (metric.value > 0 ? '#52c41a' : '#ff4d4f')
                      : (metric.value > 0 ? '#ff4d4f' : '#52c41a'),
                  }}
                />
              </Card>
            </Col>
          ))}
        </Row>
      </div>

      <Divider />

      <Card
        title="净值曲线"
        extra={
          <Radio.Group
            value={chartType}
            onChange={(e) => setChartType(e.target.value)}
            size="small"
          >
            <Radio.Button value="linear">线性刻度</Radio.Button>
            <Radio.Button value="log">对数刻度</Radio.Button>
          </Radio.Group>
        }
      >
        <ReactECharts option={getEquityOption(chartType)} style={{ height: 400 }} />
      </Card>
    </Card>
  );
};
