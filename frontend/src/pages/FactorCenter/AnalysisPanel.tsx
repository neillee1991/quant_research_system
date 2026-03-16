/**
 * 因子分析面板
 */

import React from 'react';
import {
  Card, Button, Select, InputNumber, Checkbox, Spin, Empty, Table, Tag, Toast,
} from '@douyinfe/semi-ui';
import { IconBarChartHStroked } from '@douyinfe/semi-icons';
import ReactECharts from 'echarts-for-react';
import QuantDatePicker from '../../components/QuantDatePicker';
import { useFactorAnalysis } from './hooks/useFactorAnalysis';
import { productionApi } from '../../api';

const AnalysisPanel: React.FC = () => {
  const {
    factors,
    indexPools,
    selectedFactor,
    setSelectedFactor,
    periods,
    setPeriods,
    quantiles,
    setQuantiles,
    startDate,
    setStartDate,
    endDate,
    setEndDate,
    indexPool,
    setIndexPool,
    groupbyField,
    setGroupbyField,
    useAlphalens,
    setUseAlphalens,
    analysisResult,
    loading,
    runLoading,
    analysisHistory,
    historyLoading,
    runAnalysis,
    loadAnalysis,
  } = useFactorAnalysis();

  const getICChartOption = () => {
    if (useAlphalens) {
      const icByPeriod = analysisResult?.ic_by_period;
      if (!icByPeriod || !icByPeriod.length) return {};
      return {
        backgroundColor: 'transparent',
        tooltip: { trigger: 'axis' },
        legend: { textStyle: { color: '#94A3B8' }, top: 0 },
        grid: { top: 40, bottom: 30, left: 60, right: 20 },
        xAxis: {
          type: 'category',
          data: icByPeriod.map((d: any) => d.period),
          axisLabel: { color: '#94A3B8' },
        },
        yAxis: {
          type: 'value',
          axisLabel: { color: '#94A3B8' },
          splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
        },
        series: [
          {
            name: 'IC均值',
            type: 'bar',
            data: icByPeriod.map((d: any) => d.ic_mean?.toFixed(4)),
            itemStyle: { color: '#0077FA' },
          },
          {
            name: 'ICIR',
            type: 'bar',
            data: icByPeriod.map((d: any) => d.ic_ir?.toFixed(4)),
            itemStyle: { color: '#14C9C9' },
          },
        ],
      };
    }

    if (!analysisResult?.ic_summary) return {};
    const data = analysisResult.ic_summary;
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: {
        type: 'category',
        data: data.map((d: any) => `${d.period}D`),
        axisLabel: { color: '#94A3B8' },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#94A3B8' },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
      },
      series: [
        {
          name: 'IC均值',
          type: 'bar',
          data: data.map((d: any) => d.ic_mean?.toFixed(4)),
          itemStyle: { color: '#0077FA' },
        },
        {
          name: 'ICIR',
          type: 'bar',
          data: data.map((d: any) => d.icir?.toFixed(4)),
          itemStyle: { color: '#14C9C9' },
        },
      ],
    };
  };

  const getLayerReturnOption = () => {
    const returns = useAlphalens ? analysisResult?.quantile_returns : analysisResult?.layer_returns;
    if (!returns || !returns.length) return {};
    const data = returns;
    const periodGroups = [...new Set(data.map((d: any) => d.period))];
    const quantileGroups = [...new Set(data.map((d: any) => d.quantile))];
    const colors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0077FA'];
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: quantileGroups as string[], textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: {
        type: 'category',
        data: periodGroups.map((p: any) => `${p}`),
        axisLabel: { color: '#94A3B8' },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          color: '#94A3B8',
          formatter: (v: number) => `${(v * 100).toFixed(2)}%`,
        },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
      },
      series: quantileGroups.map((q: any, i: number) => ({
        name: `Q${q}`,
        type: 'bar',
        data: periodGroups.map((p: any) => {
          const item = data.find((d: any) => d.period === p && d.quantile === q);
          return item?.mean_return || 0;
        }),
        itemStyle: { color: colors[i % colors.length] },
      })),
    };
  };

  const getICTimeSeriesOption = () => {
    const icTs = analysisResult?.ic_ts;
    if (!icTs || !icTs.length) return {};
    const dates = icTs.map((d: any) => d.date);
    const periods = Object.keys(icTs[0]).filter(k => k !== 'date');
    const colors = ['#0077FA', '#14C9C9', '#f97316', '#22c55e'];
    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis' },
      legend: { data: periods, textStyle: { color: '#94A3B8' }, top: 0 },
      grid: { top: 40, bottom: 30, left: 60, right: 20 },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: { color: '#94A3B8', rotate: 30 },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: '#94A3B8' },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
      },
      series: periods.map((p, i) => ({
        name: p,
        type: 'line',
        smooth: true,
        data: icTs.map((d: any) => d[p]),
        lineStyle: { color: colors[i % colors.length] },
        itemStyle: { color: colors[i % colors.length] },
        showSymbol: false,
      })),
    };
  };

  const icColumns = [
    { title: '周期', dataIndex: 'period', key: 'period', render: (v: any) => `${v}` },
    {
      title: 'IC均值',
      dataIndex: 'ic_mean',
      key: 'ic_mean',
      render: (v: number) => (
        <span style={{ color: v > 0 ? 'var(--color-gain)' : 'var(--color-loss)' }}>
          {v?.toFixed(4)}
        </span>
      ),
    },
    { title: 'IC标准差', dataIndex: 'ic_std', key: 'ic_std', render: (v: number) => v?.toFixed(4) },
    {
      title: 'ICIR',
      dataIndex: 'ic_ir',
      key: 'ic_ir',
      render: (v: number) => (
        <span
          style={{
            color: Math.abs(v) > 0.5 ? 'var(--color-primary)' : 'var(--text-secondary)',
            fontWeight: Math.abs(v) > 0.5 ? 700 : 400,
          }}
        >
          {v?.toFixed(4)}
        </span>
      ),
    },
    {
      title: 'IC胜率',
      dataIndex: 'ic_win_rate',
      key: 'win_rate',
      render: (v: number) => `${(v * 100).toFixed(1)}%`,
    },
  ];

  const historyColumns = [
    {
      title: '分析日期',
      dataIndex: 'analysis_date',
      key: 'analysis_date',
      render: (v: any) => (v ? new Date(v).toLocaleString('zh-CN') : '-'),
    },
    {
      title: '日期范围',
      key: 'range',
      render: (_: any, r: any) => `${r.start_date || '-'} ~ ${r.end_date || '-'}`,
    },
    {
      title: '股票池',
      dataIndex: 'index_pool',
      key: 'index_pool',
      render: (v: string) => v || '全市场',
    },
    {
      title: '分组',
      dataIndex: 'groupby_field',
      key: 'groupby_field',
      render: (v: string) => v || '-',
    },
    {
      title: '状态',
      dataIndex: 'task_status',
      key: 'task_status',
      render: (v: string) => <Tag color={v === 'completed' ? 'green' : 'orange'}>{v}</Tag>,
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, r: any) => (
        <Button
          size="small"
          onClick={async () => {
            try {
              const res = await productionApi.getLatestAlphalensAnalysis(selectedFactor);
              Toast.success('加载成功');
            } catch (error) {
              console.error('Failed to load analysis:', error);
              Toast.error('加载失败');
            }
          }}
        >
          查看
        </Button>
      ),
    },
  ];

  const icSummary = useAlphalens ? analysisResult?.ic_summary : null;

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'flex-end' }}>
          <div style={{ flex: 1, minWidth: 200 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>选择因子</div>
            <Select
              size="small"
              style={{ width: '100%' }}
              placeholder="选择因子"
              filter
              value={selectedFactor}
              onChange={(v) => {
                setSelectedFactor(v as string);
                if (v) loadAnalysis(v as string);
              }}
              optionList={factors.map(f => ({ label: `${f.factor_name} (${f.factor_id})`, value: f.factor_id }))}
            />
          </div>
          <div style={{ width: 200 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>持有周期</div>
            <Select
              size="small"
              multiple
              style={{ width: '100%' }}
              value={periods}
              onChange={(v) => setPeriods(v as number[])}
              optionList={[1, 3, 5, 10, 20].map(p => ({ label: `${p}天`, value: p }))}
            />
          </div>
          <div style={{ width: 120 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分层数</div>
            <InputNumber size="small" style={{ width: '100%' }} value={quantiles} onChange={(v) => setQuantiles(v as number)} min={3} max={10} />
          </div>
          <div style={{ width: 240 }}>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分析区间</div>
            <QuantDatePicker
              value={[startDate, endDate]}
              onChange={(s, e) => { setStartDate(s); setEndDate(e); }}
              disableFuture={false}
              style={{ width: '100%' }}
            />
          </div>
          {useAlphalens && (
            <>
              <div style={{ width: 160 }}>
                <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>股票池</div>
                <Select
                  size="small"
                  showClear
                  style={{ width: '100%' }}
                  placeholder="全市场"
                  value={indexPool}
                  onChange={(v) => setIndexPool(v as string)}
                  optionList={indexPools.map(p => ({ label: p.name, value: p.code }))}
                />
              </div>
              <div style={{ width: 140 }}>
                <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 4 }}>分组字段</div>
                <Select
                  size="small"
                  showClear
                  style={{ width: '100%' }}
                  placeholder="不分组"
                  value={groupbyField}
                  onChange={(v) => setGroupbyField(v as string)}
                  optionList={[
                    { label: '行业', value: 'industry' },
                    { label: '市值', value: 'market_cap' },
                  ]}
                />
              </div>
            </>
          )}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Checkbox checked={useAlphalens} onChange={(e) => setUseAlphalens(e.target?.checked ?? false)}>
              Alphalens
            </Checkbox>
          </div>
          <Button size="small" theme="solid" icon={<IconBarChartHStroked />} loading={runLoading} onClick={runAnalysis}>
            运行分析
          </Button>
        </div>
      </Card>

      {loading ? (
        <Spin style={{ display: 'block', margin: '60px auto' }} />
      ) : analysisResult ? (
        <>
          {icSummary && (
            <Card
              style={{ marginBottom: 16, background: 'var(--bg-card)' }}
              title={<span style={{ color: 'var(--color-primary)' }}>IC 汇总</span>}
            >
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap' }}>
                {[
                  {
                    label: 'IC 均值',
                    value: icSummary.ic_mean?.toFixed(4),
                    color: icSummary.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)',
                  },
                  { label: 'IC 标准差', value: icSummary.ic_std?.toFixed(4), color: 'var(--text-primary)' },
                  {
                    label: 'IC IR',
                    value: icSummary.ic_ir?.toFixed(4),
                    color: Math.abs(icSummary.ic_ir) > 0.5 ? 'var(--color-primary)' : 'var(--text-primary)',
                  },
                  {
                    label: 'IC 胜率',
                    value: `${(icSummary.ic_win_rate * 100).toFixed(1)}%`,
                    color: icSummary.ic_win_rate > 0.5 ? 'var(--color-gain)' : 'var(--text-primary)',
                  },
                ].map(item => (
                  <div key={item.label} className="stat-card" style={{ minWidth: 120 }}>
                    <div style={{ color: 'var(--text-secondary)', fontSize: 12 }}>{item.label}</div>
                    <div style={{ color: item.color, fontSize: 20, fontWeight: 700 }}>{item.value}</div>
                  </div>
                ))}
              </div>
            </Card>
          )}

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
            <Card
              style={{ background: 'var(--bg-card)' }}
              title={<span style={{ color: 'var(--color-primary)' }}>IC 分析</span>}
            >
              <ReactECharts option={getICChartOption()} style={{ height: 240 }} />
            </Card>
            <Card
              style={{ background: 'var(--bg-card)' }}
              title={<span style={{ color: 'var(--color-accent, #14C9C9)' }}>分层收益</span>}
            >
              <ReactECharts option={getLayerReturnOption()} style={{ height: 240 }} />
            </Card>
          </div>

          {analysisResult?.ic_ts && analysisResult.ic_ts.length > 0 && (
            <Card
              style={{ marginBottom: 16, background: 'var(--bg-card)' }}
              title={<span style={{ color: 'var(--text-secondary)' }}>IC 时间序列</span>}
            >
              <ReactECharts option={getICTimeSeriesOption()} style={{ height: 240 }} />
            </Card>
          )}

          {analysisResult?.ic_by_period && (
            <Card
              style={{ background: 'var(--bg-card)' }}
              title={<span style={{ color: 'var(--text-secondary)' }}>分周期 IC 指标</span>}
            >
              <Table
                dataSource={analysisResult.ic_by_period || []}
                columns={icColumns}
                rowKey="period"
                size="small"
                pagination={false}
              />
            </Card>
          )}
        </>
      ) : (
        <Empty description="选择因子并运行分析" style={{ marginTop: 60 }} />
      )}

      {selectedFactor && (
        <Card
          style={{ marginTop: 16, background: 'var(--bg-card)' }}
          title={<span style={{ color: 'var(--text-secondary)' }}>分析历史</span>}
        >
          <Spin spinning={historyLoading}>
            <Table
              dataSource={analysisHistory}
              columns={historyColumns}
              rowKey="id"
              size="small"
              pagination={{ pageSize: 5 }}
            />
          </Spin>
        </Card>
      )}
    </div>
  );
};

export default AnalysisPanel;
