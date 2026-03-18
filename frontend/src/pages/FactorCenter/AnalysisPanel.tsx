/**
 * 因子分析面板
 */

import React, { useMemo } from 'react';
import {
  Card, Button, Select, InputNumber, Spin, Empty, Table, Tag, Checkbox, Collapse,
} from 'antd';

/** YYYYMMDD → YYYY-MM-DD */
const formatDate = (d: string) =>
  d?.length === 8 ? `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}` : d;
import { BarChartOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import ReactECharts from 'echarts-for-react';
import QuantDatePicker from '../../components/QuantDatePicker';
import { useFactorAnalysis } from './hooks/useFactorAnalysis';
import { productionApi } from '../../api';

/**
 * 因子自相关衰减子组件
 */
const DecayAnalysisSection: React.FC<{ data: Record<string, number> }> = ({ data }) => {
  const entries = useMemo(
    () => Object.entries(data).map(([k, v]) => ({ period: k, value: v })).sort((a, b) => Number(a.period) - Number(b.period)),
    [data]
  );
  const option = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', formatter: (params: any[]) => `${params[0].name}天: ${params[0].value?.toFixed(4)}` },
    grid: { top: 30, bottom: 30, left: 60, right: 20 },
    xAxis: { type: 'category', data: entries.map(e => `${e.period}天`), axisLabel: { color: '#94A3B8' } },
    yAxis: {
      type: 'value',
      name: '自相关系数',
      axisLabel: { color: '#94A3B8' },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
    },
    series: [{
      type: 'bar',
      data: entries.map(e => ({
        value: e.value,
        itemStyle: { color: e.value >= 0 ? '#0077FA' : '#ef4444' },
      })),
    }],
  };
  return (
    <Card
      style={{ background: 'var(--bg-card)', marginTop: 16 }}
      title={
        <span style={{ color: 'var(--text-secondary)' }}>
          因子排名自相关（衰减分析）
          <span style={{ fontSize: 11, color: 'var(--text-muted)', marginLeft: 8 }}>
            衡量因子排名的稳定性，值越高说明因子信号持续性越强
          </span>
        </span>
      }
    >
      <ReactECharts option={option} style={{ height: 220 }} />
    </Card>
  );
};

/**
 * 分组分析子组件（有 groupby 时才显示）
 */
const GroupAnalysisSection: React.FC<{ icByGroup: Record<string, any>; returnsByGroup?: Record<string, any> }> = ({ icByGroup, returnsByGroup }) => {
  const groups = Object.keys(icByGroup);
  const periods = groups.length > 0 ? Object.keys(icByGroup[groups[0]]) : [];
  const colors = ['#0077FA', '#14C9C9', '#f97316', '#22c55e', '#8b5cf6', '#ef4444'];

  const icOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { data: periods, textStyle: { color: '#94A3B8' }, top: 0 },
    grid: { top: 40, bottom: 60, left: 60, right: 20 },
    xAxis: { type: 'category', data: groups, axisLabel: { color: '#94A3B8', rotate: 30 } },
    yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
    series: periods.map((p, i) => ({
      name: p,
      type: 'bar',
      data: groups.map(g => icByGroup[g]?.[p] ?? 0),
      itemStyle: { color: colors[i % colors.length] },
    })),
  };

  return (
    <Card style={{ background: 'var(--bg-card)', marginTop: 16 }} title={<span style={{ color: 'var(--text-secondary)' }}>分组 IC 分析</span>}>
      <ReactECharts option={icOption} style={{ height: 260 }} />
    </Card>
  );
};

/**
 * 分行业 IC 和收益率分析
 * icByIndustry: { [industry]: { [period]: ic_mean } }
 * returnsByIndustry: { [industry]: [{period, quantile, mean_return}] }
 */
const IndustryAnalysisSection: React.FC<{
  icByIndustry: Record<string, Record<string, number>>;
  returnsByIndustry: Record<string, Array<{ period: string; quantile: number; mean_return: number }>>;
}> = ({ icByIndustry, returnsByIndustry }) => {
  const industries = useMemo(() => Object.keys(icByIndustry), [icByIndustry]);
  const periods = useMemo(
    () => industries.length > 0 ? Object.keys(icByIndustry[industries[0]]) : [],
    [icByIndustry, industries]
  );

  // 按第一个周期的 IC 均值排序行业
  const sortedIndustries = useMemo(() => {
    if (!periods.length) return industries;
    return [...industries].sort(
      (a, b) => (icByIndustry[b][periods[0]] ?? 0) - (icByIndustry[a][periods[0]] ?? 0)
    );
  }, [industries, periods, icByIndustry]);

  const colors = ['#0077FA', '#14C9C9', '#f97316', '#22c55e', '#8b5cf6'];

  const icOption = {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { data: periods, textStyle: { color: '#94A3B8' }, top: 0 },
    grid: { top: 40, bottom: 80, left: 60, right: 20 },
    xAxis: {
      type: 'category',
      data: sortedIndustries,
      axisLabel: { color: '#94A3B8', rotate: 40, fontSize: 11 },
    },
    yAxis: {
      type: 'value',
      name: 'IC均值',
      axisLabel: { color: '#94A3B8', formatter: (v: number) => v.toFixed(3) },
      splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
    },
    series: periods.map((p, i) => ({
      name: p,
      type: 'bar',
      data: sortedIndustries.map(ind => {
        const v = icByIndustry[ind]?.[p] ?? 0;
        return { value: v, itemStyle: { color: v >= 0 ? colors[i % colors.length] : '#ef4444' } };
      }),
    })),
  };

  // Q5-Q1 spread by industry（因子溢价）
  const spreadOption = useMemo(() => {
    if (!Object.keys(returnsByIndustry).length) return null;
    const firstPeriod = periods[0];
    if (!firstPeriod) return null;

    const spreads = sortedIndustries.map(ind => {
      const rows = (returnsByIndustry[ind] || []).filter(r => r.period === firstPeriod);
      const quantiles = rows.map(r => r.quantile).sort((a, b) => a - b);
      if (quantiles.length < 2) return { ind, spread: 0 };
      const q1 = rows.find(r => r.quantile === quantiles[0])?.mean_return ?? 0;
      const qN = rows.find(r => r.quantile === quantiles[quantiles.length - 1])?.mean_return ?? 0;
      return { ind, spread: qN - q1 };
    });

    return {
      backgroundColor: 'transparent',
      tooltip: { trigger: 'axis', formatter: (params: any[]) => `${params[0].name}: ${(params[0].value * 100).toFixed(3)}%` },
      grid: { top: 30, bottom: 80, left: 70, right: 20 },
      xAxis: {
        type: 'category',
        data: spreads.map(s => s.ind),
        axisLabel: { color: '#94A3B8', rotate: 40, fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        name: 'Q5-Q1',
        axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
      },
      series: [{
        type: 'bar',
        data: spreads.map(s => ({
          value: s.spread,
          itemStyle: { color: s.spread >= 0 ? '#22c55e' : '#ef4444' },
        })),
      }],
    };
  }, [returnsByIndustry, sortedIndustries, periods]);

  return (
    <Card
      style={{ background: 'var(--bg-card)', marginTop: 16 }}
      title={<span style={{ color: 'var(--text-secondary)' }}>分行业分析</span>}
    >
      <div style={{ display: 'grid', gridTemplateColumns: spreadOption ? '1fr 1fr' : '1fr', gap: 16 }}>
        <div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 8 }}>各行业 IC 均值</div>
          <ReactECharts option={icOption} style={{ height: 300 }} />
        </div>
        {spreadOption && (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 8 }}>
              各行业因子溢价（Q5-Q1，{periods[0]}）
            </div>
            <ReactECharts option={spreadOption} style={{ height: 300 }} />
          </div>
        )}
      </div>
    </Card>
  );
};

/**
 * 单周期分析面板（在 Collapse 内展开）
 */
const PeriodPanel: React.FC<{ period: number; analysisResult: any; isFirstPeriod: boolean }> = ({
  period, analysisResult, isFirstPeriod,
}) => {
  const periodKey = `${period}D`;
  const icTsCol = `ic_${periodKey}`;
  const turnoverKey = `period_${period}`;
  const colors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0077FA', '#8b5cf6'];

  const icRow = useMemo(
    () => (analysisResult?.ic_by_period || []).find((r: any) => r.period === periodKey),
    [analysisResult, periodKey]
  );
  const rankIcRow = useMemo(
    () => (analysisResult?.rank_ic_by_period || []).find((r: any) => r.period === periodKey),
    [analysisResult, periodKey]
  );
  const icTsData = useMemo(() => {
    return (analysisResult?.ic_ts || [])
      .filter((d: any) => d[icTsCol] != null)
      .map((d: any) => ({ date: formatDate(d.date), value: d[icTsCol] }));
  }, [analysisResult, icTsCol]);
  const quantileReturns = useMemo(
    () => (analysisResult?.quantile_returns || []).filter((r: any) => r.period === periodKey),
    [analysisResult, periodKey]
  );
  const cumulativeData = useMemo(() => {
    if (!isFirstPeriod) return null;
    const raw = analysisResult?.cumulative_returns || [];
    const qKeys = Object.keys(raw[0] || {}).filter(k => k.startsWith('quantile_')).sort();
    return raw
      .filter((d: any) => qKeys.some(k => d[k] != null))
      .map((d: any) => ({ ...d, date: formatDate(d.date) }));
  }, [analysisResult, isFirstPeriod]);
  const turnoverTs = useMemo(() => {
    const tv = analysisResult?.turnover || {};
    const qs = Object.keys(tv).sort((a, b) =>
      parseInt(a.replace('quantile_', '')) - parseInt(b.replace('quantile_', ''))
    );
    if (!qs.length) return null;
    const topQ = qs[qs.length - 1];
    const botQ = qs[0];
    const allDates = Object.keys(tv[topQ]?.[turnoverKey] || {}).sort();
    const dates = allDates.filter(d =>
      tv[topQ]?.[turnoverKey]?.[d] != null || tv[botQ]?.[turnoverKey]?.[d] != null
    );
    if (!dates.length) return null;
    return {
      dates: dates.map(formatDate),
      top: dates.map(d => tv[topQ]?.[turnoverKey]?.[d] ?? null),
      bot: dates.map(d => tv[botQ]?.[turnoverKey]?.[d] ?? null),
      topLabel: `Q${topQ.replace('quantile_', '')}`,
      botLabel: `Q${botQ.replace('quantile_', '')}`,
    };
  }, [analysisResult, turnoverKey]);

  const StatCard = ({ label, value, color }: { label: string; value: string; color: string }) => (
    <div style={{ minWidth: 110, padding: '8px 12px', background: 'var(--bg-tertiary)', borderRadius: 6 }}>
      <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>{label}</div>
      <div style={{ color, fontSize: 17, fontWeight: 700 }}>{value}</div>
    </div>
  );

  const sigLabel = (p: number) => p < 0.01 ? ' ***' : p < 0.05 ? ' **' : p < 0.1 ? ' *' : '';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {icRow && (
        <>
          <div>
            <div style={{ color: 'var(--text-muted)', fontSize: 11, marginBottom: 6 }}>Pearson IC</div>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              <StatCard label="IC 均值" value={icRow.ic_mean?.toFixed(4)} color={icRow.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)'} />
              <StatCard label="IC 标准差" value={icRow.ic_std?.toFixed(4)} color="var(--text-primary)" />
              <StatCard label="ICIR" value={icRow.ic_ir?.toFixed(4)} color={Math.abs(icRow.ic_ir) > 0.5 ? 'var(--color-primary)' : 'var(--text-primary)'} />
              <StatCard label="IC 胜率" value={`${(icRow.ic_win_rate * 100).toFixed(1)}%`} color={icRow.ic_win_rate > 0.5 ? 'var(--color-gain)' : 'var(--text-primary)'} />
              <StatCard label="t 统计量" value={icRow.t_stat?.toFixed(3)} color={Math.abs(icRow.t_stat ?? 0) > 1.96 ? 'var(--color-gain)' : 'var(--text-secondary)'} />
              <StatCard label="p 值" value={`${icRow.p_value?.toFixed(4)}${sigLabel(icRow.p_value)}`} color={icRow.p_value < 0.05 ? 'var(--color-gain)' : 'var(--text-secondary)'} />
              <StatCard label="样本数" value={String(icRow.n_obs ?? '-')} color="var(--text-primary)" />
            </div>
          </div>
          {rankIcRow && (
            <div>
              <div style={{ color: 'var(--text-muted)', fontSize: 11, marginBottom: 6 }}>Rank IC (Spearman)</div>
              <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                <StatCard label="Rank IC 均值" value={rankIcRow.ic_mean?.toFixed(4)} color={rankIcRow.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)'} />
                <StatCard label="Rank ICIR" value={rankIcRow.ic_ir?.toFixed(4)} color={Math.abs(rankIcRow.ic_ir) > 0.5 ? 'var(--color-primary)' : 'var(--text-primary)'} />
                <StatCard label="Rank IC 胜率" value={`${(rankIcRow.ic_win_rate * 100).toFixed(1)}%`} color={rankIcRow.ic_win_rate > 0.5 ? 'var(--color-gain)' : 'var(--text-primary)'} />
                <StatCard label="t 统计量" value={rankIcRow.t_stat?.toFixed(3)} color={Math.abs(rankIcRow.t_stat ?? 0) > 1.96 ? 'var(--color-gain)' : 'var(--text-secondary)'} />
                <StatCard label="p 值" value={`${rankIcRow.p_value?.toFixed(4)}${sigLabel(rankIcRow.p_value)}`} color={rankIcRow.p_value < 0.05 ? 'var(--color-gain)' : 'var(--text-secondary)'} />
              </div>
            </div>
          )}
        </>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        {icTsData.length > 0 && (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>IC 时间序列</div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis', formatter: (p: any[]) => `${p[0].name}: ${p[0].value?.toFixed(4) ?? '-'}` },
                grid: { top: 10, bottom: 40, left: 60, right: 10 },
                xAxis: { type: 'category', data: icTsData.map((d: any) => d.date), axisLabel: { color: '#94A3B8', rotate: 30, fontSize: 10 } },
                yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                series: [{ type: 'line', data: icTsData.map((d: any) => d.value), showSymbol: false, lineStyle: { color: '#0077FA' }, areaStyle: { color: 'rgba(0,119,250,0.07)' } }],
              }}
              style={{ height: 180 }}
            />
          </div>
        )}
        {quantileReturns.length > 0 && (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>分层收益</div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis', formatter: (p: any[]) => `${p[0].name}: ${(p[0].value * 100).toFixed(3)}%` },
                grid: { top: 10, bottom: 30, left: 70, right: 10 },
                xAxis: { type: 'category', data: quantileReturns.map((r: any) => `Q${r.quantile}`), axisLabel: { color: '#94A3B8' } },
                yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                series: [{ type: 'bar', data: quantileReturns.map((r: any, i: number) => ({ value: r.mean_return, itemStyle: { color: colors[i % colors.length] } })) }],
              }}
              style={{ height: 180 }}
            />
          </div>
        )}
      </div>

      {cumulativeData && cumulativeData.length > 0 && (() => {
        const qKeys = Object.keys(cumulativeData[0]).filter(k => k.startsWith('quantile_')).sort();
        return (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>分层累计收益</div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis' },
                legend: { data: qKeys.map(k => k.replace('quantile_', 'Q')), textStyle: { color: '#94A3B8' }, top: 0 },
                grid: { top: 30, bottom: 40, left: 60, right: 10 },
                xAxis: { type: 'category', data: cumulativeData.map((d: any) => d.date), axisLabel: { color: '#94A3B8', rotate: 30, fontSize: 10 } },
                yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => v.toFixed(2) }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                series: qKeys.map((k, i) => ({ name: k.replace('quantile_', 'Q'), type: 'line', data: cumulativeData.map((d: any) => d[k]), showSymbol: false, lineStyle: { color: colors[i % colors.length] }, itemStyle: { color: colors[i % colors.length] } })),
              }}
              style={{ height: 200 }}
            />
          </div>
        );
      })()}

      {turnoverTs && (
        <div>
          <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>头尾分层换手率</div>
          <ReactECharts
            option={{
              backgroundColor: 'transparent',
              tooltip: { trigger: 'axis', formatter: (p: any[]) => p.map(s => `${s.seriesName}: ${s.value != null ? (s.value * 100).toFixed(2) + '%' : '-'}`).join('<br/>') },
              legend: { data: [turnoverTs.topLabel, turnoverTs.botLabel], textStyle: { color: '#94A3B8' }, top: 0 },
              grid: { top: 30, bottom: 40, left: 60, right: 10 },
              xAxis: { type: 'category', data: turnoverTs.dates, axisLabel: { color: '#94A3B8', rotate: 30, fontSize: 10 } },
              yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(0)}%` }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
              series: [
                { name: turnoverTs.topLabel, type: 'line', smooth: true, data: turnoverTs.top, showSymbol: false, lineStyle: { color: '#22c55e' }, itemStyle: { color: '#22c55e' } },
                { name: turnoverTs.botLabel, type: 'line', smooth: true, data: turnoverTs.bot, showSymbol: false, lineStyle: { color: '#ef4444' }, itemStyle: { color: '#ef4444' } },
              ],
            }}
            style={{ height: 180 }}
          />
        </div>
      )}
    </div>
  );
};

const AnalysisPanel: React.FC = () => {
  const message = useMessage();
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
    entryPrice,
    setEntryPrice,
    neutralize,
    setNeutralize,
    neutralizeControls,
    setNeutralizeControls,
    industryLevel,
    setIndustryLevel,
    resolvedConfig,
    taskStatus,
    analysisResult,
    loading,
    runLoading,
    analysisHistory,
    historyLoading,
    runAnalysis,
    loadAnalysis,
  } = useFactorAnalysis();

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
         
          onClick={async () => {
            try {
              const res = await productionApi.getLatestAlphalensAnalysis(selectedFactor);
              message.success('加载成功');
            } catch (error) {
              console.error('Failed to load analysis:', error);
              message.error('加载失败');
            }
          }}
        >
          查看
        </Button>
      ),
    },
  ];

  const labelStyle: React.CSSProperties = { color: 'var(--text-secondary)', fontSize: 11, marginBottom: 4 };
  const rowStyle: React.CSSProperties = { display: 'flex', gap: 12, alignItems: 'flex-end', flexWrap: 'wrap' };
  // 统一控件高度 28px（与 antd controlHeight token 一致）
  const ctrlH: React.CSSProperties = { height: 28 };

  const industryHint = industryLevel === 'industry_l1'
    ? (resolvedConfig?.industry_l1?.source_label || '未配置')
    : (resolvedConfig?.industry_l2?.source_label || '未配置');

  const neutralizeOptions = [
    {
      label: '有截距',
      value: 'market',
      hint: '去除因子截面均值（OLS 截距项）',
    },
    {
      label: '行业',
      value: 'industry',
      hint: industryHint,
    },
    {
      label: '市值',
      value: 'size',
      hint: resolvedConfig?.market_cap?.source_label || '未配置',
    },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }}>
        {/* 第一行：因子选择 */}
        <div style={{ marginBottom: 12 }}>
          <div style={labelStyle}>选择因子</div>
          <Select
            style={{ width: '100%', ...ctrlH }}
            placeholder="搜索或选择因子..."
            showSearch
            filterOption={(input, opt) =>
              String(opt?.label ?? '').toLowerCase().includes(input.toLowerCase())
            }
            value={selectedFactor || undefined}
            onChange={(v) => {
              setSelectedFactor(v as string);
              if (v) loadAnalysis(v as string);
            }}
            options={factors.map(f => ({
              label: f.description ? `${f.factor_id}  ·  ${f.description}` : f.factor_id,
              value: f.factor_id,
            }))}
            optionLabelProp="value"
          />
        </div>

        {/* 第二行：因子预处理 */}
        <div style={{ marginBottom: 12, padding: '8px 12px', background: 'var(--bg-tertiary)', borderRadius: 4 }}>
          <div style={{ ...labelStyle, marginBottom: 8 }}>因子预处理</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
            <Checkbox checked={neutralize} onChange={(e) => setNeutralize(e.target.checked!)}>
              因子中性化
            </Checkbox>
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
              {neutralizeOptions.map(opt => (
                <div key={opt.value} style={{ display: 'flex', alignItems: 'center', gap: 6, opacity: neutralize ? 1 : 0.4 }}>
                  <Checkbox
                    disabled={!neutralize}
                    checked={neutralizeControls.includes(opt.value)}
                    onChange={(e) => {
                      const next = e.target.checked
                        ? [...neutralizeControls, opt.value]
                        : neutralizeControls.filter(v => v !== opt.value);
                      setNeutralizeControls(next);
                    }}
                  >
                    {opt.label}
                  </Checkbox>
                  {opt.value === 'industry' ? (
                    <>
                      <Select
                        value={industryLevel}
                        onChange={(v) => setIndustryLevel(v as 'industry_l1' | 'industry_l2')}
                        size="small"
                        disabled={!neutralize || !neutralizeControls.includes('industry')}
                        style={{ width: 72 }}
                        options={[
                          { label: '一级', value: 'industry_l1' },
                          { label: '二级', value: 'industry_l2' },
                        ]}
                      />
                      <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                        {opt.hint}
                      </span>
                    </>
                  ) : (
                    <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                      {opt.hint}
                    </span>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* 第三行：交易设置 */}
        <div style={{ ...rowStyle, marginBottom: 12 }}>
          <div style={{ width: 160 }}>
            <div style={labelStyle}>股票池</div>
            <Select
              allowClear
              style={{ width: '100%', ...ctrlH }}
              placeholder="全市场"
              value={indexPool || undefined}
              onChange={(v) => setIndexPool(v as string)}
              options={indexPools.map(p => ({ label: p.name, value: p.code }))}
            />
          </div>
          <div style={{ width: 90 }}>
            <div style={labelStyle}>分层数</div>
            <InputNumber style={{ width: '100%', ...ctrlH }} value={quantiles} onChange={(v) => setQuantiles(v as number)} min={3} max={10} />
          </div>
          <div style={{ width: 200 }}>
            <div style={labelStyle}>持有周期</div>
            <Select
              mode="multiple"
              style={{ width: '100%', ...ctrlH }}
              value={periods}
              onChange={(v) => setPeriods(v as number[])}
              options={[1, 3, 5, 10, 20].map(p => ({ label: `${p}天`, value: p }))}
            />
          </div>
          <div style={{ width: 260 }}>
            <div style={labelStyle}>分析区间</div>
            <QuantDatePicker
              size="small"
              value={[startDate, endDate]}
              onChange={(s, e) => { setStartDate(s); setEndDate(e); }}
              disableFuture={false}
              style={{ width: '100%' }}
            />
          </div>
          <div style={{ width: 120 }}>
            <div style={labelStyle}>买入价格</div>
            <Select
              value={entryPrice}
              onChange={(v) => setEntryPrice(v as string)}
              style={{ width: '100%', ...ctrlH }}
              options={[
                { label: '次日开盘', value: 'open' },
                { label: '次日收盘', value: 'close' },
                { label: '次日最高', value: 'high' },
                { label: '次日最低', value: 'low' },
              ]}
            />
          </div>
          <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'flex-end', gap: 8 }}>
            {taskStatus === 'pending' && <Tag color="orange">等待中</Tag>}
            {taskStatus === 'running' && <Tag color="blue">分析中</Tag>}
            {taskStatus === 'completed' && <Tag color="green">已完成</Tag>}
            {taskStatus === 'failed' && <Tag color="red">失败</Tag>}
            <Button type="primary" icon={<BarChartOutlined />} loading={runLoading} onClick={runAnalysis}>
              运行分析
            </Button>
          </div>
        </div>
      </Card>

      {loading ? (
        <Spin style={{ display: 'block', margin: '60px auto' }} />
      ) : analysisResult ? (
        <>
          {/* ── 诊断信息（全局） ── */}
          {analysisResult?.diagnostics?.pipeline_stats && (
            <Collapse style={{ marginBottom: 16 }} items={[{
              key: 'pipeline',
              label: '数据覆盖率流水线',
              children: (
                <>
                  <Table
                    size="small"
                    dataSource={analysisResult.diagnostics.pipeline_stats}
                    columns={[
                      { title: '步骤', dataIndex: 'step' },
                      { title: '剩余行数', dataIndex: 'total_rows' },
                      { title: '过滤行数', dataIndex: 'dropped' },
                      { title: '过滤比例', dataIndex: 'drop_pct', render: (v: number) => <Tag color={v > 20 ? 'orange' : 'green'}>{v != null ? v.toFixed(1) : '0.0'}%</Tag> },
                    ]}
                    pagination={false}
                  />
                  <div style={{ marginTop: 8, color: '#94A3B8', fontSize: 12 }}>
                    最终有效数据：{analysisResult.diagnostics.final_rows} 行，
                    {analysisResult.diagnostics.final_dates} 个交易日，
                    日均覆盖 {analysisResult.diagnostics.avg_daily_coverage?.toFixed(0)} 只股票
                  </div>
                </>
              ),
            }]} />
          )}

          {analysisResult?.diagnostics?.warnings?.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              {analysisResult.diagnostics.warnings.map((w: any, i: number) => (
                <div key={i} style={{ padding: '10px 16px', marginBottom: 8, borderRadius: 6, background: w.level === 'WARNING' ? 'rgba(255,186,0,0.1)' : 'rgba(0,100,250,0.08)', border: `1px solid ${w.level === 'WARNING' ? '#FFBA00' : '#0064FA'}`, display: 'flex', alignItems: 'center', gap: 8 }}>
                  <Tag color={w.level === 'WARNING' ? 'orange' : 'blue'}>{w.level}</Tag>
                  <span style={{ color: 'var(--text-primary)', fontSize: 13 }}>{w.message}</span>
                </div>
              ))}
            </div>
          )}

          {analysisResult?.diagnostics?.distribution && (
            <Collapse style={{ marginBottom: 16 }} items={[{
              key: 'dist',
              label: '因子分布统计',
              children: (
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
                  {[
                    { label: '样本数', value: analysisResult.diagnostics.distribution.count },
                    { label: '均值', value: analysisResult.diagnostics.distribution.mean?.toFixed(4) },
                    { label: '标准差', value: analysisResult.diagnostics.distribution.std?.toFixed(4) },
                    { label: '偏度', value: analysisResult.diagnostics.distribution.skew?.toFixed(4) },
                    { label: '峰度', value: analysisResult.diagnostics.distribution.kurtosis?.toFixed(4) },
                    { label: '最小值', value: analysisResult.diagnostics.distribution.min?.toFixed(4) },
                    { label: 'P25', value: analysisResult.diagnostics.distribution.p25?.toFixed(4) },
                    { label: '中位数', value: analysisResult.diagnostics.distribution.median?.toFixed(4) },
                    { label: 'P75', value: analysisResult.diagnostics.distribution.p75?.toFixed(4) },
                    { label: '最大值', value: analysisResult.diagnostics.distribution.max?.toFixed(4) },
                  ].map(item => (
                    <div key={item.label} style={{ textAlign: 'center' }}>
                      <div style={{ color: 'var(--text-secondary)', fontSize: 11 }}>{item.label}</div>
                      <div style={{ color: 'var(--text-primary)', fontSize: 14, fontWeight: 600 }}>{item.value ?? '-'}</div>
                    </div>
                  ))}
                </div>
              ),
            }]} />
          )}

          {/* ── IC Decay（全局，展示不同 lag 的 IC 衰减） ── */}
          {analysisResult?.ic_decay?.length > 0 && (
            <Card style={{ marginBottom: 16, background: 'var(--bg-card)' }} title={<span style={{ color: 'var(--text-secondary)' }}>IC Decay</span>}>
              <ReactECharts
                option={{
                  backgroundColor: 'transparent',
                  tooltip: { trigger: 'axis' },
                  legend: { data: ['Pearson IC', 'Rank IC'], textStyle: { color: '#94A3B8' }, top: 0 },
                  grid: { top: 40, bottom: 30, left: 60, right: 20 },
                  xAxis: { type: 'category', data: analysisResult.ic_decay.map((d: any) => `${d.lag}D`), axisLabel: { color: '#94A3B8' } },
                  yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                  series: [
                    { name: 'Pearson IC', type: 'bar', data: analysisResult.ic_decay.map((d: any) => d.ic), itemStyle: { color: '#0077FA' } },
                    { name: 'Rank IC', type: 'bar', data: analysisResult.ic_decay.map((d: any) => d.rank_ic), itemStyle: { color: '#10B981' } },
                  ],
                }}
                style={{ height: 260 }}
              />
            </Card>
          )}

          {/* ── 因子排名自相关（全局） ── */}
          {analysisResult?.decay_analysis && Object.keys(analysisResult.decay_analysis).length > 0 && (
            <DecayAnalysisSection data={analysisResult.decay_analysis} />
          )}

          {/* ── 分行业分析（全局） ── */}
          {analysisResult?.ic_by_industry && Object.keys(analysisResult.ic_by_industry).length > 0 && (
            <IndustryAnalysisSection
              icByIndustry={analysisResult.ic_by_industry}
              returnsByIndustry={analysisResult.returns_by_industry || {}}
            />
          )}

          {/* ── 分组分析（有 groupby 时） ── */}
          {analysisResult?.ic_by_group && Object.keys(analysisResult.ic_by_group).length > 0 && (
            <GroupAnalysisSection icByGroup={analysisResult.ic_by_group} returnsByGroup={analysisResult.returns_by_group} />
          )}

          {/* ── 按持有周期折叠展示 ── */}
          {(() => {
            const resultPeriods: number[] = (analysisResult?.ic_by_period || [])
              .map((r: any) => parseInt(r.period))
              .filter(Boolean)
              .sort((a: number, b: number) => a - b);
            if (!resultPeriods.length) return null;
            return (
              <Collapse
                style={{ marginTop: 16 }}
                items={resultPeriods.map((p, idx) => ({
                  key: String(p),
                  label: (
                    <span style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
                      持有 {p} 天
                      {(() => {
                        const row = (analysisResult?.ic_by_period || []).find((r: any) => r.period === `${p}D`);
                        if (!row) return null;
                        const icColor = row.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)';
                        const sigColor = row.p_value < 0.05 ? 'var(--color-gain)' : 'var(--text-muted)';
                        return (
                          <span style={{ marginLeft: 16, fontSize: 12, fontWeight: 400 }}>
                            <span style={{ color: 'var(--text-muted)' }}>IC均值 </span>
                            <span style={{ color: icColor }}>{row.ic_mean?.toFixed(4)}</span>
                            <span style={{ color: 'var(--text-muted)', marginLeft: 12 }}>ICIR </span>
                            <span style={{ color: 'var(--text-secondary)' }}>{row.ic_ir?.toFixed(4)}</span>
                            <span style={{ color: 'var(--text-muted)', marginLeft: 12 }}>p值 </span>
                            <span style={{ color: sigColor }}>{row.p_value?.toFixed(4)}</span>
                          </span>
                        );
                      })()}
                    </span>
                  ),
                  children: (
                    <PeriodPanel
                      period={p}
                      analysisResult={analysisResult}
                      isFirstPeriod={idx === 0}
                    />
                  ),
                }))}
              />
            );
          })()}
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

              pagination={{ pageSize: 5 }}
            />
          </Spin>
        </Card>
      )}
    </div>
  );
};

export default AnalysisPanel;
