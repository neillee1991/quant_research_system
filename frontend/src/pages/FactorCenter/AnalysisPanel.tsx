/**
 * 因子分析面板
 */

import React, { useMemo, useState, useEffect } from 'react';
import {
  Card, Button, Select, InputNumber, Spin, Empty, Table, Tag, Checkbox, Collapse,
} from 'antd';
import { BarChartOutlined, ApartmentOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import ReactECharts from 'echarts-for-react';
import QuantDatePicker from '../../components/QuantDatePicker';
import { useFactorAnalysis } from './hooks/useFactorAnalysis';
import { productionApi } from '../../api';
import { useTaskLogs } from '../../hooks/useTaskLogs';
import FactorFlowDrawer from './FactorFlowDrawer';
import TaskLogTable from '../../components/TaskLogTable';

/** 确保值是数组，避免 DB 返回 {} 时 .map 报错 */
const asArr = <T = any,>(v: T[] | null | undefined | unknown): T[] => (Array.isArray(v) ? (v as T[]) : []);

/** 统一日期格式化：YYYYMMDD 或 datetime 字符串 → YYYY-MM-DD */
const formatDate = (d: string | null | undefined) => {
  if (!d) return '-';
  const dateStr = String(d);
  if (/^\d{8}$/.test(dateStr)) return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
  return dateStr.slice(0, 10); // 截断时间部分
};

/** 从任意日期字符串提取 YYYYMMDD */
const toYYYYMMDD = (d: string | null | undefined): string => {
  if (!d) return '';
  const dateStr = String(d);
  if (/^\d{8}$/.test(dateStr)) return dateStr;
  return dateStr.slice(0, 10).replace(/-/g, '');
};

/** 加载交易日 Set，用于过滤非交易日 */
const useTradingDaySet = (startDate?: string, endDate?: string): Set<string> => {
  const [tradingDaySet, setTradingDaySet] = useState<Set<string>>(new Set());
  useEffect(() => {
    if (!startDate || !endDate) return;
    const start = toYYYYMMDD(startDate);
    const end = toYYYYMMDD(endDate);
    if (!start || !end) return;
    productionApi.getTradingDays(start, end)
      .then((res: { data?: { data?: string[] } }) => setTradingDaySet(new Set(res.data?.data || [])))
      .catch(() => {});
  }, [startDate, endDate]);
  return tradingDaySet;
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
 * 单周期分析面板（在 Collapse 内展开）
 */
const PeriodPanel: React.FC<{ period: number; analysisResult: any; isFirstPeriod: boolean }> = ({
  period, analysisResult, isFirstPeriod,
}) => {
  const periodKey = `${period}D`;
  const icTsCol = `ic_${periodKey}`;
  const turnoverKey = `period_${period}`;
  const colors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0077FA', '#8b5cf6'];

  const tradingDaySet = useTradingDaySet(analysisResult?.start_date, analysisResult?.end_date);
  const isTradingDay = (d: string) => tradingDaySet.size === 0 || tradingDaySet.has(toYYYYMMDD(d));

  const icRow = useMemo(
    () => asArr(analysisResult?.ic_by_period).find((r: any) => r.period === periodKey),
    [analysisResult, periodKey]
  );
  const rankIcRow = useMemo(
    () => asArr(analysisResult?.rank_ic_by_period).find((r: any) => r.period === periodKey),
    [analysisResult, periodKey]
  );
  const icTsData = useMemo(() => {
    return asArr(analysisResult?.ic_ts)
      .filter((d: any) => d[icTsCol] != null && isTradingDay(d.date))
      .map((d: any) => ({ date: formatDate(d.date), value: d[icTsCol] }));
  }, [analysisResult, icTsCol, tradingDaySet]);
  const quantileReturns = useMemo(
    () => asArr(analysisResult?.quantile_returns).filter((r: any) => r.period === periodKey),
    [analysisResult, periodKey]
  );
  const cumulativeData = useMemo(() => {
    const raw = (analysisResult?.cumulative_returns?.[periodKey]) || [];
    const qKeys = Object.keys(raw[0] || {}).filter(k => k.startsWith('quantile_')).sort();
    return raw
      .filter((d: any) => qKeys.some(k => d[k] != null) && isTradingDay(d.date))
      .map((d: any) => ({ ...d, date: formatDate(d.date) }));
  }, [analysisResult, periodKey, tradingDaySet]);
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
      (tv[topQ]?.[turnoverKey]?.[d] != null || tv[botQ]?.[turnoverKey]?.[d] != null) && isTradingDay(d)
    );
    if (!dates.length) return null;
    return {
      dates: dates.map(formatDate),
      top: dates.map(d => tv[topQ]?.[turnoverKey]?.[d] ?? null),
      bot: dates.map(d => tv[botQ]?.[turnoverKey]?.[d] ?? null),
      topLabel: `Q${topQ.replace('quantile_', '')}`,
      botLabel: `Q${botQ.replace('quantile_', '')}`,
    };
  }, [analysisResult, turnoverKey, tradingDaySet]);

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
              <StatCard label="IC 均值" value={icRow.ic_mean?.toFixed(4)} color={icRow.ic_mean != null && icRow.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)'} />
              <StatCard label="IC 标准差" value={icRow.ic_std?.toFixed(4)} color="var(--text-primary)" />
              <StatCard label="ICIR" value={icRow.ic_ir?.toFixed(4)} color={icRow.ic_ir != null && Math.abs(icRow.ic_ir) > 0.5 ? 'var(--color-primary)' : 'var(--text-primary)'} />
              <StatCard label="IC 胜率" value={icRow.ic_win_rate != null ? `${(icRow.ic_win_rate * 100).toFixed(1)}%` : '-'} color={icRow.ic_win_rate != null && icRow.ic_win_rate > 0.5 ? 'var(--color-gain)' : 'var(--text-primary)'} />
              <StatCard label="t 统计量" value={icRow.t_stat?.toFixed(3)} color={Math.abs(icRow.t_stat ?? 0) > 1.96 ? 'var(--color-gain)' : 'var(--text-secondary)'} />
              <StatCard label="p 值" value={`${icRow.p_value?.toFixed(4)}${sigLabel(icRow.p_value)}`} color={icRow.p_value < 0.05 ? 'var(--color-gain)' : 'var(--text-secondary)'} />
              <StatCard label="样本数" value={String(icRow.n_obs ?? '-')} color="var(--text-primary)" />
              {(() => {
                const abRow = asArr(analysisResult?.alpha_beta).find((r: any) => r.period === periodKey);
                if (!abRow) return null;
                return (
                  <>
                    <StatCard
                      label="年化 Alpha"
                      value={abRow.ann_alpha != null ? `${(abRow.ann_alpha * 100).toFixed(2)}%` : '-'}
                      color={abRow.ann_alpha > 0 ? 'var(--color-gain)' : 'var(--color-loss)'}
                    />
                    <StatCard
                      label="Beta"
                      value={abRow.beta?.toFixed(3)}
                      color={abRow.beta != null && Math.abs(abRow.beta) < 0.1 ? 'var(--color-gain)' : 'var(--text-secondary)'}
                    />
                  </>
                );
              })()}
            </div>
          </div>
          {rankIcRow && (
            <div>
              <div style={{ color: 'var(--text-muted)', fontSize: 11, marginBottom: 6 }}>Rank IC (Spearman)</div>
              <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                <StatCard label="Rank IC 均值" value={rankIcRow.ic_mean?.toFixed(4)} color={rankIcRow.ic_mean != null && rankIcRow.ic_mean > 0 ? 'var(--color-gain)' : 'var(--color-loss)'} />
                <StatCard label="Rank ICIR" value={rankIcRow.ic_ir?.toFixed(4)} color={rankIcRow.ic_ir != null && Math.abs(rankIcRow.ic_ir) > 0.5 ? 'var(--color-primary)' : 'var(--text-primary)'} />
                <StatCard label="Rank IC 胜率" value={rankIcRow.ic_win_rate != null ? `${(rankIcRow.ic_win_rate * 100).toFixed(1)}%` : '-'} color={rankIcRow.ic_win_rate != null && rankIcRow.ic_win_rate > 0.5 ? 'var(--color-gain)' : 'var(--text-primary)'} />
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
              style={{ height: 220 }}
            />
          </div>
        )}
        {/* 月度 IC */}
        {(() => {
          const monthlyIc = asArr(analysisResult?.ic_by_month);
          const filtered = monthlyIc.filter((d: any) => d[icTsCol.replace('ic_', '')] != null);
          if (!filtered.length) return null;
          const months = filtered.map((d: any) => d.month);
          const values = filtered.map((d: any) => d[icTsCol.replace('ic_', '')] ?? null);
          const maxAbs = Math.max(...values.filter(Boolean).map(Math.abs), 0.01);
          return (
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>
                月度 IC
                <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
                  看因子预测能力的季节性和时段分布，颜色越深越强
                </span>
              </div>
              <ReactECharts
                option={{
                  backgroundColor: 'transparent',
                  tooltip: { trigger: 'axis', formatter: (p: any[]) => `${p[0].name}: ${p[0].value?.toFixed(4) ?? '-'}` },
                  grid: { top: 10, bottom: 50, left: 60, right: 10 },
                  xAxis: { type: 'category', data: months.map((m: string) => m.slice(0, 7)), axisLabel: { color: '#94A3B8', rotate: 45, fontSize: 10 } },
                  yAxis: { type: 'value', axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                  series: [{
                    type: 'bar',
                    data: values.map((v: number | null) => ({
                      value: v,
                      itemStyle: {
                        color: v == null ? 'transparent'
                          : v >= 0
                            ? `rgba(34,197,94,${Math.min(0.3 + Math.abs(v) / maxAbs * 0.7, 1)})`
                            : `rgba(239,68,68,${Math.min(0.3 + Math.abs(v) / maxAbs * 0.7, 1)})`,
                      },
                    })),
                  }],
                }}
                style={{ height: 220 }}
              />
            </div>
          );
        })()}
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
              style={{ height: 220 }}
            />
          </div>
        )}
        {(() => {
          const icByInd = analysisResult?.ic_by_industry;
          if (!icByInd || !Object.keys(icByInd).length) return null;
          const icValues: Record<string, number> = {};
          Object.keys(icByInd).forEach(ind => {
            const v = icByInd[ind]?.[periodKey];
            if (v != null) icValues[ind] = v;
          });
          const sortedInds = Object.keys(icValues).sort((a, b) => icValues[b] - icValues[a]);
          if (!sortedInds.length) return null;
          const icColors = sortedInds.map(ind => icValues[ind] >= 0 ? '#0077FA' : '#ef4444');
          return (
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>各行业 IC 均值</div>
              <ReactECharts
                option={{
                  backgroundColor: 'transparent',
                  tooltip: { trigger: 'axis' },
                  grid: { top: 10, bottom: 80, left: 60, right: 10 },
                  xAxis: { type: 'category', data: sortedInds, axisLabel: { color: '#94A3B8', rotate: 40, fontSize: 10 } },
                  yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => v.toFixed(3) }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                  series: [{ type: 'bar', data: sortedInds.map((ind, i) => ({ value: icValues[ind], itemStyle: { color: icColors[i] } })) }],
                }}
                style={{ height: 220 }}
              />
            </div>
          );
        })()}
      </div>

      {/* 各行业 Q5-Q1 Spread */}
      {(() => {
        const icByInd = analysisResult?.ic_by_industry;
        const retByInd = analysisResult?.returns_by_industry;
        if (!icByInd || !retByInd) return null;
        const icValues: Record<string, number> = {};
        Object.keys(icByInd).forEach(ind => {
          const v = icByInd[ind]?.[periodKey];
          if (v != null) icValues[ind] = v;
        });
        const sortedInds = Object.keys(icValues).sort((a, b) => icValues[b] - icValues[a]);
        const spreadByInd: Record<string, number> = {};
        sortedInds.forEach(ind => {
          const rows = (retByInd[ind] || []).filter((r: any) => r.period === periodKey);
          const q5 = rows.find((r: any) => r.quantile === Math.max(...rows.map((x: any) => x.quantile)));
          const q1 = rows.find((r: any) => r.quantile === Math.min(...rows.map((x: any) => x.quantile)));
          if (q5 && q1) spreadByInd[ind] = q5.mean_return - q1.mean_return;
        });
        if (!Object.keys(spreadByInd).length) return null;
        const spreadColors = sortedInds.map(ind => (spreadByInd[ind] ?? 0) >= 0 ? '#22c55e' : '#ef4444');
        return (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>各行业 Q5-Q1 Spread</div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis' },
                grid: { top: 10, bottom: 80, left: 70, right: 10 },
                xAxis: { type: 'category', data: sortedInds, axisLabel: { color: '#94A3B8', rotate: 40, fontSize: 10 } },
                yAxis: { type: 'value', axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                series: [{ type: 'bar', data: sortedInds.map((ind, i) => ({ value: spreadByInd[ind] ?? null, itemStyle: { color: spreadColors[i] } })) }],
              }}
              style={{ height: 220 }}
            />
          </div>
        );
      })()}

      {/* Q5-Q1 多空 Spread 时序 */}
      {(() => {
        const spreadMap = analysisResult?.spread_ts?.[periodKey] || {};
        const allDates = Object.keys(spreadMap).sort();
        const tradingDates = allDates.filter(d => spreadMap[d] != null && isTradingDay(d));
        if (!tradingDates.length) return null;
        const spreadValues = tradingDates.map(d => spreadMap[d]);
        return (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>
              Q5-Q1 多空 Spread 时序
              <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
                逐日多空收益差，均值越大越好，负值天数占比越低越好
              </span>
            </div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: {
                  trigger: 'axis',
                  formatter: (p: any[]) => `${p[0].name}: ${p[0].value != null ? (p[0].value * 100).toFixed(3) + '%' : '-'}`,
                },
                grid: { top: 10, bottom: 40, left: 70, right: 10 },
                xAxis: {
                  type: 'category',
                  data: tradingDates.map(formatDate),
                  axisLabel: { color: '#94A3B8', rotate: 30, fontSize: 10 },
                },
                yAxis: {
                  type: 'value',
                  axisLabel: { color: '#94A3B8', formatter: (v: number) => `${(v * 100).toFixed(2)}%` },
                  splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
                },
                series: [{
                  type: 'bar',
                  data: spreadValues.map((v: number | null) => ({
                    value: v,
                    itemStyle: { color: (v ?? 0) >= 0 ? '#22c55e' : '#ef4444' },
                  })),
                }],
              }}
              style={{ height: 160 }}
            />
          </div>
        );
      })()}

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
                yAxis: { type: 'value', scale: true, axisLabel: { color: '#94A3B8', formatter: (v: number) => v.toFixed(2) }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                series: qKeys.map((k, i) => ({ name: k.replace('quantile_', 'Q'), type: 'line', data: cumulativeData.map((d: any) => d[k]), showSymbol: false, lineStyle: { color: colors[i % colors.length] }, itemStyle: { color: colors[i % colors.length] } })),
              }}
              style={{ height: 200 }}
            />
          </div>
        );
      })()}

      {/* 因子加权多空组合净值 */}
      {(() => {
        const fcr = asArr(analysisResult?.factor_cumulative_returns);
        const tradingFcr = fcr.filter((d: any) => d[periodKey] != null && isTradingDay(d.date));
        if (!tradingFcr.length) return null;
        return (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>
              因子加权多空组合净值
              <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
                按因子值连续加权的多空组合，比分位数更精细，持续上升为好
              </span>
            </div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis' },
                grid: { top: 10, bottom: 40, left: 60, right: 10 },
                xAxis: {
                  type: 'category',
                  data: tradingFcr.map((d: any) => formatDate(d.date)),
                  axisLabel: { color: '#94A3B8', rotate: 30, fontSize: 10 },
                },
                yAxis: {
                  type: 'value',
                  scale: true,
                  axisLabel: { color: '#94A3B8', formatter: (v: number) => v.toFixed(2) },
                  splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
                },
                series: [{
                  name: periodKey,
                  type: 'line',
                  data: tradingFcr.map((d: any) => d[periodKey] ?? null),
                  showSymbol: false,
                  lineStyle: { color: '#0077FA' },
                  itemStyle: { color: '#0077FA' },
                  areaStyle: { color: 'rgba(0,119,250,0.07)' },
                }],
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

      {/* 因子排名自相关 */}
      {(() => {
        const decayData = analysisResult?.decay_analysis?.[periodKey];
        if (!decayData || !Object.keys(decayData).length) return null;
        const entries = Object.entries(decayData as Record<string, number>)
          .filter(([d, v]) => v != null && isTradingDay(d))
          .sort(([a], [b]) => a.localeCompare(b));
        if (!entries.length) return null;
        return (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>
              因子排名自相关
              <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
                lag={period}天，值越高说明因子信号持续性越强
              </span>
            </div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis', formatter: (p: any[]) => `${p[0].name}: ${p[0].value?.toFixed(4)}` },
                grid: { top: 10, bottom: 40, left: 60, right: 10 },
                xAxis: { type: 'category', data: entries.map(([d]) => formatDate(d)), axisLabel: { color: '#94A3B8', rotate: 30, fontSize: 10 } },
                yAxis: { type: 'value', scale: true, axisLabel: { color: '#94A3B8' }, splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } } },
                series: [{
                  type: 'line',
                  showSymbol: false,
                  data: entries.map(([, v]) => v),
                  lineStyle: { color: '#0077FA' },
                  areaStyle: { color: 'rgba(0,119,250,0.07)' },
                }],
              }}
              style={{ height: 180 }}
            />
          </div>
        );
      })()}

      {/* 事件研究：因子形成前后累积收益 */}
      {(() => {
        const es = analysisResult?.event_study;
        if (!es || !Object.keys(es).length) return null;
        const quantileKeys = Object.keys(es).sort((a, b) => Number(a) - Number(b));
        const firstQ = quantileKeys[0];
        const offsets = Object.keys(es[firstQ]?.mean || {}).sort((a, b) => Number(a) - Number(b));
        if (!offsets.length) return null;
        const esColors = ['#ef4444', '#f97316', '#eab308', '#22c55e', '#0077FA', '#8b5cf6'];
        return (
          <div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 6 }}>
              事件研究：因子形成前后累积收益
              <span style={{ color: 'var(--text-muted)', fontSize: 11, marginLeft: 8 }}>
                t=0 为因子形成日，正值区间收益持续上升说明信号有效；t=0 前已有收益需警惕前视偏差
              </span>
            </div>
            <ReactECharts
              option={{
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis' },
                legend: {
                  data: quantileKeys.map(q => `Q${q}`),
                  textStyle: { color: '#94A3B8' },
                  top: 0,
                },
                grid: { top: 30, bottom: 30, left: 60, right: 10 },
                xAxis: {
                  type: 'category',
                  data: offsets.map(o => `t${Number(o) >= 0 ? '+' : ''}${o}`),
                  axisLabel: { color: '#94A3B8' },
                  axisLine: { lineStyle: { color: 'rgba(148,163,184,0.3)' } },
                  markLine: undefined,
                },
                yAxis: {
                  type: 'value',
                  axisLabel: {
                    color: '#94A3B8',
                    formatter: (v: number) => `${(v * 100).toFixed(2)}%`,
                  },
                  splitLine: { lineStyle: { color: 'rgba(148,163,184,0.15)' } },
                },
                series: quantileKeys.map((q, i) => ({
                  name: `Q${q}`,
                  type: 'line',
                  smooth: true,
                  data: offsets.map(o => {
                    const v = es[q]?.mean?.[o];
                    return v != null ? v : null;
                  }),
                  showSymbol: false,
                  lineStyle: { color: esColors[i % esColors.length] },
                  itemStyle: { color: esColors[i % esColors.length] },
                })),
              }}
              style={{ height: 220 }}
            />
          </div>
        );
      })()}

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
    winsorize,
    setWinsorize,
    winsorizeLower,
    setWinsorizeLower,
    winsorizeUpper,
    setWinsorizeUpper,
    resolvedConfig,
    taskStatus,
    analysisResult,
    loading,
    runLoading,
    runAnalysis,
    loadAnalysis,
    loadAnalysisById,
  } = useFactorAnalysis();

  const { logs: analysisHistory, loading: historyLoading, loadLogs: loadAnalysisHistory } = useTaskLogs('analysis', 50);

  // 初始化时加载分析历史
  React.useEffect(() => { loadAnalysisHistory(); }, [loadAnalysisHistory]);

  const [flowDrawerOpen, setFlowDrawerOpen] = React.useState<boolean>(false);

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

        {/* 第二行：查看版本 */}
        {selectedFactor && (
          <div style={{ marginBottom: 12 }}>
            <div style={labelStyle}>查看版本</div>
            <Select
              allowClear
              style={{ width: '100%', ...ctrlH }}
              placeholder="默认显示最新分析结果，可切换历史版本"
              showSearch
              filterOption={(input, opt) =>
                (opt?.label as string)?.toLowerCase().includes(input.toLowerCase())
              }
              onChange={(runId: string | undefined) => {
                if (!runId) {
                  if (selectedFactor) loadAnalysis(selectedFactor);
                  return;
                }
                const record = analysisHistory.find(r => r.run_id === runId);
                if (!record) return;
                try {
                  let extra;
                  if (typeof record.extra === 'string') {
                    // 如果是字符串，尝试解析为 JSON 对象
                    extra = JSON.parse(record.extra || '{}');
                  } else if (typeof record.extra === 'object') {
                    // 如果是对象，直接使用
                    extra = record.extra || {};
                  } else {
                    // 如果是其他类型，使用空对象
                    extra = {};
                  }
                  if (extra.result_id) {
                    loadAnalysisById(record.task_id, extra.result_id);
                  }
                } catch { /* ignore */ }
              }}
              options={analysisHistory
                .filter(r => r.status === 'success' && r.task_id === selectedFactor)
                .map(r => {
                  let paramsSummary = '';
                  try {
                    let p;
                    if (typeof r.params === 'string') {
                      // 如果是字符串，尝试解析为 JSON 对象
                      p = JSON.parse(r.params || '{}');
                    } else if (typeof r.params === 'object') {
                      // 如果是对象，直接使用
                      p = r.params || {};
                    } else {
                      // 如果是其他类型，使用空对象
                      p = {};
                    }
                    const parts: string[] = [];
                    if (p.start_date || p.end_date) parts.push(`${p.start_date || ''}~${p.end_date || ''}`);
                    if (p.periods) parts.push(`T+${Array.isArray(p.periods) ? p.periods.join(',') : p.periods}`);
                    if (p.quantiles) parts.push(`${p.quantiles}分组`);
                    if (p.index_pool) parts.push(p.index_pool);
                    paramsSummary = parts.join(' ');
                  } catch { /* ignore */ }
                  return {
                    value: r.run_id,
                    label: `${(r.started_at || '').slice(0, 19)}${paramsSummary ? `  ·  ${paramsSummary}` : ''}`,
                  };
                })}
            />
          </div>
        )}

        {/* 第三行：因子预处理 */}
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
                        size="middle"
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
          <div style={{ marginTop: 12, display: 'flex', alignItems: 'center', gap: 16, flexWrap: 'wrap' }}>
            <Checkbox checked={winsorize} onChange={(e) => setWinsorize(e.target.checked!)}>
              极端值处理 (Winsorize)
            </Checkbox>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, opacity: winsorize ? 1 : 0.4 }}>
              <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>下界:</span>
              <InputNumber
                size="middle"
                disabled={!winsorize}
                value={winsorizeLower}
                onChange={(v) => setWinsorizeLower(v as number)}
                min={0}
                max={0.49}
                step={0.01}
                style={{ width: 80 }}
              />
              <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>上界:</span>
              <InputNumber
                size="middle"
                disabled={!winsorize}
                value={winsorizeUpper}
                onChange={(v) => setWinsorizeUpper(v as number)}
                min={0.51}
                max={1}
                step={0.01}
                style={{ width: 80 }}
              />
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                保留 [{(winsorizeLower * 100).toFixed(0)}%, {(winsorizeUpper * 100).toFixed(0)}%] 分位数范围内的值
              </span>
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
              size="middle"
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
            <Button icon={<ApartmentOutlined />} onClick={() => setFlowDrawerOpen(true)}>
              分布框架
            </Button>
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
                    size="middle"
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

          {/* ── 分组分析（有 groupby 时） ── */}
          {analysisResult?.ic_by_group && Object.keys(analysisResult.ic_by_group).length > 0 && (
            <GroupAnalysisSection icByGroup={analysisResult.ic_by_group} returnsByGroup={analysisResult.returns_by_group} />
          )}

          {/* ── 按持有周期折叠展示 ── */}
          {(() => {
            const resultPeriods: number[] = asArr(analysisResult?.ic_by_period)
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
                        const row = asArr(analysisResult?.ic_by_period).find((r: any) => r.period === `${p}D`);
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

      <Card
        style={{ marginTop: 16, background: 'var(--bg-card)' }}
        title={<span style={{ color: 'var(--text-secondary)' }}>分析历史</span>}
      >
        <TaskLogTable
          logs={analysisHistory}
          loading={historyLoading}
          taskIdLabel="因子ID"
          onFilter={loadAnalysisHistory}
        />
      </Card>

      <FactorFlowDrawer
        open={flowDrawerOpen}
        onClose={() => setFlowDrawerOpen(false)}
        factorId={selectedFactor}
      />
    </div>
  );
};

export default AnalysisPanel;
