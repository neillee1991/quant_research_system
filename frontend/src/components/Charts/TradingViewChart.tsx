import React, { useEffect, useRef, useState } from 'react';
import { createChart, ColorType, IChartApi, CandlestickSeries, HistogramSeries, LineSeries, AreaSeries } from 'lightweight-charts';
import { Button, Tooltip, Dropdown, Modal, Form, InputNumber, Select, TagInput } from '@douyinfe/semi-ui';
import {
  IconLineChartStroked,
  IconDeleteStroked,
  IconUndo,
  IconSetting,
  IconCamera,
  IconPlusStroked,
  IconMinusStroked,
  IconBarChartVStroked,
  IconMinusCircleStroked,
  IconGridRectangle,
  IconArrowUp,
  IconFont,
  IconCandlestickChartStroked,
} from '@douyinfe/semi-icons';
import {
  calculateSMA,
  calculateEMA,
  calculateBOLL,
  calculateMACD,
  calculateRSI,
  calculateKDJ,
  type CandleData,
} from '../../utils/indicators';

interface TradingViewChartProps {
  data: Array<{
    trade_date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    vol: number;
  }>;
  buyPoints?: string[];
  sellPoints?: string[];
}

const DEFAULT_COLORS = ['#ff6b6b', '#4ecdc4', '#ffe66d', '#a8dadc', '#ff9ff3', '#54a0ff', '#5f27cd', '#01a3a4'];

const CHART_OPTIONS_BASE = {
  layout: {
    background: { type: ColorType.Solid, color: 'transparent' },
    textColor: '#94a3b8',
  },
  grid: {
    vertLines: { color: 'rgba(128, 128, 128, 0.15)' },
    horzLines: { color: 'rgba(128, 128, 128, 0.15)' },
  },
  timeScale: {
    borderColor: 'rgba(128, 128, 128, 0.2)',
    timeVisible: true,
  },
  rightPriceScale: {
    borderColor: 'rgba(128, 128, 128, 0.2)',
    autoScale: true,
  },
  handleScroll: {
    mouseWheel: true,
    pressedMouseMove: true,
    horzTouchDrag: true,
    vertTouchDrag: false,
  },
};

const TradingViewChart: React.FC<TradingViewChartProps> = ({ data, buyPoints = [], sellPoints = [] }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const subChartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const subChartRef = useRef<IChartApi | null>(null);
  const mainSeriesRef = useRef<any>(null);
  const volumeSeriesRef = useRef<any>(null);
  const indicatorSeriesRef = useRef<Map<string, { series: any; chart: 'main' | 'sub' }>>(new Map());

  const [drawingMode, setDrawingMode] = useState(false);
  const [drawingTool, setDrawingTool] = useState<'trendline' | 'horizontal' | 'rectangle' | 'arrow' | 'text'>('trendline');
  const [lines, setLines] = useState<any[]>([]);
  const [drawingStart, setDrawingStart] = useState<any>(null);
  const [chartType, setChartType] = useState<'candlestick' | 'line' | 'area'>('candlestick');
  const [indicators, setIndicators] = useState<string[]>([]);
  const [settingsVisible, setSettingsVisible] = useState(false);
  const [currentIndicator, setCurrentIndicator] = useState<string>('');
  const [indicatorParams, setIndicatorParams] = useState<any>({
    ma: { periods: [5, 10, 20, 60] },
    ema: { periods: [12, 26] },
    boll: { period: 20, stdDev: 2 },
    macd: { fastPeriod: 12, slowPeriod: 26, signalPeriod: 9 },
    rsi: { period: 14 },
    kdj: { period: 9, k: 3, d: 3 },
  });

  const hasOscillator = indicators.some(ind => ['macd', 'rsi', 'kdj'].includes(ind));

  // Track crosshair position for OHLC + indicator display
  const [crosshairData, setCrosshairData] = useState<any>(null);
  // ---- Chart initialization ----
  useEffect(() => {
    if (!chartContainerRef.current) return;
    const chart = createChart(chartContainerRef.current, {
      ...CHART_OPTIONS_BASE,
      width: chartContainerRef.current.clientWidth,
      height: 500,
    });
    chartRef.current = chart;

    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: 'var(--color-accent)',
      priceFormat: { type: 'volume' },
      priceScaleId: '',
    });
    volumeSeriesRef.current = volumeSeries;
    chart.priceScale('').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });

    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };
    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  // ---- Sub-chart lifecycle: create/destroy based on hasOscillator ----
  useEffect(() => {
    if (hasOscillator && subChartContainerRef.current && !subChartRef.current) {
      const subChart = createChart(subChartContainerRef.current, {
        ...CHART_OPTIONS_BASE,
        width: subChartContainerRef.current.clientWidth,
        height: 200,
        timeScale: { ...CHART_OPTIONS_BASE.timeScale, visible: true },
      });
      subChartRef.current = subChart;

      const handleResize = () => {
        if (subChartContainerRef.current && subChartRef.current) {
          subChartRef.current.applyOptions({ width: subChartContainerRef.current.clientWidth });
        }
      };
      window.addEventListener('resize', handleResize);
      return () => {
        window.removeEventListener('resize', handleResize);
        subChart.remove();
        subChartRef.current = null;
      };
    }
    if (!hasOscillator && subChartRef.current) {
      subChartRef.current.remove();
      subChartRef.current = null;
    }
  }, [hasOscillator]);

  // ---- Main data rendering ----
  useEffect(() => {
    if (!chartRef.current || !volumeSeriesRef.current || data.length === 0) return;

    const candlestickData = data.map((d) => ({
      time: d.trade_date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
      open: d.open, high: d.high, low: d.low, close: d.close,
    }));
    const lineData = data.map((d) => ({
      time: d.trade_date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
      value: d.close,
    }));
    const volumeData = data.map((d) => ({
      time: d.trade_date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
      value: d.vol,
      color: d.close >= d.open ? 'rgba(16, 185, 129, 0.5)' : 'rgba(239, 68, 68, 0.5)',
    }));

    if (mainSeriesRef.current) {
      chartRef.current.removeSeries(mainSeriesRef.current);
      mainSeriesRef.current = null;
    }

    let mainSeries: any;
    switch (chartType) {
      case 'candlestick':
        mainSeries = chartRef.current.addSeries(CandlestickSeries, {
          upColor: 'var(--color-gain)', downColor: 'var(--color-loss)',
          borderUpColor: 'var(--color-gain)', borderDownColor: 'var(--color-loss)',
          wickUpColor: 'var(--color-gain)', wickDownColor: 'var(--color-loss)',
        });
        mainSeries.setData(candlestickData);
        break;
      case 'line':
        mainSeries = chartRef.current.addSeries(LineSeries, { color: 'var(--color-accent)', lineWidth: 2 });
        mainSeries.setData(lineData);
        break;
      case 'area':
        mainSeries = chartRef.current.addSeries(AreaSeries, {
          topColor: 'rgba(100, 149, 237, 0.4)', bottomColor: 'rgba(100, 149, 237, 0.0)',
          lineColor: 'var(--color-accent)', lineWidth: 2,
        });
        mainSeries.setData(lineData);
        break;
    }
    mainSeriesRef.current = mainSeries;
    volumeSeriesRef.current.setData(volumeData);

    if ((buyPoints.length > 0 || sellPoints.length > 0) && mainSeries) {
      mainSeries.setMarkers([
        ...buyPoints.map((date) => ({
          time: date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
          position: 'belowBar' as const, color: 'var(--color-gain)', shape: 'arrowUp' as const, text: '买',
        })),
        ...sellPoints.map((date) => ({
          time: date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
          position: 'aboveBar' as const, color: 'var(--color-loss)', shape: 'arrowDown' as const, text: '卖',
        })),
      ]);
    }

    if (candlestickData.length > 0) {
      const lastIndex = candlestickData.length - 1;
      const startIndex = Math.max(0, lastIndex - 179);
      chartRef.current.timeScale().setVisibleRange({
        from: candlestickData[startIndex].time as any,
        to: candlestickData[lastIndex].time as any,
      });
    }
  }, [data, buyPoints, sellPoints, chartType]);

  // ---- Crosshair move: collect OHLC + indicator values ----
  useEffect(() => {
    if (!chartRef.current) return;
    const handler = (param: any) => {
      if (!param.time || !param.seriesData) {
        setCrosshairData(null);
        return;
      }
      const values: any = { time: param.time };
      if (mainSeriesRef.current) {
        const mainData = param.seriesData.get(mainSeriesRef.current);
        if (mainData) {
          if ('open' in mainData) {
            values.open = mainData.open;
            values.high = mainData.high;
            values.low = mainData.low;
            values.close = mainData.close;
          } else if ('value' in mainData) {
            values.close = mainData.value;
          }
        }
      }
      const indValues: { name: string; value: number; color: string }[] = [];
      indicatorSeriesRef.current.forEach(({ series, chart }, key) => {
        if (chart !== 'main') return;
        const d = param.seriesData.get(series);
        if (d && 'value' in d) {
          indValues.push({ name: series.options().title || key, value: d.value, color: series.options().color || '#fff' });
        }
      });
      values.indicators = indValues;
      setCrosshairData(values);
    };
    chartRef.current.subscribeCrosshairMove(handler);
    return () => { chartRef.current?.unsubscribeCrosshairMove(handler); };
  }, [indicators, indicatorParams]);

  // ---- Crosshair move for sub-chart: collect oscillator values ----
  const [subCrosshairData, setSubCrosshairData] = useState<{ name: string; value: number; color: string }[]>([]);
  useEffect(() => {
    if (!subChartRef.current) return;
    const handler = (param: any) => {
      if (!param.time || !param.seriesData) {
        setSubCrosshairData([]);
        return;
      }
      const indValues: { name: string; value: number; color: string }[] = [];
      indicatorSeriesRef.current.forEach(({ series, chart }, key) => {
        if (chart !== 'sub') return;
        const d = param.seriesData.get(series);
        if (d && 'value' in d) {
          const title = series.options().title || key;
          if (title === key && (key === 'rsi30' || key === 'rsi70')) return;
          indValues.push({ name: title, value: d.value, color: series.options().color || '#fff' });
        }
      });
      setSubCrosshairData(indValues);
    };
    subChartRef.current.subscribeCrosshairMove(handler);
    return () => { subChartRef.current?.unsubscribeCrosshairMove(handler); };
  }, [hasOscillator, indicators, indicatorParams]);

  // ---- Indicator rendering ----
  useEffect(() => {
    if (!chartRef.current || data.length === 0) return;

    indicatorSeriesRef.current.forEach(({ series, chart }) => {
      try {
        if (chart === 'main') chartRef.current?.removeSeries(series);
        else subChartRef.current?.removeSeries(series);
      } catch (e) { /* already removed */ }
    });
    indicatorSeriesRef.current.clear();

    const candleData: CandleData[] = data.map((d) => ({
      time: d.trade_date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
      open: d.open, high: d.high, low: d.low, close: d.close, volume: d.vol,
    }));
    const addMainSeries = (key: string, options: any, seriesData: any[]) => {
      const s = chartRef.current!.addSeries(LineSeries, options);
      s.setData(seriesData);
      indicatorSeriesRef.current.set(key, { series: s, chart: 'main' });
    };

    const addSubSeries = (key: string, SeriesType: any, options: any, seriesData: any[]) => {
      if (!subChartRef.current) return;
      const s = subChartRef.current.addSeries(SeriesType, options);
      s.setData(seriesData);
      indicatorSeriesRef.current.set(key, { series: s, chart: 'sub' });
    };

    indicators.forEach((ind) => {
      const params = indicatorParams[ind];
      switch (ind) {
        case 'ma': {
          const periods = params.periods || [5, 10, 20, 60];
          periods.forEach((p: number, i: number) => {
            const smaData = calculateSMA(candleData, p);
            addMainSeries(`ma${p}`, { color: DEFAULT_COLORS[i % DEFAULT_COLORS.length], lineWidth: 1, lastValueVisible: false, title: `MA${p}` }, smaData);
          });
          break;
        }
        case 'ema': {
          const periods = params.periods || [12, 26];
          periods.forEach((p: number, i: number) => {
            const emaData = calculateEMA(candleData, p);
            addMainSeries(`ema${p}`, { color: DEFAULT_COLORS[(i + 4) % DEFAULT_COLORS.length], lineWidth: 1, lastValueVisible: false, title: `EMA${p}` }, emaData);
          });
          break;
        }
        case 'boll': {
          const boll = calculateBOLL(candleData, params.period, params.stdDev);
          addMainSeries('boll_mid', { color: '#fbbf24', lineWidth: 1, lastValueVisible: false, title: 'BOLL Mid' }, boll.middle);
          addMainSeries('boll_upper', { color: '#f87171', lineWidth: 1, lineStyle: 2, lastValueVisible: false, title: 'BOLL Up' }, boll.upper);
          addMainSeries('boll_lower', { color: '#34d399', lineWidth: 1, lineStyle: 2, lastValueVisible: false, title: 'BOLL Low' }, boll.lower);
          break;
        }
        case 'macd': {
          const macd = calculateMACD(candleData, params.fastPeriod, params.slowPeriod, params.signalPeriod);
          addSubSeries('macd_hist', HistogramSeries, { lastValueVisible: false, title: 'MACD Hist' }, macd.histogram);
          addSubSeries('macd_line', LineSeries, { color: '#ff6b6b', lineWidth: 2, lastValueVisible: false, title: 'MACD' }, macd.macd);
          addSubSeries('macd_signal', LineSeries, { color: '#4ecdc4', lineWidth: 2, lastValueVisible: false, title: 'Signal' }, macd.signal);
          break;
        }
        case 'rsi': {
          const rsi = calculateRSI(candleData, params.period);
          addSubSeries('rsi', LineSeries, { color: '#a78bfa', lineWidth: 2, lastValueVisible: false, title: 'RSI' }, rsi);
          addSubSeries('rsi70', LineSeries, { color: '#64748b', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false, title: '' }, rsi.map(d => ({ time: d.time, value: 70 })));
          addSubSeries('rsi30', LineSeries, { color: '#64748b', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false, title: '' }, rsi.map(d => ({ time: d.time, value: 30 })));
          break;
        }
        case 'kdj': {
          const kdj = calculateKDJ(candleData, params.period, params.k, params.d);
          addSubSeries('k', LineSeries, { color: '#ff6b6b', lineWidth: 2, lastValueVisible: false, title: 'K' }, kdj.k);
          addSubSeries('d', LineSeries, { color: '#4ecdc4', lineWidth: 2, lastValueVisible: false, title: 'D' }, kdj.d);
          addSubSeries('j', LineSeries, { color: '#ffe66d', lineWidth: 2, lastValueVisible: false, title: 'J' }, kdj.j);
          break;
        }
      }
    });
  }, [data, indicators, indicatorParams, hasOscillator]);

  // ---- Sync time scales (bidirectional) ----
  useEffect(() => {
    if (!chartRef.current || !subChartRef.current) return;
    let syncing = false;
    const mainHandler = (timeRange: any) => {
      if (syncing || !timeRange || !subChartRef.current) return;
      syncing = true;
      subChartRef.current.timeScale().setVisibleLogicalRange(timeRange);
      syncing = false;
    };
    const subHandler = (timeRange: any) => {
      if (syncing || !timeRange || !chartRef.current) return;
      syncing = true;
      chartRef.current.timeScale().setVisibleLogicalRange(timeRange);
      syncing = false;
    };
    chartRef.current.timeScale().subscribeVisibleLogicalRangeChange(mainHandler);
    subChartRef.current.timeScale().subscribeVisibleLogicalRangeChange(subHandler);
    return () => {
      chartRef.current?.timeScale().unsubscribeVisibleLogicalRangeChange(mainHandler);
      subChartRef.current?.timeScale().unsubscribeVisibleLogicalRangeChange(subHandler);
    };
  }, [hasOscillator]);
  // ---- Drawing tools ----
  useEffect(() => {
    if (!chartRef.current || !mainSeriesRef.current) return;

    const handleClick = (param: any) => {
      if (!drawingMode || !param.time) return;
      const price = param.seriesData?.get(mainSeriesRef.current);
      if (!price) return;

      if (!drawingStart) {
        setDrawingStart({
          time: param.time,
          price: typeof price === 'object' && 'close' in price ? price.close : price,
        });
      } else {
        let lineSeries: any = null;
        let lineData: any;
        const startPrice = drawingStart.price;
        const endPrice = typeof price === 'object' && 'close' in price ? price.close : price;

        switch (drawingTool) {
          case 'trendline':
            lineSeries = chartRef.current!.addSeries(LineSeries, { color: 'var(--color-accent)', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
            lineData = [
              { time: drawingStart.time, value: startPrice },
              { time: param.time, value: endPrice },
            ].sort((a, b) => (a.time > b.time ? 1 : -1));
            lineSeries.setData(lineData);
            break;
          case 'horizontal':
            lineSeries = chartRef.current!.addSeries(LineSeries, { color: '#fbbf24', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
            lineData = [
              { time: drawingStart.time, value: startPrice },
              { time: param.time, value: startPrice },
            ].sort((a, b) => (a.time > b.time ? 1 : -1));
            lineSeries.setData(lineData);
            break;
          case 'rectangle': {
            const rectLines: any[] = [];
            let rectLine = chartRef.current!.addSeries(LineSeries, { color: 'var(--color-gain)', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
            rectLine.setData([
              { time: drawingStart.time, value: Math.max(startPrice, endPrice) },
              { time: param.time, value: Math.max(startPrice, endPrice) },
            ].sort((a, b) => (a.time > b.time ? 1 : -1)));
            rectLines.push(rectLine);
            rectLine = chartRef.current!.addSeries(LineSeries, { color: 'var(--color-gain)', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
            rectLine.setData([
              { time: drawingStart.time, value: Math.min(startPrice, endPrice) },
              { time: param.time, value: Math.min(startPrice, endPrice) },
            ].sort((a, b) => (a.time > b.time ? 1 : -1)));
            rectLines.push(rectLine);
            setLines((prev) => [...prev, ...rectLines]);
            setDrawingStart(null);
            setDrawingMode(false);
            return;
          }
          case 'arrow':
            lineSeries = chartRef.current!.addSeries(LineSeries, { color: 'var(--color-loss)', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
            lineData = [
              { time: drawingStart.time, value: startPrice },
              { time: param.time, value: endPrice },
            ].sort((a, b) => (a.time > b.time ? 1 : -1));
            lineSeries.setData(lineData);
            break;
          case 'text':
            lineSeries = chartRef.current!.addSeries(LineSeries, { color: 'transparent', priceLineVisible: false, lastValueVisible: false });
            lineData = [{ time: drawingStart.time, value: startPrice }];
            lineSeries.setData(lineData);
            break;
        }
        if (lineSeries) setLines((prev) => [...prev, lineSeries]);
        setDrawingStart(null);
        setDrawingMode(false);
      }
    };

    chartRef.current.subscribeClick(handleClick);
    return () => { chartRef.current?.unsubscribeClick(handleClick); };
  }, [drawingMode, drawingStart, drawingTool]);

  // ---- Handlers ----
  const handleClearLines = () => {
    lines.forEach((line) => { chartRef.current?.removeSeries(line); });
    setLines([]);
    setDrawingStart(null);
  };
  const handleUndoLine = () => {
    if (lines.length > 0) {
      chartRef.current?.removeSeries(lines[lines.length - 1]);
      setLines((prev) => prev.slice(0, -1));
    }
  };
  const handleZoomIn = () => { chartRef.current?.timeScale().scrollToPosition(5, true); };
  const handleZoomOut = () => { chartRef.current?.timeScale().scrollToPosition(-5, true); };
  const handleResetZoom = () => {
    if (chartRef.current && data.length > 0) {
      const lastIndex = data.length - 1;
      const startIndex = Math.max(0, lastIndex - 179);
      const times = data.map((d) => d.trade_date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'));
      chartRef.current.timeScale().setVisibleRange({ from: times[startIndex] as any, to: times[lastIndex] as any });
    }
  };
  const handleScreenshot = () => {
    const canvas = chartContainerRef.current?.querySelector('canvas');
    if (canvas) {
      const link = document.createElement('a');
      link.download = `chart-${Date.now()}.png`;
      link.href = canvas.toDataURL('image/png');
      link.click();
    }
  };
  const handleOpenSettings = (indicator: string) => {
    setCurrentIndicator(indicator);
    setSettingsVisible(true);
  };
  const handleSaveSettings = (values: any) => {
    const updatedValues = { ...values };
    if (currentIndicator === 'ma' || currentIndicator === 'ema') {
      updatedValues.periods = values.periods.map((p: any) => typeof p === 'string' ? parseInt(p) : p);
    }
    setIndicatorParams((prev: any) => ({ ...prev, [currentIndicator]: updatedValues }));
    if (!indicators.includes(currentIndicator)) {
      setIndicators((prev) => [...prev, currentIndicator]);
    }
    setSettingsVisible(false);
  };
  const renderSettingsForm = () => {
    if (!currentIndicator) return null;
    const params = indicatorParams[currentIndicator];
    const submitBtn = (
      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', marginTop: 12 }}>
        <Button onClick={() => setSettingsVisible(false)}>取消</Button>
        <Button theme="solid" htmlType="submit">
          {indicators.includes(currentIndicator) ? '保存参数' : '添加到图表'}
        </Button>
      </div>
    );
    switch (currentIndicator) {
      case 'ma':
        return (<Form initValues={params} onSubmit={handleSaveSettings} layout="vertical">
          <Form.TagInput field="periods" label="周期" placeholder="输入周期，如 5, 10, 20, 60" allowDuplicates={false} />
          {submitBtn}</Form>);
      case 'ema':
        return (<Form initValues={params} onSubmit={handleSaveSettings} layout="vertical">
          <Form.TagInput field="periods" label="周期" placeholder="输入周期，如 12, 26" allowDuplicates={false} />
          {submitBtn}</Form>);
      case 'boll':
        return (<Form initValues={params} onSubmit={handleSaveSettings} layout="vertical">
          <Form.InputNumber field="period" label="周期" min={2} max={100} />
          <Form.InputNumber field="stdDev" label="标准差倍数" min={1} max={5} step={0.1} />
          {submitBtn}</Form>);
      case 'macd':
        return (<Form initValues={params} onSubmit={handleSaveSettings} layout="vertical">
          <Form.InputNumber field="fastPeriod" label="快线周期" min={2} max={100} />
          <Form.InputNumber field="slowPeriod" label="慢线周期" min={2} max={100} />
          <Form.InputNumber field="signalPeriod" label="信号线周期" min={2} max={100} />
          {submitBtn}</Form>);
      case 'rsi':
        return (<Form initValues={params} onSubmit={handleSaveSettings} layout="vertical">
          <Form.InputNumber field="period" label="周期" min={2} max={100} />
          {submitBtn}</Form>);
      case 'kdj':
        return (<Form initValues={params} onSubmit={handleSaveSettings} layout="vertical">
          <Form.InputNumber field="period" label="周期" min={2} max={100} />
          <Form.InputNumber field="k" label="K 平滑参数" min={1} max={10} />
          <Form.InputNumber field="d" label="D 平滑参数" min={1} max={10} />
          {submitBtn}</Form>);
      default: return null;
    }
  };
  const indicatorMenuItems = [
    { key: 'ma', name: '移动平均线 (MA)' },
    { key: 'ema', name: '指数移动平均 (EMA)' },
    { key: 'boll', name: '布林带 (BOLL)' },
    { key: 'macd', name: 'MACD' },
    { key: 'rsi', name: 'RSI' },
    { key: 'kdj', name: 'KDJ' },
  ];

  const indicatorDropdownMenu = (
    <Dropdown.Menu>
      {indicatorMenuItems.map(({ key, name }) => (
        <Dropdown.Item
          key={key}
          onClick={() => setIndicators((prev) => prev.includes(key) ? prev.filter(i => i !== key) : [...prev, key])}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', minWidth: 200 }}>
            <span>{indicators.includes(key) ? `✓ ${name}` : name}</span>
            <Button size="small" icon={<IconSetting />} onClick={(e) => { e.stopPropagation(); handleOpenSettings(key); }} style={{ marginLeft: 8 }} />
          </div>
        </Dropdown.Item>
      ))}
    </Dropdown.Menu>
  );

  return (
    <div style={{ position: 'relative' }}>
      {/* Top Toolbar */}
      <div style={{
        position: 'absolute', top: 8, left: 8, right: 8, zIndex: 10,
        background: 'var(--bg-overlay)', padding: '6px 12px', borderRadius: '4px',
        border: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ display: 'flex', gap: 4 }}>
            <Tooltip content="蜡烛图"><Button size="small" theme={chartType === 'candlestick' ? 'solid' : 'borderless'} icon={<IconBarChartVStroked />} style={{ height: '24px' }} onClick={() => setChartType('candlestick')} /></Tooltip>
            <Tooltip content="折线图"><Button size="small" theme={chartType === 'line' ? 'solid' : 'borderless'} icon={<IconLineChartStroked />} style={{ height: '24px' }} onClick={() => setChartType('line')} /></Tooltip>
            <Tooltip content="面积图"><Button size="small" theme={chartType === 'area' ? 'solid' : 'borderless'} icon={<IconCandlestickChartStroked />} style={{ height: '24px' }} onClick={() => setChartType('area')} /></Tooltip>
          </div>
          <div style={{ width: '1px', height: '20px', background: 'var(--border-color)' }} />
          <Dropdown render={indicatorDropdownMenu} position="bottomLeft">
            <Button size="small" style={{ height: '24px', fontSize: '12px' }}>指标</Button>
          </Dropdown>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <Tooltip content="放大"><Button size="small" theme="borderless" icon={<IconPlusStroked />} onClick={handleZoomIn} style={{ height: '24px' }} /></Tooltip>
          <Tooltip content="缩小"><Button size="small" theme="borderless" icon={<IconMinusStroked />} onClick={handleZoomOut} style={{ height: '24px' }} /></Tooltip>
          <Tooltip content="重置"><Button size="small" theme="borderless" onClick={handleResetZoom} style={{ height: '24px', fontSize: '12px' }}>重置</Button></Tooltip>
          <Tooltip content="截图"><Button size="small" theme="borderless" icon={<IconCamera />} onClick={handleScreenshot} style={{ height: '24px' }} /></Tooltip>
        </div>
      </div>

      {/* Drawing Toolbar */}
      <div style={{
        position: 'absolute', top: 48, left: 8, zIndex: 10,
        background: 'var(--bg-overlay)', padding: '6px', borderRadius: '4px',
        border: '1px solid var(--border-color)',
      }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {([
            { tool: 'trendline' as const, title: '趋势线', icon: <IconLineChartStroked /> },
            { tool: 'horizontal' as const, title: '水平线', icon: <IconMinusCircleStroked /> },
            { tool: 'rectangle' as const, title: '矩形', icon: <IconGridRectangle /> },
            { tool: 'arrow' as const, title: '箭头', icon: <IconArrowUp /> },
            { tool: 'text' as const, title: '文本', icon: <IconFont /> },
          ]).map(({ tool, title, icon }) => (
            <Tooltip key={tool} content={title} position="right">
              <Button size="small" theme={drawingMode && drawingTool === tool ? 'solid' : 'borderless'} icon={icon}
                style={{ width: '32px', height: '32px' }}
                onClick={() => { setDrawingMode(!drawingMode || drawingTool !== tool); setDrawingTool(tool); setDrawingStart(null); }} />
            </Tooltip>
          ))}
          <div style={{ height: '1px', background: 'var(--border-color)', margin: '4px 0' }} />
          <Tooltip content="撤销" position="right"><Button size="small" theme="borderless" icon={<IconUndo />} style={{ width: '32px', height: '32px' }} onClick={handleUndoLine} disabled={lines.length === 0} /></Tooltip>
          <Tooltip content="清除所有" position="right"><Button size="small" theme="borderless" icon={<IconDeleteStroked />} style={{ width: '32px', height: '32px', color: lines.length > 0 ? 'var(--color-loss)' : undefined }} onClick={handleClearLines} disabled={lines.length === 0} /></Tooltip>
        </div>
      </div>
      {/* OHLC + Indicator Widget Bar */}
      <div style={{
        position: 'absolute', bottom: 8, left: 8, right: 8, zIndex: 10,
        background: 'var(--bg-overlay)', padding: '6px 12px', borderRadius: '4px',
        border: '1px solid var(--border-color)', fontSize: '12px', color: 'var(--text-secondary)',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '4px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          {(() => {
            const d = crosshairData || (data.length > 0 ? { open: data[data.length - 1]?.open, high: data[data.length - 1]?.high, low: data[data.length - 1]?.low, close: data[data.length - 1]?.close, indicators: [] } : null);
            if (!d) return null;
            const chg = d.open ? ((d.close - d.open) / d.open * 100) : 0;
            return (
              <>
                <span style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: 'var(--text-tertiary)' }}>O</span><span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>{d.open?.toFixed(2)}</span></span>
                <span style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: 'var(--text-tertiary)' }}>H</span><span style={{ color: 'var(--color-gain)', fontWeight: 500 }}>{d.high?.toFixed(2)}</span></span>
                <span style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: 'var(--text-tertiary)' }}>L</span><span style={{ color: 'var(--color-loss)', fontWeight: 500 }}>{d.low?.toFixed(2)}</span></span>
                <span style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: 'var(--text-tertiary)' }}>C</span><span style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{d.close?.toFixed(2)}</span></span>
                {d.open && <span style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: chg >= 0 ? 'var(--color-gain)' : 'var(--color-loss)', fontWeight: 600 }}>{chg >= 0 ? '+' : ''}{chg.toFixed(2)}%</span></span>}
                {d.indicators?.map((ind: any) => (
                  <span key={ind.name} style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: ind.color, fontWeight: 500 }}>{ind.name}: {ind.value.toFixed(2)}</span></span>
                ))}
                {subCrosshairData.length > 0 && subCrosshairData.map((ind) => (
                  <span key={ind.name} style={{ display: 'inline-flex', gap: 4 }}><span style={{ color: ind.color, fontWeight: 500 }}>{ind.name}: {ind.value.toFixed(2)}</span></span>
                ))}
              </>
            );
          })()}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ color: 'var(--text-tertiary)' }}>数据: <span style={{ color: 'var(--color-accent)', fontWeight: 500 }}>{data.length}</span></span>
        </div>
      </div>

      <div ref={chartContainerRef} style={{ position: 'relative', borderRadius: '8px', overflow: 'hidden', border: '1px solid var(--border-color)', cursor: drawingMode ? 'crosshair' : 'default' }} />

      {hasOscillator && (
        <div ref={subChartContainerRef} style={{ position: 'relative', borderRadius: '8px', overflow: 'hidden', border: '1px solid var(--border-color)', marginTop: '8px' }} />
      )}

      <Modal title={`${currentIndicator.toUpperCase()} 参数设置`} visible={settingsVisible} onCancel={() => setSettingsVisible(false)} footer={null} width={400}>
        {renderSettingsForm()}
      </Modal>
    </div>
  );
};

export default TradingViewChart;