import React, { useEffect, useRef, useState } from 'react';
import { createChart, ColorType, IChartApi, CandlestickSeries, HistogramSeries, LineSeries, AreaSeries } from 'lightweight-charts';
import { Button, Space, Tooltip, Dropdown, Modal, Form, InputNumber, Select } from 'antd';
import {
  LineOutlined,
  DeleteOutlined,
  UndoOutlined,
  SettingOutlined,
  CameraOutlined,
  ZoomInOutlined,
  ZoomOutOutlined,
  BarChartOutlined,
  DashOutlined,
  BorderOutlined,
  ArrowUpOutlined,
  FontSizeOutlined,
  LineChartOutlined,
  AreaChartOutlined,
} from '@ant-design/icons';
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
    background: { type: ColorType.Solid, color: 'rgba(0, 0, 0, 0.3)' },
    textColor: '#94a3b8',
  },
  grid: {
    vertLines: { color: 'rgba(0, 212, 255, 0.1)' },
    horzLines: { color: 'rgba(0, 212, 255, 0.1)' },
  },
  timeScale: {
    borderColor: 'rgba(0, 212, 255, 0.2)',
    timeVisible: true,
  },
  rightPriceScale: {
    borderColor: 'rgba(0, 212, 255, 0.2)',
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
      color: '#00d4ff',
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
          upColor: '#10b981', downColor: '#ef4444',
          borderUpColor: '#10b981', borderDownColor: '#ef4444',
          wickUpColor: '#10b981', wickDownColor: '#ef4444',
        });
        mainSeries.setData(candlestickData);
        break;
      case 'line':
        mainSeries = chartRef.current.addSeries(LineSeries, { color: '#00d4ff', lineWidth: 2 });
        mainSeries.setData(lineData);
        break;
      case 'area':
        mainSeries = chartRef.current.addSeries(AreaSeries, {
          topColor: 'rgba(0, 212, 255, 0.4)', bottomColor: 'rgba(0, 212, 255, 0.0)',
          lineColor: '#00d4ff', lineWidth: 2,
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
          position: 'belowBar' as const, color: '#10b981', shape: 'arrowUp' as const, text: '买',
        })),
        ...sellPoints.map((date) => ({
          time: date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
          position: 'aboveBar' as const, color: '#ef4444', shape: 'arrowDown' as const, text: '卖',
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
      // Get main series OHLC
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
      // Get indicator values from main chart series
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
          // Skip reference lines
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

    // Remove old indicator series
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

    indicators.forEach((indicator) => {
      const params = indicatorParams[indicator];
      switch (indicator) {
        case 'ma':
          params.periods.forEach((period: number, index: number) => {
            const color = DEFAULT_COLORS[index % DEFAULT_COLORS.length];
            addMainSeries(`ma${period}`, { color, lineWidth: 1, title: `MA${period}` }, calculateSMA(candleData, period));
          });
          break;
        case 'ema':
          params.periods.forEach((period: number, index: number) => {
            const color = DEFAULT_COLORS[index % DEFAULT_COLORS.length];
            addMainSeries(`ema${period}`, { color, lineWidth: 1, title: `EMA${period}` }, calculateEMA(candleData, period));
          });
          break;
        case 'boll': {
          const boll = calculateBOLL(candleData, params.period, params.stdDev);
          addMainSeries('boll_upper', { color: '#ff6b6b', lineWidth: 1, title: 'BOLL↑' }, boll.upper);
          addMainSeries('boll_middle', { color: '#ffe66d', lineWidth: 1, title: 'BOLL' }, boll.middle);
          addMainSeries('boll_lower', { color: '#4ecdc4', lineWidth: 1, title: 'BOLL↓' }, boll.lower);
          break;
        }
        case 'macd': {
          const macd = calculateMACD(candleData, params.fastPeriod, params.slowPeriod, params.signalPeriod);
          addSubSeries('macd', LineSeries, { color: '#ff6b6b', lineWidth: 2, title: 'MACD' }, macd.macd);
          addSubSeries('signal', LineSeries, { color: '#4ecdc4', lineWidth: 2, title: 'Signal' }, macd.signal);
          addSubSeries('histogram', HistogramSeries, { color: '#ffe66d', priceFormat: { type: 'volume' }, title: 'Hist' },
            macd.histogram.map(d => ({ time: d.time, value: d.value, color: d.value >= 0 ? 'rgba(16,185,129,0.5)' : 'rgba(239,68,68,0.5)' })));
          break;
        }
        case 'rsi': {
          const rsi = calculateRSI(candleData, params.period);
          addSubSeries('rsi', LineSeries, { color: '#a8dadc', lineWidth: 2, title: 'RSI' }, rsi);
          addSubSeries('rsi30', LineSeries, { color: 'rgba(239,68,68,0.3)', lineWidth: 1, priceLineVisible: false, lastValueVisible: false, title: '' }, rsi.map(d => ({ time: d.time, value: 30 })));
          addSubSeries('rsi70', LineSeries, { color: 'rgba(16,185,129,0.3)', lineWidth: 1, priceLineVisible: false, lastValueVisible: false, title: '' }, rsi.map(d => ({ time: d.time, value: 70 })));
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
            lineSeries = chartRef.current!.addSeries(LineSeries, { color: '#00d4ff', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
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
            let rectLine = chartRef.current!.addSeries(LineSeries, { color: '#10b981', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
            rectLine.setData([
              { time: drawingStart.time, value: Math.max(startPrice, endPrice) },
              { time: param.time, value: Math.max(startPrice, endPrice) },
            ].sort((a, b) => (a.time > b.time ? 1 : -1)));
            rectLines.push(rectLine);
            rectLine = chartRef.current!.addSeries(LineSeries, { color: '#10b981', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
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
            lineSeries = chartRef.current!.addSeries(LineSeries, { color: '#ef4444', lineWidth: 2, priceLineVisible: false, lastValueVisible: false });
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
        <Button type="primary" htmlType="submit">
          {indicators.includes(currentIndicator) ? '保存参数' : '添加到图表'}
        </Button>
      </div>
    );
    switch (currentIndicator) {
      case 'ma':
        return (<Form initialValues={params} onFinish={handleSaveSettings} layout="vertical">
          <Form.Item label="周期" name="periods">
            <Select mode="tags" placeholder="输入周期，如 5, 10, 20, 60">
              {[5, 10, 20, 60].map(v => <Select.Option key={v} value={v}>{v}</Select.Option>)}
            </Select>
          </Form.Item>{submitBtn}</Form>);
      case 'ema':
        return (<Form initialValues={params} onFinish={handleSaveSettings} layout="vertical">
          <Form.Item label="周期" name="periods">
            <Select mode="tags" placeholder="输入周期，如 12, 26">
              {[12, 26].map(v => <Select.Option key={v} value={v}>{v}</Select.Option>)}
            </Select>
          </Form.Item>{submitBtn}</Form>);
      case 'boll':
        return (<Form initialValues={params} onFinish={handleSaveSettings} layout="vertical">
          <Form.Item label="周期" name="period"><InputNumber min={2} max={100} /></Form.Item>
          <Form.Item label="标准差倍数" name="stdDev"><InputNumber min={1} max={5} step={0.1} /></Form.Item>
          {submitBtn}</Form>);
      case 'macd':
        return (<Form initialValues={params} onFinish={handleSaveSettings} layout="vertical">
          <Form.Item label="快线周期" name="fastPeriod"><InputNumber min={2} max={100} /></Form.Item>
          <Form.Item label="慢线周期" name="slowPeriod"><InputNumber min={2} max={100} /></Form.Item>
          <Form.Item label="信号线周期" name="signalPeriod"><InputNumber min={2} max={100} /></Form.Item>
          {submitBtn}</Form>);
      case 'rsi':
        return (<Form initialValues={params} onFinish={handleSaveSettings} layout="vertical">
          <Form.Item label="周期" name="period"><InputNumber min={2} max={100} /></Form.Item>
          {submitBtn}</Form>);
      case 'kdj':
        return (<Form initialValues={params} onFinish={handleSaveSettings} layout="vertical">
          <Form.Item label="周期" name="period"><InputNumber min={2} max={100} /></Form.Item>
          <Form.Item label="K 平滑参数" name="k"><InputNumber min={1} max={10} /></Form.Item>
          <Form.Item label="D 平滑参数" name="d"><InputNumber min={1} max={10} /></Form.Item>
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
  ].map(({ key, name }) => ({
    key,
    label: (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', minWidth: 200 }}>
        <span>{indicators.includes(key) ? `✓ ${name}` : name}</span>
        <Button size="small" icon={<SettingOutlined />} onClick={(e) => { e.stopPropagation(); handleOpenSettings(key); }} style={{ marginLeft: 8 }} />
      </div>
    ),
  }));

  return (
    <div style={{ position: 'relative' }}>
      {/* Top Toolbar */}
      <div style={{
        position: 'absolute', top: 8, left: 8, right: 8, zIndex: 10,
        background: 'rgba(0, 0, 0, 0.85)', padding: '6px 12px', borderRadius: '4px',
        border: '1px solid rgba(0, 212, 255, 0.2)', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      }}>
        <Space size="small">
          <Space size={4}>
            <Tooltip title="蜡烛图"><Button size="small" type={chartType === 'candlestick' ? 'primary' : 'text'} icon={<BarChartOutlined />} style={{ height: '24px', color: chartType === 'candlestick' ? '#fff' : '#94a3b8' }} onClick={() => setChartType('candlestick')} /></Tooltip>
            <Tooltip title="折线图"><Button size="small" type={chartType === 'line' ? 'primary' : 'text'} icon={<LineChartOutlined />} style={{ height: '24px', color: chartType === 'line' ? '#fff' : '#94a3b8' }} onClick={() => setChartType('line')} /></Tooltip>
            <Tooltip title="面积图"><Button size="small" type={chartType === 'area' ? 'primary' : 'text'} icon={<AreaChartOutlined />} style={{ height: '24px', color: chartType === 'area' ? '#fff' : '#94a3b8' }} onClick={() => setChartType('area')} /></Tooltip>
          </Space>
          <div style={{ width: '1px', height: '20px', background: 'rgba(0, 212, 255, 0.2)' }} />
          <Dropdown menu={{ items: indicatorMenuItems, onClick: ({ key }) => { setIndicators((prev) => prev.includes(key) ? prev.filter(i => i !== key) : [...prev, key]); } }}>
            <Button size="small" style={{ height: '24px', fontSize: '12px' }}>指标</Button>
          </Dropdown>
        </Space>
        <Space size="small">
          <Tooltip title="放大"><Button size="small" type="text" icon={<ZoomInOutlined />} onClick={handleZoomIn} style={{ height: '24px' }} /></Tooltip>
          <Tooltip title="缩小"><Button size="small" type="text" icon={<ZoomOutOutlined />} onClick={handleZoomOut} style={{ height: '24px' }} /></Tooltip>
          <Tooltip title="重置"><Button size="small" type="text" onClick={handleResetZoom} style={{ height: '24px', fontSize: '12px' }}>重置</Button></Tooltip>
          <Tooltip title="截图"><Button size="small" type="text" icon={<CameraOutlined />} onClick={handleScreenshot} style={{ height: '24px' }} /></Tooltip>
        </Space>
      </div>

      {/* Drawing Toolbar */}
      <div style={{
        position: 'absolute', top: 48, left: 8, zIndex: 10,
        background: 'rgba(0, 0, 0, 0.85)', padding: '6px', borderRadius: '4px',
        border: '1px solid rgba(0, 212, 255, 0.2)',
      }}>
        <Space direction="vertical" size={4}>
          {([
            { tool: 'trendline' as const, title: '趋势线', icon: <LineOutlined /> },
            { tool: 'horizontal' as const, title: '水平线', icon: <DashOutlined /> },
            { tool: 'rectangle' as const, title: '矩形', icon: <BorderOutlined /> },
            { tool: 'arrow' as const, title: '箭头', icon: <ArrowUpOutlined /> },
            { tool: 'text' as const, title: '文本', icon: <FontSizeOutlined /> },
          ]).map(({ tool, title, icon }) => (
            <Tooltip key={tool} title={title} placement="right">
              <Button size="small" type={drawingMode && drawingTool === tool ? 'primary' : 'text'} icon={icon}
                style={{ width: '32px', height: '32px' }}
                onClick={() => { setDrawingMode(!drawingMode || drawingTool !== tool); setDrawingTool(tool); setDrawingStart(null); }} />
            </Tooltip>
          ))}
          <div style={{ height: '1px', background: 'rgba(0, 212, 255, 0.2)', margin: '4px 0' }} />
          <Tooltip title="撤销" placement="right"><Button size="small" type="text" icon={<UndoOutlined />} style={{ width: '32px', height: '32px' }} onClick={handleUndoLine} disabled={lines.length === 0} /></Tooltip>
          <Tooltip title="清除所有" placement="right"><Button size="small" type="text" icon={<DeleteOutlined />} style={{ width: '32px', height: '32px' }} onClick={handleClearLines} disabled={lines.length === 0} danger /></Tooltip>
        </Space>
      </div>

      {/* OHLC + Indicator Widget Bar */}
      <div style={{
        position: 'absolute', bottom: 8, left: 8, right: 8, zIndex: 10,
        background: 'rgba(0, 0, 0, 0.85)', padding: '6px 12px', borderRadius: '4px',
        border: '1px solid rgba(0, 212, 255, 0.2)', fontSize: '12px', color: '#94a3b8',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '4px',
      }}>
        <Space size="middle" wrap>
          {(() => {
            const d = crosshairData || (data.length > 0 ? { open: data[data.length - 1]?.open, high: data[data.length - 1]?.high, low: data[data.length - 1]?.low, close: data[data.length - 1]?.close, indicators: [] } : null);
            if (!d) return null;
            const chg = d.open ? ((d.close - d.open) / d.open * 100) : 0;
            return (
              <>
                <Space size={4}><span style={{ color: '#64748b' }}>O</span><span style={{ color: '#f1f5f9', fontWeight: 500 }}>{d.open?.toFixed(2)}</span></Space>
                <Space size={4}><span style={{ color: '#64748b' }}>H</span><span style={{ color: '#10b981', fontWeight: 500 }}>{d.high?.toFixed(2)}</span></Space>
                <Space size={4}><span style={{ color: '#64748b' }}>L</span><span style={{ color: '#ef4444', fontWeight: 500 }}>{d.low?.toFixed(2)}</span></Space>
                <Space size={4}><span style={{ color: '#64748b' }}>C</span><span style={{ color: '#f1f5f9', fontWeight: 600 }}>{d.close?.toFixed(2)}</span></Space>
                {d.open && <Space size={4}><span style={{ color: chg >= 0 ? '#10b981' : '#ef4444', fontWeight: 600 }}>{chg >= 0 ? '+' : ''}{chg.toFixed(2)}%</span></Space>}
                {d.indicators?.map((ind: any) => (
                  <Space key={ind.name} size={4}><span style={{ color: ind.color, fontWeight: 500 }}>{ind.name}: {ind.value.toFixed(2)}</span></Space>
                ))}
                {subCrosshairData.length > 0 && subCrosshairData.map((ind) => (
                  <Space key={ind.name} size={4}><span style={{ color: ind.color, fontWeight: 500 }}>{ind.name}: {ind.value.toFixed(2)}</span></Space>
                ))}
              </>
            );
          })()}
        </Space>
        <Space size="middle">
          <span style={{ color: '#64748b' }}>数据: <span style={{ color: '#00d4ff', fontWeight: 500 }}>{data.length}</span></span>
        </Space>
      </div>

      <div ref={chartContainerRef} style={{ position: 'relative', borderRadius: '8px', overflow: 'hidden', border: '1px solid rgba(0, 212, 255, 0.2)', cursor: drawingMode ? 'crosshair' : 'default' }} />

      {hasOscillator && (
        <div ref={subChartContainerRef} style={{ position: 'relative', borderRadius: '8px', overflow: 'hidden', border: '1px solid rgba(0, 212, 255, 0.2)', marginTop: '8px' }} />
      )}

      <Modal title={`${currentIndicator.toUpperCase()} 参数设置`} open={settingsVisible} onCancel={() => setSettingsVisible(false)} footer={null} width={400}>
        {renderSettingsForm()}
      </Modal>
    </div>
  );
};

export default TradingViewChart;
