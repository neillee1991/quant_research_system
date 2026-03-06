// Technical Indicators Calculation Utilities

export interface CandleData {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface LineData {
  time: string;
  value: number;
}

// Simple Moving Average (SMA)
export function calculateSMA(data: CandleData[], period: number): LineData[] {
  const result: LineData[] = [];

  for (let i = period - 1; i < data.length; i++) {
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += data[i - j].close;
    }
    result.push({
      time: data[i].time,
      value: sum / period,
    });
  }

  return result;
}

// Exponential Moving Average (EMA)
export function calculateEMA(data: CandleData[], period: number): LineData[] {
  const result: LineData[] = [];
  const multiplier = 2 / (period + 1);

  // First EMA is SMA
  let ema = 0;
  for (let i = 0; i < period; i++) {
    ema += data[i].close;
  }
  ema = ema / period;

  result.push({
    time: data[period - 1].time,
    value: ema,
  });

  // Calculate EMA for remaining data
  for (let i = period; i < data.length; i++) {
    ema = (data[i].close - ema) * multiplier + ema;
    result.push({
      time: data[i].time,
      value: ema,
    });
  }

  return result;
}

// Bollinger Bands
export interface BollingerBands {
  upper: LineData[];
  middle: LineData[];
  lower: LineData[];
}

export function calculateBOLL(data: CandleData[], period: number = 20, stdDev: number = 2): BollingerBands {
  const middle: LineData[] = [];
  const upper: LineData[] = [];
  const lower: LineData[] = [];

  for (let i = period - 1; i < data.length; i++) {
    // Calculate SMA (middle band)
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += data[i - j].close;
    }
    const sma = sum / period;

    // Calculate standard deviation
    let variance = 0;
    for (let j = 0; j < period; j++) {
      variance += Math.pow(data[i - j].close - sma, 2);
    }
    const std = Math.sqrt(variance / period);

    middle.push({ time: data[i].time, value: sma });
    upper.push({ time: data[i].time, value: sma + stdDev * std });
    lower.push({ time: data[i].time, value: sma - stdDev * std });
  }

  return { upper, middle, lower };
}

// MACD
export interface MACD {
  macd: LineData[];
  signal: LineData[];
  histogram: LineData[];
}

export function calculateMACD(
  data: CandleData[],
  fastPeriod: number = 12,
  slowPeriod: number = 26,
  signalPeriod: number = 9
): MACD {
  const fastEMA = calculateEMA(data, fastPeriod);
  const slowEMA = calculateEMA(data, slowPeriod);

  // Calculate MACD line
  const macdLine: LineData[] = [];
  const startIndex = slowPeriod - fastPeriod;

  for (let i = 0; i < slowEMA.length; i++) {
    macdLine.push({
      time: slowEMA[i].time,
      value: fastEMA[i + startIndex].value - slowEMA[i].value,
    });
  }

  // Calculate signal line (EMA of MACD)
  const signalLine: LineData[] = [];
  const multiplier = 2 / (signalPeriod + 1);

  let ema = 0;
  for (let i = 0; i < signalPeriod; i++) {
    ema += macdLine[i].value;
  }
  ema = ema / signalPeriod;
  signalLine.push({ time: macdLine[signalPeriod - 1].time, value: ema });

  for (let i = signalPeriod; i < macdLine.length; i++) {
    ema = (macdLine[i].value - ema) * multiplier + ema;
    signalLine.push({ time: macdLine[i].time, value: ema });
  }

  // Calculate histogram
  const histogram: LineData[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    histogram.push({
      time: signalLine[i].time,
      value: macdLine[i + signalPeriod - 1].value - signalLine[i].value,
    });
  }

  return { macd: macdLine, signal: signalLine, histogram };
}

// RSI
export function calculateRSI(data: CandleData[], period: number = 14): LineData[] {
  const result: LineData[] = [];

  if (data.length < period + 1) return result;

  let gains = 0;
  let losses = 0;

  // Calculate initial average gain and loss
  for (let i = 1; i <= period; i++) {
    const change = data[i].close - data[i - 1].close;
    if (change > 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  let rs = avgGain / avgLoss;
  let rsi = 100 - (100 / (1 + rs));

  result.push({ time: data[period].time, value: rsi });

  // Calculate RSI for remaining data
  for (let i = period + 1; i < data.length; i++) {
    const change = data[i].close - data[i - 1].close;
    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? Math.abs(change) : 0;

    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;

    rs = avgGain / avgLoss;
    rsi = 100 - (100 / (1 + rs));

    result.push({ time: data[i].time, value: rsi });
  }

  return result;
}

// KDJ
export interface KDJ {
  k: LineData[];
  d: LineData[];
  j: LineData[];
}

export function calculateKDJ(data: CandleData[], period: number = 9, k: number = 3, d: number = 3): KDJ {
  const kLine: LineData[] = [];
  const dLine: LineData[] = [];
  const jLine: LineData[] = [];

  let prevK = 50;
  let prevD = 50;

  for (let i = period - 1; i < data.length; i++) {
    let highest = data[i - period + 1].high;
    let lowest = data[i - period + 1].low;

    for (let j = i - period + 2; j <= i; j++) {
      if (data[j].high > highest) highest = data[j].high;
      if (data[j].low < lowest) lowest = data[j].low;
    }

    const rsv = highest === lowest ? 0 : ((data[i].close - lowest) / (highest - lowest)) * 100;

    const currentK = (prevK * (k - 1) + rsv) / k;
    const currentD = (prevD * (d - 1) + currentK) / d;
    const currentJ = 3 * currentK - 2 * currentD;

    kLine.push({ time: data[i].time, value: currentK });
    dLine.push({ time: data[i].time, value: currentD });
    jLine.push({ time: data[i].time, value: currentJ });

    prevK = currentK;
    prevD = currentD;
  }

  return { k: kLine, d: dLine, j: jLine };
}

// ATR (Average True Range)
export function calculateATR(data: CandleData[], period: number = 14): LineData[] {
  const result: LineData[] = [];

  if (data.length < period + 1) return result;

  const trueRanges: number[] = [];

  // Calculate True Range for each period
  for (let i = 1; i < data.length; i++) {
    const high = data[i].high;
    const low = data[i].low;
    const prevClose = data[i - 1].close;

    const tr = Math.max(
      high - low,
      Math.abs(high - prevClose),
      Math.abs(low - prevClose)
    );

    trueRanges.push(tr);
  }

  // Calculate initial ATR (SMA of TR)
  let atr = 0;
  for (let i = 0; i < period; i++) {
    atr += trueRanges[i];
  }
  atr = atr / period;

  result.push({ time: data[period].time, value: atr });

  // Calculate ATR for remaining data (smoothed)
  for (let i = period; i < trueRanges.length; i++) {
    atr = (atr * (period - 1) + trueRanges[i]) / period;
    result.push({ time: data[i + 1].time, value: atr });
  }

  return result;
}
