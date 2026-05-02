/**
 * 公式渲染组件
 * 用于展示数学公式和代码片段
 */

import React from 'react';

interface FormulaBlockProps {
  title?: string;
  formula: string;
  description?: string;
}

export const FormulaBlock: React.FC<FormulaBlockProps> = ({ title, formula, description }) => (
  <div style={{ marginBottom: 16, padding: 12, background: 'var(--bg-tertiary)', borderRadius: 6 }}>
    {title && <div style={{ fontWeight: 600, marginBottom: 8, color: 'var(--text-primary)' }}>{title}</div>}
    <pre style={{
      margin: 0,
      padding: 12,
      background: 'var(--bg-card)',
      borderRadius: 4,
      overflow: 'auto',
      fontFamily: 'var(--font-mono)',
      fontSize: 13,
      color: 'var(--text-primary)',
      lineHeight: 1.6,
    }}>
      {formula}
    </pre>
    {description && (
      <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-secondary)' }}>
        {description}
      </div>
    )}
  </div>
);

interface FlowFormulaRendererProps {
  type: 'calculation' | 'analysis';
}

export const FlowFormulaRenderer: React.FC<FlowFormulaRendererProps> = ({ type }) => {
  if (type === 'calculation') {
    return (
      <div>
        <FormulaBlock
          title="3. 价格复权"
          formula={`adj_close = close × adj_factor
adj_open = open × adj_factor
adj_high = high × adj_factor
adj_low = low × adj_factor`}
          description="使用复权因子调整价格，消除分红拆股的影响"
        />
        <FormulaBlock
          title="5. 因子计算"
          formula={`factor_value = f(df, params)

其中 f 是用户定义的因子函数，
df 是预处理后的 DataFrame，
params 是参数字典`}
          description="使用 Polars 向量化计算因子值"
        />
        <FormulaBlock
          title="7. 质量检查"
          formula={`null_rate = count(null) / total
extreme_flag = |z-score| > 3

z-score = (value - mean) / std`}
          description="计算空值率和极端值标记"
        />
      </div>
    );
  }

  return (
    <div>
      <FormulaBlock
        title="IC (Information Coefficient)"
        formula={`IC_t = corr(factor_{t-1}, forward_return_t)

IC_mean = mean(IC_t)
IC_std = std(IC_t)
ICIR = IC_mean / IC_std`}
        description="IC 衡量因子值与下期收益的秩相关系数，ICIR 是信息比率"
      />
      <FormulaBlock
        title="分组收益"
        formula={`quantile_i = quantile(factor, i / N),  i = 1..N

mean_return_i = mean(forward_return | factor ∈ quantile_i)
spread = mean_return_N - mean_return_1`}
        description="将股票按因子值分为 N 组，计算每组的平均收益"
      />
      <FormulaBlock
        title="换手率"
        formula={`turnover_{t,i} = fraction_not_in_quantile_i(
    quantile_{t,i},
    quantile_{t-1,i}
)`}
        description="衡量分组的稳定性，换手率越低越好"
      />
    </div>
  );
};

export default FlowFormulaRenderer;
