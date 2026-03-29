# 因子流程可视化组件实施计划

&gt; **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在因子分析页面增加"分布框架"按钮，点击后弹出抽屉，通过文字公式和图形展示因子计算和分析流程。

**Architecture:** 创建独立的 FactorFlowDrawer 组件，包含三个标签页（概览、计算流程、分析流程），使用 ECharts 绘制流程图，在 AnalysisPanel 中集成按钮。

**Tech Stack:** React + TypeScript + Ant Design + ECharts

---

## 前置检查

**Step 1: 确认项目依赖**
- 确认已安装 `antd`、`echarts`、`echarts-for-react`
- 检查 `frontend/package.json` 确认依赖

**Step 2: 确认现有组件结构**
- 读取 `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`
- 读取 `frontend/src/pages/FactorCenter/FactorDrawer.tsx` 了解抽屉模式

---

## Task 1: 创建 FlowFormulaRenderer 公式渲染组件

**Files:**
- Create: `frontend/src/pages/FactorCenter/components/FlowFormulaRenderer.tsx`

**Step 1: 创建组件文件**

```tsx
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

export const FormulaBlock: React.FC&lt;FormulaBlockProps&gt; = ({ title, formula, description }) =&gt; (
  &lt;div style={{ marginBottom: 16, padding: 12, background: 'var(--bg-tertiary)', borderRadius: 6 }}&gt;
    {title &amp;&amp; &lt;div style={{ fontWeight: 600, marginBottom: 8, color: 'var(--text-primary)' }}&gt;{title}&lt;/div&gt;}
    &lt;pre style={{
      margin: 0,
      padding: 12,
      background: 'var(--bg-card)',
      borderRadius: 4,
      overflow: 'auto',
      fontFamily: 'var(--font-mono)',
      fontSize: 13,
      color: 'var(--text-primary)',
      lineHeight: 1.6,
    }}&gt;
      {formula}
    &lt;/pre&gt;
    {description &amp;&amp; (
      &lt;div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-secondary)' }}&gt;
        {description}
      &lt;/div&gt;
    )}
  &lt;/div&gt;
);

interface FlowFormulaRendererProps {
  type: 'calculation' | 'analysis';
}

export const FlowFormulaRenderer: React.FC&lt;FlowFormulaRendererProps&gt; = ({ type }) =&gt; {
  if (type === 'calculation') {
    return (
      &lt;div&gt;
        &lt;FormulaBlock
          title="3. 价格复权"
          formula={`adj_close = close × adj_factor
adj_open = open × adj_factor
adj_high = high × adj_factor
adj_low = low × adj_factor`}
          description="使用复权因子调整价格，消除分红拆股的影响"
        /&gt;
        &lt;FormulaBlock
          title="5. 因子计算"
          formula={`factor_value = f(df, params)

其中 f 是用户定义的因子函数，
df 是预处理后的 DataFrame，
params 是参数字典`}
          description="使用 Polars 向量化计算因子值"
        /&gt;
        &lt;FormulaBlock
          title="7. 质量检查"
          formula={`null_rate = count(null) / total
extreme_flag = |z-score| &gt; 3

z-score = (value - mean) / std`}
          description="计算空值率和极端值标记"
        /&gt;
      &lt;/div&gt;
    );
  }

  return (
    &lt;div&gt;
      &lt;FormulaBlock
        title="IC (Information Coefficient)"
        formula={`IC_t = corr(factor_{t-1}, forward_return_t)

IC_mean = mean(IC_t)
IC_std = std(IC_t)
ICIR = IC_mean / IC_std`}
        description="IC 衡量因子值与下期收益的秩相关系数，ICIR 是信息比率"
      /&gt;
      &lt;FormulaBlock
        title="分组收益"
        formula={`quantile_i = quantile(factor, i / N),  i = 1..N

mean_return_i = mean(forward_return | factor ∈ quantile_i)
spread = mean_return_N - mean_return_1`}
        description="将股票按因子值分为 N 组，计算每组的平均收益"
      /&gt;
      &lt;FormulaBlock
        title="换手率"
        formula={`turnover_{t,i} = fraction_not_in_quantile_i(
    quantile_{t,i},
    quantile_{t-1,i}
)`}
        description="衡量分组的稳定性，换手率越低越好"
      /&gt;
    &lt;/div&gt;
  );
};

export default FlowFormulaRenderer;
```

**Step 2: 验证组件结构**
- 确保没有语法错误
- 导出两个组件：`FormulaBlock` 和 `FlowFormulaRenderer`

---

## Task 2: 创建 FactorCalculationFlow 计算流程组件

**Files:**
- Create: `frontend/src/pages/FactorCenter/components/FactorCalculationFlow.tsx`

**Step 1: 创建组件文件**

```tsx
/**
 * 因子计算流程可视化组件
 * 展示8步 Pipeline 流程图
 */

import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import { FlowFormulaRenderer } from './FlowFormulaRenderer';

interface FactorCalculationFlowProps {
  height?: number;
}

const FactorCalculationFlow: React.FC&lt;FactorCalculationFlowProps&gt; = ({ height = 400 }) =&gt; {
  const chartOption = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c}',
    },
    animationDurationUpdate: 1500,
    animationEasingUpdate: 'quinticInOut',
    series: [
      {
        type: 'graph',
        layout: 'none',
        symbolSize: 60,
        roam: true,
        label: {
          show: true,
          fontSize: 12,
          color: '#fff',
        },
        edgeSymbol: ['circle', 'arrow'],
        edgeSymbolSize: [4, 10],
        edgeLabel: {
          fontSize: 10,
        },
        data: [
          { name: '1. 日期解析', x: 100, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '2. 数据加载', x: 250, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '3. 价格复权', x: 400, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '4. 状态过滤', x: 550, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '5. 因子计算', x: 700, y: 100, itemStyle: { color: '#10B981' } },
          { name: '6. 停牌处理', x: 850, y: 100, itemStyle: { color: '#0077FA' } },
          { name: '7. 质量检查', x: 550, y: 220, itemStyle: { color: '#0077FA' } },
          { name: '8. 结果保存', x: 700, y: 220, itemStyle: { color: '#0077FA' } },
        ],
        links: [
          { source: '1. 日期解析', target: '2. 数据加载' },
          { source: '2. 数据加载', target: '3. 价格复权' },
          { source: '3. 价格复权', target: '4. 状态过滤' },
          { source: '4. 状态过滤', target: '5. 因子计算' },
          { source: '5. 因子计算', target: '6. 停牌处理' },
          { source: '6. 停牌处理', target: '7. 质量检查' },
          { source: '7. 质量检查', target: '8. 结果保存' },
        ],
        lineStyle: {
          opacity: 0.9,
          width: 2,
          curveness: 0,
        },
      },
    ],
  };

  const steps = [
    {
      title: '1. 日期解析',
      desc: '确定数据加载范围：data_start = start_date - lookback_days',
    },
    {
      title: '2. 数据加载',
      desc: '按 depends_on 从 DolphinDB 查询原始数据',
    },
    {
      title: '3. 价格复权',
      desc: '使用复权因子调整 OHLCV 价格',
    },
    {
      title: '4. 状态过滤',
      desc: '过滤 ST、新股，标记涨跌停',
    },
    {
      title: '5. 因子计算',
      desc: '执行用户定义的因子函数（Polars 向量化）',
    },
    {
      title: '6. 停牌处理',
      desc: '停牌期间因子值置空',
    },
    {
      title: '7. 质量检查',
      desc: '计算空值率、极端值标记',
    },
    {
      title: '8. 结果保存',
      desc: 'Upsert 到 factor_values 表',
    },
  ];

  return (
    &lt;div&gt;
      &lt;Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;8步 Pipeline 流程&lt;/span&gt;}
      &gt;
        &lt;ReactECharts option={chartOption} style={{ height }} /&gt;
      &lt;/Card&gt;

      &lt;Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;流程说明&lt;/span&gt;}
      &gt;
        &lt;div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 12 }}&gt;
          {steps.map((step, idx) =&gt; (
            &lt;div
              key={idx}
              style={{
                padding: 12,
                background: 'var(--bg-tertiary)',
                borderRadius: 6,
                borderLeft: `3px solid ${idx === 4 ? '#10B981' : '#0077FA'}`,
              }}
            &gt;
              &lt;div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}&gt;
                {step.title}
              &lt;/div&gt;
              &lt;div style={{ fontSize: 12, color: 'var(--text-secondary)' }}&gt;
                {step.desc}
              &lt;/div&gt;
            &lt;/div&gt;
          ))}
        &lt;/div&gt;
      &lt;/Card&gt;

      &lt;Card
        style={{ background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;核心公式&lt;/span&gt;}
      &gt;
        &lt;FlowFormulaRenderer type="calculation" /&gt;
      &lt;/Card&gt;
    &lt;/div&gt;
  );
};

export default FactorCalculationFlow;
```

---

## Task 3: 创建 FactorAnalysisFlow 分析流程组件

**Files:**
- Create: `frontend/src/pages/FactorCenter/components/FactorAnalysisFlow.tsx`

**Step 1: 创建组件文件**

```tsx
/**
 * 因子分析流程可视化组件
 * 展示因子分析完整流程
 */

import React from 'react';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import { FlowFormulaRenderer } from './FlowFormulaRenderer';

interface FactorAnalysisFlowProps {
  height?: number;
}

const FactorAnalysisFlow: React.FC&lt;FactorAnalysisFlowProps&gt; = ({ height = 350 }) =&gt; {
  const chartOption = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c}',
    },
    animationDurationUpdate: 1500,
    animationEasingUpdate: 'quinticInOut',
    series: [
      {
        type: 'graph',
        layout: 'none',
        symbolSize: 55,
        roam: true,
        label: {
          show: true,
          fontSize: 11,
          color: '#fff',
        },
        edgeSymbol: ['circle', 'arrow'],
        edgeSymbolSize: [4, 10],
        edgeLabel: {
          fontSize: 10,
        },
        data: [
          { name: '加载因子数据', x: 150, y: 80, itemStyle: { color: '#0077FA' } },
          { name: '加载价格数据', x: 350, y: 80, itemStyle: { color: '#0077FA' } },
          { name: '股票池过滤', x: 250, y: 180, itemStyle: { color: '#0077FA' } },
          { name: '中性化处理', x: 250, y: 280, itemStyle: { color: '#0077FA' } },
          { name: '计算远期收益', x: 450, y: 280, itemStyle: { color: '#0077FA' } },
          { name: 'IC 分析', x: 150, y: 380, itemStyle: { color: '#10B981' } },
          { name: '分组回测', x: 350, y: 380, itemStyle: { color: '#10B981' } },
          { name: '换手率分析', x: 550, y: 380, itemStyle: { color: '#10B981' } },
          { name: '衰减分析', x: 750, y: 380, itemStyle: { color: '#10B981' } },
        ],
        links: [
          { source: '加载因子数据', target: '股票池过滤' },
          { source: '加载价格数据', target: '股票池过滤' },
          { source: '股票池过滤', target: '中性化处理' },
          { source: '中性化处理', target: '计算远期收益' },
          { source: '计算远期收益', target: 'IC 分析' },
          { source: '计算远期收益', target: '分组回测' },
          { source: '计算远期收益', target: '换手率分析' },
          { source: '计算远期收益', target: '衰减分析' },
        ],
        lineStyle: {
          opacity: 0.9,
          width: 2,
          curveness: 0.2,
        },
      },
    ],
  };

  const analysisModules = [
    {
      title: 'IC 分析',
      desc: '计算 IC 均值、ICIR、胜率、t 统计量等',
      color: '#10B981',
    },
    {
      title: '分组回测',
      desc: '按因子值分层，计算各组收益及多空 Spread',
      color: '#0077FA',
    },
    {
      title: '换手率分析',
      desc: '计算各分层的换手率，衡量策略稳定性',
      color: '#8B5CF6',
    },
    {
      title: '衰减分析',
      desc: '分析因子排名自相关性的衰减速度',
      color: '#F97316',
    },
    {
      title: '行业分析',
      desc: '分行业计算 IC 和收益，查看行业暴露',
      color: '#06B6D4',
    },
    {
      title: '事件研究',
      desc: '分析因子形成前后的累积收益',
      color: '#EC4899',
    },
  ];

  return (
    &lt;div&gt;
      &lt;Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;分析流程图&lt;/span&gt;}
      &gt;
        &lt;ReactECharts option={chartOption} style={{ height }} /&gt;
      &lt;/Card&gt;

      &lt;Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;分析模块说明&lt;/span&gt;}
      &gt;
        &lt;div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 12 }}&gt;
          {analysisModules.map((module, idx) =&gt; (
            &lt;div
              key={idx}
              style={{
                padding: 12,
                background: 'var(--bg-tertiary)',
                borderRadius: 6,
                borderLeft: `3px solid ${module.color}`,
              }}
            &gt;
              &lt;div style={{ fontWeight: 600, color: 'var(--text-primary)', marginBottom: 4 }}&gt;
                {module.title}
              &lt;/div&gt;
              &lt;div style={{ fontSize: 12, color: 'var(--text-secondary)' }}&gt;
                {module.desc}
              &lt;/div&gt;
            &lt;/div&gt;
          ))}
        &lt;/div&gt;
      &lt;/Card&gt;

      &lt;Card
        style={{ background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;核心公式&lt;/span&gt;}
      &gt;
        &lt;FlowFormulaRenderer type="analysis" /&gt;
      &lt;/Card&gt;
    &lt;/div&gt;
  );
};

export default FactorAnalysisFlow;
```

---

## Task 4: 创建 FactorFlowDrawer 主抽屉组件

**Files:**
- Create: `frontend/src/pages/FactorCenter/FactorFlowDrawer.tsx`

**Step 1: 创建组件文件**

```tsx
/**
 * 因子流程可视化抽屉
 * 展示因子计算和分析的完整流程
 */

import React from 'react';
import { Drawer, Tabs } from 'antd';
import { ApartmentOutlined, ApiOutlined, BarChartOutlined } from '@ant-design/icons';
import ReactECharts from 'echarts-for-react';
import { Card } from 'antd';
import FactorCalculationFlow from './components/FactorCalculationFlow';
import FactorAnalysisFlow from './components/FactorAnalysisFlow';

interface FactorFlowDrawerProps {
  open: boolean;
  onClose: () =&gt; void;
  factorId?: string;
}

const FactorFlowDrawer: React.FC&lt;FactorFlowDrawerProps&gt; = ({ open, onClose, factorId }) =&gt; {
  // 概览流程图
  const overviewOption = {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      formatter: '{b}',
    },
    animationDurationUpdate: 1500,
    animationEasingUpdate: 'quinticInOut',
    series: [
      {
        type: 'graph',
        layout: 'none',
        symbolSize: 65,
        roam: true,
        label: {
          show: true,
          fontSize: 11,
          color: '#fff',
        },
        edgeSymbol: ['circle', 'arrow'],
        edgeSymbolSize: [4, 10],
        edgeLabel: {
          fontSize: 10,
        },
        data: [
          { name: '数据加载', x: 100, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '价格复权', x: 250, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '状态过滤', x: 400, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '因子计算', x: 550, y: 120, itemStyle: { color: '#10B981' } },
          { name: '停牌处理', x: 700, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '质量检查', x: 850, y: 120, itemStyle: { color: '#0077FA' } },
          { name: '结果保存', x: 550, y: 240, itemStyle: { color: '#0077FA' } },
          { name: '因子分析', x: 550, y: 360, itemStyle: { color: '#8B5CF6' } },
          { name: 'IC 分析', x: 300, y: 460, itemStyle: { color: '#10B981' } },
          { name: '分组回测', x: 500, y: 460, itemStyle: { color: '#10B981' } },
          { name: '换手率分析', x: 700, y: 460, itemStyle: { color: '#10B981' } },
        ],
        links: [
          { source: '数据加载', target: '价格复权' },
          { source: '价格复权', target: '状态过滤' },
          { source: '状态过滤', target: '因子计算' },
          { source: '因子计算', target: '停牌处理' },
          { source: '停牌处理', target: '质量检查' },
          { source: '质量检查', target: '结果保存' },
          { source: '结果保存', target: '因子分析' },
          { source: '因子分析', target: 'IC 分析' },
          { source: '因子分析', target: '分组回测' },
          { source: '因子分析', target: '换手率分析' },
        ],
        lineStyle: {
          opacity: 0.9,
          width: 2,
          curveness: 0.2,
        },
      },
    ],
  };

  const OverviewTab = () =&gt; (
    &lt;div&gt;
      &lt;Card
        style={{ marginBottom: 16, background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;端到端流程图&lt;/span&gt;}
      &gt;
        &lt;ReactECharts option={overviewOption} style={{ height: 520 }} /&gt;
      &lt;/Card&gt;

      &lt;Card
        style={{ background: 'var(--bg-card)' }}
        title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;快速说明&lt;/span&gt;}
      &gt;
        &lt;div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}&gt;
          &lt;div style={{ padding: 12, background: 'var(--bg-tertiary)', borderRadius: 6 }}&gt;
            &lt;div style={{ fontWeight: 600, color: '#0077FA', marginBottom: 8 }}&gt;
              🔵 因子计算阶段
            &lt;/div&gt;
            &lt;ul style={{ margin: 0, paddingLeft: 16, color: 'var(--text-secondary)', fontSize: 12 }}&gt;
              &lt;li&gt;数据加载和预处理&lt;/li&gt;
              &lt;li&gt;价格复权和状态过滤&lt;/li&gt;
              &lt;li&gt;用户定义因子计算&lt;/li&gt;
              &lt;li&gt;质量检查和结果保存&lt;/li&gt;
            &lt;/ul&gt;
          &lt;/div&gt;
          &lt;div style={{ padding: 12, background: 'var(--bg-tertiary)', borderRadius: 6 }}&gt;
            &lt;div style={{ fontWeight: 600, color: '#10B981', marginBottom: 8 }}&gt;
              🟢 因子分析阶段
            &lt;/div&gt;
            &lt;ul style={{ margin: 0, paddingLeft: 16, color: 'var(--text-secondary)', fontSize: 12 }}&gt;
              &lt;li&gt;IC/IR 计算&lt;/li&gt;
              &lt;li&gt;分组回测&lt;/li&gt;
              &lt;li&gt;换手率分析&lt;/li&gt;
              &lt;li&gt;行业和衰减分析&lt;/li&gt;
            &lt;/ul&gt;
          &lt;/div&gt;
        &lt;/div&gt;
      &lt;/Card&gt;
    &lt;/div&gt;
  );

  const tabItems = [
    {
      key: 'overview',
      label: &lt;span&gt;&lt;ApartmentOutlined /&gt; 概览&lt;/span&gt;,
      children: &lt;OverviewTab /&gt;,
    },
    {
      key: 'calculation',
      label: &lt;span&gt;&lt;ApiOutlined /&gt; 计算流程&lt;/span&gt;,
      children: &lt;FactorCalculationFlow /&gt;,
    },
    {
      key: 'analysis',
      label: &lt;span&gt;&lt;BarChartOutlined /&gt; 分析流程&lt;/span&gt;,
      children: &lt;FactorAnalysisFlow /&gt;,
    },
  ];

  return (
    &lt;Drawer
      title={
        &lt;div style={{ display: 'flex', alignItems: 'center', gap: 12 }}&gt;
          &lt;ApartmentOutlined style={{ color: 'var(--color-primary)' }} /&gt;
          &lt;span&gt;因子流程框架&lt;/span&gt;
          {factorId &amp;&amp; (
            &lt;span style={{ fontSize: 13, color: 'var(--text-muted)' }}&gt;
              · {factorId}
            &lt;/span&gt;
          )}
        &lt;/div&gt;
      }
      open={open}
      onClose={onClose}
      width={900}
      styles={{ body: { padding: '16px 24px' } }}
    &gt;
      &lt;Tabs defaultActiveKey="overview" items={tabItems} size="small" /&gt;
    &lt;/Drawer&gt;
  );
};

export default FactorFlowDrawer;
```

---

## Task 5: 修改 AnalysisPanel 集成按钮

**Files:**
- Modify: `frontend/src/pages/FactorCenter/AnalysisPanel.tsx:778-790` (在"运行分析"按钮旁添加)

**Step 1: 导入必要的依赖**

在文件顶部的 import 区域添加：
```tsx
import { ApartmentOutlined } from '@ant-design/icons';
import FactorFlowDrawer from './FactorFlowDrawer';
```

**Step 2: 添加状态管理**

在 AnalysisPanel 组件内部添加：
```tsx
const [flowDrawerOpen, setFlowDrawerOpen] = useState&lt;boolean&gt;(false);
```

**Step 3: 添加"分布框架"按钮**

找到第 937-946 行的"运行分析"按钮区域，修改为：
```tsx
&lt;div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'flex-end', gap: 8 }}&gt;
  {taskStatus === 'pending' &amp;&amp; &lt;Tag color="orange"&gt;等待中&lt;/Tag&gt;}
  {taskStatus === 'running' &amp;&amp; &lt;Tag color="blue"&gt;分析中&lt;/Tag&gt;}
  {taskStatus === 'completed' &amp;&amp; &lt;Tag color="green"&gt;已完成&lt;/Tag&gt;}
  {taskStatus === 'failed' &amp;&amp; &lt;Tag color="red"&gt;失败&lt;/Tag&gt;}
  &lt;Button
    icon={&lt;ApartmentOutlined /&gt;}
    onClick={() =&gt; setFlowDrawerOpen(true)}
  &gt;
    分布框架
  &lt;/Button&gt;
  &lt;Button type="primary" icon={&lt;BarChartOutlined /&gt;} loading={runLoading} onClick={runAnalysis}&gt;
    运行分析
  &lt;/Button&gt;
&lt;/div&gt;
```

**Step 4: 添加抽屉组件**

在组件的 return 语句最后（AnalysisPanel.tsx:1110 后）添加：
```tsx
  {selectedFactor &amp;&amp; (
    &lt;Card
      style={{ marginTop: 16, background: 'var(--bg-card)' }}
      title={&lt;span style={{ color: 'var(--text-secondary)' }}&gt;分析历史&lt;/span&gt;}
    &gt;
      &lt;Spin spinning={historyLoading}&gt;
        &lt;Table
          dataSource={analysisHistory}
          columns={historyColumns}
          rowKey="id"
          pagination={{ pageSize: 5 }}
          scroll={{ x: 1200 }}
        /&gt;
      &lt;/Spin&gt;
    &lt;/Card&gt;
  )}

  &lt;FactorFlowDrawer
    open={flowDrawerOpen}
    onClose={() =&gt; setFlowDrawerOpen(false)}
    factorId={selectedFactor}
  /&gt;
&lt;/div&gt;
```

---

## Task 6: 验证和测试

**Step 1: 检查 TypeScript 编译**
```bash
cd frontend
npm run build
```

**Step 2: 启动开发服务器验证**
```bash
cd frontend
npm start
```

**Step 3: 手动测试清单**
- [ ] 访问因子分析页面
- [ ] 确认"分布框架"按钮显示在"运行分析"旁边
- [ ] 点击按钮打开抽屉
- [ ] 确认三个标签页正常显示
- [ ] 概览页显示完整流程图
- [ ] 计算流程页显示8步流程
- [ ] 分析流程页显示分析方法
- [ ] 流程图可缩放平移
- [ ] 暗色/亮色主题切换正常
- [ ] 关闭抽屉功能正常

---

## Task 7: 代码审查和格式化

**Step 1: 运行 lint 检查**
```bash
cd frontend
npm run lint
```

**Step 2: 修复任何 lint 错误**

**Step 3: 格式化代码（如需要）**
```bash
cd frontend
npm run format
```

---

## 验收标准

- [ ] 所有新建文件创建完成
- [ ] AnalysisPanel 修改完成
- [ ] TypeScript 编译无错误
- [ ] "分布框架"按钮在 AnalysisPanel 正确显示
- [ ] 点击按钮打开抽屉，显示三个标签页
- [ ] 概览标签页展示端到端流程图
- [ ] 计算流程标签页展示8步流程及公式
- [ ] 分析流程标签页展示分析方法及公式
- [ ] 流程图节点可交互
- [ ] 响应式设计正常工作
- [ ] 暗色/亮色主题适配
- [ ] 代码通过 lint 检查

---

## 文件清单

### 新建文件
- `frontend/src/pages/FactorCenter/components/FlowFormulaRenderer.tsx`
- `frontend/src/pages/FactorCenter/components/FactorCalculationFlow.tsx`
- `frontend/src/pages/FactorCenter/components/FactorAnalysisFlow.tsx`
- `frontend/src/pages/FactorCenter/FactorFlowDrawer.tsx`

### 修改文件
- `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`
