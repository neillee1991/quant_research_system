# 因子流程可视化组件设计文档

**日期**: 2026-03-28
**作者**: Claude Code
**版本**: 1.0

## 概述

在因子分析页面增加"分布框架"按钮，点击后弹出抽屉，通过文字公式和图形展示因子计算和分析流程。

## 目标

- 帮助用户理解因子计算的8步Pipeline流程
- 展示因子分析的核心方法和公式
- 提供交互式的流程图可视化
- 保持与现有UI风格一致

## 方案选择

采用**方案一：独立流程可视化组件**

### 理由
1. 用户在分析页面时最需要查看流程，独立抽屉体验最好
2. 遵循单一职责原则，代码更清晰
3. 便于后续扩展（如添加动画、交互等）

## 组件结构

```
frontend/src/pages/FactorCenter/
├── AnalysisPanel.tsx              # 已存在，添加按钮
├── FactorFlowDrawer.tsx           # 新建，主抽屉组件
└── components/
    ├── FactorCalculationFlow.tsx  # 计算流程可视化
    ├── FactorAnalysisFlow.tsx     # 分析流程可视化
    └── FlowFormulaRenderer.tsx    # 公式渲染器
```

## 详细设计

### 1. FactorFlowDrawer 组件

**Props**:
```typescript
interface FactorFlowDrawerProps {
  open: boolean;
  onClose: () => void;
  factorId?: string;  // 可选，传入后展示特定因子的流程
}
```

**UI 布局**:
- 宽度：900px（比 FactorDrawer 稍宽，便于展示流程图）
- 顶部：标题 + 关闭按钮
- Tab 栏：三个标签页
  - 📊 概览
  - ⚙️ 计算流程
  - 📈 分析流程

### 2. 概览标签页

**内容**:
1. 端到端完整流程图（从数据加载 → 因子计算 → 因子分析）
2. 关键节点可点击展开详情
3. 右侧面板展示选中节点的详细说明

**流程图节点**:
```
数据加载 → 价格复权 → 状态过滤 → 因子计算 → 停牌处理 → 质量检查 → 结果保存
                                                          ↓
                                                    因子分析
                                                          ↓
                    ┌─────────────────────────────────────┼─────────────────────────────────────┐
                    ↓                                     ↓                                     ↓
                IC 分析                              分组回测                              换手率分析
```

### 3. 计算流程标签页

**内容**:
1. 8步 Pipeline 详细流程图
2. 每步的数学公式/伪代码
3. 数据变换说明（输入输出 schema）

**8步流程详细说明**:

| 步骤 | 名称 | 公式/说明 | 输入 | 输出 |
|------|------|----------|------|------|
| 1 | 日期解析 | 确定 data_start = start_date - lookback_days | 起始/结束日期 | 数据加载日期范围 |
| 2 | 数据加载 | 按 depends_on 从 DolphinDB 查询 | 表名、日期范围 | 原始 DataFrame |
| 3 | 价格复权 | adj_close = close × adj_factor<br>adj_open = open × adj_factor | OHLCV + 复权因子 | 复权后 DataFrame |
| 4 | 状态过滤 | 过滤：is_st = true<br>过滤：list_date < 60天<br>标记：is_limit = true | 状态表 | 过滤后 DataFrame + 标记列 |
| 5 | 因子计算 | factor_value = f(df, params) | 处理后 DataFrame | 含 factor_value 的 DataFrame |
| 6 | 停牌处理 | 停牌期间 factor_value = null | 停牌标记 | 最终因子值 |
| 7 | 质量检查 | null_rate = count(null) / total<br>extreme_flag = \|z-score\| > 3 | 因子值 | 质量标记 |
| 8 | 结果保存 | upsert into factor_values | 因子值 + 质量标记 | - |

### 4. 分析流程标签页

**内容**:
1. 因子分析完整流程图
2. 核心公式展示（IC、IR、分组收益等）
3. 数据流向说明

**核心公式**:

**1. IC (Information Coefficient)**
```
IC_t = corr(factor_{t-1}, forward_return_t)
IC_mean = mean(IC_t)
IC_std = std(IC_t)
ICIR = IC_mean / IC_std
```

**2. 分组收益**
```
quantile_i = quantile(factor, i / N)  i=1..N
mean_return_i = mean(forward_return | factor ∈ quantile_i)
spread = mean_return_N - mean_return_1
```

**3. 换手率**
```
turnover_{t,i} = fraction_not_in_quantile_i(quantile_{t,i}, quantile_{t-1,i})
```

### 5. AnalysisPanel 集成

在"运行分析"按钮旁边添加：
```tsx
<Button
  icon={<ApartmentOutlined />}
  onClick={() => setFlowDrawerOpen(true)}
>
  分布框架
</Button>
```

## 技术实现

### 流程图库选择

使用 **ECharts Graph** 绘制节点连接图，理由：
- 项目已使用 ECharts，无需引入新依赖
- 支持节点点击交互
- 样式自定义能力强

### 公式渲染

使用简单的 HTML/CSS 配合 sub/sup 标签展示数学公式，如需要更复杂的公式可考虑引入 KaTeX。

### 响应式设计

- 抽屉宽度固定 900px
- 流程图支持缩放和平移
- 在小屏幕上自动调整布局

## 文件清单

### 新建文件
- `frontend/src/pages/FactorCenter/FactorFlowDrawer.tsx`
- `frontend/src/pages/FactorCenter/components/FactorCalculationFlow.tsx`
- `frontend/src/pages/FactorCenter/components/FactorAnalysisFlow.tsx`
- `frontend/src/pages/FactorCenter/components/FlowFormulaRenderer.tsx`

### 修改文件
- `frontend/src/pages/FactorCenter/AnalysisPanel.tsx`

## 风险与注意事项

1. **性能**: 流程图节点较多时注意渲染性能，使用 ECharts 的虚拟滚动
2. **兼容性**: 确保在暗色/亮色主题下都有良好的显示效果
3. **可维护性**: 流程描述文本考虑国际化，虽然当前只有中文
4. **用户体验**: 添加加载状态和错误处理

## 验收标准

- [ ] "分布框架"按钮在 AnalysisPanel 正确显示
- [ ] 点击按钮打开抽屉，显示三个标签页
- [ ] 概览标签页展示端到端流程图
- [ ] 计算流程标签页展示8步流程及公式
- [ ] 分析流程标签页展示分析方法及公式
- [ ] 流程图节点可点击交互
- [ ] 响应式设计正常工作
- [ ] 暗色/亮色主题适配
- [ ] 代码通过 lint 检查

## 后续扩展

- [ ] 添加流程动画演示
- [ ] 支持选择特定因子展示其具体计算逻辑
- [ ] 添加流程图导出为图片功能
- [ ] 集成用户操作指引（新手引导）
