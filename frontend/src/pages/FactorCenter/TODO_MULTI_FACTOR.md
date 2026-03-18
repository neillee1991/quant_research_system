# TODO: 多因子分析模块

## 目标

在 FactorCenter 中新增独立的「多因子分析」Tab，实现多因子合成与相关性分析。

## 功能规划

### 1. 因子相关性矩阵
- 选择多个因子 + 时间区间
- 计算因子值的 Pearson / Spearman 相关系数矩阵
- 热力图展示（ECharts heatmap）

### 2. 多因子合成
- 支持三种合成方式：
  - 等权合成
  - IC 加权（按历史 IC 均值）
  - IC_IR 加权（按历史 ICIR）
- 合成后因子可保存为新因子（调用 productionApi.createFactor）

### 3. 因子正交化
- Gram-Schmidt 正交化
- 去除因子间共线性
- 后端实现：`engine/analysis/orthogonalization.py`（待创建）

### 4. 合成因子分析
- 合成因子可直接进入 Alphalens 分析流程
- 与单因子分析结果对比展示

## 前端实现要点

- 新增 Tab: `FactorCenter/index.tsx` 中加入 `多因子分析` tab
- 新增页面: `FactorCenter/MultiFactorPanel.tsx`
- 新增 Hook: `FactorCenter/hooks/useMultiFactor.ts`
- 后端 API: `productionApi.computeMultiFactor(factorIds, method, startDate, endDate)`

## 后端实现要点

- 新增接口: `POST /api/v1/production/multi-factor/synthesize`
- 新增接口: `POST /api/v1/production/multi-factor/correlation`
- 实现文件: `backend/engine/analysis/multi_factor.py`

## 优先级

低（当前单因子分析功能完善后再实现）
