# TODO

## 待实现功能

### 因子后处理流水线

处理顺序：去极值 → 中性化 → 标准化，在分析/使用时按需执行，用户可配置。

#### 去极值（Winsorization）
- [ ] MAD（中位数绝对偏差），默认 n=5
- [ ] 百分位截断（Percentile Clip），如 [1%, 99%]
- [ ] σ 截断（Sigma Clip），默认 n=3

#### 中性化（Neutralization）
截面回归取残差：`factor = β₀ + β₁·ln(mv) + Σβᵢ·industry_dummy + ε`

- [ ] 行业中性化（申万一级哑变量回归）
- [ ] 市值中性化（ln(total_mv) 回归）
- [ ] 联合中性化（行业 + 市值一步完成）

前置依赖：
- [ ] 行业分类数据接入（申万一级分类表）
- [ ] daily_basic 表 total_mv / circ_mv 字段同步

#### 标准化（Standardization）
- [ ] Z-Score（截面均值0、标准差1），默认
- [ ] Rank 标准化（截面排名映射到 [0,1]）

#### IC 计算加权
- [ ] 等权（默认）
- [ ] 流通市值加权（circ_mv）
- [ ] 总市值加权（total_mv）
- [ ] 根号市值加权（sqrt(mv)）

#### 配置结构
```python
"postprocess": {
    "winsorize": {"method": "mad", "n": 5},
    "neutralize": {"industry": True, "industry_source": "sw_l1", "market_cap": True},
    "standardize": {"method": "zscore"},
    "weight": "equal",
}
```

---

## 系统改进

### DAG 模块
- [ ] 增加任务启用/禁用开关

### 缓存优化
- [ ] 缓存预热机制（系统启动时预加载热点数据）
- [ ] 缓存键版本号支持
- [ ] 本地内存缓存作为 Redis 备份

### 监控增强
- [ ] 慢查询日志记录
- [ ] API 性能监控中间件
- [ ] 系统资源监控（CPU、内存、磁盘）

---

## 文档完善

- [x] 重构项目文档结构
- [x] 统一配置文件管理
- [x] 优化启动脚本
- [ ] 添加开发者指南
- [ ] 添加 API 使用示例
