# TODO

> 更新日期: 2026-03-07 | QA 验证: 2026-03-07

## P0 — 安全漏洞（立即修复）

- [x] **C-02** `data_merged.py` 18+ 处 f-string SQL 拼接改为 `%s` 参数化查询
- [ ] **C-05** `/production/factors/test` 端点 `exec()` 执行用户代码 — 添加身份验证 + 限制 `__builtins__`
- [ ] **C-04** ETL 脚本端点允许任意 DolphinDB 命令执行 — 添加授权验证
- [x] **C-06** `production.py` 和 `flows.py` 路径遍历漏洞 — ✅ QA 确认已有正则验证
- [x] **H-07** `engine.py:_finish_run_record` error_msg 拼接 SQL 注入 — 移除分号和反引号
- [x] **H-11** `utils.py:QueryBuilder.build_where_clause` f-string 拼接 — 改用参数化

---

## P1 — 计算错误（影响结果正确性）

- [x] **C-07** `vector_engine.py:125` 年化收益率用了总收益率 — 改为 `stats.get("Annualized Return [%]")`
- [x] **H-02** `momentum.py:68-69` RSI 用 SMA 而非 EWM — ✅ QA 确认已使用 `ewm_mean`
- [x] **H-03** `analyzer.py:210-213` 分位数分组 off-by-one — ✅ QA 确认已使用 `ceil().clip(1, quantiles)`
- [x] **H-21** `analyzer.py:314-316` Sharpe Ratio 未年化 — 乘以 `sqrt(252)`
- [ ] **H-22** `production.py:82-89` 不同周期重复使用相同 IC 值
- [ ] **H-23** `factor.py:76-77` IC 计算始终用 20 日 SMA 而非请求的因子
- [x] **H-04** `technical.py:26` RSI 中 `replace(0, 1e-10)` API 不兼容 — ✅ QA 确认已使用 `fill_nan + epsilon`
- [x] **M-26** `momentum.py:112` 波动率因子硬编码窗口，忽略 `params` 参数 — N/A: 文件不存在

---

## P1 — 前端功能缺陷

- [x] **C-08** FlowEditor 节点表单修改不写回 ReactFlow 节点 data — 添加 `onValueChange` 回调
- [x] **C-09** FlowEditor 本地状态不同步回 Zustand Store — 添加 `useEffect` 同步
- [x] **C-10** DAGEditor 不响应外部任务变更 — 添加 `useEffect` 监听 `initialNodes`/`initialEdges`
- [x] **H-13** `TaskSelector.tsx:36` 响应结构错误 `factorRes.data` → `factorRes.data?.data`
- [x] **H-16** `StrategyCenter.tsx:192` `metrics.max_drawdown * 100` 在 null 时崩溃 — 加 `?? 0`
- [x] **H-15** `Toolbar.tsx:9` `idCounter` HMR 重置导致重复节点 ID — 改用 `crypto.randomUUID()`

---

## P2 — 数据查询问题

- [x] **H-01** `dolphindb_client.py:99-103` `_escape_value` 自动转换 YYYYMMDD 破坏 STRING 列查询 — 移除自动转换
- [x] **H-10** `data_service.py:56-66` `get_daily_data` 静默忽略 `end_date` — ✅ QA 确认已正确处理
- [ ] **M-14** `sync_components.py:405` 增量同步遍历所有日历日而非交易日
- [ ] **M-20** `data_sync_flow.py:127` `sync_all_data` 忽略 `end_date` 参数

---

## P2 — QA 新发现问题

- [ ] **QA-01** `analyzer.py:_build_summary` 分位数标签 off-by-one：对 1-based 分组再 `+1`，标签偏移（Q1→Q2），多空收益取 Q0 永远为 0
- [ ] **QA-02** `data_merged.py:147` LIMIT 未做上限约束，可被恶意请求拉取大量数据
- [ ] **QA-03** `data_merged.py:88/93` f-string 拼接表名（内部配置，低风险，建议改为白名单）
- [ ] **QA-04** `production.py:_delete_factor_dates` 中 `factor_id` 直接插入 DolphinDB 脚本（已有正则缓解，建议改为参数化）

---

## P2 — 架构改进

- [x] **H-12** `dolphindb_client.py:1018` 模块导入时立即连接 DB — 改为延迟初始化
- [x] **H-09** `ml.py:13` `_job_status` 字典无限增长 — ✅ QA 确认已有 TTL 清理
- [x] **H-06** `flow_parser.py:194` 拓扑排序不检测循环 — 添加环检测并抛出异常
- [ ] **H-08** `production.py:614` `sys.modules` 操作非线程安全 — 使用线程锁或独立进程
- [x] **M-01** `main.py:42` CORS 通配符 + credentials 配置冲突 — ✅ QA 确认已正确处理

---

## P3 — 性能优化

- [x] **M-28** `analyzer.py:246` 换手率计算 O(N) Python 循环 — 向量化
- [x] **M-29** `analyzer.py:160` IC 序列计算 O(N) Python 循环 — 向量化
- [x] **M-21** `technical.py:67` ATR 使用 Python 循环 — 改为 Polars 向量化操作
- [ ] 缓存预热机制（系统启动时预加载热点数据）
- [ ] 慢查询日志记录
- [ ] API 性能监控中间件

---

## 新功能 — 因子后处理流水线

处理顺序：去极值 → 中性化 → 标准化，分析/使用时按需执行，用户可配置。

### 去极值（Winsorization）
- [ ] MAD（中位数绝对偏差），默认 n=5
- [ ] 百分位截断（Percentile Clip），如 [1%, 99%]
- [ ] σ 截断（Sigma Clip），默认 n=3

### 中性化（Neutralization）
截面回归取残差：`factor = β₀ + β₁·ln(mv) + Σβᵢ·industry_dummy + ε`

- [ ] 行业中性化（申万一级哑变量回归）
- [ ] 市值中性化（ln(total_mv) 回归）
- [ ] 联合中性化（行业 + 市值一步完成）

前置依赖：
- [ ] 行业分类数据接入（申万一级分类表）
- [ ] `daily_basic` 表 `total_mv` / `circ_mv` 字段同步

### 标准化（Standardization）
- [ ] Z-Score（截面均值0、标准差1），默认
- [ ] Rank 标准化（截面排名映射到 [0,1]）

### IC 计算加权
- [ ] 等权（默认）
- [ ] 流通市值加权（circ_mv）
- [ ] 总市值加权（total_mv）
- [ ] 根号市值加权（sqrt(mv)）

---

## 系统改进

- [ ] DAG 模块增加任务启用/禁用开关
- [ ] 全面添加身份验证中间件（所有危险端点）
- [ ] 系统资源监控（CPU、内存、磁盘）
- [x] 添加 404 catch-all 路由（`App.tsx`）

---

## 已完成

- [x] 重构项目文档结构（2026-02-24）
- [x] 统一配置文件管理（`config/scripts.config.sh`）
- [x] 优化启动脚本（`start.sh`, `stop.sh`, `check_status.sh`）
- [x] 生产因子框架（`@factor` 装饰器 + 8步流水线）
- [x] Alphalens 因子分析集成（IC/IR + 分层收益）
- [x] 清理顶层散乱文件和临时报告（2026-03-07）
- [x] QA 审查：新增测试 53 个（test_technical_factors.py +10, test_analyzer.py +17, test_security.py +16, test_data_service.py +10）（2026-03-07）
