# 系统架构重构与改进方案

**日期**: 2026-04-11  
**版本**: v1.0  
**状态**: 待审批

---

## 一、背景与目标

### 1.1 背景

基于 2026-04-11 的全面系统审计，发现 **186 个问题**：
- 🔴 CRITICAL: 11个
- 🟠 HIGH: 110个
- 🟡 MEDIUM: 50个
- 🟢 LOW: 15个

### 1.2 目标

- **安全性**: 通过 OWASP Top 10 检查
- **架构**: 统一数据库职责，消除架构债务
- **代码质量**: 类型注解 ≥ 80%，测试覆盖 ≥ 80%
- **数据安全**: 自动化备份 + 恢复测试 + 灾难恢复计划
- **可维护性**: 架构文档完整，新人上手时间减少 30%

---

## 二、团队组织

### 2.1 团队架构

| 小组 | 人数 | 职责 | 负责人 |
|------|------|------|--------|
| **安全组** | 2人 | 认证、授权、SQL注入修复、漏洞修复 | 安全架构师 |
| **架构组** | 2人 | 数据库职责划分、架构债务清理、监控 | 系统架构师 |
| **后端组** | 3人 | 代码质量、类型注解、长函数重构 | 后端负责人 |
| **前端组** | 2人 | 状态管理、类型安全、性能优化 | 前端负责人 |
| **DBA组** | 2人 | 数据库约束、索引、备份、迁移 | 数据库管理员 |

**总计**: 11人

### 2.2 沟通机制

- **每日站会**（15分钟）: 各小组同步进度和 blockers
- **周技术评审**（2小时）: 代码审查，设计讨论
- **双周集成测试**（4小时）: 全链路集成测试
- **月进度汇报**: 向利益相关者汇报进度

---

## 三、阶段规划

### Phase 1: 安全加固（Week 1-2）- P0 优先级

**目标**: 修复所有 11 个 CRITICAL 安全问题

#### Week 1: 认证与代码执行保护

**安全组**:
- [ ] 实现 JWT/OAuth2 认证中间件
- [ ] 为所有敏感端点添加 `Depends(get_current_user)`
- [ ] 实现 RBAC 角色权限控制
- [ ] 保护 `/factor/factors/test` 端点（认证 + RestrictedPython 沙箱）
- [ ] 从 Git 历史移除 `.env`，轮换所有凭证

#### Week 2: SQL注入与备份策略

**安全组**:
- [ ] 修复所有 18+ 处 SQL 注入
- [ ] 表名/列名白名单验证
- [ ] 修改 DolphinDB 默认密码
- [ ] 生产环境密码强制验证

**DBA组**:
- [ ] PostgreSQL `pg_dump` 定时备份
- [ ] DolphinDB 备份工具
- [ ] 定义备份保留策略和完整性校验

---

### Phase 2: 架构统一（Week 3-6）- P0/P1 优先级

**目标**: 明确数据库职责边界，消除架构债务

#### Week 3-4: 数据库职责划分

**架构组**:
- [ ] 制定数据库迁移计划（`stocks`/`trade_cal` PG → DolphinDB）
- [ ] 删除 `store/dolphindb_client.py`，统一使用 `infrastructure/database/`
- [ ] 实现 DolphinDB 连接池
- [ ] 消除循环依赖
- [ ] 统一导入路径

**DBA组**:
- [ ] 添加 PostgreSQL 外键约束
- [ ] 添加 CHECK 约束（状态、类型字段）
- [ ] 添加 `updated_at` 自动更新触发器
- [ ] 创建 `schema_migrations` 表

#### Week 5-6: 监控与缓存

**架构组**:
- [ ] 添加 `/health` 健康检查端点
- [ ] 集成 Prometheus metrics
- [ ] 实现缓存失效机制（数据更新后清理）
- [ ] 评估引入 Redis（可选）

**DBA组**:
- [ ] 添加缺失索引（外键列、常用查询列）
- [ ] 编写恢复测试计划

---

### Phase 3: 代码质量（Week 7-10）- P1 优先级

**目标**: 提高代码质量，修复所有 HIGH 问题

#### Week 7-8: 大文件与长函数重构

**后端组**:
- [ ] 拆分 `etl_api.py`（883行）→ `etl_router.py` + `etl_service.py` + `etl_repository.py`
- [ ] 拆分 `test_pipeline_integration.py`（860行）→ 按功能分类
- [ ] 重构 `run_full_analysis()`（331行）→ 拆分为 7+ 个独立函数
- [ ] 重构 Top 10 长函数（共 71 个）

#### Week 9-10: 类型注解与测试覆盖

**后端组**:
- [ ] 为 31+ 个无类型注解文件添加类型注解
- [ ] 目标：类型注解覆盖率 50.9% → 80%+
- [ ] 按类型组织测试目录（`unit/`, `integration/`, `api/`）
- [ ] 配置 pytest + coverage
- [ ] 目标：测试覆盖率 ≥ 80%
- [ ] 完善错误处理（不吞掉异常）
- [ ] 结构化错误响应

**前端组**:
- [ ] 后端新增批量状态 API
- [ ] 前端修改 `useTasks` Hook（修复 N+1）
- [ ] 移除所有 `any` 类型
- [ ] 为 API 层定义完整 interfaces
- [ ] 移除 FlowEditor 双重状态（直接用 Zustand）
- [ ] 创建 `useModal` Hook 管理 17 个模态框
- [ ] 修复 useEffect 依赖数组问题

---

### Phase 4: 完善优化（Week 11-12）- P2/P3 优先级

**目标**: 完善功能，优化性能

#### Week 11: 文档整合与前端优化

**架构组**:
- [ ] 合并重复文档
- [ ] 删除过时文档
- [ ] 建立 ADR 流程

**前端组**:
- [ ] 移除所有 console.log（27个文件）
- [ ] 实现长列表虚拟化（`react-window`）
- [ ] 图表配置用 `useMemo` 缓存
- [ ] 添加 React Error Boundaries
- [ ] 考虑使用 React Query

**DBA组**:
- [ ] JSON 字段重构（配置类拆分为关联表）
- [ ] 统一主键设计（BIGSERIAL 代理主键）
- [ ] 清理 `task_runs` 冗余字段
- [ ] 考虑使用迁移工具（Alembic/Flyway）

#### Week 12: 性能调优与可观测性

**架构组**:
- [ ] 查询分析和优化
- [ ] 批量操作优化
- [ ] 分布式追踪
- [ ] SLI/SLO 定义
- [ ] 开发环境 Docker Compose
- [ ] 热重载优化

---

## 四、数据库职责边界

### 4.1 PostgreSQL 职责（系统配置和日志）

**保留的表**:
- ✅ `flow_configs`, `flow_runs`, `task_runs` - 调度配置和日志
- ✅ `sync_task_configs`, `etl_task_configs`, `factor_configs` - 配置表
- ✅ `factor_field_mappings` - 字段映射配置
- ✅ `user_preferences` - 用户偏好
- ✅ `factor_analysis_results`, `backtest_results` - 分析结果
- ✅ `index_configs` - 指数配置

**新增的表**:
- ✅ `schema_migrations` - 迁移版本控制
- ✅ `audit_logs` - 审计日志

**移除的表**:
- ❌ `stocks`, `trading_calendar` - 迁移到 DolphinDB

### 4.2 DolphinDB 职责（业务数据）

**保留的表**:
- ✅ 所有 TSDB 表（`sync_daily_data`, `sync_daily_basic`, `sync_adj_factor`, `sync_index_daily`, `sync_moneyflow`）
- ✅ `factor_values` - 因子计算结果
- ✅ `stock_basic`, `trade_cal` - 保持不变

**新增的表**:
- ✅ `stocks`, `trade_cal` - 从 PostgreSQL 迁移

**移除的表**:
- ❌ 无（配置表保留在 PostgreSQL）

---

## 五、详细任务分配

### 安全组（2人）
**负责人**: 安全架构师

**Week 1-2**:
- [ ] 实现 JWT/OAuth2 认证中间件
- [ ] 为所有敏感端点添加 `Depends(get_current_user)`
- [ ] 实现 RBAC 角色权限控制
- [ ] 保护 `/factor/factors/test` 端点（认证 + RestrictedPython 沙箱）
- [ ] 从 Git 历史移除 `.env`，轮换所有凭证
- [ ] 修复所有 18+ 处 SQL 注入
- [ ] 表名/列名白名单验证
- [ ] 修改 DolphinDB 默认密码
- [ ] 生产环境密码强制验证

### 架构组（2人）
**负责人**: 系统架构师

**Week 3-6**:
- [ ] 制定数据库迁移计划（`stocks`/`trade_cal` PG → DolphinDB）
- [ ] 删除 `store/dolphindb_client.py`，统一使用 `infrastructure/database/`
- [ ] 实现 DolphinDB 连接池
- [ ] 添加 `/health` 健康检查端点
- [ ] 集成 Prometheus metrics
- [ ] 实现缓存失效机制（数据更新后清理）
- [ ] 评估引入 Redis（可选）
- [ ] 消除循环依赖
- [ ] 统一导入路径

**Week 11-12**:
- [ ] 合并重复文档
- [ ] 删除过时文档
- [ ] 建立 ADR 流程
- [ ] 查询分析和优化
- [ ] 批量操作优化
- [ ] 分布式追踪
- [ ] SLI/SLO 定义
- [ ] 开发环境 Docker Compose
- [ ] 热重载优化

### 后端组（3人）
**负责人**: 后端负责人

**Week 7-10**:
- [ ] 拆分 `etl_api.py`（883行）→ `etl_router.py` + `etl_service.py` + `etl_repository.py`
- [ ] 拆分 `test_pipeline_integration.py`（860行）→ 按功能分类
- [ ] 重构 `run_full_analysis()`（331行）→ 拆分为 7+ 个独立函数
- [ ] 重构 Top 10 长函数（共 71 个）
- [ ] 为 31+ 个无类型注解文件添加类型注解
- [ ] 目标：类型注解覆盖率 50.9% → 80%+
- [ ] 按类型组织测试目录（`unit/`, `integration/`, `api/`）
- [ ] 配置 pytest + coverage
- [ ] 目标：测试覆盖率 ≥ 80%
- [ ] 完善错误处理（不吞掉异常）
- [ ] 结构化错误响应

### 前端组（2人）
**负责人**: 前端负责人

**Week 3-12**:
- [ ] 后端新增批量状态 API
- [ ] 前端修改 `useTasks` Hook（修复 N+1）
- [ ] 移除所有 `any` 类型
- [ ] 为 API 层定义完整 interfaces
- [ ] 移除 FlowEditor 双重状态（直接用 Zustand）
- [ ] 创建 `useModal` Hook 管理 17 个模态框
- [ ] 修复 useEffect 依赖数组问题
- [ ] 移除所有 console.log（27个文件）
- [ ] 实现长列表虚拟化（`react-window`）
- [ ] 图表配置用 `useMemo` 缓存
- [ ] 添加 React Error Boundaries
- [ ] 考虑使用 React Query

### DBA组（2人）
**负责人**: 数据库管理员

**Week 1-12**:
- [ ] PostgreSQL `pg_dump` 定时备份
- [ ] DolphinDB 备份工具
- [ ] 定义备份保留策略和完整性校验
- [ ] 添加 PostgreSQL 外键约束
- [ ] 添加 CHECK 约束（状态、类型字段）
- [ ] 添加 `updated_at` 自动更新触发器
- [ ] 完成数据迁移决策（`stocks`/`trade_cal`）
- [ ] 创建 `schema_migrations` 表
- [ ] 添加缺失索引（外键列、常用查询列）
- [ ] JSON 字段重构（配置类拆分为关联表）
- [ ] 统一主键设计（BIGSERIAL 代理主键）
- [ ] 清理 `task_runs` 冗余字段
- [ ] 编写恢复测试计划
- [ ] 考虑使用迁移工具（Alembic/Flyway）

---

## 六、风险管理

| 风险 | 影响 | 概率 | 应对措施 |
|------|------|------|---------|
| 认证机制引入新 bug | 高 | 中 | 先在 staging 测试，灰度发布 |
| 数据库迁移数据丢失 | 严重 | 低 | 迁移前完整备份，迁移后校验 |
| 长函数重构引入 regression | 高 | 中 | 完善测试，重构后全面回归 |
| 团队沟通不畅 | 中 | 中 | 每日站会，周同步会议 |
| 时间不足，任务延期 | 高 | 高 | 优先级管理，可裁剪 P2/P3 任务 |

---

## 七、成功标准

### Phase 1 完成标准
- [ ] 所有 11 个 CRITICAL 安全问题已修复
- [ ] 认证授权机制上线
- [ ] 自动化备份策略实施

### Phase 2 完成标准
- [ ] 数据库职责边界清晰
- [ ] 架构债务消除
- [ ] 健康检查和监控上线

### Phase 3 完成标准
- [ ] 类型注解覆盖率 ≥ 80%
- [ ] 测试覆盖率 ≥ 80%
- [ ] 所有 HIGH 问题已修复

### Phase 4 完成标准
- [ ] 文档整合完成
- [ ] 性能优化完成
- [ ] 可观测性完善

---

**文档结束**
