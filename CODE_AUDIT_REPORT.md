# Quant Research System 代码审计报告

**审计时间:** 2026-04-25  
**审计范围:** 完整代码库，包括架构、安全、规范和性能问题

---

## 1. 系统架构与一致性分析

### 1.1 实际架构与文档的差异

#### ✅ 已完成的优化
- 已彻底移除 `store/dolphindb_client` 向后兼容层
- 已统一使用 `infrastructure/database/dolphindb_client`
- 已从架构文档删除过期的 `engine/factors` 相关内容

#### ⚠️ 架构不一致问题

1. **数据库职责不明确**
   - **问题:** 系统同时使用 DolphinDB 和 PostgreSQL，但职责划分不清晰
   - **影响:** 
     - PostgreSQL 现在处理配置（`factor_configs`、`sync_task_configs`、`flow_configs`）
     - DolphinDB 处理时序数据和部分元数据
     - 导致代码中需要同时维护两套数据库连接
   - **建议:** 明确架构文档，定义两种数据库的严格职责边界

2. **调度器架构文档与实现不一致**
   - **问题:** 文档中仍提到 Prefect，但实际已使用自研调度器
   - **位置:** `CLAUDE.md` 第 244 行
   - **已更新:** 已在本次审计中修正此问题

### 1.2 前后端API调用一致性分析

#### 前端API调用 (`frontend/src/api/index.ts`)
- ✅ 使用 `/api/v1` 前缀，与后端一致
- ✅ 正确区分 `api`（60秒超时）和 `longRunningApi`（5分钟超时）
- ⚠️ 仍引用已删除的 `mlApi` 端点（第87-97行）
- ⚠️ API调用中使用了不存在的 `/data/etl/tasks` 端点，与实际 `/tasks/etl` 不匹配

#### 后端API结构
- ✅ 统一的任务管理 API (`tasks.py`)
- ✅ 因子 API 位于 `/factor/` 目录下
- ✅ 数据 API 位于 `/data/` 目录下
- ⚠️ 缺少认证中间件覆盖（见安全部分）

---

## 2. 文件组织与命名规范

### 2.1 ✅ 做得好的方面
- 模块化架构设计良好，职责分离明确
- 核心功能按功能域划分目录
- 测试文件与业务文件分离
- 文档文件已清理，保留核心文档

### 2.2 ⚠️ 需要改进的方面

1. **测试文件目录结构问题**
   - 所有测试文件平铺在 `tests/` 目录下
   - 建议按以下组织结构：
     ```
     tests/
     ├── unit/          # 单元测试
     ├── integration/   # 集成测试
     ├── api/          # API测试
     └── performance/  # 性能测试
     ```

2. **文件命名一致性**
   - ✅ 后端文件: 蛇形命名 (snake_case)
   - ✅ 前端文件: 骆驼式/kebab-case 
   - ⚠️ 部分文件名称不够语义化

3. **文件和函数大小**
   - 部分文件超过800行（`tasks.py`、`factor_service.py`）
   - 建议拆分为更小的模块，单一职责原则

---

## 3. 安全问题分析 (高优先级)

### 3.1 ⚠️ 严重安全问题

#### 问题1: 硬编码默认密码
**位置:** `app/core/config.py`
```python
dolphindb_password: str = Field(default="123456", env="DOLPHINDB_PASSWORD")
postgres_password: str = Field(default="quant123", env="POSTGRES_PASSWORD")
```
**风险:** 
- 默认密码在生产环境中极不安全
- 容易被暴力破解或扫描到

**建议:**
- 在生产环境强制要求用户修改
- 启动时检查是否仍使用默认密码并发出警告
- 提供密码强度验证

---

#### 问题2: JWT密钥硬编码默认值
**位置:** `app/core/config.py:75`
```python
secret_key: str = Field(default="change-this-in-production-use-openssl-rand-hex-32", env="AUTH_SECRET_KEY")
```

**风险:**
- 默认密钥公开已知
- JWT token可能被伪造

**建议:**
- 生产环境启动时强制要求设置此参数
- 如果检测到默认密钥，阻止应用启动或记录严重警告

---

#### 问题3: 代码执行端点缺少认证
**位置:** `app/api/v1/factor/factor_compute.py:184-424`
```python
@router.post("/factor/factors/test")
async def test_factor_code(req: FactorTestRequest):
    # ... 编译并执行用户输入的代码
    compiled = compile(req.code, "<factor_test>", "exec")
    exec(compiled, namespace)
```

**风险:**
- ⚠️ **极度危险** - 该端点允许任意Python代码执行
- 没有认证保护（无 `Depends(get_current_active_user)`）
- 虽然有安全检查，但检查不充分

**安全检查缺陷:**
```python
# sandbox.py:165-173 - 仅做字符串模式匹配，易被绕过
dangerous_imports = [
    "import os", "from os",
    "import sys", "from sys",
    # ... 
]
for pattern in dangerous_imports:
    if pattern in code:  # 简单字符串匹配，可通过注释/编码绕过
        violations.append(f"禁止的导入: {pattern}")
```

**建议:**
1. ✅ 必须添加认证 (使用 `Depends(get_current_active_user)`)
2. ✅ 添加权限控制，仅允许管理员使用
3. ✅ 考虑移除该端点，或使用真正的容器沙箱环境
4. ✅ 增加更严格的安全检查，使用AST分析而非简单字符串匹配

---

#### 问题4: SQL查询端点无认证
**位置:** 前端 `api/index.ts:83-84` 引用的端点
```typescript
executeQuery: (sql: string, limit = 1000) =>
  api.post('/data/query', null, { params: { sql, limit } }),
```

**风险:**
- 任意SQL查询执行（可能导致数据泄露/删除）
- 缺少认证保护
- 缺少SQL注入防护

**建议:**
- ❗ 立即添加认证
- ❗ 严格限制该端点的权限（仅管理员）
- ❗ 考虑完全移除该端点，或使用安全的查询API
- ❗ 添加白名单验证，仅允许查询特定表

---

#### 问题5: 动态SQL查询中的注入风险
**位置:** `app/api/v1/tasks.py:173-176`、`app/api/v1/tasks.py:219-222`
```python
rows = await DatabasePool.fetch(
    f"SELECT * FROM task_runs WHERE {where} ORDER BY started_at DESC",
    *params,
)
```
- 虽然使用了参数化查询，但 `limit` 直接放入 SQL 中

**位置:** `app/api/v1/factor/factor_registry.py:56`
```python
db_client.execute(f"DELETE FROM factor_values WHERE factor_id = {TypeConverter.escape_symbol(factor_id)}")
```

**风险:**
- 即使使用转义函数，也可能存在绕过风险
- 更好的做法是使用参数化查询

**建议:**
- 始终使用参数化查询
- 对所有用户输入进行验证和类型检查
- 避免字符串拼接构建SQL

---

### 3.2 中等安全问题

#### 认证检查不完整
- **问题:** 大多数 API 端点没有认证要求
- **位置:** 查看所有 API 文件，大部分缺少 `Depends(get_current_active_user)`
- **建议:**
  - 为所有写操作和敏感端点添加认证
  - 实现基于角色的访问控制 (RBAC)

#### 密码认证未实际使用
- **位置:** `app/core/auth.py` 有认证功能，但很少在 API 中使用
- **建议:** 一致地应用认证机制

---

## 4. 代码质量与可维护性

### 4.1 优点
1. ✅ 使用 Pydantic 进行请求验证
2. ✅ 良好的日志记录
3. ✅ 使用环境变量配置
4. ✅ 核心功能有测试覆盖

### 4.2 需要改进的方面

1. **测试覆盖率**
   - 核心业务逻辑测试良好，但API安全测试不足
   - 缺少集成测试和端到端测试
   - 建议：增加API安全测试

2. **代码重复**
   - 数据库操作代码在多处重复
   - 建议：创建统一的数据库访问层

3. **错误处理**
   - 部分错误过于宽泛，使用 `Exception` 捕获所有
   - 建议：使用更具体的异常类型

---

## 5. 优化建议与优先级

### 🔴 紧急修复（立即）
1. 为 `/factor/factors/test` 端点添加认证
2. 为 `/data/query` 端点添加认证或直接删除
3. 生成生产部署文档，强制要求修改默认密码

### 🟠 高优先级（1周内）
1. 为所有敏感API端点添加认证
2. 改进安全沙箱机制，使用AST分析而非简单字符串匹配
3. 统一使用参数化查询，避免字符串拼接
4. 添加API访问日志和监控

### 🟡 中优先级（1个月内）
1. 重构大型文件为更小的模块
2. 完善测试覆盖，增加安全专项测试
3. 完善认证机制，从假用户改为真实数据库用户
4. 实现RBAC权限控制

### 🟢 低优先级（优化改进）
1. 改进API文档
2. 添加性能监控和度量
3. 优化数据库查询
4. 添加更严格的类型检查

---

## 6. 架构建议

### 6.1 数据库架构建议
保持双数据库架构，但明确职责边界：

**PostgreSQL (元数据与配置)**
- ✅ 用户认证与授权
- ✅ 任务配置 (`factor_configs`, `sync_task_configs`, `etl_task_configs`)
- ✅ 任务运行历史 (`task_runs`, `flow_runs`)
- ✅ 系统配置

**DolphinDB (时序数据)**
- ✅ 因子值 (`factor_values`)
- ✅ 行情数据 (`daily_basic`, `daily_data`, `adj_factor`)
- ✅ 其他时序业务数据

### 6.2 API安全建议
1. 实现完整的认证体系
2. 所有API端点都应该有认证
3. 敏感端点需要双重验证
4. 定期轮换JWT密钥

---

## 7. 审计总结

### 整体评价
Quant Research System 是一个设计良好的量化研究系统，具有清晰的模块划分和较好的架构设计。核心功能实现良好，测试覆盖基本满足要求。

### 主要问题
- **安全是最大短板** - 多个高风险安全问题需要立即修复
- **认证覆盖不足** - 大部分API缺少认证
- **架构文档需更新** - 部分文档与实现不一致

### 下一步行动
1. 立即修复🔴优先级的安全问题
2. 在生产部署前完成🟠优先级修复
3. 按计划完成其他优化

---

### 附录 A: 快速检查清单

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 删除旧store/dolphindb_client | ✅ 完成 | 已彻底移除向后兼容层 |
| 统一种子数据管理 | ✅ 完成 | 已明确配置导入方式 |
| 更新架构文档 | 🔄 进行中 | CLAUDE.md已更新 |
| 修复安全问题 | ⚠️ 待处理 | 多个高优先级问题 |
| 增加测试覆盖 | ✅ 核心测试通过 | 核心测试已通过 |

### 附录 B: 关键文件清单

| 文件路径 | 功能 | 问题 |
|----------|------|------|
| `app/core/config.py` | 配置管理 | 默认密码不安全 |
| `app/core/auth.py` | 认证 | 认证功能未充分利用 |
| `app/core/sandbox.py` | 代码沙箱 | 安全检查不够严格 |
| `app/api/v1/factor/factor_compute.py` | 因子测试API | 代码执行无认证 |
| `infrastructure/database/dolphindb_client.py` | 数据库客户端 | ✅ 架构良好 |
| `engine/factor/registry.py` | 因子注册 | ✅ 设计良好 |

---

**审计完成时间:** 2026-04-25  
**审计员:** Claude AI Code Audit Agent
