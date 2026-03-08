# Changelog

All notable changes to QuantSystem Backend will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-03-08

### 🚨 BREAKING CHANGES

#### 移除版本管理功能

系统移除了配置的版本管理功能，配置更新现在采用直接覆盖模式。

**影响范围:**
- 数据同步任务配置 (`sync_task_config`)
- 因子配置 (`factor_data_config`)
- ETL 任务配置 (`etl_task_config`)

**主要变更:**

1. **数据库变更**
   - 移除 `version` 字段
   - 移除 `created_at` 字段
   - 移除 `created_by` 字段
   - 删除版本历史表 (`*_history`)
   - 添加 `updated_at` 字段用于跟踪最后更新时间

2. **API 变更**
   - 移除版本历史查询端点: `GET /api/v1/tasks/{type}/{id}/versions`
   - 移除特定版本查询端点: `GET /api/v1/tasks/{type}/{id}/versions/{version}`
   - 移除版本回滚端点: `POST /api/v1/tasks/{type}/{id}/rollback/{version}`
   - 配置更新端点不再返回 `version` 字段

3. **前端变更**
   - 移除版本历史查看器组件
   - 移除版本对比功能
   - 移除回滚按钮
   - 添加配置导出功能
   - 更新确认对话框增强警告提示

**迁移指南:**
- 详细迁移步骤请参考: [MIGRATION_GUIDE_NO_VERSION.md](./docs/MIGRATION_GUIDE_NO_VERSION.md)
- 配置管理最佳实践: [USER_GUIDE_CONFIG_MANAGEMENT.md](./docs/USER_GUIDE_CONFIG_MANAGEMENT.md)

**替代方案:**
- 使用配置导出功能进行手动备份
- 将重要配置纳入 Git 版本控制
- 定期备份 DolphinDB 数据库
- 通过审计日志追踪配置变更历史

### Added

- 配置导出功能 - 支持导出配置为 JSON 文件
- 批量配置备份脚本 - 一键备份所有配置
- 配置验证工具 - 更新前验证配置正确性
- 增强的审计日志 - 记录所有配置变更操作

### Changed

- 配置更新采用直接覆盖模式
- API 响应格式简化，移除版本相关字段
- 前端更新确认对话框增强警告提示
- 文档更新以反映新的配置管理流程

### Removed

- 版本管理相关的数据库字段和表
- 版本历史查询 API 端点
- 版本回滚 API 端点
- 前端版本历史查看器
- 前端版本对比功能
- 前端回滚按钮

### Fixed

- 配置更新时的并发冲突问题
- 配置查询性能优化（移除版本关联查询）

### Security

- 配置更新操作增加审计日志记录
- 敏感配置信息建议使用环境变量

---

## [1.9.0] - 2026-03-03

### Added

- DolphinDB 客户端重构 - 实现单例模式连接管理
- API 模块重构 - 统一 Data API 和 Production API 接口
- 数据管道架构 - 可组合的数据处理管道
- 因子计算服务 - 新的 FactorComputeService

### Changed

- 采用不可变数据模式提高代码可维护性
- 优化查询性能 30-50%
- 改进错误处理和异常管理

### Fixed

- DolphinDB 连接池泄漏问题
- 因子计算中的数据类型转换错误
- 停牌股票处理逻辑优化

---

## [1.8.0] - 2026-02-15

### Added

- 任务管理抽象层 - 统一的 CRUD 操作
- 版本控制功能 - 配置版本管理和回滚
- 通用任务服务 - 60% 代码复用率

### Changed

- API 端点结构统一化
- 配置模型使用 Pydantic 验证

---

## [1.7.0] - 2026-02-01

### Added

- Prefect 3.x 工作流编排
- 增量数据同步引擎
- 因子分析模块 (IC, IR, Sharpe)

### Changed

- 数据同步性能优化
- 因子计算引擎重构

---

## [1.6.0] - 2026-01-15

### Added

- 向量化回测引擎
- AutoML 模型训练集成
- 技术指标库扩展 (60+ 指标)

### Changed

- 回测性能提升 10x
- 因子计算采用 Polars 向量化

---

## [1.5.0] - 2025-12-01

### Added

- DolphinDB 时间序列数据库集成
- 生产因子计算引擎
- 8 步因子计算流程

### Changed

- 从 PostgreSQL 迁移到 DolphinDB
- 数据存储架构重构

---

## [1.0.0] - 2025-10-01

### Added

- 初始版本发布
- FastAPI 后端框架
- 基础数据同步功能
- 简单因子计算
- 基础回测功能

---

## 版本说明

### 版本号规则

遵循语义化版本 (Semantic Versioning):

- **主版本号 (MAJOR)**: 不兼容的 API 变更
- **次版本号 (MINOR)**: 向后兼容的功能新增
- **修订号 (PATCH)**: 向后兼容的问题修正

### 变更类型

- **Added**: 新增功能
- **Changed**: 功能变更
- **Deprecated**: 即将废弃的功能
- **Removed**: 已移除的功能
- **Fixed**: 问题修复
- **Security**: 安全相关变更

### 重大变更标识

使用 🚨 标识重大变更 (BREAKING CHANGES)，需要用户采取行动。

---

## 升级指南

### 从 v1.x 升级到 v2.0

**必读文档:**
- [版本移除迁移指南](./docs/MIGRATION_GUIDE_NO_VERSION.md)
- [配置管理用户指南](./docs/USER_GUIDE_CONFIG_MANAGEMENT.md)

**升级步骤:**

1. **备份数据**
   ```bash
   # 备份 DolphinDB 数据库
   docker exec dolphindb /opt/dolphindb/server/dolphindb \
     -script backup_database.dos

   # 导出所有配置
   python scripts/backup_all_configs.py
   ```

2. **更新代码**
   ```bash
   git fetch origin
   git checkout v2.0.0
   ```

3. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

4. **运行数据库迁移**
   ```bash
   python database/migrations/remove_version_fields.py
   ```

5. **验证系统**
   ```bash
   # 运行测试
   pytest tests/

   # 验证配置完整性
   python scripts/validate_configs.py
   ```

6. **启动服务**
   ```bash
   ./start.sh
   ```

7. **验证功能**
   - 测试配置查询功能
   - 测试配置更新功能
   - 验证数据同步任务
   - 验证因子计算任务

**回滚计划:**

如果升级后遇到问题，可以回滚到 v1.9.0:

```bash
# 停止服务
./stop.sh

# 恢复数据库
docker exec -i dolphindb dolphindb < backups/dolphindb_v1.sql

# 切换代码版本
git checkout v1.9.0
pip install -r requirements.txt

# 重启服务
./start.sh
```

---

## 支持

如有问题或建议:

1. 查看文档: `/docs/`
2. 检查日志: `backend/logs/app.log`
3. 提交 Issue 或联系开发团队

---

## 致谢

感谢所有为 QuantSystem 做出贡献的开发者和用户。

---

**最后更新:** 2026-03-08
**维护者:** QuantSystem Team
