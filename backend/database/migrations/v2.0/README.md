# v2.0 数据库迁移说明

## 概述
本目录包含 v2.0 版本的数据库迁移脚本和回滚脚本。

## 文件说明
- `migrate.py` - 数据库迁移脚本
- `rollback.py` - 数据库回滚脚本
- `README.md` - 本说明文件

## 迁移内容

### 1. 新增字段
- `factor_metadata.version` - 因子版本号（示例）

### 2. 索引优化
- 验证分区表配置
- 优化查询性能

### 3. 数据迁移
- 更新因子元数据默认值
- 数据清理和规范化

## 使用方法

### 执行迁移
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
source .venv/bin/activate
python database/migrations/v2.0/migrate.py
```

### 执行回滚
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
source .venv/bin/activate
python database/migrations/v2.0/rollback.py
```

## 注意事项

### 迁移前
1. **备份数据库**: 确保已完整备份生产数据
2. **测试环境验证**: 在 staging 环境完整测试迁移流程
3. **停机时间**: 预计迁移时间 10-30 分钟
4. **通知用户**: 提前通知用户系统维护时间

### 迁移中
1. **监控日志**: 实时监控迁移日志输出
2. **数据验证**: 每个步骤完成后验证数据完整性
3. **准备回滚**: 如遇问题立即执行回滚

### 迁移后
1. **健康检查**: 执行完整的健康检查
2. **功能验证**: 验证核心业务功能
3. **性能监控**: 监控系统性能指标
4. **保留备份**: 至少保留备份 7 天

## DolphinDB 特殊说明

### 表结构变更
DolphinDB 不支持标准的 `ALTER TABLE ADD/DROP COLUMN`，需要：
1. 创建新表结构
2. 迁移数据到新表
3. 删除旧表
4. 重命名新表

### 分区表处理
- 分区表的迁移需要特别注意分区键
- 建议使用 `createPartitionedTable` 重建
- 数据迁移使用 `append!` 函数

### 备份策略
- 维度表: 使用 `select * from table` 创建内存表备份
- 分区表: 使用 DolphinDB 的备份功能或导出到文件

## 回滚策略

### 自动回滚
- 迁移失败时自动触发回滚
- 从备份表恢复数据
- 验证数据完整性

### 手动回滚
- 执行 `rollback.py` 脚本
- 手动恢复备份数据
- 重启相关服务

## 验证清单

### 迁移后验证
- [ ] 所有表结构正确
- [ ] 数据行数一致
- [ ] 索引/分区正常
- [ ] 查询性能正常
- [ ] 应用连接正常
- [ ] 核心功能正常

### 回滚后验证
- [ ] 表结构恢复
- [ ] 数据完整性
- [ ] 应用功能正常
- [ ] 无数据丢失

## 故障排查

### 常见问题

#### 1. 连接失败
```
错误: 无法连接到 DolphinDB
解决: 检查 DolphinDB 服务状态和网络连接
```

#### 2. 权限不足
```
错误: Permission denied
解决: 确认数据库用户有足够权限
```

#### 3. 表不存在
```
错误: Table not found
解决: 检查表名和数据库路径是否正确
```

#### 4. 数据类型不匹配
```
错误: Type mismatch
解决: 检查字段类型定义
```

## 联系方式
如遇问题，请联系:
- DevOps Team: devops@example.com
- DBA: dba@example.com
- 紧急联系: 值班电话

## 变更历史
- 2026-03-07: 创建 v2.0 迁移脚本
