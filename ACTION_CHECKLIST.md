# 🎯 立即行动清单 - 安全加固后续步骤

**生成时间**: 2026-04-11 21:42 UTC  
**优先级**: 🔴 立即处理

---

## ✅ 已完成项目

- [x] SQL注入漏洞修复 (4处)
- [x] API速率限制实现
- [x] 数据库备份系统
- [x] 修改DolphinDB默认密码
- [x] 代码提交到Git

---

## ⏳ 本周必做项目

### 1. 安装PostgreSQL客户端工具 🔴 优先级最高

**macOS用户**:
```bash
brew install postgresql
```

**Ubuntu/Debian用户**:
```bash
sudo apt-get update
sudo apt-get install postgresql-client
```

**CentOS/RHEL用户**:
```bash
sudo yum install postgresql
```

**验证安装**:
```bash
pg_dump --version
psql --version
```

### 2. 配置Cron定时备份 🔴 优先级最高

**编辑crontab**:
```bash
crontab -e
```

**添加以下行**:
```bash
# 每日凌晨2点备份
0 2 * * * /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh >> /var/log/quant_backup.log 2>&1

# 每周日凌晨3点备份
0 3 * * 0 /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh weekly >> /var/log/quant_backup.log 2>&1

# 每月1号凌晨4点备份
0 4 1 * * /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh monthly >> /var/log/quant_backup.log 2>&1
```

**验证配置**:
```bash
crontab -l
```

### 3. 测试备份和恢复流程 🟠 优先级高

**创建测试备份**:
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
source .venv/bin/activate
python scripts/maintenance/backup_manager.py create
```

**验证备份**:
```bash
python scripts/maintenance/backup_manager.py list
python scripts/maintenance/backup_manager.py info
```

**测试恢复流程** (可选):
```bash
# 注意: 这会覆盖现有数据库，仅在测试环境执行
python scripts/maintenance/backup_manager.py restore <backup_file>
```

### 4. 修改PostgreSQL默认密码 🟠 优先级高

**编辑 .env 文件**:
```bash
# 修改以下行
POSTGRES_PASSWORD=your-secure-password-here
```

**应用新密码**:
```bash
# 重启PostgreSQL容器
docker-compose restart postgres
```

### 5. 添加数据库约束 🟠 优先级高

**执行迁移脚本**:
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
python scripts/migrations/run_migrations.py
```

---

## 📋 本月计划项目

- [ ] 拆分 tasks.py 模块 (789行 → 多个模块)
- [ ] 完成数据迁移 (stock_basic/trade_cal)
- [ ] 添加更多API认证保护
- [ ] 编写集成测试
- [ ] 更新项目文档

---

## 🔍 验证清单

### 安全性验证
- [ ] PostgreSQL客户端已安装
- [ ] Cron任务已配置
- [ ] 备份脚本可正常运行
- [ ] 速率限制已启用
- [ ] SQL注入防护已验证

### 功能验证
- [ ] 系统启动正常
- [ ] API端点可访问
- [ ] 备份功能正常
- [ ] 恢复流程可用
- [ ] 所有测试通过

### 文档验证
- [ ] 快速启动指南已阅读
- [ ] 备份配置指南已阅读
- [ ] 安全加固报告已阅读

---

## 📞 故障排查

### 问题: pg_dump command not found
**解决方案**: 安装PostgreSQL客户端工具（见上面的安装步骤）

### 问题: Permission denied
**解决方案**: 确保脚本有执行权限
```bash
chmod +x /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh
```

### 问题: 备份失败
**解决方案**: 检查日志
```bash
tail -100 /var/log/quant_backup.log
```

### 问题: Cron任务未执行
**解决方案**: 检查crontab配置
```bash
crontab -l
# 检查系统日志
log stream --predicate 'process == "cron"' --level debug
```

---

## 📚 相关文档

- [安全加固完成报告](./docs/SECURITY_HARDENING_REPORT.md)
- [备份Cron配置指南](./docs/BACKUP_CRON_SETUP.md)
- [快速启动指南](./QUICK_START_SECURITY.md)
- [修复计划详情](./docs/plans/2026-04-11-comprehensive-fix-plan.md)

---

## 🚀 快速命令参考

```bash
# 进入项目目录
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

# 激活虚拟环境
source .venv/bin/activate

# 查看备份信息
python scripts/maintenance/backup_manager.py info

# 创建备份
python scripts/maintenance/backup_manager.py create

# 列出备份
python scripts/maintenance/backup_manager.py list

# 清理旧备份
python scripts/maintenance/backup_manager.py cleanup --keep-days 30 --keep-count 10

# 恢复备份
python scripts/maintenance/backup_manager.py restore <backup_file>

# 查看备份日志
tail -f /var/log/quant_backup.log
```

---

## ✨ 完成标志

当以下所有项目都完成时，系统安全加固工作完全就绪：

- [x] SQL注入防护 ✅
- [x] API速率限制 ✅
- [x] 数据库备份系统 ✅
- [ ] PostgreSQL客户端工具已安装
- [ ] Cron定时备份已配置
- [ ] 备份和恢复流程已测试
- [ ] PostgreSQL密码已修改
- [ ] 数据库约束已添加

---

**最后更新**: 2026-04-11 21:42 UTC  
**维护人**: Claude Code  
**状态**: 🟢 优先级0-1任务完成，系统已加固
