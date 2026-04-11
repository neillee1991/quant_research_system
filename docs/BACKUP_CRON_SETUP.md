# 数据库备份 Cron 配置指南

## 安装 PostgreSQL 客户端工具

### macOS
```bash
brew install postgresql
```

### Ubuntu/Debian
```bash
sudo apt-get install postgresql-client
```

### CentOS/RHEL
```bash
sudo yum install postgresql
```

## 配置 Cron 定时任务

### 1. 编辑 crontab
```bash
crontab -e
```

### 2. 添加备份任务

**每日备份（凌晨2点）**
```bash
0 2 * * * /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh >> /var/log/quant_backup.log 2>&1
```

**每周备份（周日凌晨3点）**
```bash
0 3 * * 0 /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh weekly >> /var/log/quant_backup.log 2>&1
```

**每月备份（1号凌晨4点）**
```bash
0 4 1 * * /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh monthly >> /var/log/quant_backup.log 2>&1
```

### 3. 验证 Cron 配置
```bash
crontab -l
```

## 手动备份命令

### 创建备份
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend
source .venv/bin/activate
python scripts/maintenance/backup_manager.py create
```

### 列出备份
```bash
python scripts/maintenance/backup_manager.py list
```

### 清理旧备份
```bash
python scripts/maintenance/backup_manager.py cleanup --keep-days 30 --keep-count 10
```

### 恢复备份
```bash
python scripts/maintenance/backup_manager.py restore <backup_file>
```

### 查看备份信息
```bash
python scripts/maintenance/backup_manager.py info
```

## 备份文件位置

所有备份文件存储在：
```
/Users/lisheng/Code/quantsystem/quant_research_system/data/backups/
```

## 备份保留策略

- **保留天数**: 30天
- **保留数量**: 最多10个备份
- **自动清理**: 每次备份后自动清理超过保留策略的旧备份

## 监控备份日志

```bash
# 查看最近的备份日志
tail -f /var/log/quant_backup.log

# 查看备份统计
grep "Backup" /var/log/quant_backup.log | tail -20
```

## 故障排查

### 问题：pg_dump command not found
**解决方案**: 安装 PostgreSQL 客户端工具（见上面的安装步骤）

### 问题：Permission denied
**解决方案**: 确保脚本有执行权限
```bash
chmod +x /Users/lisheng/Code/quantsystem/quant_research_system/backend/scripts/maintenance/backup.sh
```

### 问题：备份失败
**解决方案**: 检查日志文件
```bash
tail -100 /var/log/quant_backup.log
```

## 恢复流程

1. **停止应用**
   ```bash
   ./stop.sh
   ```

2. **恢复备份**
   ```bash
   python scripts/maintenance/backup_manager.py restore <backup_file>
   ```

3. **启动应用**
   ```bash
   ./start.sh
   ```

4. **验证数据**
   ```bash
   # 检查数据库连接
   psql -h localhost -U quant -d quantsystem -c "SELECT COUNT(*) FROM flow_configs;"
   ```

## 备份验证

定期验证备份的完整性：

```bash
# 检查备份文件大小
ls -lh /Users/lisheng/Code/quantsystem/quant_research_system/data/backups/

# 查看备份元数据
cat /Users/lisheng/Code/quantsystem/quant_research_system/data/backups/backup_*_metadata.json
```

## 生产环境建议

1. **备份频率**: 每天至少备份一次
2. **异地备份**: 定期将备份复制到远程存储
3. **备份验证**: 定期测试恢复流程
4. **监控告警**: 配置备份失败告警
5. **文档维护**: 保持恢复文档最新

---

**最后更新**: 2026-04-11  
**维护人**: DevOps Team
