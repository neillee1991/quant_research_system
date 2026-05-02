# 配置管理迁移指南

本指南用于帮助从旧的分散配置迁移到新的集中配置管理系统。

## 迁移步骤

### 第一步：备份现有配置

在开始迁移之前，先备份所有现有配置：

```bash
# 使用新创建的配置管理工具进行备份
./config/scripts/manage_config.sh backup

# 或者手动备份
cp .env .env.backup
cp -r config config.backup
cp -r backend/config backend/config.backup
```

### 第二步：迁移环境变量

旧位置的环境变量已经大部分迁移到了：
- `config/environments/development.env` - 开发环境
- `config/environments/production.env` - 生产环境

要使用新配置：

```bash
# 复制开发环境配置作为当前配置
cp config/environments/development.env .env

# 或者使用配置管理工具
./config/scripts/manage_config.sh use development
```

### 第三步：更新脚本引用

**需要更新的文件：**

1. `setup.sh` - 修改配置加载方式
2. `start.sh` - 更新配置路径
3. `stop.sh` - 更新配置路径
4. `check_status.sh` - 更新配置路径
5. `backend/scripts/maintenance/setup_cron.sh` - 更新配置路径

**通用迁移方法：**

将脚本开头的：
```bash
source "config/scripts.config.sh"
```
替换为：
```bash
source "config/scripts/scripts.config.sh"
```

### 第四步：验证配置

运行配置验证：

```bash
./config/scripts/manage_config.sh validate
```

### 第五步：清理旧配置

**警告：请确保备份已完成后再执行此步骤！**

```bash
# 删除旧的配置目录（如果确定不需要）
mv config/config.backup /tmp/old_config_backup  # 先移动而不是删除
rm -f config/scripts.config.sh
rmdir config/flows 2>/dev/null || true
```

## 配置文件对照

### 环境变量配置

| 旧位置 | 新位置 | 状态 |
|--------|--------|------|
| `.env` | `.env` | 保持不变 |
| `.env.example` | `config/environments/development.env` | 作为开发环境示例 |

### 后端应用配置

| 旧位置 | 新位置 | 状态 |
|--------|--------|------|
| `backend/config/development.yaml` | `config/app/backend.yaml` | 合并到统一配置 |
| `backend/config/production.yaml` | `config/app/backend.yaml` | 合并到统一配置 |
| `backend/config/staging.yaml` | `config/environments/staging.env` | 作为环境配置 |
| `backend/config/seed_data/` | `config/app/seed_data/` | 已移动 |
| `backend/config/initial_config.json` | `config/app/initial_config.json` | 已移动 |

### 任务调度配置

| 旧位置 | 新位置 | 状态 |
|--------|--------|------|
| `config/flows/daily-sync.yaml` | `config/tasks/daily-sync.yaml` | 已移动 |
| `config/flows/weekly-analysis.yaml` | `config/tasks/weekly-analysis.yaml` | 已移动 |

### 脚本配置

| 旧位置 | 新位置 | 状态 |
|--------|--------|------|
| `config/scripts.config.sh` | `config/scripts/scripts.config.sh` | 已重写 |

## 新配置管理工具使用

### 查看可用环境

```bash
./config/scripts/manage_config.sh list
```

### 切换环境

```bash
# 切换到开发环境
./config/scripts/manage_config.sh use development

# 切换到生产环境
./config/scripts/manage_config.sh use production
```

### 查看当前配置

```bash
# 查看当前环境配置（会隐藏密码等敏感信息）
./config/scripts/manage_config.sh show

# 查看指定环境配置
./config/scripts/manage_config.sh show production
```

### 验证配置

```bash
./config/scripts/manage_config.sh validate
```

### 备份和恢复

```bash
# 备份配置
./config/scripts/manage_config.sh backup

# 从备份恢复
./config/scripts/manage_config.sh restore /path/to/backup.tar.gz
```

### 导出配置为环境变量

```bash
# 导出为shell脚本格式
./config/scripts/manage_config.sh export

# 直接在当前shell中加载
source <(./config/scripts/manage_config.sh export)
```

## 常见问题

### Q: 我的旧配置文件会被删除吗？

A: 不会。新系统只是创建了新的配置结构，旧文件保持原样。只有在您执行清理步骤时才会删除旧文件。

### Q: 如何继续使用旧的配置方式？

A: 旧的配置文件仍然可以使用，建议逐步迁移到新的配置系统。

### Q: 如果迁移出错了怎么办？

A: 在开始迁移前，请先执行备份操作：
```bash
./config/scripts/manage_config.sh backup
```
如果出现问题，可以使用恢复功能：
```bash
./config/scripts/manage_config.sh restore /path/to/backup.tar.gz
```

### Q: 新配置系统与Docker Compose如何配合？

A: 新配置系统与Docker Compose完全兼容。`docker-compose.yml`文件中已经配置了从环境变量读取配置的机制，这与新系统完美配合。

### Q: 需要修改代码以支持新配置系统吗？

A: 不需要。新配置系统保持了与代码读取配置方式的兼容性。代码仍然从`.env`文件和环境变量中读取配置，就像以前一样。

### Q: 如何添加新的环境？

A: 在`config/environments/`目录下创建新的`.env`文件：
```bash
cp config/environments/development.env config/environments/custom.env
# 编辑 custom.env 文件
```
然后就可以使用了：
```bash
./config/scripts/manage_config.sh use custom
```

## 快速开始（迁移完成后）

```bash
# 1. 验证配置
./config/scripts/manage_config.sh validate

# 2. 选择开发环境
./config/scripts/manage_config.sh use development

# 3. 启动服务
./start.sh
```

## 需要更新的脚本列表

这些是需要更新以使用新配置路径的脚本：

- [ ] `setup.sh`
- [ ] `start.sh`
- [ ] `stop.sh`
- [ ] `check_status.sh`
- [ ] `backend/scripts/maintenance/setup_cron.sh`

更新方式：将 `source "config/scripts.config.sh"` 改为 `source "config/scripts/scripts.config.sh"`
