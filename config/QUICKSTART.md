# 配置管理快速入门

## 5分钟快速开始

### 第一步：了解当前状态

```bash
# 查看项目中的所有配置文件
ls -la config/

# 使用新的配置管理工具
./config/scripts/manage_config.sh help
```

### 第二步：选择并设置环境

```bash
# 查看可用环境
./config/scripts/manage_config.sh list

# 选择开发环境
./config/scripts/manage_config.sh use development

# 验证配置
./config/scripts/manage_config.sh validate
```

### 第三步：更新脚本引用（如需要）

检查并更新以下脚本中的配置路径引用：

- `setup.sh`
- `start.sh`
- `stop.sh`
- `check_status.sh`

将 `source "config/scripts.config.sh"` 修改为：
```bash
source "config/scripts/scripts.config.sh"
```

### 第四步：开始使用

现在您可以继续使用项目，配置会自动从统一管理的配置系统加载。

## 新配置结构总览

```
config/
├── README.md                     # 完整文档
├── QUICKSTART.md                 # 本文档 - 快速入门
├── MIGRATION_GUIDE.md            # 迁移指南
├── environments/                 # 环境配置
│   ├── development.env           # 开发环境
│   └── production.env            # 生产环境
├── app/                          # 应用配置
│   ├── backend.yaml              # 后端统一配置
│   ├── database.yaml             # 数据库配置
│   ├── security.yaml             # 安全配置
│   └── seed_data/                # 种子数据配置
├── tasks/                        # 任务配置
│   ├── daily-sync.yaml           # 每日同步任务
│   └── weekly-analysis.yaml      # 每周分析任务
└── scripts/                      # 脚本工具
    ├── scripts.config.sh         # 脚本统一配置
    ├── manage_config.sh          # 配置管理工具
    └── maintenance/              # 运维脚本配置
```

## 核心命令

| 命令 | 功能 |
|------|------|
| `./config/scripts/manage_config.sh list` | 列出可用环境 |
| `./config/scripts/manage_config.sh use development` | 切换到开发环境 |
| `./config/scripts/manage_config.sh show` | 显示当前配置 |
| `./config/scripts/manage_config.sh validate` | 验证配置 |
| `./config/scripts/manage_config.sh backup` | 备份配置 |

## 关键原则

1. **配置分层** - 环境特定配置覆盖通用默认配置
2. **安全第一** - 敏感信息始终放在 `.env` 文件中，不提交到版本控制
3. **统一入口** - 所有配置通过 `.env` 和 `config/scripts/scripts.config.sh` 加载
4. **可扩展性** - 添加新环境只需在 `config/environments/` 下创建新文件

## 下一步

- 阅读完整文档：`config/README.md`
- 如需要迁移：参考 `config/MIGRATION_GUIDE.md`
- 深入了解每个配置文件的用途
