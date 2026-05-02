# 配置管理中心

本目录是量化研究系统的统一配置管理中心，旨在解决配置文件分散、重复定义和环境混乱的问题。

## 配置架构

```
config/
├── README.md                     # 配置管理文档
├── environments/                 # 环境特定配置
│   ├── development.env           # 开发环境配置
│   ├── staging.env               # 测试环境配置
│   └── production.env            # 生产环境配置
├── app/                          # 应用程序配置
│   ├── backend.yaml              # 后端应用配置（统一管理）
│   ├── frontend.yaml             # 前端应用配置
│   └── database.yaml             # 数据库配置（DolphinDB + PostgreSQL）
├── docker/                       # Docker配置
│   ├── docker-compose.yml        # 容器编排配置
│   └── dolphindb/                # DolphinDB容器配置
│   └── postgres/                 # PostgreSQL容器配置
├── tasks/                        # 任务调度配置
│   ├── daily-sync.yaml           # 每日同步任务
│   ├── weekly-analysis.yaml      # 每周分析任务
│   └── sync-tasks/               # 数据同步任务详细配置
├── scripts/                      # 脚本配置
│   ├── scripts.config.sh         # Shell脚本配置
│   └── maintenance/              # 运维脚本配置
└── security/                     # 安全配置
    ├── cors.yaml                 # CORS配置
    └── rate-limit.yaml           # 速率限制配置
```

## 配置加载优先级

系统会按照以下优先级加载配置：
1. 命令行参数（最高优先级）
2. 环境变量
3. 当前环境配置文件（environments/{ENVIRONMENT}.env）
4. 应用程序默认配置（app/目录下的配置）
5. 代码内置默认值（最低优先级）

## 使用方法

### 1. 设置环境变量
```bash
# 开发环境
cp config/environments/development.env .env

# 生产环境
cp config/environments/production.env .env
```

### 2. 运行时配置覆盖
```bash
# 使用环境变量临时覆盖
DOLPHINDB_HOST=192.168.1.100 ./start.sh

# 使用配置文件指定运行环境
ENVIRONMENT=staging ./start.sh
```

### 3. 配置验证
```bash
# 检查配置完整性
./config/scripts/validate_config.sh

# 打印当前配置
./config/scripts/print_config.sh
```

## 配置分类说明

### 核心配置文件
- `.env` - 主配置入口，包含敏感信息（在.gitignore中，不提交）
- `app/backend.yaml` - 后端应用程序统一配置（含所有环境的默认值）
- `app/database.yaml` - 数据库连接和存储配置
- `docker/docker-compose.yml` - Docker容器编排配置

### 敏感信息管理
所有敏感信息（API Tokens、密码、密钥等）都应放在：
1. `.env` 文件中
2. 或者通过环境变量传入
3. **绝对禁止** 提交到版本控制系统

## 迁移说明

### 从旧配置到新配置

#### 1. 系统基础配置
原位置 | 新位置 | 说明
-------|--------|------
`.env` | `.env` | 保持不变，但内容会重新组织
`.env.example` | `environments/development.env` | 作为新开发环境的示例配置

#### 2. 后端配置
原位置 | 新位置 | 说明
-------|--------|------
`backend/config/development.yaml` | `app/backend.yaml` | 合并到统一配置中
`backend/config/production.yaml` | `app/backend.yaml` + `environments/production.env` | 通用配置在backend.yaml，环境特定值在env文件
`backend/config/staging.yaml` | `app/backend.yaml` + `environments/staging.env` | 同上

#### 3. 调度配置
原位置 | 新位置 | 说明
-------|--------|------
`config/flows/daily-sync.yaml` | `tasks/daily-sync.yaml` | 保持结构，添加分类前缀
`config/flows/weekly-analysis.yaml` | `tasks/weekly-analysis.yaml` | 保持结构

#### 4. 种子数据和初始化配置
原位置 | 新位置 | 说明
-------|--------|------
`backend/config/seed_data/` | `app/seed_data/` | 合并到app/目录下
`backend/config/initial_config.json` | `app/initial_config.json` | 保持不变，但使用统一路径
`backend/data_manager/` | `tasks/sync-tasks/` | 重分类到任务调度配置中

## 维护指南

### 添加新配置项
1. 在 `app/backend.yaml` 中添加新配置项的默认值
2. 在 `environments/development.env` 中添加开发环境值（如果需要）
3. 在相应的环境配置文件中添加该环境的值
4. 更新文档和注释

### 修改现有配置
1. 找到配置项的主要定义位置（通常在app/backend.yaml）
2. 修改默认值
3. 更新所有环境配置文件中的对应值
4. 运行配置验证脚本

### 删除配置项
1. 从所有环境配置文件中删除该配置项
2. 从app/backend.yaml中删除默认值
3. 检查代码中是否还有引用，如有需要一并删除
4. 更新相关文档

## 注意事项

1. 所有YAML配置文件必须保持格式正确（使用空格缩进，避免制表符）
2. 环境变量名使用大写字母和下划线，与代码中保持一致
3. 敏感信息必须标记为`[SENSITIVE]`并放置在env文件中
4. 配置变更需要经过测试验证
