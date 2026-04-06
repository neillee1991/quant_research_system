# 配置备份与导入功能设计

**日期**: 2026-03-29
**设计目标**: 支持系统配置的备份与快速导入，便于系统重新初始化时恢复配置

## 1. 需求概述

### 1.1 核心功能
- **可选择备份**: 用户可选择要备份的配置类型
- **JSON 格式**: 备份文件使用 JSON 格式
- **浏览器下载**: 备份文件直接通过浏览器下载
- **导入预览**: 导入前预览差异，让用户选择
- **严格验证**: 导入前验证配置有效性
- **独立页面**: 配置管理作为独立页面

### 1.2 使用场景
- 系统重新初始化时快速导入历史配置
- 配置迁移（从测试环境到生产环境）
- 配置版本管理（定期备份重要配置）

## 2. 整体架构

### 2.1 后端 API
| 端点 | 方法 | 功能 |
|------|------|------|
| `/api/v1/config/export` | POST | 导出配置 |
| `/api/v1/config/import/verify` | POST | 验证并预览导入 |
| `/api/v1/config/import/apply` | POST | 执行导入 |

### 2.2 服务层
- `ConfigExportService` - 配置导出服务
- `ConfigImportService` - 配置导入服务
- `ConfigDiffService` - 配置差异计算服务

### 2.3 数据流向
```
导出：用户选择配置类型 → 从各表读取 → 组装 JSON → 浏览器下载
导入：用户选择文件 → 验证 JSON → 计算差异 → 用户确认 → 写入数据库
```

## 3. 数据格式

### 3.1 备份 JSON 结构
```json
{
  "version": "1.0",
  "exported_at": "2026-03-29T10:00:00Z",
  "system_version": "1.0.0",
  "configs": {
    "sync_tasks": [...],
    "etl_tasks": [...],
    "factor_metadata": [...],
    "factor_data_config": [...]
  }
}
```

### 3.2 配置类型枚举
```python
class ConfigType(str, Enum):
    SYNC_TASKS = "sync_tasks"
    ETL_TASKS = "etl_tasks"
    FACTOR_METADATA = "factor_metadata"
    FACTOR_DATA_CONFIG = "factor_data_config"
```

## 4. API 设计

### 4.1 导出 API
```python
# 请求
POST /api/v1/config/export
{
  "config_types": ["sync_tasks", "etl_tasks", "factor_metadata"]
}

# 响应
{
  "filename": "config_backup_20260329_100000.json",
  "content": "base64_encoded_json_string"
}
```

### 4.2 导入验证 API
```python
class ImportMode(str, Enum):
    FAST = "fast"      # 快速导入，直接覆盖
    SAFE = "safe"      # 安全导入，预览差异

# 请求
POST /api/v1/config/import/verify
{
  "content": "base64_encoded_json_string",
  "mode": "safe"
}

# 响应
{
  "valid": true,
  "errors": [],
  "diffs": [
    {
      "config_type": "sync_tasks",
      "summary": {"new": 2, "modified": 1, "unchanged": 5, "deleted": 0},
      "items": [
        {
          "item_id": "stock_basic",
          "status": "unchanged",
          "current": {...},
          "imported": {...}
        }
      ]
    }
  ]
}
```

### 4.3 导入执行 API
```python
# 请求
POST /api/v1/config/import/apply
{
  "content": "base64_encoded_json_string",
  "mode": "safe",
  "selections": {
    "sync_tasks": ["stock_basic", "daily_quote"],
    "etl_tasks": ["etl_index_member"]
  }
}

# 响应
{
  "success": true,
  "summary": {
    "sync_tasks": {"created": 2, "updated": 1, "skipped": 5},
    "etl_tasks": {"created": 1, "updated": 0, "skipped": 0}
  }
}
```

## 5. 前端设计

### 5.1 页面结构
- 配置管理页面（独立路由）
  - 导出配置区域（多选框选择配置类型）
  - 导入配置区域（文件上传 + 模式选择）
  - 导入预览区域（差异展示 + 确认按钮）

### 5.2 交互流程
**导出流程：**
1. 用户勾选要导出的配置类型
2. 点击"导出"按钮
3. 浏览器下载 JSON 文件

**导入流程：**
1. 用户选择导入模式（快速/安全）
2. 上传 JSON 文件
3. 系统验证并显示差异（安全模式）
4. 用户选择要导入的配置项
5. 点击"确认导入"
6. 显示导入结果

## 6. 验证规则

### 6.1 严格验证项
- JSON 格式正确性
- 必需字段完整性
- 外键依赖存在性（如表名、依赖的因子等）
- API 名称有效性（同步任务）
- ETL 脚本语法检查
- 因子代码语法检查

### 6.2 验证失败处理
- 返回所有错误列表
- 阻止导入执行
- 提示用户修复后重试

## 7. 导入模式

### 7.1 快速模式（FAST）
- 跳过差异预览
- 直接覆盖已存在的配置
- 创建不存在的配置
- 适用于系统初始化场景

### 7.2 安全模式（SAFE）
- 显示详细差异
- 让用户选择要导入的配置项
- 适用于非空环境的配置合并
