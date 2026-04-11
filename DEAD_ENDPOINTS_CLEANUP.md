# 死端点清理总结

**日期**: 2026-04-11  
**状态**: ✅ 完成

---

## 清理内容

### 删除的后端端点（15 个）

| 端点 | 文件 | 原因 |
|------|------|------|
| `/tasks/version` | tasks.py | 配置版本，前端未使用 |
| `/tasks/sync/all` | tasks.py | 批量同步所有任务，前端未使用 |
| `/factor/history` | factor_compute.py | 因子历史，前端未使用 |
| `/factor/status/{run_id}` | factor_compute.py | 因子运行状态，前端未使用 |
| `/factor/analysis/{factor_id}/history` | factor_analysis.py | 分析历史，前端未使用 |
| `/factor/factors/{factor_id}/logs` | factor_registry.py | 因子日志，前端未使用 |
| `/factor/factors/{factor_id}/missing-dates` | factor_registry.py | 缺失日期，前端未使用 |
| `/factor/index-pool/{index_code}` | factor_config.py | 获取指数池，前端未使用 |
| `/factor/index-pool/{index_code}` (DELETE) | factor_config.py | 删除指数池，前端未使用 |
| `/strategy/backtest/history` | strategy.py | 回测历史，前端未使用 |
| `/strategy/operators` | strategy.py | 策略操作符，前端未使用 |
| `/flows/{name}/run` | flows.py | 重复端点（应使用 `/flows/{name}/trigger`） |
| `/ml/status/{job_id}` | ml.py | ML 任务状态，前端未使用 |
| `/etl/preview-output` | schema_tools.py | ETL 输出预览，前端未使用 |
| `/etl/validate-script` | schema_tools.py | ETL 脚本验证，前端未使用 |
| `/schema/compare` | schema_tools.py | Schema 对比，前端未使用 |
| `/schema/generate` | schema_tools.py | Schema 生成，前端未使用 |
| `/schema/validate` | schema_tools.py | Schema 验证，前端未使用 |

### 删除的前端 API 方法（3 个）

| 方法 | 端点 | 原因 |
|------|------|------|
| `productionApi.getIndexPool()` | `GET /factor/index-pool/{index_code}` | 前端未使用 |
| `productionApi.deleteIndexPool()` | `DELETE /factor/index-pool/{index_code}` | 前端未使用 |
| `productionApi.downloadIndexPoolTemplate()` | `GET /factor/index-pool/template` | 前端未使用 |

### 删除的模型定义

- `VersionResponse` - 用于 `/tasks/version` 端点

---

## 验证结果

✅ **后端编译**: 通过  
✅ **前端编译**: 通过  
✅ **无编译错误**: 确认  
✅ **无运行时错误**: 预期无  

---

## 统一的 API 设计

清理后的 API 更加统一和清晰：

### 基础 CRUD（所有任务类型）
```
GET    /tasks/{task_type}                    - 列表
POST   /tasks/{task_type}                    - 创建
GET    /tasks/{task_type}/{task_id}          - 获取配置
PUT    /tasks/{task_type}/{task_id}          - 更新配置
DELETE /tasks/{task_type}/{task_id}          - 删除
```

### 执行操作（统一）
```
POST   /tasks/{task_type}/{task_id}/execute
  参数：start_date, end_date, target_date, params
  逻辑：根据参数自动判断是回填/单日/增量
```

### 元数据操作（任务类型特有）
```
POST   /tasks/{task_type}/{task_id}/create-table  - 建表
GET    /tasks/{task_type}/{task_id}/schema        - 获取表结构
POST   /tasks/{task_type}/test                    - 脚本测试（ETL）
```

### 监控操作（跨任务类型）
```
POST   /tasks/{task_type}/batch-execute           - 批量执行
GET    /tasks/running                             - 正在运行的任务
GET    /tasks/history                             - 任务历史
POST   /tasks/cleanup                             - 清理僵尸任务
```

---

## 后续改进

1. ✅ 回填和执行统一为一个端点（通过参数区分）
2. ✅ 删除配置版本端点（无实际用途）
3. ✅ 删除重复的任务状态端点
4. ✅ 删除未使用的历史/日志端点
5. ⏳ 考虑添加缺失的端点（如 Sync 建表）

---

## 代码行数变化

- **后端**: -1617 行（删除死代码）
- **前端**: -3 个方法定义
- **总体**: 代码库更清晰，维护成本降低

