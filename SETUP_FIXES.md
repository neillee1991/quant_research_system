# Setup 脚本修复报告

> 日期: 2026-03-14
> 状态: ✅ 已完成并验证

## 修复的问题

### 1. init_meta_tables.py 导入错误

**问题**: ModuleNotFoundError: No module named 'store'

**原因**:
- 脚本位于 `backend/database/init_meta_tables.py`
- 第 7 行添加的 Python 路径错误：`sys.path.insert(0, str(Path(__file__).resolve().parent))`
- 这会添加 `backend/database/` 目录，但需要添加 `backend/` 目录才能正确导入 `store` 模块

**修复**:
```python
# 修复前
sys.path.insert(0, str(Path(__file__).resolve().parent))

# 修复后
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
```

**文件**: `backend/database/init_meta_tables.py:8`

---

### 2. DolphinDB 数据目录权限问题

**问题**: Failed to create transaction directory [/data/ddb/server/data/local8848/storage/LOG/4] : No such file or directory

**原因**:
- Docker 容器内的 DolphinDB 需要创建事务日志目录
- 宿主机的数据目录没有预先创建必要的子目录结构
- 容器内进程可能没有足够权限创建目录

**修复 (setup.sh)**:
```bash
# 修复前
if [ ! -d "$DOLPHINDB_DATA_DIR" ]; then
    mkdir -p "$DOLPHINDB_DATA_DIR"
fi
docker-compose up -d

# 修复后
if [ ! -d "$DOLPHINDB_DATA_DIR" ]; then
    mkdir -p "$DOLPHINDB_DATA_DIR"
fi

# 创建必要的子目录结构
mkdir -p "$DOLPHINDB_DATA_DIR/local8848/storage"
mkdir -p "$DOLPHINDB_DATA_DIR/local8848/storage/LOG"

# 设置目录权限（确保容器可写）
chmod -R 777 "$DOLPHINDB_DATA_DIR"

export DOLPHINDB_DATA_DIR
docker-compose up -d
```

**文件**: `setup.sh:289-302`, `start.sh:86-99`

---

## 验证结果

### 1. 数据库初始化成功

```bash
$ python database/init_dolphindb.py
已创建数据库 dfs://quant
========================================
DolphinDB 数据库初始化完成!
  dfs://quant : 数据库已就绪
========================================
```

### 2. 元数据表初始化成功

```bash
$ python database/init_meta_tables.py
开始初始化元数据表...
✓ 元数据表创建成功
✓ 同步任务配置种子数据已写入 (12 条)
✓ ETL 任务配置种子数据已写入 (3 条)
✓ 因子数据配置种子数据已写入 (8 条)
✓ 种子因子定义已写入 (8 条)

所有元数据表初始化完成！
```

### 3. 服务启动成功

```bash
$ ./start.sh
========================================
   所有服务启动成功！
========================================

访问地址:
  前端界面:    http://localhost:3000
  API 文档:    http://localhost:8000/docs
  Prefect UI:  http://localhost:4200
  DolphinDB:   http://localhost:8848 (Web管理)
```

### 4. API 测试成功

```bash
$ curl http://localhost:8000/api/v1/tasks/sync
{
  "tasks": [
    {
      "task_id": "sync_adj_factor",
      "api_name": "adj_factor",
      "enabled": true,
      ...
    },
    ...
  ]
}
```

---

## 修改的文件

| 文件 | 修改内容 | 行数 |
|------|---------|------|
| `backend/database/init_meta_tables.py` | 修复 Python 路径导入 | 8 |
| `setup.sh` | 添加目录结构创建和权限设置 | 289-302 |
| `start.sh` | 添加目录结构创建和权限设置 | 86-99 |

---

## 技术细节

### Python 路径解析

```python
# 文件位置: /Users/lisheng/Code/quantsystem/quant_research_system/backend/database/init_meta_tables.py

Path(__file__).resolve()                    # 完整文件路径
Path(__file__).resolve().parent             # backend/database/
Path(__file__).resolve().parent.parent      # backend/
```

需要添加 `backend/` 到 Python 路径，这样才能导入：
- `from store.dolphindb_client import db_client`
- `from app.core.logger import logger`

### DolphinDB 目录结构

```
/Users/lisheng/Code/application/dolphin/    # 宿主机挂载点
└── local8848/                               # DolphinDB 实例目录
    └── storage/                             # 存储目录
        └── LOG/                             # 事务日志目录
            ├── 0/                           # 事务日志分片
            ├── 1/
            ├── 2/
            └── ...
```

Docker 容器内映射为：`/data/ddb/server/data/`

### 权限设置

使用 `chmod -R 777` 确保：
- 容器内的 DolphinDB 进程（可能以非 root 用户运行）可以创建子目录
- 可以写入数据文件和日志文件
- 避免权限相关的启动失败

---

## 后续建议

### 1. 生产环境权限优化

当前使用 `777` 权限是为了确保开发环境正常工作。生产环境建议：

```bash
# 创建专用用户组
sudo groupadd dolphindb
sudo usermod -aG dolphindb $USER

# 设置更严格的权限
chmod -R 770 "$DOLPHINDB_DATA_DIR"
chown -R $USER:dolphindb "$DOLPHINDB_DATA_DIR"
```

### 2. 添加健康检查

在 `setup.sh` 和 `start.sh` 中添加服务健康检查：

```bash
# 检查 DolphinDB 是否真正可用
check_dolphindb_health() {
    if curl -sf http://localhost:8848/ > /dev/null 2>&1; then
        echo "✓ DolphinDB 健康检查通过"
        return 0
    else
        echo "✗ DolphinDB 健康检查失败"
        return 1
    fi
}
```

### 3. 添加回滚机制

如果初始化失败，自动清理并提供重试选项：

```bash
cleanup_on_failure() {
    echo "初始化失败，清理环境..."
    docker-compose down
    rm -rf "$DOLPHINDB_DATA_DIR/local8848"
    echo "环境已清理，可以重新运行 setup.sh"
}
```

---

## 总结

✅ **所有问题已修复**
- Python 导入路径错误已修复
- DolphinDB 目录权限问题已解决
- 数据库和元数据表初始化成功
- 所有服务正常启动
- API 端点测试通过

✅ **系统状态**
- DolphinDB: 运行中，数据库已创建
- 后端 API: 运行中，端口 8000
- 前端: 运行中，端口 3000
- Prefect: 运行中，端口 4200

✅ **下一步**
- 系统已就绪，可以开始使用
- 可以运行数据同步任务
- 可以执行因子计算
- 可以进行回测分析

---

**最后更新**: 2026-03-14 15:20
**验证状态**: ✅ 完全通过
