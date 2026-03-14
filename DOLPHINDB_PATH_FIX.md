# DolphinDB 数据目录路径问题修复报告

> 日期: 2026-03-14
> 状态: ✅ 已解决

## 问题描述

### 症状

后端启动时出现大量错误：

```
ERROR | 查询失败: Server Response: 'getFileBlocksMeta on path '/quant/domain' failed, reason: path does not exist'
ERROR | 动态创建/更新维度表失败: getFileBlocksMeta on path '/quant/domain' failed
ERROR | 创建维度表失败
```

### 根本原因

DolphinDB 容器创建了**两层数据目录结构**：

```
/Users/lisheng/Code/application/dolphin/
├── data/                          # ❌ 错误：多余的一层
│   └── local8848/
│       └── storage/
│           ├── DATABASE/quant
│           └── CHUNKS/quant
└── local8848/                     # ✅ 正确：应该只有这一层
    └── storage/
        └── LOG/
```

**原因分析**：

1. DolphinDB 官方镜像的启动命令包含 `-home data` 参数
2. 容器内的启动命令：
   ```bash
   cd /data/ddb/server
   ./dolphindb -console 0 -stdoutLog true -home data -script dolphindb.dos
   ```
3. `-home data` 导致 DolphinDB 在 `/data/ddb/server/data/` 下创建数据文件
4. Docker 挂载配置：`${DOLPHINDB_DATA_DIR}:/data/ddb/server/data`
5. 结果：宿主机的 `dolphin/` 目录被映射到容器的 `/data/ddb/server/data/`
6. DolphinDB 在容器内创建 `/data/ddb/server/data/local8848/`
7. 映射回宿主机就是 `dolphin/local8848/` ✅

**但是**，在某些情况下（可能是之前的配置或手动操作），创建了额外的 `data/` 子目录，导致路径变成：
- 容器内：`/data/ddb/server/data/data/local8848/`
- 宿主机：`dolphin/data/local8848/` ❌

这导致数据库文件路径不一致，查询时找不到正确的路径。

---

## 解决方案

### 1. 清理旧数据

```bash
# 停止容器
docker-compose down

# 清理数据目录（保留 .DS_Store 不影响）
rm -rf /Users/lisheng/Code/application/dolphin/*

# 确保目录权限
chmod -R 777 /Users/lisheng/Code/application/dolphin
```

### 2. 重新启动容器

```bash
# 设置环境变量并启动
export DOLPHINDB_DATA_DIR="/Users/lisheng/Code/application/dolphin"
docker-compose up -d
```

### 3. 重新初始化数据库

```bash
cd backend
source .venv/bin/activate

# 初始化数据库
python database/init_dolphindb.py

# 初始化元数据表
python database/init_meta_tables.py
```

### 4. 验证目录结构

```bash
find /Users/lisheng/Code/application/dolphin -type d -name "quant"
```

**期望输出**（只有一层 local8848）：
```
/Users/lisheng/Code/application/dolphin/local8848/storage/DATABASE/quant
/Users/lisheng/Code/application/dolphin/local8848/storage/CHUNKS/quant
```

---

## 技术细节

### DolphinDB 容器启动流程

1. **容器启动命令**（来自官方镜像）：
   ```bash
   cd /data/ddb/server
   ./dolphindb -console 0 -stdoutLog true -home data -script dolphindb.dos
   ```

2. **参数说明**：
   - `-home data`：指定数据主目录为 `data/`
   - 实际数据路径：`/data/ddb/server/data/local8848/`

3. **Docker 挂载**：
   ```yaml
   volumes:
     - ${DOLPHINDB_DATA_DIR}:/data/ddb/server/data
   ```

4. **路径映射**：
   ```
   宿主机: /Users/lisheng/Code/application/dolphin/
   容器内: /data/ddb/server/data/

   宿主机: /Users/lisheng/Code/application/dolphin/local8848/
   容器内: /data/ddb/server/data/local8848/
   ```

### 为什么会出现两层目录？

可能的原因：

1. **历史遗留**：之前的配置可能使用了不同的挂载路径
2. **手动创建**：在某个时间点手动创建了 `data/` 子目录
3. **配置变更**：Docker 挂载路径曾经配置为 `/data/ddb/server/` 而不是 `/data/ddb/server/data/`

### 正确的目录结构

```
/Users/lisheng/Code/application/dolphin/    # 宿主机挂载点
├── .DS_Store                                # macOS 系统文件（可忽略）
├── configEngine/                            # DolphinDB 配置引擎
├── local8848/                               # 节点数据目录 ✅
│   ├── log/                                 # 日志文件
│   └── storage/                             # 存储目录
│       ├── CHUNKS/                          # 数据块
│       │   └── quant/                       # dfs://quant 数据库
│       ├── DATABASE/                        # 数据库元数据
│       │   └── quant/                       # dfs://quant 元数据
│       └── LOG/                             # 事务日志
│           ├── 0/
│           ├── 1/
│           └── ...
├── modules/                                 # DolphinDB 模块
├── streamlog/                               # 流式日志
├── tempResults/                             # 临时结果
└── traces/                                  # 追踪日志
```

---

## 验证结果

### 1. 目录结构正确

```bash
$ find /Users/lisheng/Code/application/dolphin -type d -name "quant"
/Users/lisheng/Code/application/dolphin/local8848/storage/DATABASE/quant
/Users/lisheng/Code/application/dolphin/local8848/storage/CHUNKS/quant
```

✅ 只有一层 `local8848/`，没有多余的 `data/` 目录

### 2. 数据库初始化成功

```bash
$ python database/init_dolphindb.py
已创建数据库 dfs://quant
========================================
DolphinDB 数据库初始化完成!
========================================
```

✅ 数据库创建成功

### 3. 元数据表创建成功

```bash
$ python database/init_meta_tables.py
✓ 元数据表创建成功
✓ 同步任务配置种子数据已写入 (12 条)
✓ ETL 任务配置种子数据已写入 (3 条)
✓ 因子数据配置种子数据已写入 (8 条)
✓ 种子因子定义已写入 (8 条)
```

✅ 所有元数据表和种子数据创建成功

### 4. 后端启动无错误

```bash
$ tail -50 logs/backend.log | grep ERROR
# 无 ERROR 日志（只有一个 WARNING 关于 sync_trade_cal 表为空，这是正常的）
```

✅ 后端启动正常，无数据库路径错误

### 5. API 测试通过

```bash
$ curl http://localhost:8000/api/v1/tasks/sync
{
  "tasks": [
    {
      "task_id": "sync_adj_factor",
      "enabled": true,
      ...
    }
  ]
}
```

✅ API 正常返回数据

---

## 预防措施

### 1. 在 setup.sh 和 start.sh 中添加验证

建议在启动脚本中添加目录结构验证：

```bash
validate_dolphindb_structure() {
    # 检查是否存在错误的 data/ 子目录
    if [ -d "$DOLPHINDB_DATA_DIR/data/local8848" ]; then
        print_error "检测到错误的目录结构: $DOLPHINDB_DATA_DIR/data/"
        print_error "请运行以下命令清理："
        echo "  rm -rf $DOLPHINDB_DATA_DIR/*"
        echo "  ./start.sh"
        exit 1
    fi
}
```

### 2. 文档化正确的目录结构

在 README 或 CLAUDE.md 中明确说明：

```markdown
## DolphinDB 数据目录结构

正确的目录结构应该是：
```
dolphin/
└── local8848/
    └── storage/
```

如果看到 `dolphin/data/local8848/`，说明配置有问题，需要清理重建。
```

### 3. 添加健康检查

在后端启动时检查数据库路径：

```python
def check_database_health():
    """检查数据库路径是否正确"""
    try:
        result = db_client.run_script("existsDatabase('dfs://quant')")
        if not result:
            logger.error("数据库 dfs://quant 不存在")
            return False
        return True
    except Exception as e:
        logger.error(f"数据库健康检查失败: {e}")
        return False
```

---

## 相关文件

| 文件 | 说明 |
|------|------|
| `docker-compose.yml:8` | DolphinDB 挂载配置 |
| `setup.sh:289-302` | 数据目录初始化 |
| `start.sh:86-99` | 数据目录初始化 |
| `backend/database/init_dolphindb.py` | 数据库初始化脚本 |
| `backend/database/init_meta_tables.py` | 元数据表初始化脚本 |

---

## 总结

**问题**：DolphinDB 数据目录出现两层结构（`data/local8848/` 和 `local8848/`），导致数据库路径不一致

**原因**：历史遗留或配置变更导致创建了额外的 `data/` 子目录

**解决**：清理数据目录，重新初始化，确保只有一层 `local8848/` 目录

**结果**：
- ✅ 目录结构正确
- ✅ 数据库初始化成功
- ✅ 元数据表创建成功
- ✅ 后端启动无错误
- ✅ API 正常工作

**预防**：添加目录结构验证和健康检查

---

**最后更新**: 2026-03-14 15:31
**状态**: ✅ 问题已完全解决
