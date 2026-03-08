# Schema Validator

验证任务配置中的 schema 定义，确保数据结构的正确性和演化兼容性。

## 功能特性

1. **格式验证**: 验证 schema 是否为 dict，每个字段是否包含 type/nullable/comment
2. **类型验证**: 验证字段类型是否为有效的 DolphinDB 类型
3. **主键验证**: 验证主键字段是否都在 schema 中
4. **演化验证**: 验证 schema 演化规则（只允许新增字段，不允许删除或修改类型）

## 支持的 DolphinDB 类型

```
BOOL, CHAR, SHORT, INT, LONG, FLOAT, DOUBLE,
STRING, SYMBOL, DATE, TIMESTAMP, TIME
```

## 使用示例

### 1. 验证 Schema 格式

```python
from app.validators import SchemaValidator

schema = {
    "ts_code": {
        "type": "SYMBOL",
        "nullable": False,
        "comment": "股票代码"
    },
    "trade_date": {
        "type": "DATE",
        "nullable": False,
        "comment": "交易日期"
    },
    "close": {
        "type": "DOUBLE",
        "nullable": True,
        "comment": "收盘价"
    }
}

is_valid, errors = SchemaValidator.validate_schema(schema)
if not is_valid:
    print("Schema validation failed:")
    for error in errors:
        print(f"  - {error}")
```

### 2. 验证主键

```python
is_valid, errors = SchemaValidator.validate_schema(
    schema,
    primary_keys=["ts_code", "trade_date"]
)
```

### 3. 比较 Schema 变更

```python
old_schema = {
    "ts_code": {"type": "SYMBOL", "nullable": False, "comment": "股票代码"},
    "close": {"type": "FLOAT", "nullable": False, "comment": "收盘价"}
}

new_schema = {
    "ts_code": {"type": "SYMBOL", "nullable": False, "comment": "股票代码"},
    "close": {"type": "DOUBLE", "nullable": False, "comment": "收盘价"},
    "volume": {"type": "LONG", "nullable": True, "comment": "成交量"}
}

is_compatible, errors, changes = SchemaValidator.compare_schemas(
    old_schema,
    new_schema
)

print(f"Compatible: {is_compatible}")
print(f"Added fields: {changes['added']}")
print(f"Removed fields: {changes['removed']}")
print(f"Type changed: {changes['type_changed']}")
```

### 4. 验证 Schema 演化

```python
is_valid, errors = SchemaValidator.validate_schema_evolution(
    old_schema,
    new_schema,
    primary_keys=["ts_code", "trade_date"]
)

if not is_valid:
    print("Schema evolution validation failed:")
    for error in errors:
        print(f"  - {error}")
```

## Schema 演化规则

### 允许的操作
- ✅ 新增字段
- ✅ 修改字段的 nullable 属性（记录但不报错）
- ✅ 修改字段的 comment

### 禁止的操作
- ❌ 删除字段
- ❌ 修改字段类型

## 集成到 API

在任务配置更新时使用：

```python
from app.validators import SchemaValidator

@router.put("/sync/tasks/{task_id}")
async def update_sync_task(task_id: str, config: dict):
    # 获取旧配置
    old_config = await get_task_config(task_id)
    old_schema = old_config.get("schema", {})
    new_schema = config.get("schema", {})

    # 验证 schema 演化
    is_valid, errors = SchemaValidator.validate_schema_evolution(
        old_schema,
        new_schema,
        primary_keys=config.get("primary_keys", [])
    )

    if not is_valid:
        raise HTTPException(
            status_code=400,
            detail={"message": "Schema validation failed", "errors": errors}
        )

    # 更新配置
    await update_task_config(task_id, config)
    return {"message": "Task updated successfully"}
```
