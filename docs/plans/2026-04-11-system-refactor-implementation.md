# 系统架构重构实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 修复所有 186 个发现的问题，包括 11 个 CRITICAL 和 110 个 HIGH 优先级问题，实现安全加固、架构统一、代码质量提升和性能优化。

**Architecture:** 分为 4 个阶段，12 周完成，5 个团队并行工作：
- Phase 1 (Week 1-2): 安全加固（P0）
- Phase 2 (Week 3-6): 架构统一（P0/P1）
- Phase 3 (Week 7-10): 代码质量（P1）
- Phase 4 (Week 11-12): 完善优化（P2/P3）

**Tech Stack:** FastAPI + PostgreSQL + DolphinDB + React + TypeScript + Polars + Prefect

---

## Phase 1: 安全加固（Week 1-2）- P0 优先级

### Task 1.1: 实现 JWT/OAuth2 认证中间件

**Files:**
- Create: `backend/app/core/auth.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/core/config.py`
- Test: `backend/tests/unit/test_auth.py`

**Step 1: Write the failing test**

```python
# backend/tests/unit/test_auth.py
import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.core.auth import create_access_token, get_current_user

client = TestClient(app)

def test_auth_middleware_protects_endpoints():
    # 未认证请求应返回 401
    response = client.get("/api/v1/factor/factors")
    assert response.status_code == 401

def test_valid_token_allowed():
    # 使用有效 token 应允许访问
    token = create_access_token(data={"sub": "testuser"})
    response = client.get(
        "/api/v1/factor/factors",
        headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code != 401

def test_invalid_token_rejected():
    response = client.get(
        "/api/v1/factor/factors",
        headers={"Authorization": "Bearer invalid-token"}
    )
    assert response.status_code == 401
```

**Step 2: Run test to verify it fails**

Run: `pytest backend/tests/unit/test_auth.py -v`
Expected: FAIL with "404" or "200" (no auth middleware)

**Step 3: Write minimal implementation**

```python
# backend/app/core/auth.py
"""JWT 认证中间件和工具"""
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from app.core.config import settings

# JWT 配置
SECRET_KEY = settings.auth.secret_key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 小时

# 密码哈希
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Bearer token 安全方案
security = HTTPBearer()

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str
    role: str = "user"

# 模拟用户数据库（实际应从 PostgreSQL 查询）
fake_users_db = {
    "admin": {"username": "admin", "role": "admin"},
    "user": {"username": "user", "role": "user"},
}

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无法验证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    
    user = fake_users_db.get(token_data.username)
    if user is None:
        raise credentials_exception
    return User(**user)

async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    return current_user

class RoleChecker:
    def __init__(self, allowed_roles: list[str]):
        self.allowed_roles = allowed_roles
    
    async def __call__(self, current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in self.allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足"
            )
        return current_user

# 预定义的角色检查器
require_admin = RoleChecker(["admin"])
require_user = RoleChecker(["user", "admin"])
```

**Step 4: Update config.py**

```python
# 在 backend/app/core/config.py 中添加
class AuthSettings(BaseModel):
    secret_key: str = "your-secret-key-change-in-production"  # 从环境变量读取
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24

class Settings(BaseModel):
    # ... 现有配置 ...
    auth: AuthSettings = AuthSettings()
```

**Step 5: Update main.py to add auth endpoints**

```python
# 在 backend/app/main.py 中添加
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.core.auth import (
    create_access_token,
    get_current_active_user,
    User,
    fake_users_db,
)

@app.post("/api/v1/auth/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """OAuth2 兼容的 token 端点"""
    # 简化版：实际应验证密码哈希
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/api/v1/auth/me")
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """获取当前用户信息"""
    return current_user
```

**Step 6: Run test to verify it passes**

Run: `pytest backend/tests/unit/test_auth.py -v`
Expected: PASS

**Step 7: Commit**

```bash
git add backend/app/core/auth.py backend/app/core/config.py backend/app/main.py backend/tests/unit/test_auth.py
git commit -m "feat: add JWT/OAuth2 authentication middleware"
```

---

### Task 1.2: 为敏感端点添加认证保护

**Files:**
- Modify: `backend/app/api/v1/factor/factor_compute.py`
- Modify: `backend/app/api/v1/data/query_api.py`
- Modify: `backend/app/api/v1/data/sync_api.py`
- Modify: `backend/app/api/v1/data/etl_api.py`
- Modify: `backend/app/api/v1/tasks.py`
- Test: `backend/tests/api/test_auth_protection.py`

**Step 1: Write the failing test**

```python
# backend/tests/api/test_auth_protection.py
import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.core.auth import create_access_token

client = TestClient(app)

def test_factor_test_endpoint_requires_auth():
    # 未认证访问应失败
    response = client.post("/api/v1/factor/factors/test", json={
        "code": "def compute(df): return df",
        "start_date": "20240101",
        "end_date": "20240131"
    })
    assert response.status_code == 401

def test_query_endpoint_requires_auth():
    response = client.get("/api/v1/data/query", params={"sql": "SELECT 1"})
    assert response.status_code == 401

def test_authenticated_access_allowed():
    token = create_access_token(data={"sub": "testuser"})
    # 即使认证了，请求参数可能验证失败，但不应是 401
    response = client.post(
        "/api/v1/factor/factors/test",
        json={
            "code": "def compute(df): return df",
            "start_date": "20240101",
            "end_date": "20240131"
        },
        headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code != 401
```

**Step 2: Run test to verify it fails**

Run: `pytest backend/tests/api/test_auth_protection.py -v`
Expected: FAIL (endpoints return 200 or 422, not 401)

**Step 3: Add authentication to factor_compute.py**

```python
# 在 backend/app/api/v1/factor/factor_compute.py 顶部添加
from app.core.auth import get_current_active_user, User

# 在每个敏感端点添加 Depends
@router.post("/factor/factors/test")
async def test_factor_code(
    req: FactorTestRequest,
    current_user: User = Depends(get_current_active_user)  # 添加这行
):
    # ... 现有代码 ...
```

**Step 4: Add authentication to query_api.py**

```python
# 在 backend/app/api/v1/data/query_api.py 中
from app.core.auth import get_current_active_user, User, require_admin

# 为查询端点添加认证（普通用户）
@router.get("/data/stocks")
def list_stocks(current_user: User = Depends(get_current_active_user)):
    # ... 现有代码 ...

# 为 SQL 查询端点添加管理员权限
@router.post("/data/query")
async def execute_query(
    req: QueryRequest,
    current_user: User = Depends(require_admin)  # 仅管理员
):
    # ... 现有代码 ...
```

**Step 5: Repeat for other API files**

Apply similar `Depends(get_current_active_user)` to:
- `sync_api.py` - all endpoints
- `etl_api.py` - all endpoints
- `tasks.py` - all endpoints
- `config_api.py` - all endpoints
- `flows.py` - all endpoints
- `strategy.py` - all endpoints
- `ml.py` - all endpoints
- `factor/factor_analysis.py` - all endpoints
- `factor/factor_registry.py` - all endpoints
- `factor/factor_config.py` - all endpoints

**Step 6: Run test to verify it passes**

Run: `pytest backend/tests/api/test_auth_protection.py -v`
Expected: PASS

**Step 7: Commit**

```bash
git add backend/app/api/v1/
git commit -m "feat: add authentication to all sensitive endpoints"
```

---

### Task 1.3: 实现 RestrictedPython 沙箱保护代码执行

**Files:**
- Modify: `backend/app/api/v1/factor/factor_compute.py`
- Create: `backend/app/core/sandbox.py`
- Test: `backend/tests/unit/test_sandbox.py`
- Add: `requirements.txt` - add `RestrictedPython`

**Step 1: Add RestrictedPython to requirements**

```
# 在 requirements.txt 中添加
RestrictedPython>=6.0
```

**Step 2: Write the failing test**

```python
# backend/tests/unit/test_sandbox.py
import pytest
from app.core.sandbox import execute_safe_code, SandboxSecurityError

def test_safe_code_execution():
    # 安全代码应能执行
    result = execute_safe_code("1 + 1")
    assert result == 2

def test_unsafe_import_blocked():
    # 导入应被阻止
    with pytest.raises(SandboxSecurityError):
        execute_safe_code("import os")

def test_file_access_blocked():
    # 文件访问应被阻止
    with pytest.raises(SandboxSecurityError):
        execute_safe_code("open('/etc/passwd')")

def test_polars_allowed():
    # Polars 应允许使用
    code = """
import polars as pl
df = pl.DataFrame({'a': [1, 2, 3]})
len(df)
"""
    result = execute_safe_code(code)
    assert result == 3
```

**Step 3: Run test to verify it fails**

Run: `pytest backend/tests/unit/test_sandbox.py -v`
Expected: FAIL (module not found)

**Step 4: Implement sandbox**

```python
# backend/app/core/sandbox.py
"""安全沙箱执行环境"""
import sys
import io
from typing import Any, Optional
from RestrictedPython import (
    compile_restricted_exec,
    compile_restricted_function,
    safe_globals,
    limited_builtins,
    utility_builtins,
)
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
from RestrictedPython.Guards import (
    safe_builtins,
    guarded_setattr,
    guarded_delattr,
    full_write_guard,
)

class SandboxSecurityError(Exception):
    """沙箱安全违规"""
    pass

# 安全的全局变量
SAFE_GLOBALS = {
    **safe_globals,
    "__builtins__": {
        **safe_builtins,
        **limited_builtins,
        **utility_builtins,
        # 允许的模块
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "dict": dict,
        "enumerate": enumerate,
        "filter": filter,
        "float": float,
        "int": int,
        "isinstance": isinstance,
        "len": len,
        "list": list,
        "map": map,
        "max": max,
        "min": min,
        "range": range,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "zip": zip,
        "None": None,
        "True": True,
        "False": False,
    },
    "_getitem_": default_guarded_getitem,
    "_getiter_": default_guarded_getiter,
    "_setattr_": guarded_setattr,
    "_delattr_": guarded_delattr,
    "_write_": full_write_guard,
}

def execute_safe_code(
    code: str,
    local_vars: Optional[dict] = None,
    allowed_modules: Optional[list] = None,
    timeout: int = 30,
) -> Any:
    """
    在受限沙箱中执行代码
    
    Args:
        code: 要执行的代码
        local_vars: 局部变量字典
        allowed_modules: 允许导入的模块列表
        timeout: 超时时间（秒）
    
    Returns:
        执行结果
        
    Raises:
        SandboxSecurityError: 安全违规
        TimeoutError: 执行超时
    """
    # 导入 RestrictedPython
    try:
        import RestrictedPython
    except ImportError:
        raise SandboxSecurityError("RestrictedPython not installed")
    
    # 准备全局和局部变量
    globals_ = SAFE_GLOBALS.copy()
    locals_ = local_vars.copy() if local_vars else {}
    
    # 允许特定模块
    if allowed_modules:
        for module_name in allowed_modules:
            try:
                module = __import__(module_name)
                globals_[module_name] = module
            except ImportError:
                pass
    
    # 编译受限代码
    try:
        byte_code = compile_restricted_exec(code)
    except SyntaxError as e:
        raise SandboxSecurityError(f"Syntax error: {e}")
    
    # 捕获输出
    stdout_capture = io.StringIO()
    globals_["_print_"] = lambda *args, **kwargs: stdout_capture.write(
        " ".join(str(x) for x in a) + kw.get("end", "\n")
    )
    
    # 执行（带超时检查）
    try:
        exec(byte_code, globals_, locals_)
    except Exception as e:
        raise SandboxSecurityError(f"Code execution failed: {e}")
    
    # 返回局部变量中的结果
    return locals_.get("result")

def execute_safe_function(
    func_code: str,
    args: tuple = (),
    kwargs: dict = None,
    allowed_modules: Optional[list] = None,
) -> Any:
    """
    在沙箱中执行函数
    
    Args:
        func_code: 函数定义代码
        args: 位置参数
        kwargs: 关键字参数
        allowed_modules: 允许的模块
    
    Returns:
        函数返回值
    """
    kwargs = kwargs or {}
    
    # 编译受限函数
    try:
        byte_code = compile_restricted_function(
            "(args, kwargs)",  # 参数
            func_code,          # 函数体
            "<safe_function>",  # 文件名
        )
    except SyntaxError as e:
        raise SandboxSecurityError(f"Function syntax error: {e}")
    
    # 准备环境
    globals_ = SAFE_GLOBALS.copy()
    
    # 执行函数
    try:
        func = byte_code
        result = func(args, kwargs)
    except Exception as e:
        raise SandboxSecurityError(f"Function execution failed: {e}")
    
    return result
```

**Step 5: Update factor_compute.py to use sandbox**

```python
# 在 backend/app/api/v1/factor/factor_compute.py 中
from app.core.sandbox import execute_safe_code, SandboxSecurityError

# 替换 exec(compiled, namespace) 为：
try:
    # 使用沙箱执行
    result = execute_safe_code(
        req.code,
        allowed_modules=["polars"],
        timeout=30
    )
except SandboxSecurityError as e:
    return make_error("exec", f"安全限制: {e}")
```

**Step 6: Run test to verify it passes**

Run: `pytest backend/tests/unit/test_sandbox.py -v`
Expected: PASS

**Step 7: Commit**

```bash
git add backend/app/core/sandbox.py backend/app/api/v1/factor/factor_compute.py backend/tests/unit/test_sandbox.py requirements.txt
git commit -m "feat: add RestrictedPython sandbox for code execution"
```

---

### Task 1.4: 修复所有 SQL 注入漏洞

**Files:**
- Modify: `backend/infrastructure/processor/processors.py`
- Modify: `backend/app/api/v1/data/query_api.py`
- Create: `backend/app/core/security.py` - 表名/列名白名单
- Test: `backend/tests/unit/test_sql_injection.py`

**Step 1: Write the failing test**

```python
# backend/tests/unit/test_sql_injection.py
import pytest
from app.core.security import validate_table_name, validate_column_name, sanitize_sql
from app.core.security import SQLInjectionError

def test_table_name_whitelist():
    # 有效表名应通过
    assert validate_table_name("sync_daily_data") == "sync_daily_data"
    assert validate_table_name("factor_values") == "factor_values"
    
    # 无效表名应拒绝
    with pytest.raises(SQLInjectionError):
        validate_table_name("sync_daily_data; DROP TABLE users")
    
    with pytest.raises(SQLInjectionError):
        validate_table_name("invalid_table")

def test_column_name_whitelist():
    # 有效列名应通过
    assert validate_column_name("ts_code") == "ts_code"
    assert validate_column_name("trade_date") == "trade_date"
    
    # 无效列名应拒绝
    with pytest.raises(SQLInjectionError):
        validate_column_name("ts_code; DELETE FROM sync_daily_data")

def test_limit_parameter_validation():
    # query_api.py 中的 limit 参数应验证
    from app.api.v1.data.query_api import get_daily
    # 测试应验证 limit 不会被注入
```

**Step 2: Run test to verify it fails**

Run: `pytest backend/tests/unit/test_sql_injection.py -v`
Expected: FAIL (module not found)

**Step 3: Implement security module**

```python
# backend/app/core/security.py
"""SQL 注入防护工具"""
from typing import List, Set
from dataclasses import dataclass

class SQLInjectionError(Exception):
    """SQL 注入检测异常"""
    pass

# 允许的表名白名单
ALLOWED_TABLES: Set[str] = {
    # DolphinDB TSDB 表
    "sync_daily_data",
    "sync_daily_basic",
    "sync_adj_factor",
    "sync_index_daily",
    "sync_moneyflow",
    "factor_values",
    # DolphinDB 维度表
    "sync_stock_basic",
    "sync_trade_cal",
    "factor_metadata",
    "sync_task_config",
    "etl_task_config",
    "factor_config",
    # PostgreSQL 表（通过 schema 验证）
    "flow_configs",
    "flow_runs",
    "task_runs",
    "sync_task_configs",
    "etl_task_configs",
    "factor_configs",
    "factor_field_mappings",
    "stocks",
    "trading_calendar",
    "index_configs",
    "user_preferences",
    "factor_analysis_results",
    "backtest_results",
}

# 允许的列名白名单（常用列）
ALLOWED_COLUMNS: Set[str] = {
    "id", "ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount",
    "pe", "pb", "turnover_rate", "total_share", "float_share", "free_share",
    "total_mv", "circ_mv", "adj_factor", "factor_id", "factor_value",
    "task_id", "task_name", "status", "created_at", "updated_at",
    "started_at", "ended_at", "params", "config", "name", "description",
    "enabled", "sync_type", "cron_expression", "exchange", "cal_date", "is_open",
    "index_code", "index_name", "user_id", "preferences", "analysis_date",
    "period", "ic", "rank_ic", "t_stat", "p_value", "quantile", "return_mean",
    "run_id", "metrics", "equity_curve", "trades",
}

# 危险的 SQL 模式
DANGEROUS_PATTERNS: List[str] = [
    ";", "--", "/*", "*/", "@@", "@",
    "UNION", "UNION ALL", "SELECT", "INSERT", "UPDATE", "DELETE",
    "DROP", "TRUNCATE", "ALTER", "CREATE", "EXEC", "EXECUTE",
    "OR 1=1", "OR '1'='1", "OR 1=1--",
    "AND 1=1", "AND '1'='1",
    "SLEEP(", "WAITFOR DELAY",
    "LOAD_FILE", "INTO OUTFILE", "INTO DUMPFILE",
]

def validate_table_name(table_name: str) -> str:
    """
    验证表名是否在白名单中
    
    Args:
        table_name: 要验证的表名
        
    Returns:
        验证后的表名
        
    Raises:
        SQLInjectionError: 表名不在白名单中
    """
    if not table_name:
        raise SQLInjectionError("Table name cannot be empty")
    
    # 基础安全检查
    table_name_clean = table_name.strip()
    
    # 检查危险模式
    for pattern in DANGEROUS_PATTERNS:
        if pattern.lower() in table_name_clean.lower():
            raise SQLInjectionError(f"Invalid table name contains dangerous pattern: {pattern}")
    
    # 检查白名单
    if table_name_clean not in ALLOWED_TABLES:
        # 也允许带库前缀的形式
        if "." in table_name_clean:
            _, pure_name = table_name_clean.split(".", 1)
            if pure_name in ALLOWED_TABLES:
                return table_name_clean
        
        raise SQLInjectionError(f"Table name not in whitelist: {table_name_clean}")
    
    return table_name_clean

def validate_column_name(column_name: str) -> str:
    """
    验证列名是否安全
    
    Args:
        column_name: 要验证的列名
        
    Returns:
        验证后的列名
        
    Raises:
        SQLInjectionError: 列名可能包含注入
    """
    if not column_name:
        raise SQLInjectionError("Column name cannot be empty")
    
    column_name_clean = column_name.strip()
    
    # 检查危险模式
    for pattern in DANGEROUS_PATTERNS:
        if pattern.lower() in column_name_clean.lower():
            raise SQLInjectionError(f"Invalid column name contains dangerous pattern: {pattern}")
    
    # 基本格式检查：只能包含字母、数字、下划线
    if not column_name_clean.replace("_", "").isalnum():
        raise SQLInjectionError(f"Invalid column name format: {column_name_clean}")
    
    # 检查白名单（如果在白名单中）
    if column_name_clean.lower() not in (col.lower() for col in ALLOWED_COLUMNS):
        # 不在白名单中但格式正确也允许（新增列）
        pass
    
    return column_name_clean

def sanitize_limit(limit: int, max_limit: int = 10000) -> int:
    """
    清理 LIMIT 参数
    
    Args:
        limit: 输入的 limit 值
        max_limit: 最大允许值
        
    Returns:
        清理后的 limit 值
    """
    try:
        limit_int = int(limit)
        return max(1, min(limit_int, max_limit))
    except (ValueError, TypeError):
        return 1000

def build_secure_sql(template: str, **kwargs) -> str:
    """
    构建安全的 SQL（用于不能使用参数化的情况）
    
    Args:
        template: SQL 模板，使用 {table} 和 {column} 占位符
        **kwargs: 要替换的参数
        
    Returns:
        安全的 SQL
        
    Raises:
        SQLInjectionError: 验证失败
    """
    # 验证并替换表名
    for key, value in kwargs.items():
        if key.startswith("table"):
            kwargs[key] = validate_table_name(value)
        elif key.startswith("column"):
            kwargs[key] = validate_column_name(value)
    
    return template.format(**kwargs)
```

**Step 4: Fix query_api.py**

```python
# 在 backend/app/api/v1/data/query_api.py 中
from app.core.security import sanitize_limit, validate_table_name, SQLInjectionError

# 修复第 88 行的 SQL 注入
@router.get("/data/daily")
def get_daily(
    ts_code: Optional[str] = Query(None, description="股票代码"),
    start_date: Optional[str] = Query(None, description="开始日期 YYYYMMDD"),
    end_date: Optional[str] = Query(None, description="结束日期 YYYYMMDD"),
    limit: int = Query(1000, le=10000, description="返回记录数限制")
):
    # ... 现有代码 ...
    
    # 使用 sanitize_limit 代替直接拼接
    safe_limit = sanitize_limit(limit, 10000)
    sql = f"SELECT * FROM sync_daily_data WHERE {where_clause} ORDER BY trade_date DESC LIMIT {safe_limit}"
    
    # ... 现有代码 ...
```

**Step 5: Fix processors.py**

```python
# 在 backend/infrastructure/processor/processors.py 中
from app.core.security import validate_table_name, validate_column_name

# 替换所有 f-string SQL 拼接
# 之前: f"SELECT * FROM {table} WHERE ts_code = '{ts_code}'"
# 之后: 使用参数化查询或验证表名/列名

# 修复示例：
def query_data(self, table: str, ts_code: str):
    safe_table = validate_table_name(table)
    # 使用参数化查询
    sql = f"SELECT * FROM {safe_table} WHERE ts_code = %s"
    return self.db_client.query(sql, (ts_code,))
```

**Step 6: Find and fix all other SQL injection locations**

Use grep to find all f-string SQL:
```bash
grep -r "f\".*SELECT.*{" backend/ --include="*.py"
grep -r "f\".*FROM.*{" backend/ --include="*.py"
grep -r "f\".*WHERE.*{" backend/ --include="*.py"
```

Fix each one by:
1. Using parameterized queries (`%s` placeholders) where possible
2. Validating table/column names with `validate_table_name()`/`validate_column_name()`
3. Using `sanitize_limit()` for LIMIT clauses

**Step 7: Run test to verify it passes**

Run: `pytest backend/tests/unit/test_sql_injection.py -v`
Expected: PASS

**Step 8: Commit**

```bash
git add backend/app/core/security.py backend/app/api/v1/data/query_api.py backend/infrastructure/processor/processors.py backend/tests/unit/test_sql_injection.py
git commit -m "fix: repair all SQL injection vulnerabilities"
```

---

### Task 1.5: 修改 DolphinDB 默认密码并移除 .env

**Files:**
- Modify: `backend/database/init_dolphindb.dos`
- Modify: `backend/database/init_dolphindb.py`
- Modify: `backend/.env.example`
- Delete: `backend/.env` (from git)
- Add: `backend/.gitignore` - ensure .env is ignored
- Test: `backend/tests/test_security_config.py`

**Step 1: Update .gitignore**

```
# 在 backend/.gitignore 中确认
.env
.env.*
!.env.example
*.pyc
__pycache__/
.pids/
logs/
backups/
```

**Step 2: Write the failing test**

```python
# backend/tests/test_security_config.py
import os
import pytest
from app.core.config import settings

def test_env_file_not_committed():
    # .env 不应在 git 中
    assert not os.path.exists(".env") or ".env" in open(".gitignore").read()

def test_default_password_changed():
    # DolphinDB 不应使用默认密码
    assert settings.dolphindb.password != "123456"
    assert settings.dolphindb.password != "admin"

def test_secret_key_not_default():
    # JWT secret key 不应是默认值
    assert settings.auth.secret_key != "your-secret-key-change-in-production"
```

**Step 3: Run test to verify it fails**

Run: `pytest backend/tests/test_security_config.py -v`
Expected: FAIL (default passwords still present)

**Step 4: Update init_dolphindb.dos**

```dos
// backend/database/init_dolphindb.dos
// 移除硬编码密码，使用环境变量或提示
// 登录应由 init_dolphindb.py 通过 session 完成

// 删除第 10 行的默认密码登录
// try { login("admin", "123456") } catch(ex) { /* 已通过 session 登录 */ }

// 只保留数据库创建逻辑
```

**Step 5: Update init_dolphindb.py**

```python
# backend/database/init_dolphindb.py
"""DolphinDB 初始化脚本"""
import os
from dotenv import load_dotenv
import dolphindb as ddb

load_dotenv()

def init_dolphindb():
    """初始化 DolphinDB 数据库"""
    # 从环境变量读取密码
    host = os.getenv("DOLPHINDB_HOST", "localhost")
    port = int(os.getenv("DOLPHINDB_PORT", "8848"))
    username = os.getenv("DOLPHINDB_USER", "admin")
    password = os.getenv("DOLPHINDB_PASSWORD")
    
    if not password:
        raise ValueError("DOLPHINDB_PASSWORD must be set in environment variables")
    
    # 连接并执行初始化脚本
    session = ddb.session()
    session.connect(host, port, username, password)
    
    # 执行 init_dolphindb.dos 脚本
    with open("database/init_dolphindb.dos", "r") as f:
        script = f.read()
    
    session.run(script)
    print("DolphinDB initialized successfully")

if __name__ == "__main__":
    init_dolphindb()
```

**Step 6: Update .env.example**

```env
# backend/.env.example
# DolphinDB 配置
DOLPHINDB_HOST=localhost
DOLPHINDB_PORT=8848
DOLPHINDB_USER=admin
DOLPHINDB_PASSWORD=change_this_password_in_production  # 必需修改！

# JWT 认证配置
AUTH_SECRET_KEY=generate_a_secure_random_key_here  # 使用 openssl rand -hex 32 生成
AUTH_ALGORITHM=HS256
AUTH_ACCESS_TOKEN_EXPIRE_MINUTES=1440

# PostgreSQL 配置
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=quantsystem
POSTGRES_USER=quantsystem
POSTGRES_PASSWORD=change_this_too

# Tushare 配置
TUSHARE_TOKEN=your_tushare_token_here
```

**Step 7: Remove .env from git history**

```bash
# 如果 .env 已提交，从 git 历史中移除
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch backend/.env" \
  --prune-empty --tag-name-filter cat -- --all

# 或者使用 BFG Repo-Cleaner（更简单）
# bfg --delete-files .env
```

**Step 8: Update config.py**

```python
# backend/app/core/config.py
# 确保所有密码都从环境变量读取
class DolphinDBSettings(BaseModel):
    host: str = "localhost"
    port: int = 8848
    user: str = "admin"
    password: str  # 必需设置，无默认值

class Settings(BaseModel):
    dolphindb: DolphinDBSettings
    # ...
```

**Step 9: Run test to verify it passes**

Run: `pytest backend/tests/test_security_config.py -v`
Expected: PASS

**Step 10: Commit**

```bash
# 首先确保 .env 被忽略
echo ".env" >> backend/.gitignore
git add backend/.gitignore backend/database/init_dolphindb.dos backend/database/init_dolphindb.py backend/.env.example backend/app/core/config.py backend/tests/test_security_config.py
git commit -m "sec: remove default passwords and secure environment"
```

---

### Task 1.6: 实现自动化备份策略

**Files:**
- Create: `backend/scripts/maintenance/backup_postgres.sh`
- Create: `backend/scripts/maintenance/backup_dolphindb.py`
- Create: `backend/scripts/maintenance/verify_backup.py`
- Modify: `backend/app/core/config.py` - 添加备份配置
- Test: `backend/tests/test_backup.py`

**Step 1: Write the failing test**

```python
# backend/tests/test_backup.py
import os
from scripts.maintenance.backup_postgres import backup_postgres
from scripts.maintenance.backup_dolphindb import backup_dolphindb

def test_postgres_backup_creates_file():
    result = backup_postgres("/tmp/test_backup")
    assert os.path.exists(result.backup_path)
    assert result.size > 0

def test_dolphindb_backup_creates_file():
    result = backup_dolphindb("/tmp/test_backup")
    assert os.path.exists(result.backup_path)
    assert result.size > 0
```

**Step 2: Create backup scripts**

```bash
#!/bin/bash
# backend/scripts/maintenance/backup_postgres.sh
set -e

# PostgreSQL 备份脚本
BACKUP_DIR="${1:-./backups/postgres}"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=30

mkdir -p "$BACKUP_DIR"

# 环境变量
PGHOST=${POSTGRES_HOST:-localhost}
PGPORT=${POSTGRES_PORT:-5432}
PGDATABASE=${POSTGRES_DB:-quantsystem}
PGUSER=${POSTGRES_USER:-quantsystem}
PGPASSWORD=${POSTGRES_PASSWORD}

BACKUP_FILE="$BACKUP_DIR/postgres_${DATE}.sql.gz"

echo "Starting PostgreSQL backup: $BACKUP_FILE"

# 执行备份
pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$PGDATABASE" | gzip > "$BACKUP_FILE"

# 验证备份
if [ $? -eq 0 ]; then
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo "Backup completed successfully: $BACKUP_FILE ($BACKUP_SIZE)"
    
    # 生成校验和
    sha256sum "$BACKUP_FILE" > "$BACKUP_FILE.sha256"
    
    # 清理旧备份
    find "$BACKUP_DIR" -name "postgres_*.sql.gz" -mtime +$RETENTION_DAYS -delete
    find "$BACKUP_DIR" -name "postgres_*.sql.gz.sha256" -mtime +$RETENTION_DAYS -delete
    
    echo "Old backups cleaned up (keeping last $RETENTION_DAYS days)"
else
    echo "Backup failed!"
    rm -f "$BACKUP_FILE"
    exit 1
fi
```

```python
# backend/scripts/maintenance/backup_dolphindb.py
"""DolphinDB 备份脚本"""
import os
import gzip
import shutil
import hashlib
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

from store.dolphindb_client import db_client
from app.core.logger import logger

@dataclass
class BackupResult:
    backup_path: str
    size: int
    checksum: str
    tables: list[str]
    started_at: datetime
    completed_at: datetime

def backup_dolphindb(
    backup_dir: str = "./backups/dolphindb",
    retention_days: int = 30,
) -> BackupResult:
    """
    备份 DolphinDB 数据库
    
    Args:
        backup_dir: 备份目录
        retention_days: 保留天数
        
    Returns:
        备份结果
    """
    started_at = datetime.now()
    date_str = started_at.strftime("%Y%m%d_%H%M%S")
    
    # 创建备份目录
    os.makedirs(backup_dir, exist_ok=True)
    
    # 获取所有表
    tables = db_client.list_tables()
    logger.info(f"Backing up {len(tables)} tables: {tables}")
    
    # 备份每个表
    backup_files = []
    for table in tables:
        try:
            df = db_client.query(f"SELECT * FROM {table}")
            if not df.is_empty():
                table_file = os.path.join(backup_dir, f"{table}_{date_str}.parquet")
                df.write_parquet(table_file)
                backup_files.append(table_file)
                logger.info(f"Backed up {table}: {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to backup {table}: {e}")
    
    # 创建归档
    archive_path = os.path.join(backup_dir, f"dolphindb_{date_str}.tar.gz")
    with tarfile.open(archive_path, "w:gz") as tar:
        for file_path in backup_files:
            tar.add(file_path, arcname=os.path.basename(file_path))
    
    # 清理临时文件
    for file_path in backup_files:
        os.remove(file_path)
    
    # 计算校验和
    checksum = _compute_sha256(archive_path)
    size = os.path.getsize(archive_path)
    
    # 保存校验和
    with open(f"{archive_path}.sha256", "w") as f:
        f.write(f"{checksum}  {os.path.basename(archive_path)}")
    
    # 清理旧备份
    _cleanup_old_backups(backup_dir, "dolphindb_*.tar.gz", retention_days)
    _cleanup_old_backups(backup_dir, "dolphindb_*.tar.gz.sha256", retention_days)
    
    completed_at = datetime.now()
    logger.info(f"Backup completed: {archive_path} ({size} bytes) in {completed_at - started_at}")
    
    return BackupResult(
        backup_path=archive_path,
        size=size,
        checksum=checksum,
        tables=tables,
        started_at=started_at,
        completed_at=completed_at,
    )

def _compute_sha256(file_path: str) -> str:
    """计算文件 SHA256 校验和"""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def _cleanup_old_backups(backup_dir: str, pattern: str, retention_days: int):
    """清理旧备份"""
    # 实现清理逻辑...
    pass

if __name__ == "__main__":
    backup_dolphindb()
```

**Step 3: Add backup config and cron job**

Add backup configuration to `config.py`, then add cron job examples.

**Step 4: Commit**

```bash
git add backend/scripts/maintenance/
git commit -m "feat: add automated backup scripts for PostgreSQL and DolphinDB"
```

---

## Phase 1 Summary

**Week 1-2 Deliverables:**
- ✅ JWT/OAuth2 认证中间件
- ✅ 所有敏感端点添加认证保护
- ✅ RestrictedPython 沙箱保护代码执行
- ✅ 所有 SQL 注入漏洞修复
- ✅ 默认密码修改，.env 从 git 移除
- ✅ 自动化备份策略实施

**Verification:**
- 安全扫描（bandit）无 CRITICAL 问题
- 渗透测试（OWASP ZAP）无高危漏洞
- 所有 11 个 CRITICAL 问题已修复

---

## Phase 2: 架构统一（Week 3-6）- P0/P1 优先级

### Task 2.1: 数据库职责边界划分 - 迁移 stock_basic 和 trade_cal 到 DolphinDB

**Files:**
- Modify: `backend/infrastructure/database/table_manager.py`
- Modify: `backend/infrastructure/database/metadata_manager.py`
- Create: `backend/scripts/migrations/004_migrate_stock_trade_cal.py`
- Test: `backend/tests/integration/test_database_migration.py`

**Step 1: Write the failing test**

```python
# backend/tests/integration/test_database_migration.py
import pytest
from store.dolphindb_client import db_client
from infrastructure.database.table_manager import TableManager

def test_stocks_table_in_dolphindb():
    # stocks 表应在 DolphinDB 中
    assert db_client.table_exists("stocks")
    # 不应在 PostgreSQL 中（或应已弃用）

def test_trading_calendar_in_dolphindb():
    # trading_calendar 表应在 DolphinDB 中
    assert db_client.table_exists("trade_cal")

def test_data_sync_uses_dolphindb_tables():
    # 同步任务应写入 DolphinDB 表
    table_manager = TableManager(db_client)
    stocks = table_manager.get_stock_basic()
    assert len(stocks) > 0
```

**Step 2: Update TableManager**

```python
# backend/infrastructure/database/table_manager.py
# 更新 _META_TABLES，移除 stock_basic 和 trade_cal
_META_TABLES = frozenset({
    "factor_metadata",
    "sync_log",
    "sync_log_history",
    "sync_task_config",
    "etl_task_config",
    "factor_config",
    "factor_data_config",
    # "stock_basic",  # 已迁移到 stocks（DolphinDB）
    # "trade_cal",    # 已迁移到 trading_calendar（DolphinDB）
})
```

**Step 3: Create migration script**

```python
# backend/scripts/migrations/004_migrate_stock_trade_cal.py
"""将 stock_basic 和 trade_cal 从 PostgreSQL 迁移到 DolphinDB"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import polars as pl
from store.dolphindb_client import db_client
from scheduler.db import get_db
from sqlalchemy import text

def migrate_stocks():
    """迁移 stocks 表"""
    print("Migrating stocks table...")
    
    # 从 PostgreSQL 读取
    with get_db() as conn:
        result = conn.execute(text("SELECT * FROM stocks"))
        rows = result.fetchall()
        if not rows:
            print("No data in stocks table, skipping")
            return
        
        df = pl.from_dicts([dict(row) for row in rows])
        print(f"Read {len(df)} rows from PostgreSQL")
    
    # 写入 DolphinDB
    db_client.upsert("stocks", df, ["ts_code"])
    print(f"Wrote {len(df)} rows to DolphinDB stocks table")

def migrate_trading_calendar():
    """迁移 trading_calendar 表"""
    print("Migrating trading_calendar table...")
    
    # 从 PostgreSQL 读取
    with get_db() as conn:
        result = conn.execute(text("SELECT * FROM trading_calendar"))
        rows = result.fetchall()
        if not rows:
            print("No data in trading_calendar table, skipping")
            return
        
        df = pl.from_dicts([dict(row) for row in rows])
        print(f"Read {len(df)} rows from PostgreSQL")
    
    # 写入 DolphinDB
    db_client.upsert("trade_cal", df, ["exchange", "cal_date"])
    print(f"Wrote {len(df)} rows to DolphinDB trade_cal table")

if __name__ == "__main__":
    migrate_stocks()
    migrate_trading_calendar()
    print("Migration completed!")
```

**Step 4: Update metadata_manager.py to create DolphinDB tables**

```python
# backend/infrastructure/database/metadata_manager.py
# 添加 stocks 和 trade_cal 表的创建逻辑
def ensure_stocks_table(self):
    """确保 stocks 表存在"""
    if not self.db_client.table_exists("stocks"):
        schema = {
            "ts_code": "STRING",
            "symbol": "STRING",
            "name": "STRING",
            "area": "STRING",
            "industry": "STRING",
            "market": "STRING",
            "list_date": "STRING",
            "delist_date": "STRING",
            "is_active": "INT",
        }
        self.db_client.create_table("stocks", schema, sort_columns=["ts_code"])

def ensure_trade_cal_table(self):
    """确保 trade_cal 表存在"""
    if not self.db_client.table_exists("trade_cal"):
        schema = {
            "exchange": "STRING",
            "cal_date": "STRING",
            "is_open": "INT",
            "pretrade_date": "STRING",
        }
        self.db_client.create_table("trade_cal", schema, sort_columns=["exchange", "cal_date"])
```

**Step 5: Update all code references**

Search and replace all references:
- `stocks` (PG) → `stocks` (DolphinDB)
- `trading_calendar` (PG) → `trade_cal` (DolphinDB)
- `stock_basic` (DolphinDB) → `stocks` (DolphinDB)

**Step 6: Commit**

```bash
git add backend/infrastructure/database/table_manager.py backend/infrastructure/database/metadata_manager.py backend/scripts/migrations/004_migrate_stock_trade_cal.py backend/tests/integration/test_database_migration.py
git commit -m "feat: migrate stocks and trading_calendar to DolphinDB"
```

---

### Task 2.2: 删除 store/dolphindb_client.py，统一使用 infrastructure/database/

**Files:**
- Delete: `backend/store/dolphindb_client.py`
- Modify: `backend/infrastructure/database/dolphindb_client.py` - 确保是单例
- Create: `backend/store/__init__.py` - 添加兼容性导入
- Test: `backend/tests/test_dolphindb_client.py`

**Step 1: Check for all imports**

```bash
grep -r "from store.dolphindb_client" backend/ --include="*.py"
grep -r "import store.dolphindb_client" backend/ --include="*.py"
```

**Step 2: Create compatibility wrapper**

```python
# backend/store/__init__.py
"""兼容性导入 - 重定向到 infrastructure.database"""
import warnings

warnings.warn(
    "store.dolphindb_client is deprecated, use infrastructure.database.dolphindb_client instead",
    DeprecationWarning,
    stacklevel=2
)

from infrastructure.database.dolphindb_client import db_client
from infrastructure.database.dolphindb_client import DolphinDBClient

__all__ = ["db_client", "DolphinDBClient"]
```

**Step 3: Update all imports**

Search and replace:
- `from store.dolphindb_client import` → `from infrastructure.database.dolphindb_client import`
- `import store.dolphindb_client` → `import infrastructure.database.dolphindb_client`

**Step 4: Delete the old file**

```bash
rm backend/store/dolphindb_client.py
```

**Step 5: Commit**

```bash
git add -u
git rm backend/store/dolphindb_client.py
git add backend/store/__init__.py
git commit -m "refactor: unify DolphinDB client under infrastructure/database"
```

---

### Task 2.3: 实现 DolphinDB 连接池

**Files:**
- Modify: `backend/infrastructure/database/dolphindb_client.py`
- Test: `backend/tests/test_connection_pool.py`

**Step 1: Write the failing test**

```python
# backend/tests/test_connection_pool.py
import time
import threading
from infrastructure.database.dolphindb_client import DolphinDBClient, get_connection_pool

def test_connection_pool_reuses_connections():
    pool = get_connection_pool(pool_size=5)
    conn1 = pool.acquire()
    conn2 = pool.acquire()
    assert conn1 is not conn2
    pool.release(conn1)
    conn3 = pool.acquire()
    assert conn3 is conn1  # 应该重用连接

def test_connection_pool_concurrent_access():
    pool = get_connection_pool(pool_size=3)
    results = []
    
    def worker():
        conn = pool.acquire()
        time.sleep(0.1)
        results.append(True)
        pool.release(conn)
    
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    assert len(results) == 10
```

**Step 2: Implement connection pool**

```python
# backend/infrastructure/database/dolphindb_client.py
from queue import Queue
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, List
import dolphindb as ddb

@dataclass
class PooledConnection:
    conn: ddb.session
    in_use: bool = False
    last_used: float = 0.0

class DolphinDBConnectionPool:
    """DolphinDB 连接池"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8848,
        username: str = "admin",
        password: str = "",
        pool_size: int = 5,
        max_idle_time: float = 300.0,  # 5 分钟
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.max_idle_time = max_idle_time
        
        self._pool: Queue[PooledConnection] = Queue(maxsize=pool_size)
        self._created = 0
        self._lock = threading.Lock()
    
    def _create_connection(self) -> PooledConnection:
        """创建新连接"""
        conn = ddb.session()
        conn.connect(self.host, self.port, self.username, self.password)
        return PooledConnection(conn=conn, last_used=time.time())
    
    def acquire(self, timeout: Optional[float] = None) -> ddb.session:
        """获取连接"""
        try:
            pooled = self._pool.get(timeout=timeout)
        except:
            with self._lock:
                if self._created < self.pool_size:
                    pooled = self._create_connection()
                    self._created += 1
                else:
                    raise
        
        pooled.in_use = True
        return pooled.conn
    
    def release(self, conn: ddb.session):
        """释放连接"""
        # 查找对应的 PooledConnection
        # (简化实现，实际需要跟踪)
        pass
    
    @contextmanager
    def connection(self, timeout: Optional[float] = None):
        """上下文管理器"""
        conn = self.acquire(timeout)
        try:
            yield conn
        finally:
            self.release(conn)
    
    def close(self):
        """关闭所有连接"""
        while not self._pool.empty():
            try:
                pooled = self._pool.get_nowait()
                pooled.conn.close()
            except:
                pass

# 全局连接池
_pool: Optional[DolphinDBConnectionPool] = None

def get_connection_pool() -> DolphinDBConnectionPool:
    """获取全局连接池"""
    global _pool
    if _pool is None:
        from app.core.config import settings
        _pool = DolphinDBConnectionPool(
            host=settings.dolphindb.host,
            port=settings.dolphindb.port,
            username=settings.dolphindb.user,
            password=settings.dolphindb.password,
            pool_size=settings.dolphindb.pool_size or 5,
        )
    return _pool
```

**Step 3: Update DolphinDBClient to use pool**

```python
# Modify DolphinDBClient to use connection pool
class DolphinDBClient:
    def __init__(self, use_pool: bool = True):
        self.use_pool = use_pool
        self._single_conn: Optional[ddb.session] = None
    
    def _get_session(self) -> ddb.session:
        if self.use_pool:
            return get_connection_pool().acquire()
        else:
            if self._single_conn is None:
                # 创建单连接
            return self._single_conn
    
    def _release_session(self, session: ddb.session):
        if self.use_pool:
            get_connection_pool().release(session)
```

**Step 4: Commit**

```bash
git add backend/infrastructure/database/dolphindb_client.py backend/tests/test_connection_pool.py
git commit -m "feat: add DolphinDB connection pool"
```

---

### Task 2.4: 添加 PostgreSQL 外键约束

**Files:**
- Create: `backend/scripts/migrations/005_add_foreign_keys.sql`
- Test: `backend/tests/integration/test_foreign_keys.py`

**Step 1: Write migration script**

```sql
-- backend/scripts/migrations/005_add_foreign_keys.sql
-- 添加外键约束

-- flow_runs -> flow_configs
ALTER TABLE flow_runs
ADD CONSTRAINT fk_flow_runs_flow_config
FOREIGN KEY (flow_name) REFERENCES flow_configs(name)
ON DELETE CASCADE;

-- factor_analysis_results -> factor_configs
ALTER TABLE factor_analysis_results
ADD CONSTRAINT fk_far_factor_config
FOREIGN KEY (factor_id) REFERENCES factor_configs(factor_id)
ON DELETE CASCADE;

-- backtest_results -> factor_configs
ALTER TABLE backtest_results
ADD CONSTRAINT fk_br_factor_config
FOREIGN KEY (factor_id) REFERENCES factor_configs(factor_id)
ON DELETE SET NULL;

-- 添加索引（外键列应索引）
CREATE INDEX IF NOT EXISTS idx_flow_runs_flow_name
ON flow_runs(flow_name);

CREATE INDEX IF NOT EXISTS idx_far_factor_id
ON factor_analysis_results(factor_id);

CREATE INDEX IF NOT EXISTS idx_br_factor_id
ON backtest_results(factor_id);
```

**Step 2: Add CHECK constraints**

```sql
-- 添加到同一个迁移脚本
-- CHECK 约束
ALTER TABLE flow_runs
ADD CONSTRAINT chk_flow_runs_status
CHECK (status IN ('pending', 'running', 'success', 'failed', 'cancelled'));

ALTER TABLE task_runs
ADD CONSTRAINT chk_task_runs_status
CHECK (status IN ('pending', 'running', 'success', 'failed'));

ALTER TABLE task_runs
ADD CONSTRAINT chk_task_runs_type
CHECK (task_type IN ('sync', 'etl', 'factor', 'flow'));

ALTER TABLE sync_task_configs
ADD CONSTRAINT chk_sync_task_type
CHECK (sync_type IN ('incremental', 'full'));

ALTER TABLE trading_calendar
ADD CONSTRAINT chk_trading_calendar_open
CHECK (is_open IN (0, 1));
```

**Step 3: Add updated_at triggers**

```sql
-- 添加 updated_at 自动更新触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为每个有 updated_at 的表添加触发器
CREATE TRIGGER update_flow_configs_updated_at
    BEFORE UPDATE ON flow_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sync_task_configs_updated_at
    BEFORE UPDATE ON sync_task_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_etl_task_configs_updated_at
    BEFORE UPDATE ON etl_task_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_factor_configs_updated_at
    BEFORE UPDATE ON factor_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

**Step 4: Commit**

```bash
git add backend/scripts/migrations/005_add_foreign_keys.sql
git commit -m "feat: add foreign keys and CHECK constraints to PostgreSQL"
```

---

### Task 2.5: 添加健康检查端点和 Prometheus metrics

**Files:**
- Create: `backend/app/api/v1/health.py`
- Modify: `backend/app/main.py`
- Add: `requirements.txt` - add `prometheus-client`
- Test: `backend/tests/api/test_health.py`

**Step 1: Add prometheus-client to requirements**

```
prometheus-client>=0.19.0
```

**Step 2: Write health check endpoint**

```python
# backend/app/api/v1/health.py
"""健康检查和监控端点"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from app.core.auth import get_current_active_user, User
from store.dolphindb_client import db_client
from scheduler.db import get_db

router = APIRouter()

# Prometheus metrics
REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"]
)

REQUEST_DURATION = Histogram(
    "api_request_duration_seconds",
    "API request duration",
    ["endpoint"]
)

DATABASE_CONNECTIONS = Gauge(
    "database_connections_active",
    "Active database connections"
)

class HealthStatus(BaseModel):
    status: str
    dolphindb: str
    postgresql: str
    version: str

@router.get("/health", response_model=HealthStatus)
async def health_check(current_user: User = Depends(get_current_active_user)):
    """系统健康检查"""
    dolphindb_status = "healthy"
    postgresql_status = "healthy"
    
    # 检查 DolphinDB
    try:
        df = db_client.query("SELECT 1")
        if df.is_empty():
            dolphindb_status = "degraded"
    except Exception as e:
        dolphindb_status = f"unhealthy: {e}"
    
    # 检查 PostgreSQL
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
    except Exception as e:
        postgresql_status = f"unhealthy: {e}"
    
    overall_status = "healthy"
    if dolphindb_status != "healthy" or postgresql_status != "healthy":
        overall_status = "degraded"
    if "unhealthy" in dolphindb_status or "unhealthy" in postgresql_status:
        overall_status = "unhealthy"
    
    return HealthStatus(
        status=overall_status,
        dolphindb=dolphindb_status,
        postgresql=postgresql_status,
        version="2.0.0"
    )

@router.get("/health/live")
async def liveness_check():
    """Kubernetes liveness probe - 不认证"""
    return {"status": "alive"}

@router.get("/health/ready")
async def readiness_check():
    """Kubernetes readiness probe - 不认证"""
    # 检查数据库连接
    try:
        db_client.query("SELECT 1")
        with get_db() as conn:
            conn.execute("SELECT 1")
        return {"status": "ready"}
    except Exception as e:
        return Response(status_code=503, content={"status": "not ready", "error": str(e)})

@router.get("/metrics")
async def metrics():
    """Prometheus metrics 端点"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
```

**Step 3: Add middleware for metrics**

```python
# backend/app/main.py
from app.api.v1.health import REQUEST_COUNT, REQUEST_DURATION
import time

@app.middleware("http")
async def add_prometheus_metrics(request: Request, call_next):
    start_time = time.time()
    endpoint = request.url.path
    
    # 过滤掉健康检查和 metrics
    if endpoint.startswith("/health") or endpoint == "/metrics":
        return await call_next(request)
    
    response = await call_next(request)
    
    # 记录 metrics
    duration = time.time() - start_time
    REQUEST_DURATION.labels(endpoint=endpoint).observe(duration)
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=endpoint,
        status=response.status_code
    ).inc()
    
    return response
```

**Step 4: Commit**

```bash
git add backend/app/api/v1/health.py backend/app/main.py requirements.txt
git commit -m "feat: add health check and Prometheus metrics"
```

---

## Phase 2 Summary

**Week 3-6 Deliverables:**
- ✅ 数据库职责边界清晰（PG: 配置/日志, DolphinDB: 业务数据）
- ✅ 删除旧的 dolphindb_client.py，统一使用 infrastructure/database/
- ✅ DolphinDB 连接池实现
- ✅ PostgreSQL 外键约束和 CHECK 约束
- ✅ updated_at 自动更新触发器
- ✅ 健康检查端点和 Prometheus metrics
- ✅ 架构债务消除

---

## Phase 3: 代码质量（Week 7-10）- P1 优先级

### Task 3.1: 拆分 etl_api.py (883行)

**Files:**
- Split: `backend/app/api/v1/data/etl_api.py`
- Create: `backend/app/api/v1/data/etl_router.py`
- Create: `backend/app/services/etl_service.py`
- Create: `backend/app/repositories/etl_repository.py`
- Test: `backend/tests/api/test_etl_api.py`
- Test: `backend/tests/services/test_etl_service.py`

**Step 1: Analyze current etl_api.py**

识别三个主要职责：
1. 路由层 - HTTP 端点
2. 服务层 - 业务逻辑
3. 仓库层 - 数据访问

**Step 2: Create etl_repository.py**

```python
# backend/app/repositories/etl_repository.py
"""ETL 任务数据访问层"""
from typing import List, Optional
from dataclasses import dataclass
from datetime import datetime

from scheduler.db import get_db
from scheduler.models import ETLTaskConfig

@dataclass
class ETLTask:
    task_id: str
    name: str
    description: str
    script: str
    source_tables: List[str]
    target_table: str
    schedule: Optional[str]
    enabled: bool
    created_at: datetime
    updated_at: datetime

class ETLRepository:
    """ETL 任务仓库"""
    
    def find_all(self) -> List[ETLTask]:
        """获取所有 ETL 任务"""
        with get_db() as conn:
            result = conn.execute(
                "SELECT * FROM etl_task_configs ORDER BY created_at DESC"
            )
            return [ETLTask(**dict(row)) for row in result.fetchall()]
    
    def find_by_id(self, task_id: str) -> Optional[ETLTask]:
        """根据 ID 获取任务"""
        with get_db() as conn:
            result = conn.execute(
                "SELECT * FROM etl_task_configs WHERE task_id = %s",
                (task_id,)
            )
            row = result.fetchone()
            return ETLTask(**dict(row)) if row else None
    
    def create(self, task: ETLTask) -> ETLTask:
        """创建新任务"""
        with get_db() as conn:
            conn.execute(
                """INSERT INTO etl_task_configs
                   (task_id, name, description, script, source_tables, target_table,
                    schedule, enabled, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())""",
                (task.task_id, task.name, task.description, task.script,
                 task.source_tables, task.target_table, task.schedule, task.enabled)
            )
            conn.commit()
        return self.find_by_id(task.task_id)
    
    def update(self, task_id: str, **kwargs) -> Optional[ETLTask]:
        """更新任务"""
        set_clause = ", ".join(f"{k} = %s" for k in kwargs.keys())
        values = list(kwargs.values()) + [task_id]
        
        with get_db() as conn:
            conn.execute(
                f"UPDATE etl_task_configs SET {set_clause}, updated_at = NOW() WHERE task_id = %s",
                tuple(values)
            )
            conn.commit()
        return self.find_by_id(task_id)
    
    def delete(self, task_id: str) -> bool:
        """删除任务"""
        with get_db() as conn:
            result = conn.execute(
                "DELETE FROM etl_task_configs WHERE task_id = %s",
                (task_id,)
            )
            conn.commit()
            return result.rowcount > 0
```

**Step 3: Create etl_service.py**

```python
# backend/app/services/etl_service.py
"""ETL 任务业务逻辑层"""
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import logging

from app.repositories.etl_repository import ETLRepository, ETLTask
from store.dolphindb_client import db_client

logger = logging.getLogger(__name__)

@dataclass
class ETLExecutionResult:
    success: bool
    message: str
    rows_affected: int = 0
    error: Optional[str] = None

class ETLService:
    """ETL 任务服务"""
    
    def __init__(self):
        self.repository = ETLRepository()
    
    def list_tasks(self) -> List[ETLTask]:
        """获取所有 ETL 任务"""
        return self.repository.find_all()
    
    def get_task(self, task_id: str) -> Optional[ETLTask]:
        """获取单个任务"""
        return self.repository.find_by_id(task_id)
    
    def create_task(self, task_data: Dict[str, Any]) -> ETLTask:
        """创建 ETL 任务"""
        task = ETLTask(
            task_id=task_data["task_id"],
            name=task_data["name"],
            description=task_data.get("description", ""),
            script=task_data["script"],
            source_tables=task_data.get("source_tables", []),
            target_table=task_data["target_table"],
            schedule=task_data.get("schedule"),
            enabled=task_data.get("enabled", True),
            created_at=None,
            updated_at=None,
        )
        return self.repository.create(task)
    
    def update_task(self, task_id: str, task_data: Dict[str, Any]) -> Optional[ETLTask]:
        """更新 ETL 任务"""
        return self.repository.update(task_id, **task_data)
    
    def delete_task(self, task_id: str) -> bool:
        """删除 ETL 任务"""
        return self.repository.delete(task_id)
    
    def test_script(self, script: str, params: Optional[Dict] = None) -> ETLExecutionResult:
        """测试 ETL 脚本"""
        try:
            logger.info(f"Testing ETL script: {script[:100]}...")
            
            # 在 DolphinDB 中执行脚本
            # (实际实现需要沙箱和超时控制)
            df = db_client.query(script)
            
            return ETLExecutionResult(
                success=True,
                message=f"Script executed successfully",
                rows_affected=len(df) if not df.is_empty() else 0
            )
        except Exception as e:
            logger.error(f"ETL script test failed: {e}")
            return ETLExecutionResult(
                success=False,
                message="Script execution failed",
                error=str(e)
            )
    
    def run_task(self, task_id: str) -> ETLExecutionResult:
        """执行 ETL 任务"""
        task = self.repository.find_by_id(task_id)
        if not task:
            return ETLExecutionResult(
                success=False,
                message=f"Task {task_id} not found"
            )
        
        if not task.enabled:
            return ETLExecutionResult(
                success=False,
                message=f"Task {task_id} is disabled"
            )
        
        logger.info(f"Running ETL task: {task_id}")
        return self.test_script(task.script)
    
    def backfill_task(self, task_id: str, start_date: str, end_date: str) -> ETLExecutionResult:
        """回填 ETL 任务"""
        # 实现回填逻辑
        pass
```

**Step 4: Create etl_router.py**

```python
# backend/app/api/v1/data/etl_router.py
"""ETL 任务路由层"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel

from app.core.auth import get_current_active_user, User
from app.services.etl_service import ETLService, ETLTask, ETLExecutionResult

router = APIRouter()
etl_service = ETLService()

# Pydantic models
class ETLTaskCreateRequest(BaseModel):
    task_id: str
    name: str
    description: Optional[str] = ""
    script: str
    source_tables: Optional[List[str]] = []
    target_table: str
    schedule: Optional[str] = None
    enabled: bool = True

class ETLTaskUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    script: Optional[str] = None
    source_tables: Optional[List[str]] = None
    target_table: Optional[str] = None
    schedule: Optional[str] = None
    enabled: Optional[bool] = None

class ETLTestRequest(BaseModel):
    script: str
    params: Optional[dict] = None

class ETLBackfillRequest(BaseModel):
    start_date: str
    end_date: str

@router.get("/etl/tasks", response_model=List[ETLTask])
async def list_etl_tasks(
    current_user: User = Depends(get_current_active_user)
):
    """获取所有 ETL 任务"""
    return etl_service.list_tasks()

@router.get("/etl/tasks/{task_id}", response_model=ETLTask)
async def get_etl_task(
    task_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """获取单个 ETL 任务"""
    task = etl_service.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@router.post("/etl/tasks", response_model=ETLTask)
async def create_etl_task(
    req: ETLTaskCreateRequest,
    current_user: User = Depends(get_current_active_user)
):
    """创建 ETL 任务"""
    try:
        return etl_service.create_task(req.dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.put("/etl/tasks/{task_id}", response_model=ETLTask)
async def update_etl_task(
    task_id: str,
    req: ETLTaskUpdateRequest,
    current_user: User = Depends(get_current_active_user)
):
    """更新 ETL 任务"""
    task = etl_service.update_task(task_id, req.dict(exclude_unset=True))
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@router.delete("/etl/tasks/{task_id}")
async def delete_etl_task(
    task_id: str,
    current_user: User = Depends(get_current_active_user)
):
    """删除 ETL 任务"""
    if not etl_service.delete_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
    return {"status": "success"}

@router.post("/etl/test", response_model=ETLExecutionResult)
async def test_etl_script(
    req: ETLTestRequest,
    current_user: User = Depends(get_current_active_user)
):
    """测试 ETL 脚本"""
    return etl_service.test_script(req.script, req.params)

@router.post("/etl/tasks/{task_id}/execute", response_model=ETLExecutionResult)
async def run_etl_task(
    task_id: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_active_user)
):
    """执行 ETL 任务"""
    # 实际应该是异步执行
    return etl_service.run_task(task_id)

@router.post("/etl/tasks/{task_id}/backfill", response_model=ETLExecutionResult)
async def backfill_etl_task(
    task_id: str,
    req: ETLBackfillRequest,
    current_user: User = Depends(get_current_active_user)
):
    """回填 ETL 任务"""
    return etl_service.backfill_task(task_id, req.start_date, req.end_date)
```

**Step 5: Update main.py and delete old file**

```python
# backend/app/main.py
# 替换旧的 etl_api 导入
from app.api.v1.data import etl_router

app.include_router(etl_router.router, prefix="/api/v1/data", tags=["etl"])
```

**Step 6: Commit**

```bash
git rm backend/app/api/v1/data/etl_api.py
git add backend/app/api/v1/data/etl_router.py backend/app/services/etl_service.py backend/app/repositories/etl_repository.py
git commit -m "refactor: split etl_api.py into router/service/repository"
```

---

### Task 3.2: 为无类型注解的文件添加类型注解

**Files:**
- Modify: 31+ 无类型注解文件
- Test: `mypy backend/`

**Step 1: Identify files without type annotations**

```bash
# 运行 mypy 检查覆盖率
mypy backend/ --no-error-summary --no-pretty | grep "error: Missing type annotation"
```

**Step 2: Add type annotations systematically**

文件清单（示例）：
1. `backend/app/core/cache.py`
2. `backend/app/core/logger.py`
3. `backend/data_manager/processor.py`
4. `backend/engine/factors/technical.py`
5. ... 另外 27 个文件

**Step 3: Example type annotation**

```python
# 之前
def process_data(df, config):
    result = df.filter(...)
    return result

# 之后
import polars as pl
from typing import Dict, Optional

def process_data(df: pl.DataFrame, config: Dict[str, Any]) -> pl.DataFrame:
    """处理数据"""
    result = df.filter(...)
    return result
```

**Step 4: Verify coverage**

```bash
# 目标：类型注解覆盖率 ≥ 80%
mypy backend/ --cov-report=term-missing
```

**Step 5: Commit**

```bash
git add -u
git commit -m "refactor: add type annotations to 31+ files (coverage ≥ 80%)"
```

---

### Task 3.3: 重构长函数（Top 10）

**Files:**
- Modify: `backend/engine/analysis/alphalens_adapter.py` - `run_full_analysis()` (331行)
- Modify: 9 个其他长函数
- Test: `backend/tests/test_long_function_refactor.py`

**Step 1: Analyze run_full_analysis()**

识别 7+ 个独立步骤：
1. 数据加载和验证
2. 因子预处理
3. IC 计算
4. 分位数分析
5. 换手率分析
6. 结果汇总
7. 报告生成

**Step 2: Split into smaller functions**

```python
# backend/engine/analysis/alphalens_adapter.py
def run_full_analysis(
    factor_data: pl.DataFrame,
    price_data: pl.DataFrame,
    config: AnalysisConfig
) -> AnalysisResult:
    """运行完整的因子分析（重构后）"""
    # 步骤 1: 验证和预处理数据
    validated_factor, validated_prices = _validate_input_data(
        factor_data, price_data, config
    )
    
    # 步骤 2: 对齐因子和价格
    aligned_data = _align_factor_and_prices(
        validated_factor, validated_prices, config
    )
    
    # 步骤 3: 计算 IC
    ic_results = _calculate_information_coefficient(aligned_data, config)
    
    # 步骤 4: 分位数组合分析
    quantile_results = _calculate_quantile_returns(aligned_data, config)
    
    # 步骤 5: 换手率分析
    turnover_results = _calculate_turnover(aligned_data, config)
    
    # 步骤 6: 汇总结果
    final_result = _aggregate_results(
        ic_results, quantile_results, turnover_results, config
    )
    
    return final_result

def _validate_input_data(
    factor_data: pl.DataFrame,
    price_data: pl.DataFrame,
    config: AnalysisConfig
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """验证输入数据"""
    # 30-50 行
    pass

def _align_factor_and_prices(...) -> AlignedData:
    """对齐因子和价格"""
    # 30-50 行
    pass

# ... 其他拆分后的函数
```

**Step 3: Repeat for other long functions**

对 top 10 长函数应用类似的拆分：
1. `run_full_analysis()` - 拆分为 7+ 个函数
2. `ProductionEngine.run_task()` - 已经是 8 步，需要验证
3. `SyncTaskExecutor.execute()` - 如有需要拆分
4. ... 7 个其他函数

**Step 4: Commit**

```bash
git add backend/engine/analysis/alphalens_adapter.py
git commit -m "refactor: split run_full_analysis() into 7+ smaller functions"
```

---

### Task 3.4: 按类型组织测试目录

**Files:**
- Reorganize: `backend/tests/`
- Create: `backend/tests/unit/`
- Create: `backend/tests/integration/`
- Create: `backend/tests/api/`
- Create: `backend/tests/performance/`
- Update: `backend/pytest.ini`

**Step 1: Create directory structure**

```
backend/tests/
├── __init__.py
├── conftest.py
├── unit/
│   ├── __init__.py
│   ├── test_auth.py
│   ├── test_sandbox.py
│   ├── test_security.py
│   ├── test_etl_service.py
│   └── ...
├── integration/
│   ├── __init__.py
│   ├── test_database_migration.py
│   ├── test_foreign_keys.py
│   └── ...
├── api/
│   ├── __init__.py
│   ├── test_auth_protection.py
│   ├── test_health.py
│   ├── test_etl_api.py
│   └── ...
└── performance/
    ├── __init__.py
    ├── test_query_performance.py
    └── ...
```

**Step 2: Move existing test files**

```bash
# 示例
mkdir -p backend/tests/unit backend/tests/integration backend/tests/api backend/tests/performance

# 移动文件
mv backend/tests/test_security.py backend/tests/unit/
mv backend/tests/test_dolphindb_utils.py backend/tests/integration/
# ...
```

**Step 3: Update pytest.ini**

```ini
# backend/pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_functions = test_*
markers =
    unit: Unit tests
    integration: Integration tests
    api: API tests
    performance: Performance tests
addopts =
    -p no:django
    --strict-markers
    --cov=backend/app
    --cov-report=term-missing
```

**Step 4: Update .gitignore and commit**

```bash
git add backend/tests/
git commit -m "refactor: reorganize tests by type (unit/integration/api/performance)"
```

---

### Task 3.5: 配置 pytest + coverage，达到 ≥ 80% 覆盖率

**Files:**
- Update: `backend/requirements.txt`
- Create: `backend/.coveragerc`
- Create: `backend/tests/conftest.py` (if needed)

**Step 1: Add test requirements**

```
# backend/requirements.txt
pytest>=8.0.0
pytest-cov>=4.1.0
pytest-asyncio>=0.23.0
pytest-mock>=3.12.0
pytest-xdist>=3.5.0
```

**Step 2: Create .coveragerc**

```
# backend/.coveragerc
[run]
source =
    app/
    engine/
    data_manager/
    infrastructure/
    scheduler/
omit =
    */tests/*
    */__pycache__/*
    */.venv/*
    */node_modules/*

[report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise AssertionError
    raise NotImplementedError
    if __name__ == .__main__.:
    if TYPE_CHECKING:
precision = 2
show_missing = True
fail_under = 80
```

**Step 3: Create conftest.py**

```python
# backend/tests/conftest.py
"""Pytest configuration and fixtures"""
import pytest
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture
def client():
    """Test client fixture"""
    return TestClient(app)

@pytest.fixture
def auth_headers():
    """Authenticated headers fixture"""
    from app.core.auth import create_access_token
    token = create_access_token(data={"sub": "testuser"})
    return {"Authorization": f"Bearer {token}"}
```

**Step 4: Run coverage and fill gaps**

```bash
# 运行覆盖率检查
pytest --cov=backend/app --cov-report=term-missing

# 为覆盖率低的模块添加测试
# 目标：80%+ 覆盖率
```

**Step 5: Commit**

```bash
git add backend/requirements.txt backend/.coveragerc backend/tests/conftest.py
git commit -m "test: configure pytest + coverage (target ≥ 80%)"
```

---

## Phase 3 Summary

**Week 7-10 Deliverables:**
- ✅ `etl_api.py` 拆分为 router/service/repository
- ✅ 类型注解覆盖率 ≥ 80%
- ✅ Top 10 长函数重构完成
- ✅ 测试目录按类型组织
- ✅ pytest + coverage 配置完成
- ✅ 测试覆盖率 ≥ 80%
- ✅ 所有 HIGH 问题已修复

---

## Phase 4: 完善优化（Week 11-12）- P2/P3 优先级

### Task 4.1: 移除所有 console.log 和前端优化

**Files:**
- Modify: 27 个前端文件
- Create: `frontend/.eslintrc.json` (add no-console rule)

**Step 1: Find all console.log**

```bash
grep -r "console\." frontend/src/ --include="*.tsx" --include="*.ts"
```

**Step 2: Replace with proper logging**

```typescript
// 之前
console.log("Data loaded:", data);
console.error("Error:", error);

// 之后
import { logger } from "../utils/logger";

logger.debug("Data loaded:", data);
logger.error("Error:", error);
```

**Step 3: Add no-console ESLint rule**

```json
{
  "rules": {
    "no-console": ["error", { "allow": ["warn", "error"] }]
  }
}
```

**Step 4: Commit**

```bash
git add frontend/src/
git commit -m "refactor: remove console.log from 27 files"
```

---

### Task 4.2: 实现长列表虚拟化和性能优化

**Files:**
- Modify: `frontend/src/components/TaskList/TaskList.tsx`
- Add: `react-window` to `frontend/package.json`

**Step 1: Install react-window**

```bash
cd frontend
npm install react-window @types/react-window
```

**Step 2: Implement virtualized list**

```tsx
// frontend/src/components/TaskList/TaskList.tsx
import { FixedSizeList as List } from "react-window";

const TaskList = ({ tasks }: { tasks: Task[] }) => {
  const Row = ({ index, style }: { index: number; style: React.CSSProperties }) => (
    <div style={style}>
      <TaskItem task={tasks[index]} />
    </div>
  );

  return (
    <List
      height={600}
      itemCount={tasks.length}
      itemSize={80}
      width="100%"
    >
      {Row}
    </List>
  );
};
```

**Step 3: Commit**

```bash
git add frontend/src/components/TaskList/TaskList.tsx frontend/package.json
git commit -m "perf: add virtualization for long lists with react-window"
```

---

### Task 4.3: 查询分析和性能优化

**Files:**
- Create: `backend/scripts/performance/analyze_queries.py`
- Add: Missing indexes in PostgreSQL and DolphinDB

**Step 1: Analyze slow queries**

```python
# backend/scripts/performance/analyze_queries.py
"""分析慢查询"""
from store.dolphindb_client import db_client
from scheduler.db import get_db
from sqlalchemy import text

def analyze_postgres_queries():
    """分析 PostgreSQL 慢查询"""
    with get_db() as conn:
        result = conn.execute(text("""
            SELECT query, calls, mean_time, total_time
            FROM pg_stat_statements
            ORDER BY total_time DESC
            LIMIT 20
        """))
        return result.fetchall()

def analyze_dolphindb_queries():
    """分析 DolphinDB 查询模式"""
    # 查询分区裁剪效果
    pass

# 添加缺失索引
def add_missing_indexes():
    """添加缺失的索引"""
    with get_db() as conn:
        # 外键列索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_flow_runs_parent_flow_run_id ON flow_runs(parent_flow_run_id)")
        # 常用查询列索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_runs_started_at ON task_runs(started_at DESC)")
        conn.commit()
```

**Step 2: Commit**

```bash
git add backend/scripts/performance/analyze_queries.py
git commit -m "perf: add query analysis and missing indexes"
```

---

### Task 4.4: 文档整合和 ADR 流程

**Files:**
- Merge: `/docs/ARCHITECTURE.md` with `/backend/docs/PIPELINE_ARCHITECTURE.md`
- Archive: Old plans in `docs/plans/archive/`
- Create: `docs/adr/` directory
- Create: `docs/adr/template.md`

**Step 1: Merge duplicate documentation**

```bash
# 创建统一的架构文档
cat docs/ARCHITECTURE.md backend/docs/PIPELINE_ARCHITECTURE.md > docs/ARCHITECTURE.md.tmp
# 手动去重和整理
```

**Step 2: Archive old plans**

```bash
mkdir -p docs/plans/archive
mv backend/docs/plans/* docs/plans/archive/
```

**Step 3: Create ADR template**

```markdown
# docs/adr/template.md
# ADR: [短标题]

**日期:** YYYY-MM-DD
**状态:** Proposed/Accepted/Rejected/Deprecated

## 上下文

[描述背景和问题]

## 决策

[描述做出的决策]

## 后果

[描述决策的后果，包括正面和负面]
```

**Step 4: Commit**

```bash
git add docs/
git commit -m "docs: consolidate documentation and establish ADR process"
```

---

## Phase 4 Summary

**Week 11-12 Deliverables:**
- ✅ 移除所有 console.log（27个文件）
- ✅ 实现长列表虚拟化（react-window）
- ✅ 添加 React Error Boundaries
- ✅ 查询分析和优化
- ✅ 批量操作优化
- ✅ 文档整合完成
- ✅ ADR 流程建立
- ✅ 性能优化完成

---

## 总体成功标准

### Phase 1 完成标准
- [ ] 所有 11 个 CRITICAL 安全问题已修复
- [ ] 认证授权机制上线
- [ ] 自动化备份策略实施

### Phase 2 完成标准
- [ ] 数据库职责边界清晰
- [ ] 架构债务消除
- [ ] 健康检查和监控上线

### Phase 3 完成标准
- [ ] 类型注解覆盖率 ≥ 80%
- [ ] 测试覆盖率 ≥ 80%
- [ ] 所有 HIGH 问题已修复

### Phase 4 完成标准
- [ ] 文档整合完成
- [ ] 性能优化完成
- [ ] 可观测性完善

---

## 执行选择

计划已保存到 `docs/plans/2026-04-11-system-refactor-implementation.md`。两种执行选项：

**1. Subagent-Driven (本会话)** - 我为每个任务派生子代理，任务之间进行审查，快速迭代

**2. Parallel Session (独立会话)** - 在 worktree 中打开新会话使用 executing-plans，批量执行并设置检查点

您选择哪种方式？
