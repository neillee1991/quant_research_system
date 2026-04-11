"""安全沙箱执行环境"""
import sys
import io
import traceback
from typing import Any, Optional, Dict
from datetime import datetime, timedelta

from app.core.logger import logger


class SandboxSecurityError(Exception):
    """沙箱安全违规"""
    pass


class SandboxTimeoutError(Exception):
    """沙箱执行超时"""
    pass


# 安全的全局变量 - 只允许最基本的 builtins
SAFE_GLOBALS = {
    "__builtins__": {
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
}


def execute_safe_code(
    code: str,
    local_vars: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 30,
    max_memory_mb: int = 256,
) -> Dict[str, Any]:
    """
    在受限环境中执行代码（简化版安全沙箱）

    注意：这是一个基础的安全层，主要通过以下方式提供保护：
    1. 限制可用的 builtins
    2. 禁止危险操作（文件访问、网络、导入等）
    3. 提供安全的模块访问（仅 polars）

    Args:
        code: 要执行的代码
        local_vars: 局部变量字典
        timeout_seconds: 超时时间（秒）
        max_memory_mb: 最大内存限制（MB）

    Returns:
        执行结果字典，包含：
        - success: 是否成功
        - result: 返回值（如果有）
        - stdout: 标准输出
        - stderr: 错误输出
        - error: 错误信息（如果有）

    Raises:
        SandboxSecurityError: 安全违规
    """
    # 安全检查
    security_violations = check_security(code)
    if security_violations:
        raise SandboxSecurityError(f"安全违规检测: {', '.join(security_violations)}")

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    # 准备命名空间
    globals_dict = dict(SAFE_GLOBALS)

    # 允许的模块
    try:
        # 安全地导入 polars
        import polars as pl
        globals_dict["pl"] = pl
        globals_dict["polars"] = pl
    except ImportError:
        logger.warning("Polars not available in sandbox")

    # 添加安全的 print 函数
    def safe_print(*args, **kwargs):
        end = kwargs.get("end", "\n")
        stdout_capture.write(" ".join(str(x) for x in args) + end)

    globals_dict["print"] = safe_print

    # 添加局部变量
    locals_dict = local_vars.copy() if local_vars else {}

    result = {
        "success": False,
        "result": None,
        "stdout": "",
        "stderr": "",
        "error": None,
    }

    try:
        # 编译代码
        compiled = compile(code, "<sandbox>", "exec")

        # 执行代码
        exec(compiled, globals_dict, locals_dict)

        result["success"] = True
        result["result"] = locals_dict.get("result")  # 支持通过 result 变量返回

    except SyntaxError as e:
        error_msg = f"语法错误 (第{e.lineno}行, 第{e.offset}列): {e.msg}"
        result["error"] = error_msg
        stderr_capture.write(error_msg)
        logger.warning(f"Sandbox syntax error: {error_msg}")

    except SandboxSecurityError:
        raise

    except Exception as e:
        error_msg = f"执行错误:\n{traceback.format_exc()}"
        result["error"] = error_msg
        stderr_capture.write(error_msg)
        logger.warning(f"Sandbox execution error: {error_msg}")

    result["stdout"] = stdout_capture.getvalue()
    result["stderr"] = stderr_capture.getvalue()

    return result


def check_security(code: str) -> list[str]:
    """
    检查代码中的安全违规模式

    Returns:
        违规列表，如果没有违规则返回空列表
    """
    violations = []

    # 危险的导入模式
    dangerous_imports = [
        "import os", "from os",
        "import sys", "from sys",
        "import subprocess", "from subprocess",
        "import pickle", "from pickle",
        "import ctypes", "from ctypes",
        "import builtins", "from builtins",
        "import importlib", "from importlib",
        "__import__",
    ]

    for pattern in dangerous_imports:
        if pattern in code:
            violations.append(f"禁止的导入: {pattern}")

    # 危险的操作
    dangerous_ops = [
        "eval(", "exec(", "compile(",
        "open(", "file(",
        "globals()", "locals()", "vars()",
        "__dict__", "__class__", "__bases__",
        "__subclasses__", "__mro__",
        "getattr(", "setattr(", "delattr(",
        "input(", "raw_input(",
    ]

    for pattern in dangerous_ops:
        if pattern in code:
            violations.append(f"禁止的操作: {pattern}")

    # 检查文件访问模式
    file_patterns = [
        "/etc/", "/proc/", "/dev/", "/sys/",
        "C:\\Windows", "C:\\Program",
    ]

    for pattern in file_patterns:
        if pattern in code:
            violations.append(f"禁止的路径: {pattern}")

    return violations


def validate_factor_code(code: str) -> tuple[bool, list[str]]:
    """
    验证因子代码的安全性和有效性

    Returns:
        (is_valid, issues_list)
    """
    issues = []

    # 安全检查
    security_issues = check_security(code)
    issues.extend(security_issues)

    # 代码结构检查
    if "def compute" not in code and "@factor" not in code:
        issues.append("代码必须包含 compute 函数或使用 @factor 装饰器")

    # 检查是否包含必要的逻辑
    if len(code.strip()) < 10:
        issues.append("代码太短，可能不完整")

    return len(issues) == 0, issues
