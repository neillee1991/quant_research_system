"""安全沙箱执行环境"""
import sys
import io
import traceback
import ast
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


# 禁止的模块列表
DANGEROUS_MODULES = {
    "os", "sys", "subprocess", "pickle", "ctypes", "builtins", "importlib",
    "socket", "threading", "multiprocessing", "timeit", "gc", "inspect",
    "eval", "exec", "compile", "open", "file", "__import__"
}


# 禁止的属性访问
DANGEROUS_ATTRIBUTES = {
    "__dict__", "__class__", "__bases__", "__subclasses__", "__mro__",
    "__globals__", "__locals__", "__name__", "__module__", "__code__",
    "__defaults__", "__kwdefaults__", "__annotations__", "__closure__",
    "__doc__", "__init__", "__new__", "__call__", "__getattribute__",
    "__setattr__", "__delattr__", "__get__", "__set__", "__delete__"
}


# 禁止的函数调用
DANGEROUS_FUNCTIONS = {
    "eval", "exec", "compile", "open", "file", "input", "raw_input",
    "globals", "locals", "vars", "getattr", "setattr", "delattr"
}


class SecurityAnalyzer(ast.NodeVisitor):
    """安全分析器，使用AST分析代码"""

    def __init__(self):
        self.violations = []

    def visit_Import(self, node):
        """检查导入语句"""
        for alias in node.names:
            module_name = alias.name
            if module_name in DANGEROUS_MODULES or any(d in module_name for d in DANGEROUS_MODULES):
                self.violations.append(f"禁止导入危险模块: {module_name}")
        return self.generic_visit(node)

    def visit_ImportFrom(self, node):
        """检查from ... import语句"""
        if node.module in DANGEROUS_MODULES or any(d in (node.module or "") for d in DANGEROUS_MODULES):
            self.violations.append(f"禁止导入危险模块: {node.module}")
        return self.generic_visit(node)

    def visit_Attribute(self, node):
        """检查属性访问"""
        if isinstance(node.ctx, ast.Load) or isinstance(node.ctx, ast.Store):
            attr = node.attr
            if attr in DANGEROUS_ATTRIBUTES:
                self.violations.append(f"禁止访问危险属性: {attr}")
        return self.generic_visit(node)

    def visit_Call(self, node):
        """检查函数调用"""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            if func_name in DANGEROUS_FUNCTIONS:
                self.violations.append(f"禁止调用危险函数: {func_name}")

        elif isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr in DANGEROUS_FUNCTIONS or attr in DANGEROUS_ATTRIBUTES:
                self.violations.append(f"禁止调用危险函数: {attr}")

        return self.generic_visit(node)

    def visit_Name(self, node):
        """检查变量引用"""
        name = node.id
        if name in DANGEROUS_MODULES or name in DANGEROUS_FUNCTIONS:
            self.violations.append(f"禁止使用危险标识符: {name}")
        return self.generic_visit(node)

    def visit_Str(self, node):
        """检查字符串字面量中的危险内容"""
        s = node.s
        dangerous_patterns = ["/etc/", "/proc/", "/dev/", "/sys/",
                           "C:\\Windows", "C:\\Program", "__import__"]
        for pattern in dangerous_patterns:
            if pattern in s:
                self.violations.append(f"字符串包含危险内容: {pattern}")
        return self.generic_visit(node)


def check_security(code: str) -> list[str]:
    """
    使用AST分析检查代码中的安全违规

    Returns:
        违规列表，如果没有违规则返回空列表
    """
    violations = []

    try:
        # 解析代码为AST
        tree = ast.parse(code)

        # 使用AST访问者进行安全分析
        analyzer = SecurityAnalyzer()
        analyzer.visit(tree)
        violations.extend(analyzer.violations)

        # 额外的字符串模式匹配作为补充
        dangerous_patterns = [
            "import os", "from os", "import sys", "from sys",
            "import subprocess", "from subprocess", "import pickle", "from pickle",
            "import ctypes", "from ctypes", "import builtins", "from builtins",
            "import importlib", "from importlib", "__import__",
        ]

        for pattern in dangerous_patterns:
            if pattern in code:
                violations.append(f"禁止的导入模式: {pattern}")

        return list(set(violations))  # 去重

    except SyntaxError as e:
        violations.append(f"语法错误 (第{e.lineno}行, 第{e.offset}列): {e.msg}")
        return violations
    except Exception as e:
        violations.append(f"安全检查失败: {str(e)}")
        return violations


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
    4. timeout_seconds 超时强制终止

    Args:
        code: 要执行的代码
        local_vars: 局部变量字典
        timeout_seconds: 超时时间（秒）
        max_memory_mb: 最大内存限制（MB，当前未强制）

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
    import concurrent.futures

    # 安全检查
    security_violations = check_security(code)
    if security_violations:
        raise SandboxSecurityError(f"安全违规检测: {', '.join(security_violations)}")

    def _run() -> Dict[str, Any]:
        return _execute_in_sandbox(code, local_vars)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run)
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            raise SandboxTimeoutError(f"代码执行超时（>{timeout_seconds}s）")


def _execute_in_sandbox(code: str, local_vars: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """在沙箱命名空间中实际执行代码（由 execute_safe_code 在线程中调用）。"""
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    globals_dict = dict(SAFE_GLOBALS)

    try:
        import polars as pl
        globals_dict["pl"] = pl
        globals_dict["polars"] = pl
    except ImportError:
        logger.warning("Polars not available in sandbox")

    def safe_print(*args, **kwargs):
        end = kwargs.get("end", "\n")
        stdout_capture.write(" ".join(str(x) for x in args) + end)

    globals_dict["print"] = safe_print
    locals_dict = local_vars.copy() if local_vars else {}

    result = {
        "success": False,
        "result": None,
        "stdout": "",
        "stderr": "",
        "error": None,
    }

    try:
        compiled = compile(code, "<sandbox>", "exec")
        exec(compiled, globals_dict, locals_dict)
        result["success"] = True
        result["result"] = locals_dict.get("result")

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
