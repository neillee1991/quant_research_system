"""AST 白名单校验器

对用户提交的 Python 脚本做安全校验，拒绝危险操作。
仅依赖 Python 标准库 ast 模块，不执行任何用户代码。
"""
import ast
import hashlib
from dataclasses import dataclass, field

# 脚本限制
MAX_SCRIPT_SIZE = 10_240  # 10KB
MAX_FUNCTIONS = 10

# 禁止导入的模块
BLOCKED_MODULES = frozenset({
    "os", "sys", "subprocess", "socket", "http", "urllib",
    "ctypes", "multiprocessing", "threading", "signal",
    "shutil", "pathlib", "glob", "tempfile", "pickle",
    "shelve", "marshal", "importlib", "pkgutil",
    "builtins", "code", "codeop", "compile", "compileall",
    "py_compile", "zipimport", "asyncio",
})

# 禁止使用的内置函数 / 名称
BLOCKED_BUILTINS = frozenset({
    "exec", "eval", "compile", "__import__", "open",
    "globals", "locals", "vars", "dir", "getattr",
    "setattr", "delattr", "breakpoint", "input",
})

# 禁止使用的 dunder 名称模式
BLOCKED_DUNDER_PREFIXES = ("__",)


@dataclass
class ValidationResult:
    valid: bool
    language: str
    script_hash: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _compute_hash(script: str) -> str:
    return hashlib.sha256(script.encode("utf-8")).hexdigest()


def validate_script(
    script: str,
    language: str = "python",
    entry_point: str = "build_strategy",
) -> ValidationResult:
    """校验脚本安全性，不执行用户代码。"""
    errors: list[str] = []
    warnings: list[str] = []
    script_hash = _compute_hash(script)

    if language != "python":
        return ValidationResult(
            valid=False, language=language, script_hash=script_hash,
            errors=[f"不支持的语言: {language}，当前仅支持 python"],
        )

    if not script.strip():
        return ValidationResult(
            valid=False, language=language, script_hash=script_hash,
            errors=["脚本内容为空"],
        )

    if len(script.encode("utf-8")) > MAX_SCRIPT_SIZE:
        errors.append(f"脚本过大（>{MAX_SCRIPT_SIZE} 字节），请精简代码")

    # 1. 解析 AST
    try:
        tree = ast.parse(script)
    except SyntaxError as e:
        return ValidationResult(
            valid=False, language=language, script_hash=script_hash,
            errors=[f"语法错误 (行 {e.lineno}): {e.msg}"],
        )

    # 2. 检查 entry_point 存在
    top_level_funcs = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    if entry_point not in top_level_funcs:
        errors.append(f"未找到入口函数 '{entry_point}'，请定义 def {entry_point}(): ...")

    # 3. 函数数量限制
    if len(top_level_funcs) > MAX_FUNCTIONS:
        errors.append(f"函数数量 {len(top_level_funcs)} 超过限制 {MAX_FUNCTIONS}")

    # 4. 遍历 AST 检查安全规则
    for node in ast.walk(tree):
        _check_imports(node, errors)
        _check_blocked_builtins(node, errors)
        _check_dunder_access(node, errors)

    return ValidationResult(
        valid=len(errors) == 0,
        language=language,
        script_hash=script_hash,
        errors=errors,
        warnings=warnings,
    )


def _check_imports(node: ast.AST, errors: list[str]) -> None:
    """检查禁止的 import 语句。"""
    if isinstance(node, ast.Import):
        for alias in node.names:
            root_module = alias.name.split(".")[0]
            if root_module in BLOCKED_MODULES:
                errors.append(f"禁止导入模块: {alias.name}")

    elif isinstance(node, ast.ImportFrom):
        if node.module:
            root_module = node.module.split(".")[0]
            if root_module in BLOCKED_MODULES:
                errors.append(f"禁止从模块导入: {node.module}")


def _check_blocked_builtins(node: ast.AST, errors: list[str]) -> None:
    """检查禁止的内置函数调用。"""
    if isinstance(node, ast.Call):
        func_name = _get_call_name(node)
        if func_name in BLOCKED_BUILTINS:
            errors.append(f"禁止调用: {func_name}()")


def _get_call_name(node: ast.Call) -> str:
    """提取 Call 节点的函数名。"""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""


def _check_dunder_access(node: ast.AST, errors: list[str]) -> None:
    """检查禁止的 dunder 属性访问。"""
    if isinstance(node, ast.Attribute):
        if node.attr.startswith(BLOCKED_DUNDER_PREFIXES) and node.attr.endswith("__"):
            if node.attr not in ("__init__",):
                errors.append(f"禁止访问 dunder 属性: {node.attr}")
