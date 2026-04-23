"""进程级沙箱执行器

使用 multiprocessing 实现进程隔离，提供超时和内存限制
"""
import io
import json
import multiprocessing
import os
import resource
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

# 在 macOS 上尝试使用 fork 模式（兼容性更好）
try:
    multiprocessing.set_start_method('fork')
except (RuntimeError, ValueError):
    # 如果 fork 不可用，使用默认的 spawn 模式
    pass


@dataclass
class SandboxResult:
    """沙箱执行结果"""
    success: bool = False
    result: Any = None
    stdout: str = ""
    stderr: str = ""
    error: Optional[str] = None
    execution_time_ms: float = 0.0
    memory_used_mb: float = 0.0


@dataclass
class SandboxConfig:
    """沙箱配置"""
    timeout_seconds: int = 30
    max_memory_mb: int = 256
    max_cpu_time_seconds: int = 30
    allow_network: bool = False
    allowed_modules: list[str] = field(default_factory=lambda: ["polars"])


class SandboxTimeoutError(Exception):
    """沙箱执行超时"""
    pass


class SandboxMemoryError(Exception):
    """沙箱内存超限"""
    pass


class SandboxSecurityError(Exception):
    """沙箱安全违规"""
    pass


# 安全的全局变量 - 只允许最基本的 builtins
SAFE_BUILTINS = {
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
        "__import__": __import__,
        "None": None,
        "True": True,
        "False": False,
    },
}


def _set_resource_limits(config: SandboxConfig) -> None:
    """设置进程资源限制（子进程内调用）"""
    # 获取当前的软限制和硬限制
    def get_current_limit(res: int) -> tuple[int, int]:
        try:
            return resource.getrlimit(res)
        except Exception:
            return (resource.RLIM_INFINITY, resource.RLIM_INFINITY)

    # CPU 时间限制
    cpu_soft, cpu_hard = get_current_limit(resource.RLIMIT_CPU)
    target_cpu_soft = min(config.max_cpu_time_seconds, cpu_hard if cpu_hard != resource.RLIM_INFINITY else config.max_cpu_time_seconds)
    target_cpu_hard = min(config.max_cpu_time_seconds + 5, cpu_hard if cpu_hard != resource.RLIM_INFINITY else config.max_cpu_time_seconds + 5)
    try:
        resource.setrlimit(
            resource.RLIMIT_CPU,
            (target_cpu_soft, target_cpu_hard)
        )
    except Exception:
        pass  # 忽略 CPU 限制设置错误

    # 内存限制 (AS - address space)
    memory_bytes = config.max_memory_mb * 1024 * 1024
    mem_soft, mem_hard = get_current_limit(resource.RLIMIT_AS)
    target_mem_soft = min(memory_bytes, mem_hard if mem_hard != resource.RLIM_INFINITY else memory_bytes)
    target_mem_hard = min(memory_bytes, mem_hard if mem_hard != resource.RLIM_INFINITY else memory_bytes)
    try:
        resource.setrlimit(
            resource.RLIMIT_AS,
            (target_mem_soft, target_mem_hard)
        )
    except Exception:
        pass  # 忽略内存限制设置错误

    # 文件描述符限制
    nofile_soft, nofile_hard = get_current_limit(resource.RLIMIT_NOFILE)
    target_nofile_soft = min(128, nofile_hard if nofile_hard != resource.RLIM_INFINITY else 128)
    target_nofile_hard = min(256, nofile_hard if nofile_hard != resource.RLIM_INFINITY else 256)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target_nofile_soft, target_nofile_hard))
    except Exception:
        pass  # 忽略文件描述符限制设置错误

    # 禁止创建核心文件
    try:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    except Exception:
        pass  # 忽略核心文件限制设置错误


def _sandbox_worker(
    code: str,
    local_vars: Dict[str, Any],
    config: SandboxConfig,
    result_queue: multiprocessing.Queue,
) -> None:
    """沙箱工作进程函数"""
    import time
    start_time = time.time()

    result = SandboxResult()
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    # 保存旧的输出句柄
    old_stdout = sys.stdout
    old_stderr = sys.stderr

    # 预先导入允许的模块（在设置安全环境之前）
    preloaded_modules = {}
    for module_name in config.allowed_modules:
        try:
            module = __import__(module_name)
            preloaded_modules[module_name] = module
            # 添加常用别名
            if module_name == "polars":
                preloaded_modules["pl"] = module
        except ImportError:
            pass

    try:
        # 设置资源限制
        _set_resource_limits(config)

        # 重定向输出
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture

        # 准备命名空间
        globals_dict = dict(SAFE_BUILTINS)

        # 添加预先导入的模块
        globals_dict.update(preloaded_modules)

        # 添加安全的 print 函数
        def safe_print(*args, **kwargs):
            end = kwargs.get("end", "\n")
            stdout_capture.write(" ".join(str(x) for x in args) + end)

        globals_dict["print"] = safe_print

        # 复制局部变量
        locals_dict = dict(local_vars)

        # 安全检查
        violations = _check_security(code)
        if violations:
            raise SandboxSecurityError(f"安全违规: {', '.join(violations)}")

        # 编译并执行代码
        compiled = compile(code, "<sandbox>", "exec")
        exec(compiled, globals_dict, locals_dict)

        result.success = True
        result.result = locals_dict.get("result")

    except SandboxSecurityError as e:
        result.error = str(e)
        stderr_capture.write(str(e))
    except MemoryError:
        result.error = f"内存超限 (最大 {config.max_memory_mb}MB)"
        stderr_capture.write(result.error)
    except Exception as e:
        error_msg = f"执行错误:\n{traceback.format_exc()}"
        result.error = error_msg
        stderr_capture.write(error_msg)
    finally:
        # 恢复输出
        sys.stdout = old_stdout
        sys.stderr = old_stderr

        # 获取执行时间
        result.execution_time_ms = (time.time() - start_time) * 1000

        # 获取内存使用（粗略估计）
        try:
            import psutil
            process = psutil.Process()
            result.memory_used_mb = process.memory_info().rss / 1024 / 1024
        except (ImportError, Exception):
            pass

        result.stdout = stdout_capture.getvalue()
        result.stderr = stderr_capture.getvalue()

        # 发送结果
        try:
            result_queue.put(result)
        except Exception as e:
            # 如果无法序列化，发送简化结果
            simple_result = SandboxResult(
                success=result.success,
                stdout=result.stdout,
                stderr=result.stderr,
                error=result.error or str(e) if not result.success else None,
                execution_time_ms=result.execution_time_ms,
                memory_used_mb=result.memory_used_mb,
            )
            result_queue.put(simple_result)


def _check_security(code: str) -> list[str]:
    """检查代码中的安全违规模式"""
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


def execute_sandbox(
    code: str,
    local_vars: Optional[Dict[str, Any]] = None,
    config: Optional[SandboxConfig] = None,
) -> SandboxResult:
    """
    在独立进程中执行代码

    Args:
        code: 要执行的代码
        local_vars: 局部变量字典
        config: 沙箱配置

    Returns:
        SandboxResult 执行结果
    """
    if config is None:
        config = SandboxConfig()

    if local_vars is None:
        local_vars = {}

    # 创建结果队列
    result_queue = multiprocessing.Queue()

    # 创建并启动进程
    process = multiprocessing.Process(
        target=_sandbox_worker,
        args=(code, local_vars, config, result_queue),
        daemon=True,
    )

    process.start()

    try:
        # 等待结果（带超时）
        result = result_queue.get(timeout=config.timeout_seconds)

        # 确保进程结束
        process.join(timeout=1)
        if process.is_alive():
            process.terminate()
            process.join(timeout=1)
            if process.is_alive():
                process.kill()

        return result

    except Exception as e:
        # 超时处理（包括 _queue.Empty）
        from queue import Empty
        if isinstance(e, (multiprocessing.TimeoutError, Empty)) or "Empty" in str(e):
            error_msg = f"执行超时 (最大 {config.timeout_seconds} 秒)"
        else:
            error_msg = f"沙箱错误: {type(e).__name__}: {str(e)}"

        process.terminate()
        process.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join()

        return SandboxResult(
            success=False,
            error=error_msg,
        )


def execute_sandbox_json(
    code: str,
    local_vars: Optional[Dict[str, Any]] = None,
    config: Optional[SandboxConfig] = None,
) -> str:
    """
    在独立进程中执行代码，返回 JSON 字符串

    Args:
        code: 要执行的代码
        local_vars: 局部变量字典
        config: 沙箱配置

    Returns:
        JSON 格式的执行结果
    """
    result = execute_sandbox(code, local_vars, config)

    # 转换为可序列化的字典
    result_dict = {
        "success": result.success,
        "result": _make_json_serializable(result.result),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "error": result.error,
        "execution_time_ms": result.execution_time_ms,
        "memory_used_mb": result.memory_used_mb,
    }

    return json.dumps(result_dict, ensure_ascii=False)


def _make_json_serializable(obj: Any) -> Any:
    """使对象可 JSON 序列化"""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool, list, dict)):
        return obj
    if isinstance(obj, tuple):
        return list(obj)
    try:
        # 尝试转换为字符串
        return str(obj)
    except Exception:
        return f"<不可序列化的对象: {type(obj).__name__}>"


# 为了兼容性，保留旧的接口名称
execute_safe_code_process = execute_sandbox
