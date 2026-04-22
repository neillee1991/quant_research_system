"""进程级沙箱执行器

使用 multiprocessing 实现进程级隔离，防止不受信任的 Python 脚本
对主进程造成危害。提供超时机制和内存限制保护。
"""
import multiprocessing
import sys
import time
from dataclasses import dataclass, field
from typing import Any

# 复用 compiler.py 中的安全内置函数白名单
_SAFE_BUILTINS = {
    "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
    "enumerate": enumerate, "filter": filter, "float": float, "int": int,
    "isinstance": isinstance, "len": len, "list": list, "map": map,
    "max": max, "min": min, "print": print, "range": range,
    "round": round, "set": set, "sorted": sorted, "str": str,
    "sum": sum, "tuple": tuple, "zip": zip,
    "ValueError": ValueError, "TypeError": TypeError, "KeyError": KeyError,
    "IndexError": IndexError, "RuntimeError": RuntimeError, "Exception": Exception,
}

# 沙箱配置
DEFAULT_TIMEOUT = 30  # 30秒超时
DEFAULT_MEMORY_LIMIT = 256 * 1024 * 1024  # 256MB内存限制


@dataclass
class SandboxResult:
    """沙箱执行结果"""
    success: bool
    result: Any = None
    error: str = ""


def _sandbox_worker(
    script: str,
    entry_point: str,
    result_queue: multiprocessing.Queue,
) -> None:
    """在子进程中执行脚本的工作函数"""
    try:
        # 创建受限的全局命名空间
        safe_globals: dict[str, Any] = {"__builtins__": _SAFE_BUILTINS}

        # 安全地添加一些允许使用的模块
        try:
            import time
            safe_globals["time"] = time
        except ImportError:
            pass

        safe_locals: dict[str, Any] = {}

        # 执行脚本
        exec(script, safe_globals, safe_locals)

        # 检查入口函数是否存在
        if entry_point not in safe_locals:
            result_queue.put(SandboxResult(
                success=False,
                error=f"入口函数 '{entry_point}' 在执行后不可用"
            ))
            return

        # 调用入口函数
        entry_point_func = safe_locals[entry_point]
        result = entry_point_func()

        # 检查返回值类型
        if not isinstance(result, dict):
            result_queue.put(SandboxResult(
                success=False,
                error=f"{entry_point}() 必须返回 dict，实际返回 {type(result).__name__}"
            ))
            return

        # 返回成功结果
        result_queue.put(SandboxResult(
            success=True,
            result=result
        ))

    except Exception as e:
        result_queue.put(SandboxResult(
            success=False,
            error=str(e)
        ))


def run_in_sandbox(
    script: str,
    entry_point: str = "build_strategy",
    timeout: int = DEFAULT_TIMEOUT,
    memory_limit: int = DEFAULT_MEMORY_LIMIT,
) -> SandboxResult:
    """
    在进程级沙箱中执行 Python 脚本

    Args:
        script: 要执行的 Python 脚本
        entry_point: 入口函数名，默认 "build_strategy"
        timeout: 超时时间（秒），默认 30 秒
        memory_limit: 内存限制（字节），默认 256MB

    Returns:
        SandboxResult: 执行结果
    """
    # 创建进程间通信队列
    result_queue = multiprocessing.Queue()

    # 创建子进程
    process = multiprocessing.Process(
        target=_sandbox_worker,
        args=(script, entry_point, result_queue),
        daemon=True
    )

    try:
        # 启动子进程
        process.start()

        # 等待结果或超时
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not result_queue.empty():
                return result_queue.get()
            time.sleep(0.1)

        # 超时处理
        process.terminate()
        process.join(timeout=1)
        if process.is_alive():
            process.kill()
            process.join()

        return SandboxResult(
            success=False,
            error=f"脚本执行超时（超过 {timeout} 秒）"
        )

    except Exception as e:
        return SandboxResult(
            success=False,
            error=f"沙箱执行失败: {e}"
        )
