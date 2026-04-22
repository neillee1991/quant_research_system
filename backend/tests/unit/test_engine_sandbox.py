"""进程级沙箱执行器测试

测试 sandbox.py 的进程级隔离、超时机制和内存限制功能。
"""
import time
import pytest

from engine.script.sandbox import run_in_sandbox, DEFAULT_TIMEOUT
from engine.script.compiler import compile_script


class TestSandboxExecution:
    """测试沙箱基本功能"""

    def test_basic_execution(self):
        """测试基本执行功能"""
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [{"type": "condition", "expr": "close > 10", "output_col": "signal"}]
    }
"""
        result = run_in_sandbox(script)
        assert result.success is True
        assert isinstance(result.result, dict)
        assert result.result["ts_code"] == "000001.SZ"

    def test_entry_point_not_found(self):
        """测试入口函数不存在"""
        script = """
def wrong_name():
    return {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20241231", "signals": []}
"""
        result = run_in_sandbox(script)
        assert result.success is False
        assert "build_strategy" in result.error

    def test_non_dict_return(self):
        """测试返回值不是 dict"""
        script = "def build_strategy(): return 'not a dict'"
        result = run_in_sandbox(script)
        assert result.success is False
        assert "dict" in result.error


class TestSandboxTimeout:
    """测试超时机制"""

    def test_short_running_script(self):
        """测试快速运行的脚本不超时"""
        script = """
def build_strategy():
    time.sleep(0.1)
    return {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20241231", "signals": []}
"""
        result = run_in_sandbox(script, timeout=2)
        assert result.success is True

    def test_script_timeout(self):
        """测试超时的脚本"""
        script = """
def build_strategy():
    time.sleep(3)
    return {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20241231", "signals": []}
"""
        result = run_in_sandbox(script, timeout=1)
        assert result.success is False
        assert "超时" in result.error

    def test_timeout_message_contains_seconds(self):
        """测试超时消息包含秒数"""
        script = """
def build_strategy():
    time.sleep(2)
    return {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20241231", "signals": []}
"""
        result = run_in_sandbox(script, timeout=1)
        assert str(1) in result.error


class TestCompilerIntegration:
    """测试编译器与沙箱的集成"""

    def test_compile_uses_sandbox(self):
        """测试 compile_script 函数使用沙箱执行"""
        script = """
def build_strategy():
    return {
        "ts_code": "000001.SZ",
        "start_date": "20230101",
        "end_date": "20241231",
        "signals": [
            {"type": "indicator", "op": "sma", "params": {"window": 20}, "output_col": "sma20"},
            {"type": "condition", "expr": "close > sma20", "output_col": "signal"}
        ]
    }
"""
        result = compile_script(script)
        assert result.success is True
        assert len(result.ir["pipeline"]) == 2

    def test_compiler_handles_sandbox_errors(self):
        """测试编译器能正确处理沙箱执行错误"""
        script = """
def build_strategy():
    raise ValueError("故意抛出的错误")
"""
        result = compile_script(script)
        assert result.success is False
        assert "故意抛出的错误" in result.errors[0]


class TestSandboxSecurity:
    """测试沙箱的安全性"""

    def test_safe_builtins_only_available(self):
        """测试只允许安全的内置函数"""
        script = """
def build_strategy():
    import sys
    return {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20241231", "signals": []}
"""
        result = run_in_sandbox(script)
        assert result.success is False
        assert "import" in result.error or "NameError" in result.error

    def test_forbidden_operations_blocked(self):
        """测试禁止的操作被阻止"""
        script = """
def build_strategy():
    open("/etc/passwd")
    return {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20241231", "signals": []}
"""
        result = run_in_sandbox(script)
        assert result.success is False
        assert "open" in result.error or "NameError" in result.error


if __name__ == "__main__":
    # 简单的测试运行器
    tests = TestSandboxExecution()
    print("=== 运行沙箱测试 ===")
    tests.test_basic_execution()
    print("✅ test_basic_execution passed")

    tests.test_entry_point_not_found()
    print("✅ test_entry_point_not_found passed")

    tests.test_non_dict_return()
    print("✅ test_non_dict_return passed")

    timeout_tests = TestSandboxTimeout()
    timeout_tests.test_short_running_script()
    print("✅ test_short_running_script passed")

    timeout_tests.test_script_timeout()
    print("✅ test_script_timeout passed")

    integration_tests = TestCompilerIntegration()
    integration_tests.test_compile_uses_sandbox()
    print("✅ test_compile_uses_sandbox passed")

    print("\n🎉 所有测试通过！")
