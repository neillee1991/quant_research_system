"""沙箱安全测试"""
import pytest
from app.core.sandbox import (
    check_security,
    execute_safe_code,
    validate_factor_code,
    SandboxSecurityError,
)


class TestCheckSecurity:

    def test_clean_code_passes(self):
        code = "result = 1 + 1"
        violations = check_security(code)
        assert violations == []

    def test_import_os_blocked(self):
        code = "import os"
        violations = check_security(code)
        assert len(violations) > 0

    def test_import_sys_blocked(self):
        code = "import sys"
        violations = check_security(code)
        assert len(violations) > 0

    def test_import_subprocess_blocked(self):
        code = "import subprocess"
        violations = check_security(code)
        assert len(violations) > 0

    def test_from_os_import_blocked(self):
        code = "from os import path"
        violations = check_security(code)
        assert len(violations) > 0

    def test_from_os_path_import_blocked(self):
        code = "from os.path import join"
        violations = check_security(code)
        assert len(violations) > 0

    def test_eval_call_blocked(self):
        code = "eval('1+1')"
        violations = check_security(code)
        assert len(violations) > 0

    def test_exec_call_blocked(self):
        code = "exec('x=1')"
        violations = check_security(code)
        assert len(violations) > 0

    def test_open_call_blocked(self):
        code = "open('/etc/passwd')"
        violations = check_security(code)
        assert len(violations) > 0

    def test_dunder_import_string_blocked(self):
        code = "x = '__import__'"
        violations = check_security(code)
        assert len(violations) > 0

    def test_dangerous_path_string_blocked(self):
        code = "x = '/etc/passwd'"
        violations = check_security(code)
        assert len(violations) > 0

    def test_dunder_class_attribute_blocked(self):
        code = "x = obj.__class__"
        violations = check_security(code)
        assert len(violations) > 0

    def test_dunder_globals_attribute_blocked(self):
        code = "x = func.__globals__"
        violations = check_security(code)
        assert len(violations) > 0

    def test_getattr_blocked(self):
        code = "getattr(obj, 'x')"
        violations = check_security(code)
        assert len(violations) > 0

    def test_syntax_error_reported(self):
        code = "def foo(:"
        violations = check_security(code)
        assert len(violations) > 0
        assert any("语法错误" in v for v in violations)

    def test_polars_import_not_blocked(self):
        # polars 不在黑名单，AST 检查应通过
        code = "import polars as pl\nresult = pl.DataFrame()"
        violations = check_security(code)
        # polars 不在 DANGEROUS_MODULES，不应被 AST 拦截
        # 但字符串模式匹配也不应命中
        assert not any("polars" in v for v in violations)


class TestExecuteSafeCode:

    def test_basic_arithmetic(self):
        result = execute_safe_code("result = 1 + 2")
        assert result["success"] is True
        assert result["result"] == 3

    def test_list_comprehension(self):
        result = execute_safe_code("result = [x * 2 for x in range(5)]")
        assert result["success"] is True
        assert result["result"] == [0, 2, 4, 6, 8]

    def test_print_captured(self):
        result = execute_safe_code("print('hello')")
        assert result["success"] is True
        assert "hello" in result["stdout"]

    def test_syntax_error_returns_error(self):
        # check_security 先运行，语法错误被当作安全违规抛出
        with pytest.raises(SandboxSecurityError, match="语法错误"):
            execute_safe_code("def foo(:")

    def test_runtime_error_returns_error(self):
        result = execute_safe_code("result = 1 / 0")
        assert result["success"] is False
        assert result["error"] is not None

    def test_dangerous_code_raises_security_error(self):
        with pytest.raises(SandboxSecurityError):
            execute_safe_code("import os")

    def test_local_vars_accessible(self):
        result = execute_safe_code("result = x + 1", local_vars={"x": 10})
        assert result["success"] is True
        assert result["result"] == 11

    def test_no_result_var_returns_none(self):
        result = execute_safe_code("x = 1 + 1")
        assert result["success"] is True
        assert result["result"] is None

    def test_polars_available_in_sandbox(self):
        code = "result = pl.DataFrame({'a': [1, 2, 3]}).shape"
        result = execute_safe_code(code)
        assert result["success"] is True
        assert result["result"] == (3, 1)

    def test_cannot_access_builtins_directly(self):
        # open 是危险标识符，check_security 会拦截并抛出异常
        with pytest.raises(SandboxSecurityError):
            execute_safe_code("result = type(open)")

    def test_safe_builtins_available(self):
        code = "result = sum([1, 2, 3])"
        result = execute_safe_code(code)
        assert result["success"] is True
        assert result["result"] == 6


class TestValidateFactorCode:

    def test_valid_factor_code(self):
        code = """
def compute(df, params):
    return df.with_columns([
        (pl.col('close') / pl.col('open')).alias('factor_value')
    ])
"""
        is_valid, issues = validate_factor_code(code)
        assert is_valid is True
        assert issues == []

    def test_missing_compute_function(self):
        code = "result = 1 + 1"
        is_valid, issues = validate_factor_code(code)
        assert is_valid is False
        assert any("compute" in i for i in issues)

    def test_code_too_short(self):
        is_valid, issues = validate_factor_code("x=1")
        assert is_valid is False

    def test_dangerous_code_invalid(self):
        code = "import os\ndef compute(df, params): pass"
        is_valid, issues = validate_factor_code(code)
        assert is_valid is False

    def test_factor_decorator_accepted(self):
        code = """
@factor
def my_factor(df, params):
    return df
"""
        is_valid, issues = validate_factor_code(code)
        assert is_valid is True
