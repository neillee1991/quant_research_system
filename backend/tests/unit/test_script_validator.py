"""AST 白名单校验器单测"""
import pytest

from engine.script.validator import validate_script, MAX_SCRIPT_SIZE


class TestValidateScript:
    """校验器核心逻辑"""

    def test_valid_script_passes(self):
        script = "def build_strategy():\n    return {'ts_code': '000001.SZ'}"
        result = validate_script(script)
        assert result.valid is True
        assert len(result.errors) == 0
        assert result.script_hash

    def test_empty_script_fails(self):
        result = validate_script("")
        assert result.valid is False
        assert "为空" in result.errors[0]

    def test_whitespace_only_script_fails(self):
        result = validate_script("   \n  \n  ")
        assert result.valid is False

    def test_missing_entry_point_fails(self):
        script = "def other_func():\n    pass"
        result = validate_script(script)
        assert result.valid is False
        assert "build_strategy" in result.errors[0]

    def test_custom_entry_point(self):
        script = "def my_strategy():\n    return {}"
        result = validate_script(script, entry_point="my_strategy")
        assert result.valid is True

    def test_syntax_error_fails(self):
        script = "def build_strategy(\n    return {}"
        result = validate_script(script)
        assert result.valid is False
        assert "语法错误" in result.errors[0]

    def test_blocked_import_os(self):
        script = "import os\ndef build_strategy():\n    return {}"
        result = validate_script(script)
        assert result.valid is False
        assert "os" in result.errors[0]

    def test_blocked_import_subprocess(self):
        script = "import subprocess\ndef build_strategy():\n    return {}"
        result = validate_script(script)
        assert result.valid is False

    def test_blocked_from_import(self):
        script = "from os import path\ndef build_strategy():\n    return {}"
        result = validate_script(script)
        assert result.valid is False

    def test_blocked_exec(self):
        script = "def build_strategy():\n    exec('print(1)')\n    return {}"
        result = validate_script(script)
        assert result.valid is False
        assert "exec" in result.errors[0]

    def test_blocked_eval(self):
        script = "def build_strategy():\n    eval('1+1')\n    return {}"
        result = validate_script(script)
        assert result.valid is False

    def test_blocked_open(self):
        script = "def build_strategy():\n    open('/etc/passwd')\n    return {}"
        result = validate_script(script)
        assert result.valid is False

    def test_blocked_dunder_access(self):
        script = "def build_strategy():\n    x.__class__\n    return {}"
        result = validate_script(script)
        assert result.valid is False
        assert "dunder" in result.errors[0]

    def test_init_is_allowed_dunder(self):
        script = "class Foo:\n    def __init__(self):\n        pass\ndef build_strategy():\n    return {}"
        result = validate_script(script)
        assert result.valid is True

    def test_unsupported_language(self):
        result = validate_script("code", language="javascript")
        assert result.valid is False
        assert "javascript" in result.errors[0]

    def test_script_hash_deterministic(self):
        script = "def build_strategy():\n    return {}"
        r1 = validate_script(script)
        r2 = validate_script(script)
        assert r1.script_hash == r2.script_hash

    def test_too_many_functions(self):
        funcs = "\n".join(f"def f{i}(): pass" for i in range(12))
        script = f"{funcs}\ndef build_strategy():\n    return {{}}"
        result = validate_script(script)
        assert result.valid is False
        assert "函数数量" in result.errors[0]

    def test_script_too_large(self):
        script = "def build_strategy():\n    return {}" + " " * (MAX_SCRIPT_SIZE + 1)
        result = validate_script(script)
        assert result.valid is False
        assert "过大" in result.errors[0]

    def test_safe_allowed_import(self):
        script = "import math\ndef build_strategy():\n    return {'x': math.sqrt(4)}"
        result = validate_script(script)
        assert result.valid is True

    def test_safe_builtin_usage(self):
        script = "def build_strategy():\n    return {'x': abs(-1), 'y': len([1,2,3])}"
        result = validate_script(script)
        assert result.valid is True
