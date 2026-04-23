"""脚本编译器

将校验通过的 Python 脚本编译为可执行 IR。
在受限命名空间中执行 build_strategy()，获取策略配置 dict，
校验结构后将配置转化为标准 IR。
"""
import hashlib
from dataclasses import dataclass, field
from typing import Any

from engine.parser.flow_parser import OPERATOR_REGISTRY
from engine.script.sandbox import run_in_sandbox

# 安全的内置函数和异常类型白名单
# 复用 sandbox.py 中的白名单以确保一致性
from engine.script.sandbox import _SAFE_BUILTINS

# 策略配置必要字段
_REQUIRED_FIELDS = {"ts_code", "start_date", "end_date", "signals"}

# condition 表达式允许的字符集（字母、数字、运算符、空格、点、下划线）
_ALLOWED_CONDITION_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    " <>>=<!=&|()+-*/%,._"
)

# IR 版本
IR_VERSION = "2.0"


@dataclass
class CompileResult:
    success: bool
    script_hash: str
    ir: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _compute_hash(script: str) -> str:
    return hashlib.sha256(script.encode("utf-8")).hexdigest()


def compile_script(
    script: str,
    language: str = "python",
    entry_point: str = "build_strategy",
) -> CompileResult:
    """编译脚本为 IR。在受限命名空间中执行 entry_point 函数。"""
    script_hash = _compute_hash(script)
    errors: list[str] = []
    warnings: list[str] = []

    if language != "python":
        return CompileResult(
            success=False, script_hash=script_hash,
            errors=[f"不支持的语言: {language}"],
        )

    # 1. 在受限命名空间中执行脚本
    strategy_config = _execute_sandboxed(script, entry_point, errors)
    if errors:
        return CompileResult(
            success=False, script_hash=script_hash, errors=errors,
        )

    # 2. 校验策略配置结构
    _validate_config_structure(strategy_config, errors, warnings)
    if errors:
        return CompileResult(
            success=False, script_hash=script_hash, errors=errors, warnings=warnings,
        )

    # 3. 校验 signals 算子白名单
    _validate_signals(strategy_config["signals"], errors)
    if errors:
        return CompileResult(
            success=False, script_hash=script_hash, errors=errors, warnings=warnings,
        )

    # 4. 构建 IR
    ir = _build_ir(strategy_config, language, entry_point, script_hash)

    return CompileResult(
        success=True,
        script_hash=script_hash,
        ir=ir,
        warnings=warnings,
    )


def _execute_sandboxed(
    script: str, entry_point: str, errors: list[str],
) -> dict[str, Any]:
    """在进程级沙箱中执行脚本并调用 entry_point 函数。"""
    sandbox_result = run_in_sandbox(script, entry_point)

    if not sandbox_result.success:
        errors.append(f"脚本执行失败: {sandbox_result.error}")
        return {}

    return sandbox_result.result or {}


def _validate_config_structure(
    config: dict[str, Any], errors: list[str], warnings: list[str],
) -> None:
    """校验策略配置 dict 的结构。"""
    missing = _REQUIRED_FIELDS - set(config.keys())
    if missing:
        errors.append(f"策略配置缺少必要字段: {', '.join(sorted(missing))}")
        return

    if not isinstance(config["ts_code"], str) or not config["ts_code"].strip():
        errors.append("ts_code 必须是非空字符串")

    if not isinstance(config["start_date"], str) or len(config["start_date"]) != 8:
        errors.append("start_date 格式应为 YYYYMMDD")

    if not isinstance(config["end_date"], str) or len(config["end_date"]) != 8:
        errors.append("end_date 格式应为 YYYYMMDD")

    if not isinstance(config["signals"], list) or len(config["signals"]) == 0:
        errors.append("signals 必须是非空列表")
        return

    for i, sig in enumerate(config["signals"]):
        if not isinstance(sig, dict):
            errors.append(f"signals[{i}] 必须是 dict")
            continue
        if "type" not in sig:
            errors.append(f"signals[{i}] 缺少 'type' 字段")


def _validate_signals(signals: list[dict], errors: list[str]) -> None:
    """校验 signals 中的算子是否在白名单内。"""
    for i, sig in enumerate(signals):
        sig_type = sig.get("type", "")
        if sig_type == "indicator":
            op = sig.get("op", "")
            if op not in OPERATOR_REGISTRY:
                errors.append(
                    f"signals[{i}]: 未知算子 '{op}'，"
                    f"可用: {', '.join(sorted(OPERATOR_REGISTRY.keys()))}"
                )
        elif sig_type == "condition":
            expr = sig.get("expr", "")
            _validate_condition_expr(expr, i, errors)
        else:
            errors.append(f"signals[{i}]: 未知类型 '{sig_type}'，可用: indicator, condition")


def _validate_condition_expr(expr: str, index: int, errors: list[str]) -> None:
    """校验 condition 表达式是否只包含安全字符。"""
    if not expr:
        errors.append(f"signals[{index}]: condition 表达式不能为空")
        return
    invalid_chars = set(expr) - _ALLOWED_CONDITION_CHARS
    if invalid_chars:
        errors.append(
            f"signals[{index}]: condition 包含非法字符: {invalid_chars}"
        )


def _build_ir(
    config: dict[str, Any],
    language: str,
    entry_point: str,
    script_hash: str,
) -> dict[str, Any]:
    """将策略配置编译为标准 IR。"""
    return {
        "version": IR_VERSION,
        "source_type": "script",
        "language": language,
        "entry_point": entry_point,
        "script_hash": script_hash,
        "data_source": {
            "ts_code": config["ts_code"],
            "start_date": config["start_date"],
            "end_date": config["end_date"],
        },
        "pipeline": config["signals"],
        "backtest_config": {
            "initial_capital": config.get("capital", 1_000_000),
            "commission_rate": config.get("commission_rate", 0.0003),
            "slippage_rate": config.get("slippage_rate", 0.0001),
        },
    }
