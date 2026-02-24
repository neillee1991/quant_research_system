#!/usr/bin/env python3
"""
DolphinDB 数据库初始化脚本
读取 init_dolphindb.dos 并在 DolphinDB 中执行，创建所有库表。

用法:
    python database/init_dolphindb.py

环境变量:
    DOLPHINDB_HOST  - DolphinDB 主机地址 (默认: 127.0.0.1)
    DOLPHINDB_PORT  - DolphinDB 端口 (默认: 8848)
    DOLPHINDB_USER  - 用户名 (默认: admin)
    DOLPHINDB_PASS  - 密码 (默认: 123456)
"""

import os
import sys
import time
from pathlib import Path


def get_config() -> dict:
    """从环境变量读取 DolphinDB 连接配置"""
    return {
        "host": os.getenv("DOLPHINDB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DOLPHINDB_PORT", "8848")),
        "user": os.getenv("DOLPHINDB_USER", "admin"),
        "password": os.getenv("DOLPHINDB_PASS", "123456"),
    }


def load_script() -> str:
    """加载 .dos 初始化脚本"""
    script_dir = Path(__file__).resolve().parent
    dos_path = script_dir / "init_dolphindb.dos"
    if not dos_path.exists():
        print(f"[错误] 找不到初始化脚本: {dos_path}")
        sys.exit(1)
    text = dos_path.read_text(encoding="utf-8")
    print(f"[信息] 已加载脚本: {dos_path} ({len(text)} 字节)")
    return text


def main():
    try:
        import dolphindb as ddb
    except ImportError:
        print("[错误] 未安装 dolphindb 包，请执行: pip install dolphindb")
        sys.exit(1)

    config = get_config()
    print("=" * 50)
    print("DolphinDB 数据库初始化")
    print("=" * 50)
    print(f"[信息] 目标服务器: {config['host']}:{config['port']}")

    # 建立连接
    sess = ddb.Session()
    try:
        success = sess.connect(
            config["host"],
            config["port"],
            config["user"],
            config["password"],
        )
        if not success:
            print("[错误] 连接 DolphinDB 失败，请检查服务是否启动及连接参数")
            sys.exit(1)
        print("[信息] 已连接 DolphinDB")
    except Exception as e:
        print(f"[错误] 连接 DolphinDB 异常: {e}")
        sys.exit(1)

    # 加载并执行脚本
    script = load_script()
    print("[信息] 开始执行初始化脚本...")
    start = time.time()

    try:
        result = sess.run(script)
        elapsed = time.time() - start
        print(f"[信息] 脚本执行完成，耗时 {elapsed:.2f} 秒")
        if result is not None:
            print(f"[信息] 返回值: {result}")
    except Exception as e:
        elapsed = time.time() - start
        print(f"[错误] 脚本执行失败 (耗时 {elapsed:.2f} 秒): {e}")
        sys.exit(1)
    finally:
        sess.close()
        print("[信息] 已断开 DolphinDB 连接")

    # 验证: 重新连接并检查表是否存在
    print("\n[信息] 验证数据库和表...")
    sess2 = ddb.Session()
    try:
        sess2.connect(
            config["host"],
            config["port"],
            config["user"],
            config["password"],
        )

        research_tables = [
            "daily_data", "daily_basic", "adj_factor",
            "index_daily", "moneyflow", "factor_values",
        ]
        meta_tables = [
            "sync_log", "sync_log_history", "stock_basic",
            "factor_metadata", "factor_analysis",
            "dag_run_log", "dag_task_log",
        ]

        print("\n  dfs://quant_research:")
        for t in research_tables:
            exists = sess2.run(f'existsTable("dfs://quant_research", "{t}")')
            status = "OK" if exists else "缺失"
            print(f"    {t:20s} [{status}]")

        print("\n  dfs://quant_meta:")
        for t in meta_tables:
            exists = sess2.run(f'existsTable("dfs://quant_meta", "{t}")')
            status = "OK" if exists else "缺失"
            print(f"    {t:20s} [{status}]")

    except Exception as e:
        print(f"[警告] 验证阶段出错: {e}")
    finally:
        sess2.close()

    print("\n" + "=" * 50)
    print("初始化完成!")
    print("=" * 50)


if __name__ == "__main__":
    main()
