#!/bin/bash
# 数据库重新初始化快速启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================="
echo "数据库重新初始化"
echo "=================================="
echo ""
echo "工作目录: $BACKEND_DIR"
echo ""

# 检查 Python 虚拟环境
if [ ! -d "$BACKEND_DIR/.venv" ]; then
    echo "错误: 未找到虚拟环境 .venv"
    echo "请先创建虚拟环境: python -m venv .venv"
    exit 1
fi

# 激活虚拟环境
source "$BACKEND_DIR/.venv/bin/activate"

# 执行主控脚本
python "$SCRIPT_DIR/reinit_database.py"

echo ""
echo "=================================="
echo "完成"
echo "=================================="
