#!/bin/bash
# 数据库重新初始化 - 完整执行指南

echo "========================================"
echo "数据库重新初始化 - 执行指南"
echo "========================================"
echo ""
echo "已创建的脚本文件:"
echo ""
echo "核心脚本:"
echo "  1. backup_configs.py      - 备份配置数据"
echo "  2. drop_old_tables.py     - 删除旧表"
echo "  3. restore_configs.py     - 恢复配置数据"
echo "  4. verify_integrity.py    - 验证数据完整性"
echo "  5. reinit_database.py     - 主控脚本（推荐）"
echo ""
echo "辅助文件:"
echo "  - run_reinit.sh           - Shell 快速启动脚本"
echo "  - README.md               - 详细使用文档"
echo "  - CHECKLIST.md            - 执行检查清单"
echo "  - SUMMARY.md              - 总结文档"
echo ""
echo "========================================"
echo "执行方法"
echo "========================================"
echo ""
echo "方法 1: 使用主控脚本（推荐）"
echo "  cd backend"
echo "  python scripts/reinit_database.py"
echo ""
echo "方法 2: 使用 Shell 脚本"
echo "  cd backend/scripts"
echo "  chmod +x run_reinit.sh"
echo "  ./run_reinit.sh"
echo ""
echo "方法 3: 手动执行各步骤"
echo "  cd backend"
echo "  python scripts/backup_configs.py"
echo "  python scripts/drop_old_tables.py"
echo "  python database/init_dolphindb.py"
echo "  python scripts/restore_configs.py"
echo "  python scripts/verify_integrity.py"
echo ""
echo "========================================"
echo "执行前检查"
echo "========================================"
echo ""

# 检查 DolphinDB
echo -n "检查 DolphinDB 状态... "
if docker ps | grep -q dolphindb; then
    echo "✓ 运行中"
else
    echo "✗ 未运行"
    echo ""
    echo "请先启动 DolphinDB:"
    echo "  docker-compose up -d"
    exit 1
fi

# 检查虚拟环境
echo -n "检查 Python 虚拟环境... "
if [ -d "../.venv" ]; then
    echo "✓ 存在"
else
    echo "✗ 不存在"
    echo ""
    echo "请先创建虚拟环境:"
    echo "  cd backend"
    echo "  python -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install -r requirements.txt"
    exit 1
fi

# 检查备份目录
echo -n "检查备份目录... "
if [ -d "../backups" ]; then
    echo "✓ 存在"
else
    echo "○ 不存在（将自动创建）"
fi

echo ""
echo "========================================"
echo "准备就绪！"
echo "========================================"
echo ""
echo "现在可以执行重新初始化脚本了。"
echo ""
echo "推荐命令:"
echo "  cd .."
echo "  python scripts/reinit_database.py"
echo ""
echo "详细文档:"
echo "  - README.md      - 使用说明"
echo "  - CHECKLIST.md   - 检查清单"
echo "  - SUMMARY.md     - 总结文档"
echo ""
