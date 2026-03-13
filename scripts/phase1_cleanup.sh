#!/bin/bash
# 阶段1执行脚本: 安全修复 + 清理废弃代码
# 执行前请仔细审查此脚本

set -e  # 遇到错误立即退出

echo "=========================================="
echo "阶段1: 安全修复 + 清理废弃代码"
echo "=========================================="
echo ""

# 切换到backend目录
cd /Users/lisheng/Code/quantsystem/quant_research_system/backend

echo "1. 备份当前状态..."
git status
echo ""
echo "请确认当前工作目录干净,或已提交所有更改"
echo "按 Enter 继续,或 Ctrl+C 取消"
read

echo ""
echo "2. 删除废弃文件..."
echo ""

# 2.1 删除 DolphinDB 客户端废弃文件
echo "删除: store/dolphindb_client_new.py"
rm -f store/dolphindb_client_new.py

echo "删除: store/dolphindb/seed_data.py.backup"
rm -f store/dolphindb/seed_data.py.backup

echo "删除: store/dolphindb/seed_data.py.backup2"
rm -f store/dolphindb/seed_data.py.backup2

# 2.2 删除 API 路由废弃文件
echo "删除: app/api/v1/production.py (1486行)"
rm -f app/api/v1/production.py

echo "删除: app/api/v1/data_merged.py (1613行)"
rm -f app/api/v1/data_merged.py

# 2.3 删除临时脚本
echo "删除: check_sync_logs.py"
rm -f check_sync_logs.py

echo "删除: analyze_refactor.py"
rm -f analyze_refactor.py

echo ""
echo "3. 移动临时脚本到合适位置..."
echo ""

# 3.1 移动 health_check.py
if [ -f health_check.py ]; then
    echo "移动: health_check.py -> scripts/health_check.py"
    mv health_check.py scripts/health_check.py
fi

# 3.2 移动 init_meta_tables.py
if [ -f init_meta_tables.py ]; then
    echo "移动: init_meta_tables.py -> database/init_meta_tables.py"
    mv init_meta_tables.py database/init_meta_tables.py
fi

echo ""
echo "4. 归档备份目录..."
echo ""

# 4.1 归档 backups/cleanup-2026-03-08/
if [ -d backups/cleanup-2026-03-08 ]; then
    echo "归档: backups/cleanup-2026-03-08/ -> ../backups-2026-03-08.tar.gz"
    tar -czf ../backups-2026-03-08.tar.gz backups/cleanup-2026-03-08/
    echo "删除: backups/cleanup-2026-03-08/"
    rm -rf backups/cleanup-2026-03-08/
fi

echo ""
echo "5. 清理编译缓存..."
echo ""

# 5.1 清理 .pyc 文件
echo "清理 .pyc 文件..."
find . -type f -name "*.pyc" ! -path "*/.venv/*" -delete

# 5.2 清理 __pycache__ 目录
echo "清理 __pycache__ 目录..."
find . -type d -name "__pycache__" ! -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true

echo ""
echo "6. 更新 .gitignore..."
echo ""

# 确保 .gitignore 包含必要的规则
if ! grep -q "__pycache__" .gitignore 2>/dev/null; then
    echo "添加 __pycache__/ 到 .gitignore"
    echo "__pycache__/" >> .gitignore
fi

if ! grep -q "*.py\[cod\]" .gitignore 2>/dev/null; then
    echo "添加 *.py[cod] 到 .gitignore"
    echo "*.py[cod]" >> .gitignore
fi

if ! grep -q "backups/" .gitignore 2>/dev/null; then
    echo "添加 backups/ 到 .gitignore"
    echo "backups/" >> .gitignore
fi

echo ""
echo "=========================================="
echo "阶段1 清理完成!"
echo "=========================================="
echo ""
echo "统计信息:"
echo "- 删除的文件数: $(git status --short | grep -c "^ D" || echo 0)"
echo "- 移动的文件数: 2"
echo "- 归档的目录: 1"
echo ""
echo "下一步:"
echo "1. 运行测试: pytest tests/"
echo "2. 检查 git status"
echo "3. 如果一切正常,提交更改: git add . && git commit -m 'chore: 清理废弃代码和临时文件'"
echo ""
