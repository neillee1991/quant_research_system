#!/bin/bash
# 测试覆盖率报告生成脚本

set -e

echo "=========================================="
echo "运行测试并生成覆盖率报告"
echo "=========================================="

# 切换到 backend 目录
cd "$(dirname "$0")/.."

# 激活虚拟环境（如果存在）
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# 清理旧的覆盖率数据
echo "清理旧的覆盖率数据..."
rm -rf .coverage htmlcov coverage.json

# 运行测试并生成覆盖率报告
echo ""
echo "运行测试套件..."
pytest tests/ \
    --cov=infrastructure \
    --cov=services \
    --cov=config \
    --cov-report=html \
    --cov-report=term-missing \
    --cov-report=json \
    -v \
    --tb=short

# 显示覆盖率摘要
echo ""
echo "=========================================="
echo "覆盖率报告已生成"
echo "=========================================="
echo "HTML 报告: htmlcov/index.html"
echo "JSON 报告: coverage.json"
echo ""

# 检查覆盖率是否达标
echo "检查覆盖率目标..."
python3 << 'EOF'
import json
import sys

try:
    with open('coverage.json', 'r') as f:
        data = json.load(f)

    total_coverage = data['totals']['percent_covered']

    print(f"\n总体覆盖率: {total_coverage:.2f}%")

    if total_coverage >= 80:
        print("✅ 覆盖率达标 (>= 80%)")
        sys.exit(0)
    else:
        print(f"❌ 覆盖率未达标 (目标: 80%, 当前: {total_coverage:.2f}%)")
        sys.exit(1)
except FileNotFoundError:
    print("⚠️  未找到覆盖率报告文件")
    sys.exit(1)
except Exception as e:
    print(f"⚠️  检查覆盖率时出错: {e}")
    sys.exit(1)
EOF

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "🎉 所有测试通过，覆盖率达标！"
else
    echo ""
    echo "⚠️  请查看覆盖率报告并补充测试用例"
    echo "打开 htmlcov/index.html 查看详细报告"
fi

exit $exit_code
