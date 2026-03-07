#!/bin/bash
# 验证 data API 拆分结果

echo "=========================================="
echo "Data API 拆分验证"
echo "=========================================="
echo ""

# 检查文件是否存在
echo "1. 检查文件结构..."
files=(
    "app/api/v1/data/__init__.py"
    "app/api/v1/data/sync_api.py"
    "app/api/v1/data/config_api.py"
    "app/api/v1/data/etl_api.py"
    "app/api/v1/data/query_api.py"
    "app/api/v1/data/verify_routes.py"
    "app/api/v1/data/ENDPOINT_MAPPING.md"
    "app/api/v1/data/REFACTOR_REPORT.md"
)

all_exist=true
for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✅ $file"
    else
        echo "  ❌ $file (缺失)"
        all_exist=false
    fi
done

echo ""
echo "2. 统计代码行数..."
for file in app/api/v1/data/*.py; do
    if [ -f "$file" ]; then
        lines=$(wc -l < "$file")
        echo "  $(basename $file): $lines 行"
    fi
done

echo ""
echo "3. 统计端点数量..."
echo "  query_api.py: $(grep -c "^@router\." app/api/v1/data/query_api.py) 个端点"
echo "  sync_api.py: $(grep -c "^@router\." app/api/v1/data/sync_api.py) 个端点"
echo "  config_api.py: $(grep -c "^@router\." app/api/v1/data/config_api.py) 个端点"
echo "  etl_api.py: $(grep -c "^@router\." app/api/v1/data/etl_api.py) 个端点"

total_endpoints=$(($(grep -c "^@router\." app/api/v1/data/query_api.py) + $(grep -c "^@router\." app/api/v1/data/sync_api.py) + $(grep -c "^@router\." app/api/v1/data/config_api.py) + $(grep -c "^@router\." app/api/v1/data/etl_api.py)))
echo "  总计: $total_endpoints 个端点"

echo ""
echo "4. 检查主应用导入..."
if grep -q "from app.api.v1 import data" app/main.py; then
    echo "  ✅ main.py 已更新导入"
else
    echo "  ❌ main.py 导入未更新"
fi

echo ""
echo "=========================================="
if [ "$all_exist" = true ]; then
    echo "✅ 所有文件验证通过！"
    echo "=========================================="
    exit 0
else
    echo "❌ 部分文件缺失"
    echo "=========================================="
    exit 1
fi
