#!/bin/bash

# 量化研究系统停止脚本

echo "=========================================="
echo "  停止量化研究系统服务"
echo "=========================================="
echo ""

# 停止 Docker 容器
echo "🛑 停止 PostgreSQL 和 pgAdmin..."
docker-compose down

echo ""
echo "✓ 所有服务已停止"
echo ""
echo "💡 提示:"
echo "   - 重新启动: ./start.sh"
echo "   - 删除所有数据: docker-compose down -v"
echo ""
