#!/bin/bash

# 量化研究系统启动脚本

set -e

echo "=========================================="
echo "  量化研究系统 - PostgreSQL 版本"
echo "=========================================="
echo ""

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker 未运行，请先启动 Docker"
    exit 1
fi

echo "✓ Docker 已运行"

# 启动 PostgreSQL 数据库
echo ""
echo "📦 启动 PostgreSQL 数据库..."
docker-compose up -d

# 等待数据库就绪
echo ""
echo "⏳ 等待数据库初始化..."
sleep 5

# 检查数据库健康状态
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec quant_postgres pg_isready -U quant_user -d quant_research > /dev/null 2>&1; then
        echo "✓ 数据库已就绪"
        break
    fi
    attempt=$((attempt + 1))
    echo "  等待中... ($attempt/$max_attempts)"
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ 数据库启动超时"
    exit 1
fi

# 显示服务信息
echo ""
echo "=========================================="
echo "  服务已启动"
echo "=========================================="
echo ""
echo "📊 PostgreSQL:"
echo "   - 地址: localhost:5432"
echo "   - 数据库: quant_research"
echo "   - 用户: quant_user"
echo ""
echo "🔧 pgAdmin (Web 管理界面):"
echo "   - 地址: http://localhost:5050"
echo "   - 邮箱: admin@quant.com"
echo "   - 密码: admin123"
echo ""
echo "=========================================="
echo ""
echo "💡 下一步:"
echo "   1. 安装 Python 依赖: cd backend && pip install -r requirements.txt"
echo "   2. 启动后端服务: cd backend && python main.py"
echo "   3. 访问 API 文档: http://localhost:8000/docs"
echo ""
echo "📖 详细文档: POSTGRESQL_MIGRATION.md"
echo ""
