#!/bin/bash
# 服务状态检查脚本

echo "========================================="
echo "量化研究系统 - 服务状态检查"
echo "========================================="
echo ""

# 检查后端
echo "1. 后端服务状态"
echo "-----------------------------------------"
if lsof -ti:8000 > /dev/null 2>&1; then
    echo "✅ 后端正在运行 (端口 8000)"
    echo "   进程: $(lsof -ti:8000 | head -1)"

    # 测试 API
    if curl -s http://localhost:8000/api/v1/data/stocks > /dev/null 2>&1; then
        echo "✅ API 响应正常"
    else
        echo "❌ API 无响应"
    fi
else
    echo "❌ 后端未运行"
    echo "   启动命令: cd backend && source .venv/bin/activate && uvicorn app.main:app --reload"
fi
echo ""

# 检查前端
echo "2. 前端服务状态"
echo "-----------------------------------------"
if lsof -ti:3000 > /dev/null 2>&1; then
    echo "✅ 前端正在运行 (端口 3000)"
    echo "   进程: $(lsof -ti:3000 | head -1)"

    # 测试前端
    if curl -s http://localhost:3000 > /dev/null 2>&1; then
        echo "✅ 前端页面可访问"
    else
        echo "❌ 前端页面无响应"
    fi
else
    echo "❌ 前端未运行"
    echo "   启动命令: cd frontend && npm start"
fi
echo ""

# 检查数据库
echo "3. 数据库状态"
echo "-----------------------------------------"
DB_PATH="/Users/bytedance/Claude/quant_research_system/data/quant.duckdb"
if [ -f "$DB_PATH" ]; then
    DB_SIZE=$(du -h "$DB_PATH" | cut -f1)
    echo "✅ 数据库文件存在"
    echo "   路径: $DB_PATH"
    echo "   大小: $DB_SIZE"
else
    echo "❌ 数据库文件不存在"
fi
echo ""

# 检查日志
echo "4. 最近的错误日志"
echo "-----------------------------------------"
if [ -f /tmp/backend.log ]; then
    echo "后端日志 (最后 5 行):"
    tail -5 /tmp/backend.log | grep -E "(ERROR|WARNING|Proxy error)" || echo "  无错误"
fi
if [ -f /tmp/frontend.log ]; then
    echo "前端日志 (最后 5 行):"
    tail -5 /tmp/frontend.log | grep -E "(ERROR|WARNING|Proxy error)" || echo "  无错误"
fi
echo ""

# 访问地址
echo "5. 访问地址"
echo "-----------------------------------------"
echo "📖 API 文档: http://localhost:8000/docs"
echo "🌐 前端应用: http://localhost:3000"
echo ""

echo "========================================="
echo "检查完成"
echo "========================================="
