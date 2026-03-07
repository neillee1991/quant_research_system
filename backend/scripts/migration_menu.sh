#!/bin/bash
# 因子迁移快速启动脚本

set -e

BACKEND_DIR="/Users/lisheng/Code/quantsystem/quant_research_system/backend"
cd "$BACKEND_DIR"

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}因子迁移工具 - 快速启动${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 显示菜单
show_menu() {
    echo "请选择操作："
    echo ""
    echo "  1) 列出所有因子"
    echo "  2) 分析因子"
    echo "  3) 迁移因子"
    echo "  4) 验证迁移"
    echo "  5) 生成迁移报告"
    echo "  6) 运行测试"
    echo "  7) 查看文档"
    echo "  0) 退出"
    echo ""
}

# 列出所有因子
list_factors() {
    echo -e "${YELLOW}正在列出所有因子...${NC}"
    python scripts/migrate_factor.py --list
}

# 分析因子
analyze_factor() {
    echo -e "${YELLOW}请输入因子ID:${NC}"
    read factor_id
    echo -e "${YELLOW}正在分析因子: $factor_id${NC}"
    python scripts/migrate_factor.py --factor-id "$factor_id" --analyze
}

# 迁移因子
migrate_factor() {
    echo -e "${YELLOW}请输入因子ID (多个用逗号分隔):${NC}"
    read factor_ids
    echo -e "${YELLOW}正在迁移因子: $factor_ids${NC}"

    if [[ $factor_ids == *","* ]]; then
        python scripts/migrate_factor.py --batch "$factor_ids" --migrate
    else
        python scripts/migrate_factor.py --factor-id "$factor_ids" --migrate
    fi

    echo -e "${GREEN}✓ 迁移完成！${NC}"
    echo -e "代码已生成到: factors_v2/"
}

# 验证迁移
verify_migration() {
    echo -e "${YELLOW}请输入因子ID:${NC}"
    read factor_id
    echo -e "${YELLOW}请输入测试日期 (YYYY-MM-DD, 留空使用最新日期):${NC}"
    read test_date

    if [ -z "$test_date" ]; then
        echo -e "${YELLOW}正在验证因子: $factor_id (最新日期)${NC}"
        python scripts/verify_migration.py --factor-id "$factor_id" --report
    else
        echo -e "${YELLOW}正在验证因子: $factor_id (日期: $test_date)${NC}"
        python scripts/verify_migration.py --factor-id "$factor_id" --date "$test_date" --report
    fi
}

# 生成迁移报告
generate_report() {
    echo -e "${YELLOW}正在生成迁移报告...${NC}"
    python scripts/migrate_factor.py --report
    echo -e "${GREEN}✓ 报告已生成: docs/migration_report.md${NC}"
}

# 运行测试
run_tests() {
    echo "请选择测试类型："
    echo "  1) 单元测试（无需数据库）"
    echo "  2) 集成测试（需要数据库）"
    echo "  3) 所有测试"
    read test_type

    case $test_type in
        1)
            echo -e "${YELLOW}运行单元测试...${NC}"
            pytest factors_v2/ -v -m "not integration"
            ;;
        2)
            echo -e "${YELLOW}运行集成测试...${NC}"
            pytest factors_v2/ -v -m integration
            ;;
        3)
            echo -e "${YELLOW}运行所有测试...${NC}"
            pytest factors_v2/ -v
            ;;
        *)
            echo -e "${RED}无效选择${NC}"
            ;;
    esac
}

# 查看文档
view_docs() {
    echo "请选择文档："
    echo "  1) 迁移计划"
    echo "  2) 迁移指南"
    echo "  3) 第一批迁移报告"
    read doc_choice

    case $doc_choice in
        1)
            less docs/FACTOR_MIGRATION_PLAN.md
            ;;
        2)
            less docs/FACTOR_MIGRATION_GUIDE.md
            ;;
        3)
            less docs/MIGRATION_REPORT_BATCH1.md
            ;;
        *)
            echo -e "${RED}无效选择${NC}"
            ;;
    esac
}

# 主循环
while true; do
    show_menu
    read -p "请输入选项 [0-7]: " choice
    echo ""

    case $choice in
        1)
            list_factors
            ;;
        2)
            analyze_factor
            ;;
        3)
            migrate_factor
            ;;
        4)
            verify_migration
            ;;
        5)
            generate_report
            ;;
        6)
            run_tests
            ;;
        7)
            view_docs
            ;;
        0)
            echo -e "${GREEN}再见！${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}无效选项，请重新选择${NC}"
            ;;
    esac

    echo ""
    echo -e "${YELLOW}按回车键继续...${NC}"
    read
    clear
done
