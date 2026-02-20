#!/usr/bin/env python3
"""
PostgreSQL 数据库管理工具
简单的命令行界面，用于查看和管理数据库
"""
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from store.postgres_client import db_client
from app.core.logger import logger


def print_separator(title=""):
    """打印分隔线"""
    if title:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}\n")
    else:
        print(f"{'='*60}\n")


def list_tables():
    """列出所有表及其大小"""
    try:
        df = db_client.query("""
            SELECT
                table_name,
                (SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_name = t.table_name) as columns,
                pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)

        print_separator("数据库表列表")
        if df.is_empty():
            print("没有找到任何表")
        else:
            print(f"{'表名':<20} {'列数':<10} {'大小':<15}")
            print("-" * 50)
            for row in df.iter_rows(named=True):
                print(f"{row['table_name']:<20} {row['columns']:<10} {row['size']:<15}")

            # 统计每个表的行数
            print("\n" + "-" * 50)
            print(f"{'表名':<20} {'行数':<15}")
            print("-" * 50)
            for row in df.iter_rows(named=True):
                table_name = row['table_name']
                try:
                    count_df = db_client.query(f"SELECT COUNT(*) as count FROM {table_name}")
                    count = count_df['count'][0]
                    print(f"{table_name:<20} {count:>15,}")
                except Exception as e:
                    print(f"{table_name:<20} {'错误':<15}")

    except Exception as e:
        logger.error(f"列出表失败: {e}")


def view_table(table_name: str, limit: int = 10):
    """查看表数据"""
    try:
        # 检查表是否存在
        if not db_client.table_exists(table_name):
            print(f"❌ 表 '{table_name}' 不存在")
            return

        # 获取表结构
        schema_df = db_client.query(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)

        print_separator(f"表: {table_name}")

        # 显示表结构
        print("表结构:")
        print(f"{'列名':<25} {'类型':<20} {'可空':<10}")
        print("-" * 60)
        for row in schema_df.iter_rows(named=True):
            nullable = "是" if row['is_nullable'] == 'YES' else "否"
            print(f"{row['column_name']:<25} {row['data_type']:<20} {nullable:<10}")

        # 获取行数
        count_df = db_client.query(f"SELECT COUNT(*) as count FROM {table_name}")
        total_rows = count_df['count'][0]
        print(f"\n总行数: {total_rows:,}")

        # 显示数据
        if total_rows > 0:
            df = db_client.query(f"SELECT * FROM {table_name} LIMIT {limit}")
            print(f"\n前 {min(limit, total_rows)} 行数据:")
            print(df)
        else:
            print("\n表为空")

    except Exception as e:
        logger.error(f"查看表失败: {e}")


def show_sync_status():
    """显示同步状态"""
    try:
        df = db_client.query("""
            SELECT
                data_type,
                last_date,
                updated_at
            FROM sync_log
            ORDER BY updated_at DESC
        """)

        print_separator("数据同步状态")
        if df.is_empty():
            print("没有同步记录")
        else:
            print(f"{'数据类型':<20} {'最后日期':<15} {'更新时间':<25}")
            print("-" * 65)
            for row in df.iter_rows(named=True):
                print(f"{row['data_type']:<20} {row['last_date']:<15} {str(row['updated_at']):<25}")

    except Exception as e:
        logger.error(f"查看同步状态失败: {e}")


def show_stock_stats():
    """显示股票统计信息"""
    try:
        print_separator("股票统计信息")

        # 总股票数
        stock_count = db_client.query("SELECT COUNT(*) as count FROM stock_basic")
        print(f"总股票数: {stock_count['count'][0]:,}")

        # 按市场分类
        market_df = db_client.query("""
            SELECT market, COUNT(*) as count
            FROM stock_basic
            GROUP BY market
            ORDER BY count DESC
        """)
        print("\n按市场分类:")
        for row in market_df.iter_rows(named=True):
            print(f"  {row['market']}: {row['count']:,}")

        # 按行业分类（前10）
        industry_df = db_client.query("""
            SELECT industry, COUNT(*) as count
            FROM stock_basic
            WHERE industry IS NOT NULL
            GROUP BY industry
            ORDER BY count DESC
            LIMIT 10
        """)
        print("\n按行业分类 (前10):")
        for row in industry_df.iter_rows(named=True):
            print(f"  {row['industry']}: {row['count']:,}")

    except Exception as e:
        logger.error(f"查看股票统计失败: {e}")


def execute_query(sql: str):
    """执行自定义 SQL 查询"""
    try:
        print_separator("查询结果")
        df = db_client.query(sql)
        print(df)
        print(f"\n返回 {len(df)} 行")
    except Exception as e:
        logger.error(f"查询失败: {e}")


def show_menu():
    """显示菜单"""
    print("\n" + "="*60)
    print("  PostgreSQL 数据库管理工具")
    print("="*60)
    print("\n选项:")
    print("  1. 列出所有表")
    print("  2. 查看表数据")
    print("  3. 查看同步状态")
    print("  4. 查看股票统计")
    print("  5. 执行自定义查询")
    print("  0. 退出")
    print()


def main():
    """主函数"""
    while True:
        show_menu()
        choice = input("请选择操作 (0-5): ").strip()

        if choice == "0":
            print("\n再见！")
            break
        elif choice == "1":
            list_tables()
        elif choice == "2":
            table_name = input("请输入表名: ").strip()
            limit = input("显示行数 (默认 10): ").strip()
            limit = int(limit) if limit else 10
            view_table(table_name, limit)
        elif choice == "3":
            show_sync_status()
        elif choice == "4":
            show_stock_stats()
        elif choice == "5":
            print("请输入 SQL 查询 (输入空行结束):")
            lines = []
            while True:
                line = input()
                if not line:
                    break
                lines.append(line)
            sql = " ".join(lines)
            if sql:
                execute_query(sql)
        else:
            print("❌ 无效选项，请重新选择")

        input("\n按 Enter 继续...")


if __name__ == "__main__":
    try:
        # 测试连接
        print("正在连接数据库...")
        db_client.query("SELECT 1")
        print("✓ 数据库连接成功\n")

        main()
    except KeyboardInterrupt:
        print("\n\n程序已退出")
    except Exception as e:
        logger.error(f"程序错误: {e}")
        sys.exit(1)
