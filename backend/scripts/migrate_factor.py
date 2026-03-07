#!/usr/bin/env python3
"""
因子迁移工具 - 从旧架构迁移到新架构

功能：
1. 提取因子定义
2. 生成新架构代码
3. 创建测试用例
4. 生成迁移报告
"""
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional
import argparse
from datetime import datetime

# 添加项目根目录到路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from app.core.logger import logger
from engine.production.registry import get_factor, discover_factors, FactorDefinition
from store.dolphindb_client import DolphinDBClient


class FactorMigrator:
    """因子迁移器"""

    def __init__(self, db_client):
        self.db = db_client
        discover_factors(db_client=db_client)

    def analyze_factor(self, factor_id: str) -> Optional[Dict[str, Any]]:
        """分析因子定义"""
        definition = get_factor(factor_id)
        if not definition:
            logger.error(f"Factor not found: {factor_id}")
            return None

        analysis = {
            "factor_id": factor_id,
            "description": definition.description,
            "category": definition.category,
            "compute_mode": definition.compute_mode,
            "depends_on": definition.depends_on,
            "params": definition.params,
            "storage_target": definition.storage.target,
            "complexity": self._assess_complexity(definition),
            "dependencies": self._analyze_dependencies(definition),
        }

        return analysis

    def _assess_complexity(self, definition: FactorDefinition) -> str:
        """评估因子复杂度"""
        score = 0

        # 依赖数量
        score += len(definition.depends_on)

        # 参数数量
        score += len(definition.params)

        # 是否有自定义存储
        if definition.storage.target != "factor_values":
            score += 2

        # 是否依赖其他因子
        for dep in definition.depends_on:
            if dep.startswith("factor_"):
                score += 3

        if score <= 2:
            return "simple"
        elif score <= 5:
            return "medium"
        else:
            return "complex"

    def _analyze_dependencies(self, definition: FactorDefinition) -> Dict[str, list]:
        """分析依赖关系"""
        deps = {
            "data_tables": [],
            "factors": [],
            "external": []
        }

        for dep in definition.depends_on:
            if dep.startswith("factor_"):
                deps["factors"].append(dep)
            elif dep.startswith("sync_"):
                deps["data_tables"].append(dep)
            else:
                deps["external"].append(dep)

        return deps

    def generate_migration_code(self, factor_id: str, output_dir: str = "factors_v2") -> bool:
        """生成迁移后的代码"""
        definition = get_factor(factor_id)
        if not definition:
            return False

        output_path = Path(backend_dir) / output_dir
        output_path.mkdir(parents=True, exist_ok=True)

        # 生成因子文件
        factor_file = output_path / f"{factor_id}.py"
        code = self._generate_factor_code(definition)

        with open(factor_file, "w", encoding="utf-8") as f:
            f.write(code)

        logger.info(f"Generated factor code: {factor_file}")

        # 生成测试文件
        test_file = output_path / f"test_{factor_id}.py"
        test_code = self._generate_test_code(definition)

        with open(test_file, "w", encoding="utf-8") as f:
            f.write(test_code)

        logger.info(f"Generated test code: {test_file}")

        return True

    def _generate_factor_code(self, definition: FactorDefinition) -> str:
        """生成因子代码"""
        template = f'''"""
{definition.description}

迁移自旧架构: {definition.factor_id}
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
import polars as pl
from engine.production.registry import factor


@factor(
    factor_id="{definition.factor_id}",
    description="{definition.description}",
    depends_on={definition.depends_on},
    category="{definition.category}",
    params={definition.params},
    compute_mode="{definition.compute_mode}",
)
def compute_{definition.factor_id.replace("factor_", "")}(df: pl.DataFrame, params: dict) -> pl.DataFrame:
    """
    因子计算函数

    Args:
        df: 输入数据，包含 ts_code, trade_date 及依赖字段
        params: 因子参数

    Returns:
        包含 ts_code, trade_date, factor_value 的 DataFrame
    """
    # TODO: 实现因子计算逻辑
    # 原函数: {definition.func.__name__}

    # 示例：调用原函数
    result = definition.func(df, params)

    return result
'''
        return template

    def _generate_test_code(self, definition: FactorDefinition) -> str:
        """生成测试代码"""
        template = f'''"""
测试 {definition.factor_id}

验证迁移后的因子计算结果与旧架构一致
"""
import pytest
import polars as pl
from datetime import datetime
from engine.production.engine import ProductionEngine
from services.factor_compute_service import FactorComputeService
from store.dolphindb_client import DolphinDBClient


class Test{definition.factor_id.replace("_", "").title()}:
    """测试 {definition.factor_id}"""

    @pytest.fixture
    def db_client(self):
        """数据库客户端"""
        return DolphinDBClient.get_instance()

    @pytest.fixture
    def old_engine(self, db_client):
        """旧架构引擎"""
        return ProductionEngine(db_client)

    @pytest.fixture
    def new_service(self, db_client):
        """新架构服务"""
        return FactorComputeService(db_client)

    def test_result_consistency(self, old_engine, new_service):
        """测试结果一致性"""
        factor_id = "{definition.factor_id}"
        test_date = "2024-01-15"

        # 旧架构计算
        old_success = old_engine.run_task(
            factor_id=factor_id,
            target_date=test_date,
            mode="incremental"
        )
        assert old_success, "Old engine computation failed"

        # 新架构计算
        new_result = new_service.compute_factor(
            factor_id=factor_id,
            target_date=test_date,
            mode="incremental",
            save_results=False
        )
        assert new_result.success, f"New service computation failed: {{new_result.message}}"

        # 对比结果
        # TODO: 从数据库加载结果并对比

    def test_performance(self, new_service):
        """测试性能"""
        import time

        factor_id = "{definition.factor_id}"
        test_date = "2024-01-15"

        start = time.time()
        result = new_service.compute_factor(
            factor_id=factor_id,
            target_date=test_date,
            save_results=False
        )
        elapsed = time.time() - start

        assert result.success
        assert elapsed < 10.0, f"Computation too slow: {{elapsed:.2f}}s"

    def test_data_quality(self, new_service):
        """测试数据质量"""
        factor_id = "{definition.factor_id}"
        test_date = "2024-01-15"

        result = new_service.compute_factor(
            factor_id=factor_id,
            target_date=test_date,
            save_results=False
        )

        assert result.success
        assert result.rows > 0, "No data computed"

        # TODO: 检查质量指标
'''
        return template

    def create_migration_report(self, factor_ids: list, output_file: str = "migration_report.md"):
        """创建迁移报告"""
        report_path = Path(backend_dir) / "docs" / output_file

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(f"# 因子迁移报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"## 迁移概览\n\n")
            f.write(f"- 因子总数: {len(factor_ids)}\n")

            # 按复杂度分类
            complexity_count = {"simple": 0, "medium": 0, "complex": 0}

            f.write(f"\n## 因子详情\n\n")
            f.write("| 因子ID | 描述 | 类别 | 复杂度 | 依赖 | 状态 |\n")
            f.write("|--------|------|------|--------|------|------|\n")

            for factor_id in factor_ids:
                analysis = self.analyze_factor(factor_id)
                if analysis:
                    complexity_count[analysis["complexity"]] += 1
                    deps = ", ".join(analysis["depends_on"][:3])
                    if len(analysis["depends_on"]) > 3:
                        deps += "..."

                    f.write(f"| {factor_id} | {analysis['description']} | "
                           f"{analysis['category']} | {analysis['complexity']} | "
                           f"{deps} | 待迁移 |\n")

            f.write(f"\n## 复杂度分布\n\n")
            f.write(f"- 简单: {complexity_count['simple']}\n")
            f.write(f"- 中等: {complexity_count['medium']}\n")
            f.write(f"- 复杂: {complexity_count['complex']}\n")

        logger.info(f"Migration report created: {report_path}")
        return str(report_path)


def main():
    parser = argparse.ArgumentParser(description="因子迁移工具")
    parser.add_argument("--factor-id", help="因子ID")
    parser.add_argument("--list", action="store_true", help="列出所有因子")
    parser.add_argument("--analyze", action="store_true", help="分析因子")
    parser.add_argument("--migrate", action="store_true", help="执行迁移")
    parser.add_argument("--output", default="factors_v2", help="输出目录")
    parser.add_argument("--report", action="store_true", help="生成迁移报告")
    parser.add_argument("--batch", help="批量迁移（逗号分隔的因子ID）")

    args = parser.parse_args()

    # 初始化数据库连接
    db = DolphinDBClient.get_instance()
    migrator = FactorMigrator(db)

    if args.list:
        # 列出所有因子
        from engine.production.registry import list_factors
        factors = list_factors()
        print(f"\n找到 {len(factors)} 个因子:\n")
        for f in factors:
            print(f"  - {f['factor_id']}: {f['description']} ({f['category']})")

    elif args.analyze and args.factor_id:
        # 分析单个因子
        analysis = migrator.analyze_factor(args.factor_id)
        if analysis:
            print(f"\n因子分析: {args.factor_id}\n")
            print(f"  描述: {analysis['description']}")
            print(f"  类别: {analysis['category']}")
            print(f"  复杂度: {analysis['complexity']}")
            print(f"  计算模式: {analysis['compute_mode']}")
            print(f"  依赖: {', '.join(analysis['depends_on'])}")
            print(f"  参数: {analysis['params']}")
            print(f"  存储: {analysis['storage_target']}")

    elif args.migrate and args.factor_id:
        # 迁移单个因子
        print(f"\n开始迁移因子: {args.factor_id}")
        success = migrator.generate_migration_code(args.factor_id, args.output)
        if success:
            print(f"✓ 迁移成功，代码已生成到: {args.output}/")
        else:
            print(f"✗ 迁移失败")

    elif args.batch:
        # 批量迁移
        factor_ids = [fid.strip() for fid in args.batch.split(",")]
        print(f"\n批量迁移 {len(factor_ids)} 个因子...")

        success_count = 0
        for factor_id in factor_ids:
            if migrator.generate_migration_code(factor_id, args.output):
                success_count += 1
                print(f"  ✓ {factor_id}")
            else:
                print(f"  ✗ {factor_id}")

        print(f"\n完成: {success_count}/{len(factor_ids)} 个因子迁移成功")

    elif args.report:
        # 生成迁移报告
        from engine.production.registry import list_factors
        factors = list_factors()
        factor_ids = [f["factor_id"] for f in factors]

        report_path = migrator.create_migration_report(factor_ids)
        print(f"\n迁移报告已生成: {report_path}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
