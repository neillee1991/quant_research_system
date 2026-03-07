#!/usr/bin/env python3
"""
因子迁移验证工具

功能：
1. 对比新旧架构计算结果
2. 生成差异报告
3. 统计一致性指标
4. 可视化对比
"""
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import argparse
from datetime import datetime
import polars as pl
import numpy as np

# 添加项目根目录到路径
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from app.core.logger import logger
from engine.production.engine import ProductionEngine
from services.factor_compute_service import FactorComputeService
from store.dolphindb_client import DolphinDBClient


class MigrationVerifier:
    """迁移验证器"""

    def __init__(self, db_client):
        self.db = db_client
        self.old_engine = ProductionEngine(db_client)
        self.new_service = FactorComputeService(db_client)

    def verify_factor(
        self,
        factor_id: str,
        target_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        tolerance: float = 1e-10
    ) -> Dict[str, Any]:
        """验证因子迁移结果

        Args:
            factor_id: 因子ID
            target_date: 目标日期
            start_date: 开始日期
            end_date: 结束日期
            tolerance: 容差阈值

        Returns:
            验证结果字典
        """
        logger.info(f"开始验证因子: {factor_id}")

        result = {
            "factor_id": factor_id,
            "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "success": False,
            "old_result": None,
            "new_result": None,
            "comparison": None,
            "message": ""
        }

        try:
            # 1. 旧架构计算
            logger.info("执行旧架构计算...")
            old_success = self.old_engine.run_task(
                factor_id=factor_id,
                target_date=target_date,
                start_date=start_date,
                end_date=end_date,
                mode="incremental"
            )

            if not old_success:
                result["message"] = "旧架构计算失败"
                return result

            # 加载旧架构结果
            old_data = self._load_factor_data(factor_id, target_date, start_date, end_date)
            result["old_result"] = {
                "rows": len(old_data) if old_data is not None else 0,
                "dates": self._get_date_range(old_data) if old_data is not None else []
            }

            # 2. 新架构计算
            logger.info("执行新架构计算...")
            new_result = self.new_service.compute_factor(
                factor_id=factor_id,
                target_date=target_date,
                start_date=start_date,
                end_date=end_date,
                mode="incremental",
                save_results=True
            )

            if not new_result.success:
                result["message"] = f"新架构计算失败: {new_result.message}"
                return result

            # 加载新架构结果
            new_data = self._load_factor_data(factor_id, target_date, start_date, end_date)
            result["new_result"] = {
                "rows": len(new_data) if new_data is not None else 0,
                "dates": self._get_date_range(new_data) if new_data is not None else [],
                "elapsed": new_result.elapsed_seconds
            }

            # 3. 对比结果
            if old_data is None or new_data is None:
                result["message"] = "无法加载计算结果"
                return result

            comparison = self._compare_results(old_data, new_data, tolerance)
            result["comparison"] = comparison
            result["success"] = comparison["is_consistent"]
            result["message"] = comparison["summary"]

            logger.info(f"验证完成: {result['message']}")

        except Exception as e:
            logger.error(f"验证失败: {e}", exc_info=True)
            result["message"] = f"验证异常: {str(e)}"

        return result

    def _load_factor_data(
        self,
        factor_id: str,
        target_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[pl.DataFrame]:
        """从数据库加载因子数据"""
        try:
            # 构建查询条件
            conditions = [f"factor_id = '{factor_id}'"]

            if target_date:
                conditions.append(f"trade_date = '{target_date}'")
            elif start_date and end_date:
                conditions.append(f"trade_date >= '{start_date}'")
                conditions.append(f"trade_date <= '{end_date}'")
            elif start_date:
                conditions.append(f"trade_date >= '{start_date}'")

            where_clause = " AND ".join(conditions)

            query = f"""
                SELECT ts_code, trade_date, factor_value, quality_flag
                FROM factor_values
                WHERE {where_clause}
                ORDER BY trade_date, ts_code
            """

            df = self.db.query(query)
            return df if df is not None and not df.is_empty() else None

        except Exception as e:
            logger.error(f"加载因子数据失败: {e}")
            return None

    def _get_date_range(self, df: pl.DataFrame) -> list:
        """获取日期范围"""
        if df is None or df.is_empty():
            return []
        dates = df["trade_date"].unique().sort()
        return [dates[0], dates[-1]]

    def _compare_results(
        self,
        old_data: pl.DataFrame,
        new_data: pl.DataFrame,
        tolerance: float
    ) -> Dict[str, Any]:
        """对比新旧结果

        Returns:
            对比结果字典
        """
        comparison = {
            "is_consistent": False,
            "summary": "",
            "row_count_match": False,
            "value_match": False,
            "statistics": {}
        }

        # 1. 行数对比
        old_rows = len(old_data)
        new_rows = len(new_data)
        comparison["row_count_match"] = (old_rows == new_rows)

        if not comparison["row_count_match"]:
            comparison["summary"] = f"行数不一致: 旧={old_rows}, 新={new_rows}"
            return comparison

        # 2. 合并数据进行对比
        merged = old_data.join(
            new_data,
            on=["ts_code", "trade_date"],
            how="inner",
            suffix="_new"
        )

        if merged.is_empty():
            comparison["summary"] = "无法匹配数据"
            return comparison

        # 3. 计算差异
        old_values = merged["factor_value"].to_numpy()
        new_values = merged["factor_value_new"].to_numpy()

        # 过滤 null 值
        valid_mask = ~(np.isnan(old_values) | np.isnan(new_values))
        old_valid = old_values[valid_mask]
        new_valid = new_values[valid_mask]

        if len(old_valid) == 0:
            comparison["summary"] = "所有值均为 null"
            return comparison

        # 计算统计指标
        abs_diff = np.abs(old_valid - new_valid)
        rel_diff = abs_diff / (np.abs(old_valid) + 1e-10)

        stats = {
            "total_rows": len(merged),
            "valid_rows": len(old_valid),
            "null_rows": len(old_values) - len(old_valid),
            "max_abs_diff": float(np.max(abs_diff)),
            "mean_abs_diff": float(np.mean(abs_diff)),
            "max_rel_diff": float(np.max(rel_diff)),
            "mean_rel_diff": float(np.mean(rel_diff)),
            "identical_count": int(np.sum(abs_diff < tolerance)),
            "identical_rate": float(np.sum(abs_diff < tolerance) / len(old_valid))
        }

        comparison["statistics"] = stats

        # 4. 判断一致性
        comparison["value_match"] = (stats["max_abs_diff"] < tolerance)
        comparison["is_consistent"] = comparison["row_count_match"] and comparison["value_match"]

        if comparison["is_consistent"]:
            comparison["summary"] = f"✓ 结果一致 (误差 < {tolerance})"
        else:
            comparison["summary"] = (
                f"✗ 结果不一致: 最大误差={stats['max_abs_diff']:.2e}, "
                f"平均误差={stats['mean_abs_diff']:.2e}, "
                f"一致率={stats['identical_rate']:.2%}"
            )

        return comparison

    def generate_report(
        self,
        verification_results: list,
        output_file: str = "verification_report.md"
    ):
        """生成验证报告"""
        report_path = Path(backend_dir) / "docs" / output_file

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("# 因子迁移验证报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # 统计概览
            total = len(verification_results)
            success = sum(1 for r in verification_results if r["success"])

            f.write("## 验证概览\n\n")
            f.write(f"- 验证因子数: {total}\n")
            f.write(f"- 通过: {success}\n")
            f.write(f"- 失败: {total - success}\n")
            f.write(f"- 通过率: {success/total*100:.1f}%\n\n")

            # 详细结果
            f.write("## 验证详情\n\n")
            f.write("| 因子ID | 状态 | 旧架构行数 | 新架构行数 | 最大误差 | 平均误差 | 一致率 |\n")
            f.write("|--------|------|-----------|-----------|---------|---------|--------|\n")

            for result in verification_results:
                status = "✓" if result["success"] else "✗"
                old_rows = result.get("old_result", {}).get("rows", 0)
                new_rows = result.get("new_result", {}).get("rows", 0)

                comp = result.get("comparison", {})
                stats = comp.get("statistics", {})

                max_diff = stats.get("max_abs_diff", 0)
                mean_diff = stats.get("mean_abs_diff", 0)
                identical_rate = stats.get("identical_rate", 0)

                f.write(f"| {result['factor_id']} | {status} | {old_rows} | {new_rows} | "
                       f"{max_diff:.2e} | {mean_diff:.2e} | {identical_rate:.2%} |\n")

            # 失败详情
            failed = [r for r in verification_results if not r["success"]]
            if failed:
                f.write("\n## 失败详情\n\n")
                for result in failed:
                    f.write(f"### {result['factor_id']}\n\n")
                    f.write(f"- 错误信息: {result['message']}\n")
                    if result.get("comparison"):
                        f.write(f"- 对比结果: {result['comparison']['summary']}\n")
                    f.write("\n")

        logger.info(f"验证报告已生成: {report_path}")
        return str(report_path)


def main():
    parser = argparse.ArgumentParser(description="因子迁移验证工具")
    parser.add_argument("--factor-id", required=True, help="因子ID")
    parser.add_argument("--date", help="目标日期 (YYYY-MM-DD)")
    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--tolerance", type=float, default=1e-10, help="容差阈值")
    parser.add_argument("--report", action="store_true", help="生成报告")

    args = parser.parse_args()

    # 初始化
    db = DolphinDBClient.get_instance()
    verifier = MigrationVerifier(db)

    # 执行验证
    result = verifier.verify_factor(
        factor_id=args.factor_id,
        target_date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
        tolerance=args.tolerance
    )

    # 打印结果
    print(f"\n{'='*60}")
    print(f"因子验证结果: {args.factor_id}")
    print(f"{'='*60}\n")
    print(f"状态: {'✓ 通过' if result['success'] else '✗ 失败'}")
    print(f"消息: {result['message']}\n")

    if result.get("old_result"):
        print(f"旧架构:")
        print(f"  - 行数: {result['old_result']['rows']}")
        print(f"  - 日期范围: {result['old_result']['dates']}\n")

    if result.get("new_result"):
        print(f"新架构:")
        print(f"  - 行数: {result['new_result']['rows']}")
        print(f"  - 日期范围: {result['new_result']['dates']}")
        print(f"  - 耗时: {result['new_result']['elapsed']:.2f}s\n")

    if result.get("comparison", {}).get("statistics"):
        stats = result["comparison"]["statistics"]
        print(f"对比统计:")
        print(f"  - 总行数: {stats['total_rows']}")
        print(f"  - 有效行数: {stats['valid_rows']}")
        print(f"  - 最大误差: {stats['max_abs_diff']:.2e}")
        print(f"  - 平均误差: {stats['mean_abs_diff']:.2e}")
        print(f"  - 一致率: {stats['identical_rate']:.2%}\n")

    # 生成报告
    if args.report:
        report_path = verifier.generate_report([result])
        print(f"报告已生成: {report_path}")


if __name__ == "__main__":
    main()
