"""
生成详细的测试覆盖率报告
"""
import json
import sys
from pathlib import Path


def load_coverage_data(coverage_file: str = "coverage.json") -> dict:
    """加载覆盖率数据"""
    try:
        with open(coverage_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ 未找到覆盖率文件: {coverage_file}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ 覆盖率文件格式错误: {coverage_file}")
        sys.exit(1)


def analyze_coverage(data: dict) -> dict:
    """分析覆盖率数据"""
    files = data.get('files', {})
    totals = data.get('totals', {})

    # 按模块分组
    modules = {
        'infrastructure': [],
        'services': [],
        'config': [],
        'other': []
    }

    for file_path, file_data in files.items():
        coverage = file_data['summary']['percent_covered']

        file_info = {
            'path': file_path,
            'coverage': coverage,
            'statements': file_data['summary']['num_statements'],
            'missing': file_data['summary']['missing_lines'],
            'excluded': file_data['summary']['excluded_lines']
        }

        # 分类
        if 'infrastructure' in file_path:
            modules['infrastructure'].append(file_info)
        elif 'services' in file_path:
            modules['services'].append(file_info)
        elif 'config' in file_path:
            modules['config'].append(file_info)
        else:
            modules['other'].append(file_info)

    return {
        'modules': modules,
        'totals': totals
    }


def print_report(analysis: dict):
    """打印覆盖率报告"""
    print("\n" + "=" * 80)
    print("测试覆盖率详细报告")
    print("=" * 80)

    totals = analysis['totals']
    total_coverage = totals['percent_covered']

    print(f"\n📊 总体覆盖率: {total_coverage:.2f}%")
    print(f"   总语句数: {totals['num_statements']}")
    print(f"   已覆盖: {totals['covered_lines']}")
    print(f"   未覆盖: {totals['missing_lines']}")

    # 判断是否达标
    if total_coverage >= 80:
        print(f"   状态: ✅ 达标 (目标: 80%)")
    else:
        print(f"   状态: ❌ 未达标 (目标: 80%, 差距: {80 - total_coverage:.2f}%)")

    # 按模块显示
    modules = analysis['modules']

    for module_name, files in modules.items():
        if not files:
            continue

        print(f"\n📁 {module_name.upper()} 模块")
        print("-" * 80)

        # 计算模块平均覆盖率
        if files:
            avg_coverage = sum(f['coverage'] for f in files) / len(files)
            print(f"   平均覆盖率: {avg_coverage:.2f}%")

        # 按覆盖率排序
        files.sort(key=lambda x: x['coverage'])

        for file_info in files:
            coverage = file_info['coverage']
            path = file_info['path']

            # 只显示文件名（去掉路径前缀）
            file_name = Path(path).name

            # 状态标记
            if coverage >= 80:
                status = "✅"
            elif coverage >= 60:
                status = "⚠️ "
            else:
                status = "❌"

            print(f"   {status} {file_name:40s} {coverage:6.2f}% "
                  f"({file_info['statements']} 语句, {file_info['missing']} 未覆盖)")


def print_recommendations(analysis: dict):
    """打印改进建议"""
    print("\n" + "=" * 80)
    print("改进建议")
    print("=" * 80)

    # 找出覆盖率低于 60% 的文件
    low_coverage_files = []

    for module_name, files in analysis['modules'].items():
        for file_info in files:
            if file_info['coverage'] < 60:
                low_coverage_files.append((module_name, file_info))

    if low_coverage_files:
        print("\n🔴 优先处理（覆盖率 < 60%）:")
        for module_name, file_info in low_coverage_files:
            file_name = Path(file_info['path']).name
            print(f"   - {file_name} ({file_info['coverage']:.2f}%) - "
                  f"需要补充 {file_info['missing']} 行测试")

    # 找出覆盖率在 60-80% 之间的文件
    medium_coverage_files = []

    for module_name, files in analysis['modules'].items():
        for file_info in files:
            if 60 <= file_info['coverage'] < 80:
                medium_coverage_files.append((module_name, file_info))

    if medium_coverage_files:
        print("\n🟡 次要处理（覆盖率 60-80%）:")
        for module_name, file_info in medium_coverage_files:
            file_name = Path(file_info['path']).name
            print(f"   - {file_name} ({file_info['coverage']:.2f}%) - "
                  f"需要补充 {file_info['missing']} 行测试")

    # 总结
    total_coverage = analysis['totals']['percent_covered']
    if total_coverage >= 80:
        print("\n✅ 覆盖率已达标！继续保持。")
    else:
        gap = 80 - total_coverage
        print(f"\n📈 距离目标还差 {gap:.2f}%，继续加油！")


def main():
    """主函数"""
    # 加载覆盖率数据
    data = load_coverage_data()

    # 分析数据
    analysis = analyze_coverage(data)

    # 打印报告
    print_report(analysis)

    # 打印建议
    print_recommendations(analysis)

    # 返回状态码
    total_coverage = analysis['totals']['percent_covered']
    if total_coverage >= 80:
        print("\n🎉 测试覆盖率达标！")
        sys.exit(0)
    else:
        print(f"\n⚠️  测试覆盖率未达标 (当前: {total_coverage:.2f}%, 目标: 80%)")
        sys.exit(1)


if __name__ == "__main__":
    main()
