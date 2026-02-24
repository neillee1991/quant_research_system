"""
Prefect Flow 部署脚本
注册所有 flow 到 Prefect Server，配置 cron 调度
"""
import os
import sys
from pathlib import Path

# 确保可以导入项目模块
backend_dir = str(Path(__file__).parent.parent)
sys.path.insert(0, backend_dir)

# 加载 .env 并设置 PREFECT_API_URL（必须在 import prefect 之前）
from dotenv import load_dotenv

env_path = Path(backend_dir).parent / ".env"
load_dotenv(env_path)

prefect_url = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
os.environ["PREFECT_API_URL"] = prefect_url

from flows.data_sync_flow import sync_daily_data, weekly_analysis


def main():
    """部署所有 flow"""
    print(f"Prefect API: {prefect_url}")

    # 检查 Prefect Server 是否可达
    import httpx
    try:
        resp = httpx.get(f"{prefect_url}/health", timeout=5)
        resp.raise_for_status()
        print("Prefect Server 连接成功")
    except Exception as e:
        print(f"警告: 无法连接 Prefect Server ({e})")
        print("请确认 Prefect Server 已启动: docker compose up prefect-server")
        sys.exit(1)

    # 每日数据同步: 工作日 18:00（A股收盘后）
    daily_deploy = sync_daily_data.to_deployment(
        name="daily-sync-deployment",
        cron="0 18 * * 1-5",
        tags=["data-sync", "daily"],
        description="每日数据同步流水线：同步行情数据 → 计算因子",
    )

    # 每周分析: 周六 3:00
    weekly_deploy = weekly_analysis.to_deployment(
        name="weekly-analysis-deployment",
        cron="0 3 * * 6",
        tags=["analysis", "weekly"],
        description="每周因子分析流水线：同步基础数据 → 计算技术因子",
    )

    print("启动 Prefect Flow 服务...")
    from prefect import serve
    serve(daily_deploy, weekly_deploy)


if __name__ == "__main__":
    main()
