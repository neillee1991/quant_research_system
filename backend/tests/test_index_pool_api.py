#!/usr/bin/env python3
"""
测试指数股票池 CRUD API
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fastapi.testclient import TestClient
from app.main import app
from app.core.logger import logger

client = TestClient(app)


def test_batch_upload():
    """测试批量上传指数成分股"""
    logger.info("=" * 60)
    logger.info("测试 1: 批量上传指数成分股")
    logger.info("=" * 60)

    payload = {
        "index_code": "TEST_INDEX_001",
        "index_name": "测试指数001",
        "description": "用于API测试的指数",
        "data": [
            {"trade_date": "20240101", "ts_code": "000001.SZ", "weight": 0.10},
            {"trade_date": "20240101", "ts_code": "000002.SZ", "weight": 0.08},
            {"trade_date": "20240101", "ts_code": "600000.SH", "weight": 0.12},
            {"trade_date": "20240102", "ts_code": "000001.SZ", "weight": 0.11},
            {"trade_date": "20240102", "ts_code": "000002.SZ", "weight": 0.07},
            {"trade_date": "20240102", "ts_code": "600000.SH", "weight": 0.13},
        ]
    }

    response = client.post("/api/v1/index-pool/batch-upload", json=payload)

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 批量上传成功")
        logger.info(f"  状态: {result['status']}")
        logger.info(f"  消息: {result['message']}")
        logger.info(f"  上传记录数: {result['data']['records_count']}")
        logger.info(f"  股票数量: {result['data']['stock_count']}")
        logger.info(f"  最新日期: {result['data']['latest_date']}")
        return True
    else:
        logger.error(f"✗ 批量上传失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_list_index_pools():
    """测试列出所有指数"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 2: 列出所有指数")
    logger.info("=" * 60)

    response = client.get("/api/v1/index-pool/list")

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 查询成功")
        logger.info(f"  状态: {result['status']}")
        logger.info(f"  指数数量: {len(result['data'])}")

        for idx in result['data'][:3]:  # 只显示前3个
            logger.info(f"  - {idx['index_code']}: {idx['index_name']} ({idx['stock_count']} 只股票)")

        return True
    else:
        logger.error(f"✗ 查询失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_get_index_pool():
    """测试查询指定指数成分股"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 3: 查询指定指数成分股")
    logger.info("=" * 60)

    # 测试不指定日期（获取最新）
    response = client.get("/api/v1/index-pool/TEST_INDEX_001")

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 查询成功（最新日期）")
        logger.info(f"  状态: {result['status']}")
        logger.info(f"  查询日期: {result['data']['query_date']}")
        logger.info(f"  成分股数量: {len(result['data']['constituents'])}")

        metadata = result['data']['metadata']
        logger.info(f"  指数名称: {metadata['index_name']}")
        logger.info(f"  股票数量: {metadata['stock_count']}")

        # 显示前3只成分股
        for stock in result['data']['constituents'][:3]:
            logger.info(f"  - {stock['ts_code']}: 权重 {stock['weight']:.4f}")

        # 测试指定日期
        response2 = client.get("/api/v1/index-pool/TEST_INDEX_001?trade_date=20240101")
        if response2.status_code == 200:
            result2 = response2.json()
            logger.info(f"\n✓ 查询成功（指定日期 20240101）")
            logger.info(f"  成分股数量: {len(result2['data']['constituents'])}")

        return True
    else:
        logger.error(f"✗ 查询失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_csv_upload():
    """测试 CSV 上传"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 4: CSV 上传")
    logger.info("=" * 60)

    csv_content = """trade_date,ts_code,weight
20240103,000001.SZ,0.15
20240103,000002.SZ,0.10
20240103,600000.SH,0.12
20240103,600016.SH,0.08
"""

    payload = {
        "index_code": "TEST_INDEX_002",
        "index_name": "测试指数002",
        "description": "CSV上传测试",
        "csv_content": csv_content
    }

    response = client.post("/api/v1/index-pool/csv-upload", json=payload)

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ CSV 上传成功")
        logger.info(f"  状态: {result['status']}")
        logger.info(f"  消息: {result['message']}")
        logger.info(f"  上传记录数: {result['data']['records_count']}")
        return True
    else:
        logger.error(f"✗ CSV 上传失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def test_download_template():
    """测试下载模板"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 5: 下载 CSV 模板")
    logger.info("=" * 60)

    response = client.get("/api/v1/index-pool/template")

    if response.status_code == 200:
        logger.info(f"✓ 模板下载成功")
        logger.info(f"  Content-Type: {response.headers.get('content-type')}")
        logger.info(f"  内容长度: {len(response.text)} 字节")
        logger.info(f"  前3行:")
        for line in response.text.split('\n')[:3]:
            logger.info(f"    {line}")
        return True
    else:
        logger.error(f"✗ 模板下载失败: {response.status_code}")
        return False


def test_delete_index_pool():
    """测试删除指数"""
    logger.info("\n" + "=" * 60)
    logger.info("测试 6: 删除指数")
    logger.info("=" * 60)

    # 删除测试指数
    response = client.delete("/api/v1/index-pool/TEST_INDEX_001")

    if response.status_code == 200:
        result = response.json()
        logger.info(f"✓ 删除成功")
        logger.info(f"  状态: {result['status']}")
        logger.info(f"  消息: {result['message']}")

        # 验证删除
        response2 = client.get("/api/v1/index-pool/TEST_INDEX_001")
        if response2.status_code == 404:
            logger.info(f"✓ 验证删除成功（查询返回 404）")
        else:
            logger.warning(f"⚠ 删除后仍能查询到数据")

        return True
    else:
        logger.error(f"✗ 删除失败: {response.status_code}")
        logger.error(f"  响应: {response.text}")
        return False


def cleanup():
    """清理测试数据"""
    logger.info("\n" + "=" * 60)
    logger.info("清理测试数据")
    logger.info("=" * 60)

    test_indices = ["TEST_INDEX_001", "TEST_INDEX_002"]

    for index_code in test_indices:
        try:
            response = client.delete(f"/api/v1/index-pool/{index_code}")
            if response.status_code == 200:
                logger.info(f"✓ 清理 {index_code}")
        except:
            pass


def main():
    logger.info("开始测试指数股票池 CRUD API...")
    logger.info("=" * 60)

    try:
        test_results = []

        # 运行测试
        test_results.append(("批量上传", test_batch_upload()))
        test_results.append(("列出所有指数", test_list_index_pools()))
        test_results.append(("查询指定指数", test_get_index_pool()))
        test_results.append(("CSV 上传", test_csv_upload()))
        test_results.append(("下载模板", test_download_template()))
        test_results.append(("删除指数", test_delete_index_pool()))

        # 清理
        cleanup()

        # 汇总
        logger.info("\n" + "=" * 60)
        logger.info("测试汇总")
        logger.info("=" * 60)

        for test_name, result in test_results:
            status = "✓ 通过" if result else "✗ 失败"
            logger.info(f"{status}: {test_name}")

        all_passed = all(result for _, result in test_results)

        if all_passed:
            logger.info("\n✅ 所有测试通过！Phase 3 完成。")
            logger.info("\n下一步:")
            logger.info("  - Phase 4: 实现 Alphalens 分析 API")
        else:
            logger.warning("\n⚠ 部分测试失败")

        return all_passed

    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
