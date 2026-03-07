"""
Benchmark test suite initialization.
"""

# Test data configurations
BENCHMARK_CONFIGS = {
    "small": {
        "stocks": 10,
        "days": 30,
        "description": "Small dataset - 10 stocks, 1 month"
    },
    "medium": {
        "stocks": 100,
        "days": 180,
        "description": "Medium dataset - 100 stocks, 6 months"
    },
    "large": {
        "stocks": 1000,
        "days": 365,
        "description": "Large dataset - 1000 stocks, 1 year"
    },
    "xlarge": {
        "stocks": 5000,
        "days": 1095,
        "description": "Extra large dataset - 5000 stocks, 3 years"
    }
}

__all__ = ["BENCHMARK_CONFIGS"]
