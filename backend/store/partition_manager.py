"""
PostgreSQL 分区表管理器
自动管理 factor_values 表的月分区
"""
from typing import List, Set
from app.core.logger import logger


class PartitionManager:
    """管理 factor_values 表的按月分区"""

    def __init__(self, db_client):
        self.db = db_client
        self._existing_partitions: Set[str] = set()
        self._load_existing_partitions()

    def _load_existing_partitions(self):
        """加载已存在的分区列表"""
        try:
            df = self.db.query("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
                AND tablename LIKE 'factor_values_%'
                ORDER BY tablename
            """)
            if not df.is_empty():
                self._existing_partitions = set(df['tablename'].to_list())
            logger.debug(f"Loaded {len(self._existing_partitions)} existing partitions")
        except Exception as e:
            logger.warning(f"Failed to load existing partitions: {e}")

    def ensure_partitions(self, trade_dates) -> None:
        """确保给定日期对应的分区都存在
        
        Args:
            trade_dates: 可迭代的日期字符串 (YYYYMMDD格式) 或 Polars Series
        """
        needed_months = set()
        for date_str in trade_dates:
            if date_str and len(str(date_str)) >= 6:
                ym = str(date_str)[:6]  # YYYYMM
                needed_months.add(ym)

        for ym in sorted(needed_months):
            self.create_partition(ym)

    def create_partition(self, year_month: str) -> bool:
        """创建指定月份的分区
        
        Args:
            year_month: YYYYMM 格式的年月
        """
        partition_name = f"factor_values_{year_month}"

        if partition_name in self._existing_partitions:
            return True

        try:
            year = int(year_month[:4])
            month = int(year_month[4:6])

            # 计算下个月
            if month == 12:
                next_year, next_month = year + 1, 1
            else:
                next_year, next_month = year, month + 1

            start_date = f"{year}{month:02d}01"
            end_date = f"{next_year}{next_month:02d}01"

            sql = f"""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF factor_values
                FOR VALUES FROM ('{start_date}') TO ('{end_date}')
            """
            self.db.execute(sql)
            self._existing_partitions.add(partition_name)
            logger.info(f"Created partition: {partition_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create partition {partition_name}: {e}")
            return False

    def list_partitions(self) -> List[str]:
        """列出所有已创建的分区"""
        self._load_existing_partitions()
        return sorted(self._existing_partitions)

    def drop_partition(self, year_month: str) -> bool:
        """删除指定月份的分区"""
        partition_name = f"factor_values_{year_month}"
        try:
            self.db.execute(f"DROP TABLE IF EXISTS {partition_name}")
            self._existing_partitions.discard(partition_name)
            logger.info(f"Dropped partition: {partition_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop partition {partition_name}: {e}")
            return False

    def ensure_partitions_for_range(self, start_date: str, end_date: str) -> None:
        """确保日期范围内所有月份的分区都存在"""
        start_ym = int(start_date[:6])
        end_ym = int(end_date[:6])

        current = start_ym
        while current <= end_ym:
            self.create_partition(str(current))
            # 下个月
            year = current // 100
            month = current % 100
            if month == 12:
                current = (year + 1) * 100 + 1
            else:
                current += 1
