"""
DolphinDB 数据操作模块
负责数据的插入、更新、批量写入和同步日志管理
"""
import threading
from typing import List, Optional, TYPE_CHECKING

import pandas as pd
import polars as pl

from app.core.logger import logger

if TYPE_CHECKING:
    from .connection import DolphinDBConnection
    from .query_builder import QueryBuilder


class DataOperations:
    """数据操作管理器"""

    # TSDB 分区表（时间序列数据）
    _TSDB_TABLES: frozenset = frozenset({
        "sync_daily_data", "sync_daily_basic", "sync_adj_factor",
        "sync_index_daily", "sync_moneyflow", "factor_values",
    })

    # 元数据表（维度表）
    _META_TABLES: frozenset = frozenset({
        "sync_log", "sync_log_history", "sync_stock_basic",
        "factor_metadata", "factor_analysis", "dag_run_log",
        "dag_task_log", "production_task_run", "trade_cal",
        "sync_task_config", "etl_task_config", "factor_data_config",
        "task_version_history",
    })

    # 额外的日期列配置
    _EXTRA_DATE_COLUMNS = {
        "factor_values": ["trade_date"],
    }

    def __init__(
        self,
        connection: "DolphinDBConnection",
        query_builder: "QueryBuilder",
    ) -> None:
        """
        初始化数据操作管理器

        Args:
            connection: DolphinDB 连接管理器
            query_builder: SQL 查询构建器
        """
        self.conn = connection
        self.query = query_builder

    def _prepare_upload_df(
        self,
        table_name: str,
        df: pl.DataFrame,
        db_path: str,
        known_columns: Optional[List[str]],
        var_prefix: str,
        select_columns: Optional[List[str]] = None,
    ) -> tuple:
        """
        通用的 DataFrame 写入准备逻辑：
        1. 日期列格式转换（YYYYMMDD → date）
        2. 转换为 Pandas
        3. 获取表列顺序并对齐
        4. 上传临时变量

        必须在 self.conn.lock 内调用

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            db_path: 数据库路径
            known_columns: 已知列顺序（跳过 schema 查询）
            var_prefix: 临时变量前缀
            select_columns: 选择写入的列（默认全部）

        Returns:
            (ordered_cols, tmp_var) — 有序列名列表和上传到 DolphinDB 的临时变量名
        """
        # 获取表列顺序 & 自动检测 DATE 列
        date_cols: List[str] = []
        if known_columns is not None:
            table_cols = known_columns
            # known_columns 场景无法从 schema 检测，回退到额外配置
            date_cols = self._EXTRA_DATE_COLUMNS.get(table_name, [])
        else:
            schema_info = self.conn.session.run(
                f"schema(loadTable('{db_path}', '{table_name}'))"
            )
            table_cols = []
            if isinstance(schema_info, dict) and "colDefs" in schema_info:
                col_defs_df = schema_info["colDefs"]
                if isinstance(col_defs_df, pd.DataFrame) and "name" in col_defs_df.columns:
                    table_cols = col_defs_df["name"].tolist()
                    # 自动从 schema 中提取 DATE 类型列
                    if "typeString" in col_defs_df.columns:
                        date_cols = col_defs_df.loc[
                            col_defs_df["typeString"] == "DATE", "name"
                        ].tolist()
            if not table_cols:
                logger.warning(
                    f"无法从 schema 获取 {table_name} 列信息"
                    f"（schema 类型={type(schema_info).__name__}），"
                    f"回退使用 DataFrame 列写入"
                )
                table_cols = df.columns if select_columns is None else select_columns

        # 合并额外配置的日期列（如 factor_values）
        extra = self._EXTRA_DATE_COLUMNS.get(table_name, [])
        if extra:
            date_cols = list(set(date_cols) | set(extra))

        # 转换日期列：YYYYMMDD 字符串 → date
        for col in date_cols:
            if col in df.columns and df[col].dtype == pl.Utf8:
                df = df.with_columns(
                    pl.col(col).str.to_date("%Y%m%d", strict=False).alias(col)
                )

        pdf = df.select(select_columns).to_pandas() if select_columns else df.to_pandas()

        for col in date_cols:
            if col in pdf.columns and pd.api.types.is_datetime64_any_dtype(pdf[col]):
                pdf[col] = pdf[col].dt.date

        # 对齐列顺序
        ordered_cols = [c for c in table_cols if c in pdf.columns]
        if not ordered_cols:
            raise RuntimeError(
                f"写入 {table_name} 时列名无交集: "
                f"表列={table_cols}, DataFrame列={pdf.columns.tolist()}"
            )
        pdf = pdf[ordered_cols]

        # 上传临时变量
        tmp_var = f"{var_prefix}_{table_name}_{threading.current_thread().ident}"
        self.conn.session.upload({tmp_var: pdf})

        return ordered_cols, tmp_var

    def upsert(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: List[str],
        known_columns: Optional[List[str]] = None,
    ) -> None:
        """
        插入或更新数据

        TSDB 分区表使用 keepDuplicates=LAST 自动去重
        维度表需要手动 delete + insert

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 主键列
            known_columns: 已知列顺序（跳过 schema 查询，用于刚建表后首次写入）
        """
        if df.is_empty():
            logger.warning(f"空 DataFrame，跳过写入: {table_name}")
            return

        db_path = self.conn.db_path
        is_meta = table_name in self._META_TABLES

        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "tmp"
                )
                col_select = ", ".join(ordered_cols)

                if is_meta and key_columns:
                    # 维度表：先按主键删除旧行，再插入（模拟 upsert）
                    handle = f"{table_name}_handle"
                    delete_conds = [f'{kc} in {tmp_var}.{kc}' for kc in key_columns]
                    cond_str = " and ".join(delete_conds)
                    self.conn.session.run(
                        f"{handle} = loadTable('{db_path}', '{table_name}')"
                    )
                    self.conn.session.run(
                        f"delete from {handle} where {cond_str}"
                    )
                    self.conn.session.run(
                        f"tableInsert({handle}, select {col_select} from {tmp_var});"
                        f"undef('{tmp_var}')"
                    )
                else:
                    # TSDB 表：keepDuplicates=LAST 自动去重，直接插入
                    self.conn.session.run(
                        f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                        f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
                        f"undef('{tmp_var}')"
                    )
            logger.info(
                f"写入 {len(df)} 行到 {table_name}，"
                f"主键列: {key_columns}"
            )
        except Exception as e:
            logger.error(f"写入失败 [{table_name}]: {e}")
            raise

    def upsert_daily(self, df: pl.DataFrame) -> None:
        """插入或更新日线数据（兼容旧接口）"""
        self.upsert("sync_daily_data", df, ["trade_date", "ts_code"])

    def bulk_copy(
        self,
        table_name: str,
        df: pl.DataFrame,
        columns: List[str] = None,
        known_columns: Optional[List[str]] = None,
    ) -> int:
        """
        批量写入数据

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            columns: 列名列表，默认使用 DataFrame 的列名
            known_columns: 已知列顺序（跳过 schema 查询，用于刚建表后首次写入）

        Returns:
            写入的行数
        """
        if df.is_empty():
            return 0

        db_path = self.conn.db_path
        cols = columns or df.columns
        rows = len(df)

        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "bulk",
                    select_columns=cols,
                )
                col_select = ", ".join(ordered_cols)
                self.conn.session.run(
                    f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                    f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(f"批量写入 {rows} 行到 {table_name}")
            return rows
        except Exception as e:
            logger.error(f"批量写入失败 [{table_name}]: {e}")
            raise

    def get_last_sync_date(self, source: str, data_type: str) -> Optional[str]:
        """
        获取最后同步日期

        从 meta 数据库的 sync_log 表中查询指定数据源和类型的最后同步日期

        Args:
            source: 数据源
            data_type: 数据类型

        Returns:
            最后同步日期（YYYYMMDD 格式）
        """
        try:
            safe_source = self.query._escape_value(source)
            safe_data_type = self.query._escape_value(data_type)
            sql = (
                f'SELECT last_date FROM loadTable("{self.conn.db_path}", "sync_log") '
                f'WHERE source = {safe_source} AND data_type = {safe_data_type} '
                f'ORDER BY updated_at DESC LIMIT 1'
            )
            with self.conn.lock:
                self.conn._ensure_connected()
                result = self.conn.session.run(sql)

            if result is None or (isinstance(result, pd.DataFrame) and result.empty):
                return None

            df = pl.from_pandas(result) if isinstance(result, pd.DataFrame) else pl.DataFrame({"last_date": [result]})
            if df.is_empty():
                return None

            return df["last_date"][0]
        except Exception as e:
            logger.warning(f"查询最后同步日期失败 [{source}/{data_type}]: {e}")
            return None

    def update_sync_log(
        self,
        source: str,
        data_type: str,
        last_date: str,
    ) -> None:
        """
        更新同步日志

        Args:
            source: 数据源
            data_type: 数据类型
            last_date: 最后同步日期（YYYYMMDD 格式）
        """
        from datetime import datetime

        now = datetime.now()
        log_df = pl.DataFrame({
            "source": [source],
            "data_type": [data_type],
            "last_date": [last_date],
            "updated_at": [now],
        })
        pdf = log_df.to_pandas()

        try:
            with self.conn.lock:
                self.conn._ensure_connected()
                safe_source = self.query._escape_value(source)
                safe_data_type = self.query._escape_value(data_type)
                # 先删除旧记录
                self.conn.session.run(
                    f'sync_log_handle = loadTable("{self.conn.db_path}", "sync_log");'
                    f'delete from sync_log_handle where source = {safe_source} and data_type = {safe_data_type}'
                )
                # 再插入新记录
                tmp_var = f"sync_log_{threading.current_thread().ident}"
                self.conn.session.upload({tmp_var: pdf})
                self.conn.session.run(
                    f'sync_log_handle = loadTable("{self.conn.db_path}", "sync_log");'
                    f"tableInsert(sync_log_handle, {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(f"同步日志已更新: {source}/{data_type} -> {last_date}")
        except Exception as e:
            logger.error(f"更新同步日志失败 [{source}/{data_type}]: {e}")
            raise
