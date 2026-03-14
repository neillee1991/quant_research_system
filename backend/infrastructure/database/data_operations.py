"""
DolphinDB 数据操作模块
负责数据的查询、插入、更新等操作
"""
import threading
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
import polars as pl

from app.core.logger import logger


class DataOperations:
    """DolphinDB 数据操作管理器"""

    # 额外配置的日期列（用于 known_columns 场景）
    _EXTRA_DATE_COLUMNS = {
        "factor_values": ["trade_date"],
        "sync_daily_data": ["trade_date"],
        "sync_daily_basic": ["trade_date"],
        "sync_adj_factor": ["trade_date"],
        "sync_index_daily": ["trade_date"],
        "sync_moneyflow": ["trade_date"],
    }

    def __init__(self, connection, sql_adapter, table_manager):
        """
        初始化数据操作管理器

        Args:
            connection: DolphinDBConnection 实例
            sql_adapter: SQLAdapter 实例
            table_manager: TableManager 实例
        """
        self._conn = connection
        self._sql_adapter = sql_adapter
        self._table_manager = table_manager

    def query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        return_type: str = "polars"
    ) -> pl.DataFrame:
        """
        执行查询并返回结果

        Args:
            sql: SQL 查询语句
            params: 参数元组
            return_type: 返回类型 ("polars" 或 "pandas")

        Returns:
            查询结果 DataFrame
        """
        sql = self._sql_adapter.build_sql(sql, params)
        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                result = self._conn.session.run(sql)
            if return_type == "polars":
                return self._to_polars(result)
            return result
        except Exception as e:
            logger.error(f"查询失败: {e}\nSQL: {sql}")
            raise

    def _to_polars(self, result: Any) -> pl.DataFrame:
        """
        将 DolphinDB 查询结果转换为 Polars DataFrame

        Args:
            result: DolphinDB 查询结果

        Returns:
            Polars DataFrame
        """
        if result is None:
            return pl.DataFrame()
        if isinstance(result, pd.DataFrame):
            if result.empty:
                return pl.DataFrame()
            return pl.from_pandas(result)
        if isinstance(result, dict):
            return pl.DataFrame(result)
        logger.warning(f"未知结果类型: {type(result)}, 返回空 DataFrame")
        return pl.DataFrame()

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """
        执行 SQL 语句（不返回结果）

        Args:
            sql: SQL 语句
            params: 参数元组
        """
        sql = self._sql_adapter.build_sql(sql, params)
        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                self._conn.session.run(sql)
            logger.debug(f"执行成功: {sql[:100]}...")
        except Exception as e:
            logger.error(f"执行失败: {e}\nSQL: {sql}")
            raise

    def upsert(
        self,
        table_name: str,
        df: pl.DataFrame,
        key_columns: List[str],
        known_columns: Optional[List[str]] = None,
        is_full_sync: bool = False,
        trade_date: Optional[str] = None,
        factor_id: Optional[str] = None,
    ) -> None:
        """
        插入或更新数据

        统一规则：
        - 全量任务 (is_full_sync=True): 清空整个表，然后写入新数据
        - 增量任务 (is_full_sync=False, trade_date提供): 清空指定 trade_date + factor_id 的数据，然后写入新数据
        - 如果 is_full_sync=False 且 trade_date=None: 根据 key_columns 删除已存在的行，然后插入

        Args:
            table_name: 表名
            df: Polars DataFrame
            key_columns: 主键列
            known_columns: 已知列顺序（跳过 schema 查询，用于刚建表后首次写入）
            is_full_sync: 是否全量同步
            trade_date: 交易日期（增量同步时提供）
            factor_id: 因子ID（增量同步时提供，用于精确删除）
        """
        if df.is_empty():
            logger.warning(f"空 DataFrame，跳过写入: {table_name}")
            return

        db_path = self._table_manager._resolve_db_path(table_name)
        is_meta = table_name in self._table_manager._META_TABLES

        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "upsert"
                )
                col_select = ", ".join(ordered_cols)

                # 构建删除语句
                table_handle = f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"

                if is_full_sync:
                    # 全量同步：清空整个表
                    delete_stmt = f"delete from {table_name}_handle;"
                    logger.info(f"[全量同步] 清空表 {table_name}")
                elif trade_date:
                    # 增量同步：清空指定 trade_date + factor_id 的数据
                    from infrastructure.database.type_converter import TypeConverter
                    escaped_date = TypeConverter.escape_value(trade_date)

                    # 如果提供了 factor_id，则精确删除 trade_date + factor_id 的数据
                    if factor_id:
                        escaped_factor_id = TypeConverter.escape_value(factor_id)
                        delete_stmt = f"delete from {table_name}_handle where trade_date = {escaped_date} and factor_id = {escaped_factor_id};"
                        logger.info(f"[增量同步] 清空表 {table_name} 中 trade_date={trade_date}, factor_id={factor_id} 的数据")
                    else:
                        # 兼容旧逻辑：只按 trade_date 删除（用于非因子表）
                        delete_stmt = f"delete from {table_name}_handle where trade_date = {escaped_date};"
                        logger.info(f"[增量同步] 清空表 {table_name} 中 trade_date={trade_date} 的数据")
                else:
                    # 根据 key_columns 删除已存在的行
                    if key_columns:
                        from infrastructure.database.type_converter import TypeConverter
                        # 获取要删除的 key 值
                        key_values = []
                        for key_col in key_columns:
                            if key_col in df.columns:
                                unique_vals = df[key_col].unique().to_list()
                                key_values.append((key_col, unique_vals))

                        # 构建 WHERE 条件
                        if key_values:
                            conditions = []
                            for key_col, vals in key_values:
                                if len(vals) == 1:
                                    escaped_val = TypeConverter.escape_value(vals[0])
                                    conditions.append(f"{key_col} = {escaped_val}")
                                else:
                                    escaped_vals = [TypeConverter.escape_value(v) for v in vals]
                                    conditions.append(f"{key_col} in [{', '.join(escaped_vals)}]")

                            where_clause = " and ".join(conditions)
                            delete_stmt = f"delete from {table_name}_handle where {where_clause};"
                            logger.info(f"[Upsert] 删除表 {table_name} 中 {where_clause} 的数据")
                        else:
                            delete_stmt = ""
                    else:
                        delete_stmt = ""

                # 执行：删除 + 插入
                if delete_stmt:
                    self._conn.session.run(
                        f"{table_handle}"
                        f"{delete_stmt}"
                        f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
                        f"undef('{tmp_var}')"
                    )
                else:
                    self._conn.session.run(
                        f"{table_handle}"
                        f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
                        f"undef('{tmp_var}')"
                    )

            mode_desc = "全量" if is_full_sync else (f"增量(trade_date={trade_date}, factor_id={factor_id})" if trade_date and factor_id else f"增量(trade_date={trade_date})" if trade_date else f"Upsert(keys={key_columns})")
            logger.info(f"写入 {len(df)} 行到 {table_name}，模式: {mode_desc}")
        except Exception as e:
            logger.error(f"写入失败 [{table_name}]: {e}")
            raise

    def upsert_daily(self, df: pl.DataFrame) -> None:
        """
        插入或更新日线数据（兼容旧接口）

        Args:
            df: Polars DataFrame
        """
        self.upsert("sync_daily_data", df, ["trade_date", "ts_code"])

    def append(
        self,
        table_name: str,
        df: pl.DataFrame,
        known_columns: Optional[List[str]] = None,
    ) -> int:
        """
        追加数据到表（不删除现有数据）

        Args:
            table_name: 目标表名
            df: Polars DataFrame
            known_columns: 已知列顺序（跳过 schema 查询）

        Returns:
            写入的行数
        """
        if df.is_empty():
            return 0

        db_path = self._table_manager._resolve_db_path(table_name)
        rows = len(df)

        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "append"
                )
                col_select = ", ".join(ordered_cols)
                self._conn.session.run(
                    f"{table_name}_handle = loadTable('{db_path}', '{table_name}');"
                    f"tableInsert({table_name}_handle, select {col_select} from {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(f"追加 {rows} 行到 {table_name}")
            return rows
        except Exception as e:
            logger.error(f"追加数据失败 [{table_name}]: {e}")
            raise

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

        db_path = self._table_manager._resolve_db_path(table_name)
        cols = columns or df.columns
        rows = len(df)

        try:
            with self._conn.lock:
                self._conn._ensure_connected()
                ordered_cols, tmp_var = self._prepare_upload_df(
                    table_name, df, db_path, known_columns, "bulk",
                    select_columns=cols,
                )
                col_select = ", ".join(ordered_cols)
                self._conn.session.run(
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
            最后同步日期字符串，不存在返回 None
        """
        try:
            safe_source = self._sql_adapter._type_converter.escape_value(source)
            safe_data_type = self._sql_adapter._type_converter.escape_value(data_type)
            sql = (
                f'SELECT last_date FROM loadTable("{self._conn.db_path}", "sync_log") '
                f'WHERE source = {safe_source} AND data_type = {safe_data_type} '
                f'ORDER BY updated_at DESC LIMIT 1'
            )
            with self._conn.lock:
                self._conn._ensure_connected()
                result = self._conn.session.run(sql)

            if result is None:
                return None
            if isinstance(result, pd.DataFrame):
                if result.empty:
                    return None
                val = result["last_date"].iloc[0]
                return str(val) if val is not None else None
            return None
        except Exception as e:
            logger.debug(f"获取同步日期失败 [{source}/{data_type}]: {e}")
            return None

    def update_sync_log(
        self, source: str, data_type: str, last_date: str
    ) -> None:
        """
        更新同步日志

        sync_log 现在是维度表（非分区），不支持 keepDuplicates 自动去重。
        使用 delete + insert 实现 upsert 语义。

        Args:
            source: 数据源
            data_type: 数据类型
            last_date: 最后同步日期
        """
        try:
            pdf = pd.DataFrame(
                {
                    "source": [source],
                    "data_type": [data_type],
                    "last_date": [last_date],
                    "updated_at": [datetime.now()],
                }
            )
            with self._conn.lock:
                self._conn._ensure_connected()
                safe_source = self._sql_adapter._type_converter.escape_value(source)
                safe_data_type = self._sql_adapter._type_converter.escape_value(data_type)
                # 先删除旧记录
                self._conn.session.run(
                    f'sync_log_handle = loadTable("{self._conn.db_path}", "sync_log");'
                    f'delete from sync_log_handle where source = {safe_source} and data_type = {safe_data_type}'
                )
                # 再插入新记录
                tmp_var = f"sync_log_{threading.current_thread().ident}"
                self._conn.session.upload({tmp_var: pdf})
                self._conn.session.run(
                    f'sync_log_handle = loadTable("{self._conn.db_path}", "sync_log");'
                    f"tableInsert(sync_log_handle, {tmp_var});"
                    f"undef('{tmp_var}')"
                )
            logger.info(f"同步日志已更新: {source}/{data_type} -> {last_date}")
        except Exception as e:
            logger.error(f"更新同步日志失败 [{source}/{data_type}]: {e}")
            raise

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

        必须在 self._conn.lock 内调用。

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
            schema_info = self._conn.session.run(
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

        # 转换为 Pandas
        logger.debug(f"转换前 Polars DataFrame 列: {df.columns}")
        pdf = df.select(select_columns).to_pandas() if select_columns else df.to_pandas()
        logger.debug(f"转换后 Pandas DataFrame 列: {pdf.columns.tolist()}")

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

        # 调试：检查列对齐情况
        missing_in_df = [c for c in table_cols if c not in pdf.columns]
        if missing_in_df:
            logger.warning(
                f"写入 {table_name} 时，表中存在但 DataFrame 中缺失的列: {missing_in_df}"
            )

        pdf = pdf[ordered_cols]

        # 上传临时变量
        tmp_var = f"{var_prefix}_{table_name}_{threading.current_thread().ident}"
        self._conn.session.upload({tmp_var: pdf})

        return ordered_cols, tmp_var
