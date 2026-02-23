import polars as pl
from pathlib import Path
from app.core.config import settings
from app.core.logger import logger


def save_parquet(df: pl.DataFrame, subdir: str, filename: str) -> Path:
    """Save a Polars DataFrame as a Parquet file."""
    target_dir = settings.data_dir / subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / filename
    df.write_parquet(path, compression="snappy")
    logger.info(f"Saved {len(df)} rows to {path}")
    return path


def load_parquet(subdir: str, filename: str) -> pl.DataFrame | None:
    """Load a Parquet file into a Polars DataFrame."""
    path = settings.data_dir / subdir / filename
    if not path.exists():
        logger.warning(f"Parquet file not found: {path}")
        return None
    return pl.read_parquet(path)


def list_parquet_files(subdir: str) -> list[Path]:
    target_dir = settings.data_dir / subdir
    if not target_dir.exists():
        return []
    return sorted(target_dir.glob("*.parquet"))


def load_all_parquet(subdir: str) -> pl.DataFrame | None:
    """Load and concatenate all Parquet files in a subdirectory."""
    files = list_parquet_files(subdir)
    if not files:
        return None
    frames = [pl.read_parquet(f) for f in files]
    return pl.concat(frames)
