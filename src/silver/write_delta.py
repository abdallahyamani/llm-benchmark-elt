import logging
from pathlib import Path

import pyarrow as pa
from delta import DeltaTable
from deltalake import write_deltalake
from pyspark.sql import DataFrame, SparkSession

from src.core.arrow import spark_to_arrow_schema

logger = logging.getLogger(__name__)


def write_to_delta(df: DataFrame, silver_dir: Path, partition_col: str = "snapshot_date") -> None:
    row_count = df.count()

    if row_count == 0:
        logger.warning("Silver write skipped — 0 rows in DataFrame, no data written")
        return

    table_path = str(silver_dir / "models")

    try:
        arrow_table = pa.Table.from_pandas(
            df.toPandas(),
            schema=spark_to_arrow_schema(df.schema),
            preserve_index=False,
        )
        write_deltalake(
            table_path,
            arrow_table,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=[partition_col],
        )
    except Exception as exc:
        raise RuntimeError(
            f"Silver write failed — path: {table_path}, cause: {exc}"
        ) from exc

    logger.info("Silver write complete — %d rows persisted to %s", row_count, table_path)


def verify_delta_table(silver_dir: Path, spark: SparkSession) -> dict:
    """Load Delta table and report row count, partition count, and history."""
    table_path = str(silver_dir / "models")

    try:
        df = spark.read.format("delta").load(table_path)
    except Exception as exc:
        raise FileNotFoundError(
            f"Silver Delta table missing or unreadable: {table_path}"
        ) from exc

    row_count = df.count()
    partition_count = df.select("snapshot_date").distinct().count()

    # Retrieve Delta history
    delta_table = DeltaTable.forPath(spark, table_path)
    history_df = delta_table.history().select("version", "timestamp", "operation")
    history = [row.asDict() for row in history_df.collect()]

    logger.info(
        "Silver verification passed — %d rows, %d partitions",
        row_count,
        partition_count,
    )

    return {
        "row_count": row_count,
        "partition_count": partition_count,
        "history": history,
    }
