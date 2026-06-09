"""Silver layer transformation logic."""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

logger = logging.getLogger(__name__)

LATENCY_COLS = [
    "median_output_tokens_per_second",
    "median_time_to_first_token_seconds",
    "median_time_to_first_answer_token",
]

PRICING_COLS = [
    "price_1m_input_tokens",
    "price_1m_output_tokens",
]


def transform(parquet_path: Path, spark: SparkSession) -> DataFrame:
    """Read Bronze Parquet and apply Silver cleaning rules."""
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Bronze data not found: {parquet_path}"
        )

    df = spark.read.parquet(str(parquet_path))

    # Deduplicate by model_id within the same snapshot
    df = df.dropDuplicates(["model_id", "snapshot_date"])

    # Cast date columns to DateType
    df = df.withColumn("release_date", F.to_date(F.col("release_date")))
    df = df.withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))

    # Enforce numeric types on pricing columns
    for col_name in PRICING_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    # Replace 0.0 with null in latency columns and enforce type
    for col_name in LATENCY_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) == 0.0, F.lit(None)).otherwise(F.col(col_name)),
            )

    # Drop rows with no identity (unusable records)
    df = df.filter(F.col("model_id").isNotNull() & F.col("model_slug").isNotNull())

    # Rename columns for Silver layer clarity
    df = df.withColumnRenamed("model_slug", "model_name")
    df = df.withColumnRenamed("creator_slug", "vendor")

    logger.info("Silver transform complete — %d rows produced", df.count())
    return df
