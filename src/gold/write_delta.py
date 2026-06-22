import logging
from pathlib import Path
import pyarrow as pa
from deltalake import write_deltalake
from pyspark.sql import DataFrame
from src.core.arrow import spark_to_arrow_schema
from src.gold.schemas import MODEL_LEADERBOARD_SCHEMA, MODEL_TRENDS_SCHEMA

logger = logging.getLogger(__name__)

_TABLE_SCHEMAS = {
    "model_leaderboard": MODEL_LEADERBOARD_SCHEMA,
    "model_trends": MODEL_TRENDS_SCHEMA,
}


class SchemaValidationError(Exception):
    """Raised when a Gold DataFrame does not match its expected schema."""
    pass


def validate_schema(df: DataFrame, table_name: str) -> None:
    """Validate DataFrame columns and types against expected Gold schema."""
    if table_name not in _TABLE_SCHEMAS:
        raise ValueError(f"Unknown Gold table: {table_name}")

    expected_schema = _TABLE_SCHEMAS[table_name]
    expected_fields = {f.name: f.dataType.simpleString() for f in expected_schema.fields}
    actual_fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}

    expected_cols = set(expected_fields.keys())
    actual_cols = set(actual_fields.keys())

    missing = expected_cols - actual_cols
    unexpected = actual_cols - expected_cols
    type_mismatches = {
        col: (expected_fields[col], actual_fields[col])
        for col in expected_cols & actual_cols
        if expected_fields[col] != actual_fields[col]
    }

    if missing or unexpected or type_mismatches:
        details = []
        if missing:
            details.append(f"missing columns: {sorted(missing)}")
        if unexpected:
            details.append(f"unexpected columns: {sorted(unexpected)}")
        if type_mismatches:
            details.append(f"type mismatches: {type_mismatches}")
        raise SchemaValidationError(
            f"Gold schema validation failed for '{table_name}': {'; '.join(details)}"
        )


def write_gold_table(
    df: DataFrame,
    gold_dir: Path,
    table_name: str,
    partition_col: str = "snapshot_date",
) -> None:
    """Write Gold DataFrame to Delta table with partition overwrite."""
    row_count = df.count()

    if row_count == 0:
        logger.warning("Gold write skipped — 0 rows for %s, no data written", table_name)
        return

    validate_schema(df, table_name)

    table_path = str(gold_dir / table_name)

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
        logger.error("Gold write failed — %s: %s", table_name, exc)
        raise

    logger.info("Gold write complete — %d rows persisted to %s", row_count, table_path)
