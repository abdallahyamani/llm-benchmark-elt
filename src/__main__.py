"""CLI entry point for the LLM Benchmark pipeline."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.core.logging import configure_logging
from src.core.config import load_config
from src.core.spark_session import get_or_create_spark
from src.bronze.ingest import fetch_models, flatten_data, save_raw
from src.silver.transform import transform
from src.silver.write_delta import write_to_delta, verify_delta_table
from src.gold.transform import compute_model_rankings, compute_model_trends
from src.gold.write_delta import write_gold_table

logger = logging.getLogger(__name__)


def _process_bronze(config: dict) -> Path:
    raw_data = fetch_models(config["api_url"], config["api_key"])

    # Save raw JSON
    raw_json_path = config["bronze_dir"] / f"raw_{config['snapshot_date']}.json"
    raw_json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_json_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)
    logger.info("Bronze raw JSON saved to %s", raw_json_path)

    # Flatten and save Parquet
    df_flat = flatten_data(raw_data, config["snapshot_date"])
    parquet_path = save_raw(df_flat, config["bronze_dir"], config["snapshot_date"])

    return parquet_path


def _process_silver(parquet_path: Path, config: dict, spark) -> dict:
    cleaned_df = transform(parquet_path, spark)
    write_to_delta(cleaned_df, config["silver_dir"])
    summary = verify_delta_table(config["silver_dir"], spark)
    return summary


def _process_gold(config: dict, spark) -> None:
    silver_df = spark.read.format("delta").load(str(config["silver_dir"] / "models"))
    snapshot_date = config["snapshot_date"]
    snapshot_df = silver_df.filter(silver_df["snapshot_date"] == snapshot_date)

    if snapshot_df.count() == 0:
        logger.info("Gold build skipped — Silver snapshot %s contains 0 rows", snapshot_date)
        return

    rankings = compute_model_rankings(snapshot_df, snapshot_date)
    write_gold_table(rankings, config["gold_dir"], "model_leaderboard")

    trends = compute_model_trends(silver_df)
    write_gold_table(trends, config["gold_dir"], "model_trends")


def main():
    configure_logging()

    start_time = datetime.now(timezone.utc)
    logger.info("Pipeline started at %s", start_time.isoformat())

    try:
        config = load_config()
        spark = get_or_create_spark()

        parquet_path = _process_bronze(config)
        summary = _process_silver(parquet_path, config, spark)
        logger.info("Verification summary: %s", summary)

        _process_gold(config, spark)
    except Exception as exc:
        logger.error("Pipeline failed — %s", exc, exc_info=True)
        raise

    end_time = datetime.now(timezone.utc)
    logger.info("Pipeline finished at %s", end_time.isoformat())

    sys.exit(0)


if __name__ == "__main__":
    main()
