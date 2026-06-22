import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.core.config import load_config
from src.core.spark_session import get_or_create_spark
from src.bronze.ingest import fetch_models, flatten_data, save_raw
from src.silver.transform import transform
from src.silver.write_delta import write_to_delta, verify_delta_table
from src.gold.transform import compute_model_rankings, compute_model_trends
from src.gold.write_delta import write_gold_table

logger = logging.getLogger(__name__)

default_args = {
    "owner": "abdallah_yamani",
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract():
    """Extract: fetch models from API and save raw JSON to Bronze."""
    config = load_config()
    raw_data = fetch_models(config["api_url"], config["api_key"])
    raw_json_path = config["bronze_dir"] / f"raw_{config['snapshot_date']}.json"
    raw_json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_json_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)


def _bronze_save():
    """Bronze save: read raw JSON, flatten and save Parquet."""
    config = load_config()
    raw_json_path = config["bronze_dir"] / f"raw_{config['snapshot_date']}.json"
    with open(raw_json_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    df_flat = flatten_data(raw_data, config["snapshot_date"])
    save_raw(df_flat, config["bronze_dir"], config["snapshot_date"])


def _silver_write():
    """Silver: read Bronze Parquet, transform, write Delta."""
    config = load_config()
    parquet_path = config["bronze_dir"] / f"models_{config['snapshot_date']}.parquet"
    spark = get_or_create_spark()
    cleaned_df = transform(parquet_path, spark)
    write_to_delta(cleaned_df, config["silver_dir"])


def _verify():
    """Verify: read Delta table and log summary."""
    config = load_config()
    spark = get_or_create_spark()
    verify_delta_table(config["silver_dir"], spark)


def _gold_build():
    """Gold: read Silver Delta, produce model_leaderboard and model_trends tables."""
    config = load_config()
    spark = get_or_create_spark()
    silver_path = str(config["silver_dir"] / "models")

    try:
        df = spark.read.format("delta").load(silver_path)
    except Exception as exc:
        raise FileNotFoundError(
            f"Gold build aborted — Silver table not found at {silver_path}"
        ) from exc

    snapshot_date = config["snapshot_date"]
    df_snapshot = df.filter(df["snapshot_date"] == snapshot_date)

    if df_snapshot.count() == 0:
        logger.info(
            "Gold build skipped — Silver snapshot %s contains 0 rows", snapshot_date
        )
        return

    rankings = compute_model_rankings(df_snapshot, snapshot_date)
    write_gold_table(rankings, config["gold_dir"], "model_leaderboard")

    trends = compute_model_trends(df)
    write_gold_table(trends, config["gold_dir"], "model_trends")


def _gold_quality_check():
    """Gold: basic sanity checks on written tables."""
    config = load_config()
    spark = get_or_create_spark()

    leaderboard = spark.read.format("delta").load(str(config["gold_dir"] / "model_leaderboard"))
    trends = spark.read.format("delta").load(str(config["gold_dir"] / "model_trends"))

    lb_count = leaderboard.count()
    tr_count = trends.count()

    assert lb_count > 0, "Gold quality failed — model_leaderboard is empty"
    assert tr_count > 0, "Gold quality failed — model_trends is empty"

    logger.info("Gold quality check passed — leaderboard: %d rows, trends: %d rows", lb_count, tr_count)


with DAG(
    dag_id="llm_benchmark_pipeline",
    default_args=default_args,
    description="Daily LLM Benchmark medallion pipeline: Bronze → Silver → Gold",
    schedule="0 0 * * *",  # daily at midnight UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["llm-benchmark", "medallion"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    bronze_save = PythonOperator(task_id="bronze_save", python_callable=_bronze_save)
    silver_write = PythonOperator(task_id="silver_write", python_callable=_silver_write)
    verify = PythonOperator(task_id="verify", python_callable=_verify)
    gold_build = PythonOperator(task_id="gold_build", python_callable=_gold_build)
    gold_quality_check = PythonOperator(task_id="gold_quality_check", python_callable=_gold_quality_check)

    # Airflow Task execution order
    extract >> bronze_save >> silver_write >> verify >> gold_build >> gold_quality_check
