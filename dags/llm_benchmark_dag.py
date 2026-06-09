import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.core.config import load_config
from src.core.spark_session import get_or_create_spark
from src.bronze.ingest import fetch_models, flatten_data, save_raw
from src.silver.transform import transform
from src.silver.write_delta import write_to_delta, verify_delta_table

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**context):
    """fetch models data from API and push raw to XCom."""
    config = load_config()
    raw_data = fetch_models(config["api_url"], config["api_key"])

    context["ti"].xcom_push(key="config", value={
        "api_url": config["api_url"],
        "bronze_dir": str(config["bronze_dir"]),
        "silver_dir": str(config["silver_dir"]),
        "snapshot_date": config["snapshot_date"],
    })
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    logger.info("Bronze extract complete — %d models fetched", len(raw_data["data"]))


def _bronze_save(**context):
    """write raw JSON and Parquet from extracted data."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="extract", key="config")
    raw_data = ti.xcom_pull(task_ids="extract", key="raw_data")

    bronze_dir = Path(config["bronze_dir"])
    snapshot_date = config["snapshot_date"]

    # Save raw JSON
    raw_json_path = bronze_dir / f"raw_{snapshot_date}.json"
    raw_json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_json_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, indent=2)
    logger.info("Bronze raw JSON saved to %s", raw_json_path)

    # Flatten and save Parquet
    df_flat = flatten_data(raw_data, snapshot_date)
    parquet_path = save_raw(df_flat, bronze_dir, snapshot_date)

    ti.xcom_push(key="parquet_path", value=str(parquet_path))
    logger.info("Bronze save complete — Parquet at %s", parquet_path)


def _silver_transform(**context):
    """read Bronze Parquet and apply cleaning rules."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="extract", key="config")
    parquet_path = Path(ti.xcom_pull(task_ids="bronze_save", key="parquet_path"))

    spark = get_or_create_spark()
    cleaned_df = transform(parquet_path, spark)

    silver_dir = Path(config["silver_dir"])
    staging_path = silver_dir / "_staging" / f"silver_{config['snapshot_date']}.parquet"
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned_df.write.mode("overwrite").parquet(str(staging_path))

    ti.xcom_push(key="staging_path", value=str(staging_path))  # avoids serializing Spark DF via XCom
    logger.info("Silver transform complete — staged at %s", staging_path)


def _delta_write(**context):
    """process transformed data to Delta table."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="extract", key="config")
    staging_path = ti.xcom_pull(task_ids="silver_transform", key="staging_path")

    spark = get_or_create_spark()
    silver_dir = Path(config["silver_dir"])

    df = spark.read.parquet(staging_path)
    write_to_delta(df, silver_dir)
    logger.info("Silver Delta write complete — target %s", silver_dir / "models")


def _verify(**context):
    """validate Delta table integrity."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="extract", key="config")
    silver_dir = Path(config["silver_dir"])

    spark = get_or_create_spark()
    summary = verify_delta_table(silver_dir, spark)
    logger.info("Silver verification passed — %s", summary)


with DAG(
    dag_id="llm_benchmark_pipeline",
    default_args=default_args,
    description="Daily LLM Benchmark medallion pipeline: Bronze → Silver → Delta",
    schedule="0 0 * * *",  # daily at midnight 00:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["llm-benchmark", "medallion"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
    )

    bronze_save = PythonOperator(
        task_id="bronze_save",
        python_callable=_bronze_save,
    )

    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=_silver_transform,
    )

    delta_write = PythonOperator(
        task_id="delta_write",
        python_callable=_delta_write,
    )

    verify = PythonOperator(
        task_id="verify",
        python_callable=_verify,
    )

    # Airflow Task execution order
    extract >> bronze_save >> silver_transform >> delta_write >> verify
