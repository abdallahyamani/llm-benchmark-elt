import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.core.config import load_config
from src.core.spark_session import get_or_create_spark
from src.bronze.ingest import fetch_models, flatten_data, save_raw
from src.silver.transform import transform
from src.silver.write_delta import write_to_delta, verify_delta_table

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


with DAG(
    dag_id="llm_benchmark_pipeline",
    default_args=default_args,
    description="Daily LLM Benchmark medallion pipeline: Bronze → Silver → Delta",
    schedule="0 0 * * *",  # daily at midnight UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["llm-benchmark", "medallion"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_extract)
    bronze_save = PythonOperator(task_id="bronze_save", python_callable=_bronze_save)
    silver_write = PythonOperator(task_id="silver_write", python_callable=_silver_write)
    verify = PythonOperator(task_id="verify", python_callable=_verify)

    # Airflow Task execution order
    extract >> bronze_save >> silver_write >> verify
