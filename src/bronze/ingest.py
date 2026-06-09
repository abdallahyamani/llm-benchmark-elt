import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


def fetch_models(api_url: str, api_key: str, timeout: int = 30) -> dict:
    """Fetch LLM model data from the API."""
    headers = {"x-api-key": api_key}

    try:
        response = requests.get(api_url, headers=headers, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text}"
        ) from exc
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
        raise RuntimeError(f"Connectivity failure: {exc}") from exc

    data = response.json()

    if "data" not in data or not isinstance(data["data"], list):
        raise ValueError("Invalid response format: expected 'data' key with a list")

    logger.info("Models fetched: %d items", len(data["data"]))
    return data



def flatten_data(raw_data: dict, snapshot_date: str) -> pd.DataFrame:
    """Flatten nested model records into a tabular DataFrame."""
    models = raw_data["data"]
    records = []
    for model in models:
        record = {
            "model_id": model["id"],
            "model_slug": model["slug"],
            "release_date": model["release_date"],
            "creator_slug": model["model_creator"]["slug"],
            "price_1m_input_tokens": model["pricing"]["price_1m_input_tokens"],
            "price_1m_output_tokens": model["pricing"]["price_1m_output_tokens"],
            "median_output_tokens_per_second": model["median_output_tokens_per_second"],
            "median_time_to_first_token_seconds": model["median_time_to_first_token_seconds"],
            "median_time_to_first_answer_token": model["median_time_to_first_answer_token"],
            "snapshot_date": snapshot_date,
        }
        for eval_key, eval_value in model.get("evaluations", {}).items():
            record[f"eval_{eval_key}"] = eval_value
        records.append(record)

    df = pd.DataFrame(records)
    df["release_date"] = pd.to_datetime(df["release_date"])
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df


def save_raw(df: pd.DataFrame, bronze_dir: Path, snapshot_date: str) -> Path:
    """Save raw flattened data as Parquet."""
    bronze_dir.mkdir(parents=True, exist_ok=True)
    path = bronze_dir / f"models_{snapshot_date}.parquet"
    df.to_parquet(path, index=False, coerce_timestamps="ms", allow_truncated_timestamps=True)
    logger.info("Saved Parquet to %s (%d rows)", path, len(df))
    return path
