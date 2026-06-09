import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

API_URL = "https://artificialanalysis.ai/api/v2/data/llms/models"
ROOT_DIR = Path(__file__).resolve().parents[2]


def load_config() -> dict:
    """Load configuration and compute runtime values."""
    load_dotenv(ROOT_DIR / ".env")

    # Read API key
    api_key = os.environ.get("LLM_BENCHMARK_API", "").strip()
    if not api_key:
        raise EnvironmentError(
            "Required environment variable 'LLM_BENCHMARK_API' is not set or empty. "
        )

    # Resolve output directories
    bronze_dir = ROOT_DIR / "data" / "bronze"
    silver_dir = ROOT_DIR / "data" / "silver"

    # Create directories if they don't exist
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Compute snapshot date
    snapshot_date = date.today().isoformat()

    return {
        "api_url": API_URL,
        "api_key": api_key,
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "snapshot_date": snapshot_date,
    }
