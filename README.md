# LLM Benchmark ELT

A daily ELT pipeline that ingests LLM performance benchmarks from the Artificial Analysis API, stores raw snapshots in a Bronze layer, produces a cleaned Delta Lake Silver table, and builds Gold analytics tables (a scored model leaderboard and snapshot-over-snapshot trends). Orchestrated with Apache Airflow.

## How It Works

Airflow triggers daily at midnight UTC → extracts model data (intelligence scores, latency, pricing) from the API → saves raw JSON + flattened Parquet to Bronze → PySpark applies cleaning rules (dedup, type casting, zero-to-null for sentinel values, column renaming) → writes to a partitioned Delta Lake Silver table → verifies row counts and partition integrity → builds Gold analytics tables (model leaderboard + trends) → runs a sanity check on the Gold output.

Each pipeline run captures a daily snapshot. Delta Lake preserves historical snapshots with partition-scoped overwrites and time travel.

## Data Source

[Artificial Analysis API](https://artificialanalysis.ai/api-reference) — benchmarks of 500+ LLM models covering intelligence evaluations, output speed, latency, and pricing.

## Pipeline Layers

| Layer | Storage | Contents |
|-------|---------|----------|
| Bronze | `data/bronze/` | Raw JSON (`raw_YYYY-MM-DD.json`) + flattened Parquet (`models_YYYY-MM-DD.parquet`) |
| Silver | `data/silver/models/` | Delta Lake table partitioned by `snapshot_date`, cleaned and typed |
| Gold | `data/gold/model_leaderboard/`, `data/gold/model_trends/` | Delta Lake analytics tables partitioned by `snapshot_date` |

### Gold Tables

- **`model_leaderboard`** — one row per model for the snapshot, ranked by a composite score that blends intelligence (50%), speed (30%), and price (20%) after min-max normalization. Also includes cost-efficiency columns: average price per 1M tokens, intelligence-per-dollar, a cost tier (budget / mid / premium based on price percentiles), and an efficiency rank. Only models with all scoring metrics present are ranked.
- **`model_trends`** — per-model metric deltas (intelligence, speed, price) between each snapshot and the immediately preceding one, with both current and previous values retained for context.

### How "Best Model" Is Defined (and Why It Changes)

The leaderboard scores each model as `intelligence (50%) + speed (30%) + price (20%)`, so rank 1 is the best *all-rounder under those weights*. It also exposes `efficiency_rank` (intelligence per dollar) as a separate "best value" lens. Reweighting would change the winner — a speed-optimized model can outrank a smarter, pricier one.

A few important properties of the ranking:

- **The leaderboard is per-snapshot, not cumulative.** Each day's ranking is computed only from that day's models. Ingesting more daily snapshots adds new partitions and feeds `model_trends`; it does not retroactively change a past day's leaderboard.
- **Scores are relative to the day's field.** Metrics are min-max normalized against the min/max of the models present in that snapshot, and cost tiers use that snapshot's price percentiles. So a model's rank can shift even when its own metrics are unchanged — simply because the competing set changed (a new model entered, or one was dropped).
- **Normalization is outlier-sensitive.** A single extreme model (very cheap, or very fast) stretches the range and compresses everyone else's normalized scores. As the model pool grows, a more robust scheme (percentile rank, z-score with clipping, or log-scaled price) would stabilize the scores.

In short: the leaderboard answers "best relative to today's field," and `model_trends` is what lets you watch that ranking move over time.

## Why Delta Lake

The Silver and Gold layers use Delta Lake:

- **Partition-scoped overwrites** — each daily run only replaces its own `snapshot_date` partition, preserving all historical data
- **ACID transactions** — writes either fully succeed or fully roll back, no partial/corrupt state
- **Time travel** — query any previous version of the table to see how benchmarks looked on a past date
- **Schema evolution** — when the API adds new evaluation metrics, Delta handles new columns without breaking existing queries
- **Transaction log** — every write is tracked with version, timestamp, and operation type

## Setup

```bash
pip install -r requirements.txt
```

Add your API key to `.env`:
```
LLM_BENCHMARK_API=your_api_key_here
```

Get a key from [Artificial Analysis](https://artificialanalysis.ai/login).

## Usage

CLI (single run — Bronze → Silver → Gold):
```bash
python -m src
```

Notebook (interactive):
```bash
jupyter notebook notebooks/pipeline_simulation.ipynb
```

Airflow (daily scheduled):
```bash
docker compose up airflow-init   # one-time: migrate DB + create admin user
docker compose up -d             # start webserver + scheduler
```

## Project Structure

```
llm-benchmark-elt/
├── data/
│   ├── bronze/
│   ├── silver/models/
│   └── gold/
│       ├── model_leaderboard/
│       └── model_trends/
├── src/
│   ├── __main__.py            # CLI entry point: runs full Bronze → Silver → Gold
│   ├── core/
│   │   ├── config.py          # paths, API config, snapshot date
│   │   ├── logging.py         # logging setup
│   │   └── spark_session.py   # Delta-enabled SparkSession
│   ├── bronze/
│   │   └── ingest.py          # API fetch, flatten, save raw
│   ├── silver/
│   │   ├── transform.py       # cleaning rules + column renaming
│   │   └── write_delta.py     # Delta write + verification
│   └── gold/
│       ├── schemas.py         # output table schemas
│       ├── transform.py       # leaderboard scoring + trend deltas
│       └── write_delta.py     # schema validation + Delta write
├── dags/                      
│   └── llm_benchmark_dag.py   # orchestrator
|
├── notebooks/
│   └── pipeline_simulation.ipynb
|
├── docker-compose.yml
├── requirements.txt
├── .env
└── .gitignore
```

## Airflow DAG

Schedule: `0 0 * * *` (daily at 00:00 UTC)

```
extract → bronze_save → silver_write → verify → gold_build → gold_quality_check
```

Each task is isolated — if Silver fails, Bronze doesn't re-run on retry.

## Tech Stack

- Python 3.11
- PySpark 3.5 + Delta Spark
- deltalake (delta-rs) for writes
- pandas + pyarrow
- Apache Airflow 2.9
- Docker Compose
