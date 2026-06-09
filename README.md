# LLM Benchmark ELT

A daily ELT pipeline that ingests LLM performance benchmarks from the Artificial Analysis API, stores raw snapshots in a Bronze layer, and produces a cleaned Delta Lake Silver table. Orchestrated with Apache Airflow.

## How It Works

Airflow triggers daily at midnight UTC → extracts model data (intelligence scores, latency, pricing) from the API → saves raw JSON + flattened Parquet to Bronze → PySpark applies cleaning rules (dedup, type casting, zero-to-null for sentinel values) → writes to a partitioned Delta Lake table → verifies row counts and partition integrity.

Each pipeline run captures a daily snapshot. Delta Lake preserves historical snapshots with partition-scoped overwrites and time travel.

## Data Source

[Artificial Analysis API](https://artificialanalysis.ai/api-reference) — independent benchmarks of 500+ LLM models covering intelligence evaluations, output speed, latency, and pricing.

## Pipeline Layers

| Layer | Storage | Contents |
|-------|---------|----------|
| Bronze | `data/bronze/` | Raw JSON (`raw_YYYY-MM-DD.json`) + flattened Parquet (`models_YYYY-MM-DD.parquet`) |
| Silver | `data/silver/models/` | Delta Lake table partitioned by `snapshot_date`, cleaned and typed |

## Why Delta Lake

The Silver layer uses Delta Lake instead of plain Parquet for several reasons:

- **Partition-scoped overwrites** — each daily run only replaces its own `snapshot_date` partition, preserving all historical data untouched
- **ACID transactions** — writes either fully succeed or fully roll back, no partial/corrupt state
- **Time travel** — query any previous version of the table to see how benchmarks looked on a past date
- **Schema evolution** — when the API adds new evaluation metrics, Delta handles new columns without breaking existing queries
- **Transaction log** — every write is tracked with version, timestamp, and operation type for full auditability

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

CLI (single run):
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
# UI at http://localhost:8080 (admin/admin)
```

## Project Structure

```
llm-benchmark-elt/
├── src/
│   ├── __main__.py           
│   ├── core/
│   │   ├── config.py          
│   │   ├── logging.py         
│   │   └── spark_session.py  
│   ├── bronze/
│   │   └── ingest.py         
│   └── silver/
│       ├── transform.py       
│       └── write_delta.py     
├── dags/
│   └── llm_benchmark_dag.py   
├── notebooks/
│   └── pipeline_simulation.ipynb  
├── data/                       
│   ├── bronze/                 
│   └── silver/models/          
├── docker-compose.yml          
├── requirements.txt           
├── .env                     
└── .gitignore
```

## Airflow DAG

Schedule: `0 0 * * *` (daily at 00:00 UTC)

```
extract → bronze_save → silver_transform → delta_write → verify
```

Each task is isolated — if Silver fails, Bronze doesn't re-run on retry.

## Tech Stack

- Python 3.11
- PySpark 3.5 + Delta Spark
- deltalake (delta-rs) for writes
- pandas + pyarrow
- Apache Airflow 2.9
- Docker Compose
