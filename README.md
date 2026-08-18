# Football Data Pipeline

An ELT pipeline that ingests European football results from TheSportsDB and
models them into league standings and team form.

**Stack:** Python · Google Cloud Storage · BigQuery · dbt · Airflow · Docker

## Status

| Phase | Status |
|---|---|
| Requirements | ✅ [docs/requirements.md](docs/requirements.md) |
| Architecture | ✅ [docs/architecture.md](docs/architecture.md) |
| Extraction | 🔨 In progress |
| Load | ⬜ |
| Transform (dbt) | ⬜ |
| Orchestration (Airflow) | ⬜ |

## What it does

Pulls match results for 5 European leagues across 3 seasons, lands the raw JSON
in Cloud Storage, loads it to BigQuery, and uses dbt to build a star schema at a
grain of **one row per team, per match** — which makes a league table a single
`GROUP BY`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your values
```

See [docs/requirements.md](docs/requirements.md) for the full specification.
