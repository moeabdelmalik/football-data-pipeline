# Football Data Pipeline

An ELT pipeline that ingests European football results from TheSportsDB and
models them into league standings and team form.

**Stack:** Python · Google Cloud Storage · BigQuery · dbt · Airflow · Docker

## Status

| Phase | Status |
|---|---|
| Requirements | ✅ [docs/requirements.md](docs/requirements.md) |
| Architecture | ✅ [docs/architecture.md](docs/architecture.md) |
| Extraction | ✅ Config-driven, retrying, rate-limited |
| Load | ✅ GCS → BigQuery, MERGE-based |
| Transform (dbt) | 🔨 Next |
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

## Running the extractor

No GCP account is needed - `--dest local` lands files on disk in exactly the
layout the bucket uses.

```bash
# See what a run would do, without calling the API
python -m elt.extract.run --dry-run

# Pull one league x season to disk
python -m elt.extract.run --dest local --endpoints events --leagues 4328 --seasons 2024-2025

# Full scope: 5 leagues x 3 seasons of events, plus team dimensions (20 calls)
python -m elt.extract.run --dest gcs
```

Extraction scope lives in [config/sources/thesportsdb.yml](config/sources/thesportsdb.yml).
Adding a league or season is a config change, not a code change.

Raw files land at a deterministic, Hive-partitioned path:

```
raw/thesportsdb/events/ingest_date=2026-08-18/league_id=4328/season=2024-2025/data.ndjson
```

Same task, same ingest date, same path - so a re-run overwrites instead of
accumulating. That is where idempotency (NFR-3) starts.

## Loading to BigQuery

Reads the files the extractor wrote and MERGEs them into the raw dataset.
Requires GCP credentials.

```bash
python -m elt.load.run --dry-run                 # list the files that would merge
python -m elt.load.run --ingest-date 2026-08-18  # load one day's landing
```

The raw table keeps each record **whole**, in a single `payload` column,
alongside its lineage:

| column | |
|---|---|
| `record_key` | `idEvent` / `idTeam` — what the MERGE joins on |
| `payload` | the JSON record exactly as received |
| `source`, `endpoint`, `league_id`, `season` | lineage, stamped at extract time |
| `ingested_at`, `ingest_date`, `loaded_at` | when it was fetched and loaded |

A 69-field team record would otherwise mean 69 hand-maintained columns that
break whenever the API adds a field. Here schema drift can't break the load —
dbt picks fields out with `JSON_VALUE()` in staging, where a change is a
one-line edit. Loads are `MERGE`, never `APPEND`, because the current season
mutates in place (CONSTRAINT-3).

```bash
pytest        # 56 tests, no network or GCP account required
ruff check .
```

See [docs/requirements.md](docs/requirements.md) for the full specification.
