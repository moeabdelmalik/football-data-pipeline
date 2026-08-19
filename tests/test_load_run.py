from __future__ import annotations

from datetime import date

import pytest

from elt.load.bigquery import BigQueryLoader
from elt.load.run import existing_uris, load, plan_uris, summarise
from tests.fakes import FakeBQClient, FakeStorageClient

INGEST_DATE = date(2026, 8, 18)


@pytest.fixture
def settings(make_settings):
    return make_settings()


def test_uris_are_derived_not_listed(config):
    """Extract and load agree on paths by construction, not by convention."""
    batches = plan_uris(config, bucket="my-bucket", ingest_date=INGEST_DATE, endpoints=["events"])

    assert len(batches) == 1
    assert batches[0].table == "tsdb_events"
    assert batches[0].uris[0] == (
        "gs://my-bucket/raw/thesportsdb/events/ingest_date=2026-08-18"
        "/league_id=4328/season=2024-2025/data.ndjson"
    )
    assert len(batches[0].uris) == 4  # 2 leagues x 2 seasons


def test_load_uris_match_what_extract_wrote(config, tmp_path):
    """The two layers must not drift apart - same function, same path."""
    from elt.extract.writer import partition_path

    task = config.plan(endpoints=["events"], league_ids=[4328], seasons=["2024-2025"])[0]
    written = partition_path(
        source=config.source, endpoint_name=task.endpoint_name,
        league_id=task.league.id, season=task.season, ingest_date=INGEST_DATE,
    )
    batch = plan_uris(
        config, bucket="my-bucket", ingest_date=INGEST_DATE,
        endpoints=["events"], league_ids=[4328], seasons=["2024-2025"],
    )[0]
    assert batch.uris == [f"gs://my-bucket/{written}"]


def test_one_batch_per_endpoint(config):
    """15 files in one MERGE, not 15 MERGEs."""
    batches = plan_uris(config, bucket="my-bucket", ingest_date=INGEST_DATE)
    assert {b.endpoint_name for b in batches} == {"events", "teams"}
    assert {b.table for b in batches} == {"tsdb_events", "tsdb_teams"}


def test_missing_objects_are_skipped_not_fatal(caplog):
    """An extract gap in one league must not block the other nineteen."""
    storage = FakeStorageClient(existing={"raw/a.ndjson"})
    present = existing_uris(["gs://b/raw/a.ndjson", "gs://b/raw/missing.ndjson"], storage_client=storage)
    assert present == ["gs://b/raw/a.ndjson"]


def test_load_raises_when_a_batch_has_no_files_at_all(config, settings):
    """Silently loading nothing is worse than failing."""
    with pytest.raises(FileNotFoundError, match="did the extract run"):
        load(
            config=config, settings=settings, ingest_date=INGEST_DATE, endpoints=["events"],
            loader=BigQueryLoader(settings, client=FakeBQClient()),
            storage_client=FakeStorageClient(existing=set()),
        )


def test_load_end_to_end_merges_each_endpoint_once(config, settings):
    bq = FakeBQClient(staged_rows=5, merged_rows=5)
    results = load(
        config=config, settings=settings, ingest_date=INGEST_DATE,
        loader=BigQueryLoader(settings, client=bq),
        storage_client=FakeStorageClient(all_exist=True),
    )

    assert {r.table for r in results} == {"tsdb_events", "tsdb_teams"}
    assert len([q for q in bq.queries if q.strip().startswith("MERGE")]) == 2
    assert len(bq.load_jobs) == 2
    assert bq.datasets, "dataset should be ensured before loading"


def test_dry_run_touches_neither_bigquery_nor_storage(config, settings):
    bq = FakeBQClient()
    results = load(
        config=config, settings=settings, ingest_date=INGEST_DATE, dry_run=True,
        loader=BigQueryLoader(settings, client=bq), storage_client=None,
    )
    assert results == []
    assert bq.queries == [] and bq.load_jobs == []


def test_load_requires_a_bucket(config, make_settings):
    settings = make_settings(gcs_raw_bucket=None)
    with pytest.raises(ValueError, match="GCS_RAW_BUCKET"):
        load(config=config, settings=settings, ingest_date=INGEST_DATE)


def test_summary_reports_files_staged_and_merged(config, settings):
    results = load(
        config=config, settings=settings, ingest_date=INGEST_DATE, endpoints=["events"],
        loader=BigQueryLoader(settings, client=FakeBQClient(staged_rows=20, merged_rows=18)),
        storage_client=FakeStorageClient(all_exist=True),
    )
    text = summarise(results)
    assert "tsdb_events" in text and "staged=20" in text and "merged=18" in text
