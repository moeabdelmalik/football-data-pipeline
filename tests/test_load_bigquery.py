from __future__ import annotations

import re
from datetime import date

import pytest

from elt.load.bigquery import RAW_TABLE_COLUMNS, BigQueryLoader
from tests.fakes import FakeBQClient

INGEST_DATE = date(2026, 8, 18)


@pytest.fixture
def settings(make_settings):
    return make_settings()


@pytest.fixture
def bq():
    return FakeBQClient(staged_rows=5, merged_rows=5)


@pytest.fixture
def loader(settings, bq):
    return BigQueryLoader(settings, client=bq)


def events(config):
    return config.endpoints["events"]


def test_table_is_partitioned_and_clustered(loader, bq):
    """Both cut bytes scanned, which is what keeps us in the free tier (NFR-6)."""
    loader.ensure_table("tsdb_events")
    sql = bq.sql_containing("CREATE TABLE")

    assert "CREATE TABLE IF NOT EXISTS `my-project.sports_raw.tsdb_events`" in sql
    assert "PARTITION BY ingest_date" in sql
    assert "CLUSTER BY league_id" in sql
    for name, type_ in RAW_TABLE_COLUMNS:
        assert f"{name} {type_}" in sql


def test_raw_table_keeps_the_record_whole():
    """The schema must not enumerate API fields - that is the whole point."""
    columns = {name for name, _ in RAW_TABLE_COLUMNS}
    assert "payload" in columns
    assert not any(c.startswith(("str", "int", "id")) for c in columns)


def test_staging_load_reads_ndjson_as_a_single_string_column(loader, bq):
    staging, rows = loader.stage_uris("tsdb_events", ["gs://b/a.ndjson"], INGEST_DATE)

    assert rows == 5
    assert staging == "my-project.sports_raw._stg_tsdb_events_20260818"

    config = bq.load_jobs[0]["job_config"]
    assert config.source_format == "CSV"          # NOT newline-delimited JSON
    assert config.field_delimiter == "\x1f"       # cannot occur inside JSON
    assert config.quote_character == ""           # quotes are JSON, not CSV syntax
    assert [f.name for f in config.schema] == ["payload"]
    assert config.write_disposition == "WRITE_TRUNCATE"


def test_staging_name_is_deterministic(loader):
    """A retry reuses and truncates the same table instead of leaving litter."""
    first = loader.staging_ref("tsdb_events", INGEST_DATE)
    second = loader.staging_ref("tsdb_events", INGEST_DATE)
    assert first == second


def test_merge_joins_on_the_configured_primary_key(loader, bq, config):
    loader.merge("tsdb_events", "stg", events(config), INGEST_DATE)
    sql = bq.sql_containing("MERGE")

    assert "MERGE `my-project.sports_raw.tsdb_events` AS T" in sql
    assert "JSON_VALUE(payload, '$.idEvent') AS record_key" in sql
    assert "ON T.record_key = S.record_key" in sql
    assert "INSERT" in sql and "UPDATE SET" in sql


def test_merge_never_appends_blindly(loader, bq, config):
    """CONSTRAINT-3: the current season mutates, so APPEND would duplicate."""
    loader.merge("tsdb_events", "stg", events(config), INGEST_DATE)
    sql = bq.sql_containing("MERGE")
    assert "INSERT INTO" not in sql
    assert re.search(r"WHEN NOT MATCHED\s+THEN\s+INSERT", sql)


def test_merge_dedupes_the_source_before_joining(loader, bq, config):
    """BigQuery aborts a MERGE when one target row matches two source rows."""
    loader.merge("tsdb_events", "stg", events(config), INGEST_DATE)
    sql = bq.sql_containing("MERGE")

    assert "QUALIFY ROW_NUMBER() OVER" in sql
    assert "PARTITION BY JSON_VALUE(payload, '$.idEvent')" in sql
    assert "ORDER BY TIMESTAMP(JSON_VALUE(payload, '$._ingested_at')) DESC" in sql


def test_update_is_guarded_so_a_stale_file_cannot_overwrite_a_fresh_score(loader, bq, config):
    loader.merge("tsdb_events", "stg", events(config), INGEST_DATE)
    sql = bq.sql_containing("MERGE")
    assert "WHEN MATCHED AND S.ingested_at >= T.ingested_at THEN UPDATE SET" in sql


def test_rows_without_a_primary_key_are_not_merged(loader, bq, config):
    loader.merge("tsdb_events", "stg", events(config), INGEST_DATE)
    assert "WHERE JSON_VALUE(payload, '$.idEvent') IS NOT NULL" in bq.sql_containing("MERGE")


def test_teams_endpoint_merges_on_its_own_key(loader, bq, config):
    loader.merge("tsdb_teams", "stg", config.endpoints["teams"], INGEST_DATE)
    sql = bq.sql_containing("MERGE")
    assert "JSON_VALUE(payload, '$.idTeam') AS record_key" in sql
    assert "idEvent" not in sql


def test_load_endpoint_runs_create_then_stage_then_merge_then_drop(loader, bq, config):
    result = loader.load_endpoint(
        table="tsdb_events", endpoint=events(config),
        uris=["gs://b/a.ndjson", "gs://b/b.ndjson"], ingest_date=INGEST_DATE,
    )

    order = [q.strip().split()[0] for q in bq.queries]
    assert order == ["CREATE", "MERGE", "DROP"]
    assert len(bq.load_jobs) == 1
    assert result.rows_staged == 5 and result.rows_merged == 5


def test_staging_is_dropped_even_when_the_merge_fails(settings, config):
    class ExplodingClient(FakeBQClient):
        def query(self, sql):
            if "MERGE" in sql:
                self.queries.append(sql)
                raise RuntimeError("merge blew up")
            return super().query(sql)

    bq = ExplodingClient()
    loader = BigQueryLoader(settings, client=bq)

    with pytest.raises(RuntimeError, match="merge blew up"):
        loader.load_endpoint(
            table="tsdb_events", endpoint=events(config),
            uris=["gs://b/a.ndjson"], ingest_date=INGEST_DATE,
        )
    assert any(q.strip().startswith("DROP") for q in bq.queries)


def test_keep_staging_leaves_the_table_for_inspection(loader, bq, config):
    loader.load_endpoint(
        table="tsdb_events", endpoint=events(config),
        uris=["gs://b/a.ndjson"], ingest_date=INGEST_DATE, keep_staging=True,
    )
    assert not any(q.strip().startswith("DROP") for q in bq.queries)
