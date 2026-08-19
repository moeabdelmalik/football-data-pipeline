from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from elt.extract.writer import (
    LocalRawWriter,
    build_writer,
    partition_path,
    stamp_records,
    to_ndjson,
)


def task(config, endpoint="events", league_index=0, season="2024-2025"):
    tasks = config.plan(endpoints=[endpoint])
    return next(t for t in tasks if t.season == season or t.season is None)


def test_partition_path_is_hive_style():
    path = partition_path(
        source="thesportsdb", endpoint_name="events", league_id=4328,
        season="2024-2025", ingest_date=date(2026, 8, 18),
    )
    assert path == (
        "raw/thesportsdb/events/ingest_date=2026-08-18/league_id=4328/season=2024-2025/data.ndjson"
    )


def test_partition_path_omits_season_for_league_grain():
    path = partition_path(
        source="thesportsdb", endpoint_name="teams", league_id=4328,
        season=None, ingest_date=date(2026, 8, 18),
    )
    assert "season=" not in path
    assert path.endswith("league_id=4328/data.ndjson")


def test_to_ndjson_is_one_object_per_line():
    body = to_ndjson([{"a": 1}, {"a": 2}]).decode()
    assert [json.loads(line) for line in body.splitlines()] == [{"a": 1}, {"a": 2}]


def test_to_ndjson_keeps_accented_names_readable():
    assert "München" in to_ndjson([{"strTeam": "Bayern München"}]).decode()


def test_to_ndjson_of_nothing_is_empty_not_a_blank_line():
    assert to_ndjson([]) == b""


def test_stamp_records_adds_lineage_without_mutating_the_source(config):
    original = [{"idEvent": "1", "intHomeScore": "2"}]
    stamped = stamp_records(
        original, task=task(config), source="thesportsdb",
        ingested_at=datetime(2026, 8, 18, 3, 0, tzinfo=UTC),
    )
    assert original == [{"idEvent": "1", "intHomeScore": "2"}]        # untouched
    assert stamped[0]["idEvent"] == "1"                               # source kept verbatim
    assert stamped[0]["intHomeScore"] == "2"                          # still a string
    assert stamped[0]["_league_id"] == "4328"
    assert stamped[0]["_endpoint"] == "events"
    assert stamped[0]["_ingested_at"] == "2026-08-18T03:00:00+00:00"


def test_local_writer_round_trips(tmp_path):
    writer = LocalRawWriter(tmp_path)
    result = writer.write([{"idEvent": "1"}], "raw/x/data.ndjson")
    assert result.record_count == 1
    assert json.loads((tmp_path / "raw/x/data.ndjson").read_text()) == {"idEvent": "1"}


def test_rewriting_the_same_path_overwrites(tmp_path):
    """The idempotency guarantee (NFR-3), enforced at the storage layer."""
    writer = LocalRawWriter(tmp_path)
    writer.write([{"idEvent": "1"}, {"idEvent": "2"}], "raw/x/data.ndjson")
    writer.write([{"idEvent": "1"}, {"idEvent": "2"}], "raw/x/data.ndjson")

    files = list(tmp_path.rglob("*.ndjson"))
    assert len(files) == 1
    assert len(files[0].read_text().splitlines()) == 2  # not 4


class FakeBlob:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def upload_from_string(self, data, content_type=None):
        self.store[self.name] = (data, content_type)


class FakeBucket:
    def __init__(self, store):
        self.store = store

    def blob(self, name):
        return FakeBlob(self.store, name)


class FakeGCSClient:
    def __init__(self):
        self.store = {}

    def bucket(self, name):
        return FakeBucket(self.store)


def test_gcs_writer_uploads_ndjson_and_returns_a_gs_uri():
    from elt.extract.writer import GCSRawWriter

    fake = FakeGCSClient()
    writer = GCSRawWriter("my-bucket", client=fake)
    result = writer.write([{"idEvent": "1"}], "raw/x/data.ndjson")

    assert result.uri == "gs://my-bucket/raw/x/data.ndjson"
    data, content_type = fake.store["raw/x/data.ndjson"]
    assert content_type == "application/x-ndjson"
    assert json.loads(data.decode()) == {"idEvent": "1"}


def test_build_writer_rejects_gcs_without_a_bucket(make_settings):
    settings = make_settings(gcs_raw_bucket=None)
    assert isinstance(build_writer("local", settings), LocalRawWriter)
    with pytest.raises(ValueError, match="GCS_RAW_BUCKET"):
        build_writer("gcs", settings)
    with pytest.raises(ValueError, match="unknown destination"):
        build_writer("s3", settings)
