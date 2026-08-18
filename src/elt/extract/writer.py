"""Landing raw rows as NDJSON, on disk or in Cloud Storage.

Three rules govern this layer:

1. **Never edit the payload.** Fields arrive as strings (CONSTRAINT-4) and
   they stay strings. Casting is dbt's job. If we cast here and get it wrong,
   the original is gone.
2. **NDJSON, not a JSON array.** One object per line is what BigQuery's
   ``NEWLINE_DELIMITED_JSON`` format expects, and it streams - a reader never
   has to hold the whole file in memory.
3. **Deterministic paths.** The same task on the same ingest date writes to
   the same object, so a re-run overwrites rather than accumulating. That is
   NFR-3 (idempotency) enforced at the storage layer, before BigQuery is even
   involved - a retried Airflow task cannot double-land a file.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from elt.util.config import ExtractTask, Settings

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class WriteResult:
    uri: str
    record_count: int
    byte_count: int


def partition_path(
    *,
    source: str,
    endpoint_name: str,
    league_id: int,
    season: str | None,
    ingest_date: date,
) -> str:
    """Build the object path for one task's output.

    Hive-style ``key=value`` directories are not decoration: BigQuery can read
    them as partition columns from an external table, and a human can delete
    exactly one bad league x season with a prefix match.

        raw/thesportsdb/events/ingest_date=2026-08-18/league_id=4328/season=2026-2027/data.ndjson
    """
    parts = [
        "raw",
        source,
        endpoint_name,
        f"ingest_date={ingest_date.isoformat()}",
        f"league_id={league_id}",
    ]
    if season:
        parts.append(f"season={season}")
    parts.append("data.ndjson")
    return "/".join(parts)


def stamp_records(
    records: Iterable[dict[str, Any]],
    *,
    task: ExtractTask,
    source: str,
    ingested_at: datetime | None = None,
) -> list[dict[str, Any]]:
    """Attach ingestion lineage to each row, without touching source fields.

    Underscore-prefixed keys cannot collide with TheSportsDB's ``strX``/``idX``
    naming, so the boundary between "what the API said" and "what we know
    about the fetch" stays unambiguous all the way into the warehouse.

    ``_ingested_at`` is what the load layer will use as the MERGE tie-breaker
    when the same match arrives twice with different scores (CONSTRAINT-3).
    """
    ingested_at = ingested_at or datetime.now(UTC)
    meta = {
        "_source": source,
        "_endpoint": task.endpoint_name,
        "_league_id": str(task.league.id),
        "_season": task.season,
        "_ingested_at": ingested_at.isoformat(),
    }
    # New dicts - the caller's list is left untouched.
    return [{**record, **meta} for record in records]


def to_ndjson(records: Iterable[dict[str, Any]]) -> bytes:
    """Serialise to newline-delimited JSON.

    ``ensure_ascii=False`` keeps club names such as "Bayern München" readable
    in the raw file instead of escaping them into \\uXXXX noise.
    """
    lines = [json.dumps(record, ensure_ascii=False, separators=(",", ":")) for record in records]
    return ("\n".join(lines) + "\n").encode("utf-8") if lines else b""


class RawWriter(Protocol):
    """Anything that can put bytes at a path. Keeps callers storage-agnostic."""

    def write(self, records: list[dict[str, Any]], object_path: str) -> WriteResult: ...


class LocalRawWriter:
    """Writes under a local directory.

    Not just a test fixture: it lets the whole extraction layer be developed
    and demonstrated with no GCP project, no billing account and no service
    account key. The GCS writer then only has to be correct about one thing -
    upload.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def write(self, records: list[dict[str, Any]], object_path: str) -> WriteResult:
        target = self.root / object_path
        target.parent.mkdir(parents=True, exist_ok=True)
        body = to_ndjson(records)
        target.write_bytes(body)  # truncates - same path, same result
        log.info("wrote %d record(s) -> %s", len(records), target)
        return WriteResult(uri=target.as_uri(), record_count=len(records), byte_count=len(body))


class GCSRawWriter:
    """Writes to a Cloud Storage bucket (NFR-5: raw retained indefinitely)."""

    def __init__(self, bucket_name: str, *, project: str | None = None, client: Any = None) -> None:
        if client is None:
            # Imported lazily so that a local-only run - and the test suite -
            # never needs google-cloud-storage to be configured.
            from google.cloud import storage

            client = storage.Client(project=project)
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = client.bucket(bucket_name)

    def write(self, records: list[dict[str, Any]], object_path: str) -> WriteResult:
        body = to_ndjson(records)
        blob = self.bucket.blob(object_path)
        # A GCS object write is atomic: readers see the old object until the
        # new one is complete, so a crashed run never leaves a half file.
        blob.upload_from_string(body, content_type="application/x-ndjson")
        uri = f"gs://{self.bucket_name}/{object_path}"
        log.info("wrote %d record(s) -> %s", len(records), uri)
        return WriteResult(uri=uri, record_count=len(records), byte_count=len(body))


def build_writer(destination: str, settings: Settings) -> RawWriter:
    if destination == "local":
        return LocalRawWriter(settings.local_raw_dir)
    if destination == "gcs":
        if not settings.gcs_raw_bucket:
            raise ValueError("destination 'gcs' requires GCS_RAW_BUCKET to be set (see .env.example)")
        return GCSRawWriter(settings.gcs_raw_bucket, project=settings.gcp_project_id)
    raise ValueError(f"unknown destination '{destination}' (expected 'local' or 'gcs')")
