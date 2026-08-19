"""Load entrypoint: GCS raw NDJSON -> BigQuery raw tables.

    python -m elt.load.run --dry-run
    python -m elt.load.run --endpoints events --leagues 4328 --ingest-date 2026-08-18

The object paths are not discovered by listing the bucket - they are *derived*
from the same config and the same ``partition_path`` the extractor wrote with.
Extract and load therefore agree on where data lives by construction rather
than by convention, and a narrow backfill (one league x season) selects exactly
the same slice on both sides (NFR-4).
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from elt.extract.writer import partition_path
from elt.load.bigquery import BigQueryLoader, LoadResult
from elt.util.config import Endpoint, Settings, SourceConfig
from elt.util.logging import configure_logging

log = logging.getLogger("elt.load")


@dataclass(frozen=True)
class EndpointBatch:
    """Every file belonging to one endpoint, loaded in a single MERGE."""

    endpoint_name: str
    endpoint: Endpoint
    table: str
    uris: list[str]


def plan_uris(
    config: SourceConfig,
    *,
    bucket: str,
    ingest_date: date,
    endpoints: list[str] | None = None,
    league_ids: list[int] | None = None,
    seasons: list[str] | None = None,
) -> list[EndpointBatch]:
    """Derive the gs:// URIs a matching extract run would have written."""
    tasks = config.plan(endpoints=endpoints, league_ids=league_ids, seasons=seasons)

    grouped: dict[str, list[str]] = defaultdict(list)
    endpoints_by_name: dict[str, Endpoint] = {}
    for task in tasks:
        path = partition_path(
            source=config.source,
            endpoint_name=task.endpoint_name,
            league_id=task.league.id,
            season=task.season,
            ingest_date=ingest_date,
        )
        grouped[task.endpoint_name].append(f"gs://{bucket}/{path}")
        endpoints_by_name[task.endpoint_name] = task.endpoint

    # One batch per endpoint: a single MERGE over 15 files is far cheaper than
    # 15 MERGEs, and BigQuery bills by bytes scanned, not by file.
    return [
        EndpointBatch(name, endpoints_by_name[name], endpoints_by_name[name].table, uris)
        for name, uris in grouped.items()
    ]


def existing_uris(uris: list[str], *, storage_client: Any) -> list[str]:
    """Drop URIs with no object behind them.

    Extraction tasks are independent, so a run can legitimately leave gaps: one
    league failed while nineteen succeeded. Loading should carry the nineteen
    rather than refuse everything, so missing objects are warned about and
    skipped - but a batch with *nothing* behind it is an error, not a no-op.
    """
    present = []
    for uri in uris:
        bucket_name, _, object_path = uri.removeprefix("gs://").partition("/")
        if storage_client.bucket(bucket_name).blob(object_path).exists():
            present.append(uri)
        else:
            log.warning("no object at %s - skipping", uri)
    return present


def load(
    *,
    config: SourceConfig,
    settings: Settings,
    ingest_date: date,
    endpoints: list[str] | None = None,
    league_ids: list[int] | None = None,
    seasons: list[str] | None = None,
    loader: BigQueryLoader | None = None,
    storage_client: Any = None,
    keep_staging: bool = False,
    dry_run: bool = False,
) -> list[LoadResult]:
    if not settings.gcs_raw_bucket:
        raise ValueError("GCS_RAW_BUCKET must be set to load from Cloud Storage (see .env.example)")

    batches = plan_uris(
        config,
        bucket=settings.gcs_raw_bucket,
        ingest_date=ingest_date,
        endpoints=endpoints,
        league_ids=league_ids,
        seasons=seasons,
    )
    log.info("planned %d batch(es) for ingest_date=%s", len(batches), ingest_date)

    if dry_run:
        for batch in batches:
            log.info("  would MERGE %d file(s) -> %s", len(batch.uris), batch.table)
            for uri in batch.uris:
                log.info("      %s", uri)
        return []

    if storage_client is None:
        from google.cloud import storage

        storage_client = storage.Client(project=settings.gcp_project_id)
    loader = loader or BigQueryLoader(settings)
    loader.ensure_dataset()

    results = []
    for batch in batches:
        uris = existing_uris(batch.uris, storage_client=storage_client)
        if not uris:
            raise FileNotFoundError(
                f"no raw files found for endpoint '{batch.endpoint_name}' at "
                f"ingest_date={ingest_date} - did the extract run?"
            )
        log.info("loading %s: %d file(s)", batch.table, len(uris))
        results.append(
            loader.load_endpoint(
                table=batch.table,
                endpoint=batch.endpoint,
                uris=uris,
                ingest_date=ingest_date,
                keep_staging=keep_staging,
            )
        )
    return results


def summarise(results: list[LoadResult]) -> str:
    lines = ["", "=" * 62]
    for result in results:
        lines.append(
            f"  {result.table:<14} files={len(result.source_uris):<3} "
            f"staged={result.rows_staged:<6} merged={result.rows_merged}"
        )
    lines.append("=" * 62)
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="elt.load.run",
        description="Load raw NDJSON from GCS into BigQuery with MERGE.",
    )
    parser.add_argument("--config", help="path to a source YAML")
    parser.add_argument("--endpoints", nargs="+", help="subset of configured endpoints")
    parser.add_argument("--leagues", nargs="+", type=int, help="subset of configured league ids")
    parser.add_argument("--seasons", nargs="+", help="subset of configured seasons")
    parser.add_argument(
        "--ingest-date",
        type=date.fromisoformat,
        help="which ingest_date partition to load (YYYY-MM-DD, default: today UTC)",
    )
    parser.add_argument("--keep-staging", action="store_true", help="leave staging tables for inspection")
    parser.add_argument("--dry-run", action="store_true", help="print the files that would be merged")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    config = SourceConfig.load(args.config)
    settings = Settings.from_env()

    results = load(
        config=config,
        settings=settings,
        ingest_date=args.ingest_date or datetime.now(UTC).date(),
        endpoints=args.endpoints,
        league_ids=args.leagues,
        seasons=args.seasons,
        keep_staging=args.keep_staging,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        return 0
    print(summarise(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
