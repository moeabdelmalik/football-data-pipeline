"""Extraction entrypoint.

    python -m elt.extract.run --dry-run
    python -m elt.extract.run --endpoints events --leagues 4328 --seasons 2024-2025

Design note: tasks are **independent**. One league x season failing does not
abort the other nineteen - each is fetched, written and reported on its own,
and the process exits non-zero if any failed. That mirrors how Airflow will
run this in Phase 4 (one mapped task per unit, retried individually) and it
means a single flaky call never forces a full re-extract.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime

from elt.extract.client import ExtractionError, TheSportsDBClient
from elt.extract.writer import RawWriter, build_writer, partition_path, stamp_records
from elt.util.config import ExtractTask, Settings, SourceConfig
from elt.util.logging import configure_logging

log = logging.getLogger("elt.extract")


@dataclass
class TaskOutcome:
    task: ExtractTask
    ok: bool
    record_count: int = 0
    uri: str | None = None
    error: str | None = None


def extract_task(
    task: ExtractTask,
    *,
    client: TheSportsDBClient,
    writer: RawWriter,
    source: str,
    ingest_date: date,
    ingested_at: datetime | None = None,
) -> TaskOutcome:
    """Fetch one endpoint call and land it. The single unit of work."""
    try:
        rows = client.fetch(task.endpoint, task.params)
        stamped = stamp_records(rows, task=task, source=source, ingested_at=ingested_at)
        object_path = partition_path(
            source=source,
            endpoint_name=task.endpoint_name,
            league_id=task.league.id,
            season=task.season,
            ingest_date=ingest_date,
        )
        result = writer.write(stamped, object_path)
        return TaskOutcome(task, ok=True, record_count=result.record_count, uri=result.uri)
    except ExtractionError as exc:
        # Expected failure modes are caught per task so the run continues.
        log.error("FAILED %s: %s", task.key, exc)
        return TaskOutcome(task, ok=False, error=str(exc))


def run(
    *,
    config: SourceConfig,
    settings: Settings,
    destination: str,
    endpoints: list[str] | None = None,
    league_ids: list[int] | None = None,
    seasons: list[str] | None = None,
    ingest_date: date | None = None,
    dry_run: bool = False,
) -> list[TaskOutcome]:
    tasks = config.plan(endpoints=endpoints, league_ids=league_ids, seasons=seasons)
    ingest_date = ingest_date or datetime.now(UTC).date()

    log.info("planned %d task(s) | destination=%s | ingest_date=%s", len(tasks), destination, ingest_date)
    if dry_run:
        # No client, no writer, no network - just show the plan.
        for task in tasks:
            log.info("  would fetch %-28s params=%s", task.key, task.params)
        return []

    client = TheSportsDBClient(settings.api_key, config)
    writer = build_writer(destination, settings)
    # One timestamp for the whole run, so every row of a run shares a batch time.
    ingested_at = datetime.now(UTC)

    outcomes = []
    for index, task in enumerate(tasks, start=1):
        log.info("[%d/%d] %s", index, len(tasks), task.key)
        outcomes.append(
            extract_task(
                task,
                client=client,
                writer=writer,
                source=config.source,
                ingest_date=ingest_date,
                ingested_at=ingested_at,
            )
        )
    return outcomes


def summarise(outcomes: list[TaskOutcome]) -> str:
    succeeded = [o for o in outcomes if o.ok]
    failed = [o for o in outcomes if not o.ok]
    rows = sum(o.record_count for o in succeeded)
    lines = [
        "",
        "=" * 62,
        f"  tasks ok : {len(succeeded)}/{len(outcomes)}",
        f"  records  : {rows}",
    ]
    if failed:
        lines.append(f"  FAILED   : {len(failed)}")
        lines += [f"    - {o.task.key}: {o.error}" for o in failed]
    lines.append("=" * 62)
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="elt.extract.run",
        description="Extract TheSportsDB data and land it as raw NDJSON.",
    )
    parser.add_argument("--config", help="path to a source YAML (defaults to config/sources/thesportsdb.yml)")
    parser.add_argument("--endpoints", nargs="+", help="subset of configured endpoints, e.g. events")
    parser.add_argument("--leagues", nargs="+", type=int, help="subset of configured league ids")
    parser.add_argument("--seasons", nargs="+", help="subset of configured seasons, e.g. 2024-2025")
    parser.add_argument(
        "--dest",
        choices=["local", "gcs"],
        help="where to land (default: gcs if a bucket is set)",
    )
    parser.add_argument(
        "--ingest-date",
        type=lambda s: date.fromisoformat(s),
        help="override the ingest_date partition (YYYY-MM-DD) - use to make a backfill reproducible",
    )
    parser.add_argument("--dry-run", action="store_true", help="print the plan without calling the API")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    config = SourceConfig.load(args.config)
    settings = Settings.from_env()
    destination = args.dest or settings.default_destination

    outcomes = run(
        config=config,
        settings=settings,
        destination=destination,
        endpoints=args.endpoints,
        league_ids=args.leagues,
        seasons=args.seasons,
        ingest_date=args.ingest_date,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        return 0

    print(summarise(outcomes))
    # Non-zero exit on any failure is what makes Airflow mark the task failed.
    return 1 if any(not o.ok for o in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
