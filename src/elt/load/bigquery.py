"""Load raw NDJSON from Cloud Storage into BigQuery, idempotently.

The shape of the raw table is the central decision here. A TheSportsDB team
record has 69 fields and an event 30, all of them strings (CONSTRAINT-4), and
the API is unversioned - fields appear and disappear without notice. Mapping
each to its own column would mean 69 lines of hand-maintained schema that
*breaks the pipeline* the day a field is added.

So the raw table keeps the whole record as one ``payload`` string, alongside
the handful of things we genuinely know about it (key, lineage, load time).
Consequences:

* schema drift can never break the load - a new field just rides along inside
  the payload and is available to dbt the moment someone wants it;
* the raw layer is honestly "as received", which is what makes a re-run from
  raw meaningful (see architecture.md);
* the cost is one ``JSON_VALUE()`` call per field in staging - dbt's job, in
  SQL, where changing it is a one-line edit rather than a failed load.

Idempotency (NFR-3) is enforced by MERGE on the source's natural key, never
APPEND, because the current season mutates in place (CONSTRAINT-3).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

from elt.util.config import Endpoint, Settings

log = logging.getLogger(__name__)

# Every raw table has this shape, whatever the endpoint. Written as plain
# strings so the module carries no import-time dependency on the BigQuery SDK.
RAW_TABLE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("record_key", "STRING"),    # the source's natural key - MERGE joins on this
    ("payload", "STRING"),       # the record exactly as received, verbatim
    ("source", "STRING"),
    ("endpoint", "STRING"),
    ("league_id", "STRING"),
    ("season", "STRING"),
    ("ingested_at", "TIMESTAMP"),  # when we fetched it (the MERGE tie-breaker)
    ("ingest_date", "DATE"),       # partition column
    ("loaded_at", "TIMESTAMP"),    # when this row reached BigQuery
)
# Deliberately absent: a source_uri column. BigQuery's _FILE_NAME pseudo-column
# only exists on external tables, not on one filled by a load job - and the file
# is anyway a pure function of (endpoint, ingest_date, league_id, season), all of
# which are already stamped on every row. See writer.partition_path.


@dataclass(frozen=True)
class LoadResult:
    table: str
    source_uris: list[str]
    rows_staged: int
    rows_merged: int


class BigQueryLoader:
    """Stage GCS files, then MERGE them into the raw table."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: Any = None,
        dataset: str | None = None,
    ) -> None:
        if client is None:
            # Lazy, for the same reason as the GCS writer: local work and the
            # test suite must not require the SDK to be configured.
            from google.cloud import bigquery

            client = bigquery.Client(project=settings.gcp_project_id)
        self.client = client
        self.settings = settings
        self.project = settings.gcp_project_id
        self.dataset = dataset or settings.bq_dataset_raw

    # --- naming ----------------------------------------------------------

    def table_ref(self, table: str) -> str:
        return f"{self.project}.{self.dataset}.{table}"

    def staging_ref(self, table: str, ingest_date: date) -> str:
        # Deterministic staging name, not a random one: a retry after a crash
        # reuses and truncates the same table instead of leaving litter behind.
        return f"{self.project}.{self.dataset}._stg_{table}_{ingest_date:%Y%m%d}"

    # --- DDL -------------------------------------------------------------

    def ensure_dataset(self) -> None:
        from google.cloud import bigquery

        dataset = bigquery.Dataset(f"{self.project}.{self.dataset}")
        dataset.location = self.settings.gcp_region
        self.client.create_dataset(dataset, exists_ok=True)
        log.info("dataset ready: %s.%s", self.project, self.dataset)

    def ensure_table(self, table: str) -> None:
        """Create the raw table if absent.

        Partitioned by ingest_date and clustered by league_id: both cut the
        bytes a query scans, which is what keeps this inside the BigQuery
        free tier (NFR-6).
        """
        columns = ",\n            ".join(f"{name} {type_}" for name, type_ in RAW_TABLE_COLUMNS)
        self.query(
            f"""
            CREATE TABLE IF NOT EXISTS `{self.table_ref(table)}` (
            {columns}
            )
            PARTITION BY ingest_date
            CLUSTER BY league_id
            """
        )
        log.info("table ready: %s", self.table_ref(table))

    # --- load ------------------------------------------------------------

    def stage_uris(self, table: str, uris: list[str], ingest_date: date) -> tuple[str, int]:
        """Load GCS objects into a staging table as one string column per line.

        NDJSON is loaded *as CSV* with a delimiter that cannot occur in JSON
        (unit separator, 0x1f) and quoting disabled. BigQuery therefore finds
        exactly one field per line and hands back the untouched JSON text.

        Loading it as NEWLINE_DELIMITED_JSON would instead map each JSON field
        to its own column - reintroducing the 69-column schema this design
        exists to avoid.
        """
        from google.cloud import bigquery

        staging = self.staging_ref(table, ingest_date)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="\x1f",
            quote_character="",
            schema=[bigquery.SchemaField("payload", "STRING")],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=False,
        )
        job = self.client.load_table_from_uri(uris, staging, job_config=job_config)
        job.result()  # blocks; raises on failure
        rows = int(getattr(job, "output_rows", 0) or 0)
        log.info("staged %d row(s) from %d uri(s) -> %s", rows, len(uris), staging)
        return staging, rows

    def merge(self, table: str, staging: str, endpoint: Endpoint, ingest_date: date) -> int:
        """MERGE staged rows into the raw table on the natural key.

        Two things in this statement are load-bearing:

        1. **QUALIFY dedupes the source.** BigQuery aborts a MERGE if one
           target row matches two source rows, and that is not hypothetical
           here: a match can legitimately appear in two files of the same run
           (two leagues, overlapping fixtures) or twice after a partial retry.
           Keeping only the newest row per key by ``_ingested_at`` makes the
           statement total.
        2. **The UPDATE is guarded by ingested_at.** Re-loading an older file
           must not overwrite a fresher score with a stale one - which is
           exactly the risk when backfilling while the current season is in
           flight (CONSTRAINT-3).

        Known cost trade-off: the join is on record_key, which says nothing
        about ingest_date, so this MERGE scans every partition rather than
        pruning to one. At ~10k rows that is far inside the free tier (NFR-6).
        If this table ever grew large enough to matter, the fix is to carry a
        match date on the row and partition by that instead.
        """
        columns = [name for name, _ in RAW_TABLE_COLUMNS]
        update_set = ",\n                    ".join(
            f"T.{col} = S.{col}" for col in columns if col != "record_key"
        )
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"S.{col}" for col in columns)

        sql = f"""
            MERGE `{self.table_ref(table)}` AS T
            USING (
                SELECT
                    JSON_VALUE(payload, '$.{endpoint.primary_key}') AS record_key,
                    payload,
                    JSON_VALUE(payload, '$._source')     AS source,
                    JSON_VALUE(payload, '$._endpoint')   AS endpoint,
                    JSON_VALUE(payload, '$._league_id')  AS league_id,
                    JSON_VALUE(payload, '$._season')     AS season,
                    TIMESTAMP(JSON_VALUE(payload, '$._ingested_at')) AS ingested_at,
                    DATE('{ingest_date.isoformat()}')    AS ingest_date,
                    CURRENT_TIMESTAMP()                  AS loaded_at
                FROM `{staging}`
                WHERE JSON_VALUE(payload, '$.{endpoint.primary_key}') IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY JSON_VALUE(payload, '$.{endpoint.primary_key}')
                    ORDER BY TIMESTAMP(JSON_VALUE(payload, '$._ingested_at')) DESC
                ) = 1
            ) AS S
            ON T.record_key = S.record_key
            WHEN MATCHED AND S.ingested_at >= T.ingested_at THEN UPDATE SET
                    {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """
        job = self.query(sql)
        merged = int(getattr(job, "num_dml_affected_rows", 0) or 0)
        log.info("merged %d row(s) into %s", merged, self.table_ref(table))
        return merged

    def drop_staging(self, staging: str) -> None:
        self.query(f"DROP TABLE IF EXISTS `{staging}`")

    def query(self, sql: str) -> Any:
        job = self.client.query(sql)
        job.result()
        return job

    # --- the whole operation ---------------------------------------------

    def load_endpoint(
        self,
        *,
        table: str,
        endpoint: Endpoint,
        uris: list[str],
        ingest_date: date,
        keep_staging: bool = False,
    ) -> LoadResult:
        """Stage -> MERGE -> clean up. Safe to run repeatedly."""
        self.ensure_table(table)
        staging, rows_staged = self.stage_uris(table, uris, ingest_date)
        try:
            merged = self.merge(table, staging, endpoint, ingest_date)
        finally:
            if not keep_staging:
                # Dropped even on failure: staging is scratch, and leaving it
                # behind would quietly accrue storage cost in every dataset.
                self.drop_staging(staging)
        return LoadResult(table=table, source_uris=uris, rows_staged=rows_staged, rows_merged=merged)
