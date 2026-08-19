"""Fake GCP clients.

These record what the real SDK would have been asked to do, so the load layer
can be tested without a project, credentials or billing. They deliberately do
NOT emulate BigQuery - a fake cannot tell us the SQL is valid, only that we
built the statement we intended.
"""

from __future__ import annotations

from typing import Any


class FakeJob:
    def __init__(self, *, output_rows: int = 0, num_dml_affected_rows: int = 0) -> None:
        self.output_rows = output_rows
        self.num_dml_affected_rows = num_dml_affected_rows
        self.result_called = False

    def result(self) -> FakeJob:
        self.result_called = True
        return self


class FakeBQClient:
    """Records queries and load jobs in the order they were issued."""

    def __init__(self, *, staged_rows: int = 0, merged_rows: int = 0) -> None:
        self.queries: list[str] = []
        self.load_jobs: list[dict[str, Any]] = []
        self.datasets: list[Any] = []
        self.staged_rows = staged_rows
        self.merged_rows = merged_rows

    def create_dataset(self, dataset: Any, exists_ok: bool = False) -> Any:
        self.datasets.append(dataset)
        return dataset

    def query(self, sql: str) -> FakeJob:
        self.queries.append(sql)
        return FakeJob(num_dml_affected_rows=self.merged_rows if "MERGE" in sql else 0)

    def load_table_from_uri(self, uris: Any, destination: str, job_config: Any = None) -> FakeJob:
        self.load_jobs.append({"uris": uris, "destination": destination, "job_config": job_config})
        return FakeJob(output_rows=self.staged_rows)

    # --- helpers for assertions ---
    def sql_containing(self, needle: str) -> str:
        matches = [q for q in self.queries if needle in q]
        assert matches, f"no query containing {needle!r}; issued: {self.queries}"
        return matches[0]


class FakeBlob:
    def __init__(self, store: set[str], name: str) -> None:
        self.store, self.name = store, name

    def exists(self) -> bool:
        return self.name in self.store


class FakeGCSBucket:
    def __init__(self, store: set[str], name: str) -> None:
        self.store, self.name = store, name

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.store, name)


class FakeStorageClient:
    """Knows which object paths 'exist'."""

    def __init__(self, existing: set[str] | None = None, *, all_exist: bool = False) -> None:
        self.existing = existing or set()
        self.all_exist = all_exist

    def bucket(self, name: str) -> FakeGCSBucket:
        store = self.existing
        if self.all_exist:
            class _Always(FakeGCSBucket):
                def blob(self, blob_name: str) -> Any:
                    return type("_B", (), {"exists": lambda _self: True})()

            return _Always(store, name)
        return FakeGCSBucket(store, name)
