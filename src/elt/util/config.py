"""Typed configuration for the pipeline.

Configuration is split in two, deliberately:

* **Source config** (``config/sources/*.yml``) - *what* to extract. Checked into
  git, code-reviewable, diffable. Adding a season is a one-line PR.
* **Settings** (environment / ``.env``) - *how* to reach it: API key, bucket
  names, project id. Never committed (NFR-8).

Parsing the YAML into pydantic models rather than passing dicts around means a
typo in the config fails loudly at start-up, with the offending field named,
instead of surfacing as a ``KeyError`` mid-run inside an Airflow task.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

# src/elt/util/config.py -> parents: util, elt, src, <repo root>
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_CONFIG = REPO_ROOT / "config" / "sources" / "thesportsdb.yml"

SEASON_RE = re.compile(r"^\d{4}-\d{4}$")


class RequestPolicy(BaseModel):
    """How aggressively we may call the source (CONSTRAINT-5)."""

    timeout_seconds: float = 15.0
    min_interval_seconds: float = 1.5
    max_attempts: int = Field(default=5, ge=1)
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 30.0


class Endpoint(BaseModel):
    """One API endpoint, and the shape of the fan-out it implies."""

    path: str
    root_key: str
    grain: Literal["league", "league_season"]
    params: dict[str, str] = Field(default_factory=dict)

    @property
    def needs_season(self) -> bool:
        return self.grain == "league_season"

    def resolve_params(self, *, league_id: int, season: str | None) -> dict[str, str]:
        """Fill the ``{league_id}`` / ``{season}`` placeholders from the YAML."""
        if self.needs_season and season is None:
            raise ValueError(f"endpoint '{self.path}' has grain league_season but no season was given")
        return {
            key: template.format(league_id=league_id, season=season)
            for key, template in self.params.items()
        }


class League(BaseModel):
    id: int
    name: str
    country: str


@dataclass(frozen=True)
class ExtractTask:
    """A single unit of extraction: exactly one HTTP call.

    Frozen and self-contained on purpose - this is the object Airflow will
    fan out over with dynamic task mapping in Phase 4, so it must be small,
    hashable and independently retryable.
    """

    endpoint_name: str
    endpoint: Endpoint
    league: League
    season: str | None

    @property
    def key(self) -> str:
        """Stable human-readable id, used in logs and as the batch identifier."""
        parts = [self.endpoint_name, str(self.league.id)]
        if self.season:
            parts.append(self.season)
        return "/".join(parts)

    @property
    def params(self) -> dict[str, str]:
        return self.endpoint.resolve_params(league_id=self.league.id, season=self.season)


class SourceConfig(BaseModel):
    """The parsed contents of ``config/sources/thesportsdb.yml``."""

    source: str
    base_url: str
    request: RequestPolicy = Field(default_factory=RequestPolicy)
    endpoints: dict[str, Endpoint]
    leagues: list[League]
    seasons: list[str]

    @field_validator("seasons")
    @classmethod
    def _check_season_format(cls, seasons: list[str]) -> list[str]:
        bad = [s for s in seasons if not SEASON_RE.match(s)]
        if bad:
            raise ValueError(f"seasons must look like '2024-2025', got: {bad}")
        return seasons

    @classmethod
    def load(cls, path: Path | str | None = None) -> SourceConfig:
        path = Path(path) if path else DEFAULT_SOURCE_CONFIG
        if not path.exists():
            raise FileNotFoundError(f"source config not found: {path}")
        return cls.model_validate(yaml.safe_load(path.read_text()))

    def plan(
        self,
        *,
        endpoints: list[str] | None = None,
        league_ids: list[int] | None = None,
        seasons: list[str] | None = None,
    ) -> list[ExtractTask]:
        """Expand config (+ optional filters) into the full list of API calls.

        This is the whole fan-out, computed up front and with no side effects,
        which is what lets ``--dry-run`` show exactly what a run would do and
        lets a backfill be narrowed to one league x season (NFR-4).
        """
        selected_endpoints = self._select_endpoints(endpoints)
        selected_leagues = self._select_leagues(league_ids)
        selected_seasons = self._select_seasons(seasons)

        tasks: list[ExtractTask] = []
        for name, endpoint in selected_endpoints:
            for league in selected_leagues:
                # A league-grain endpoint is called once; a league_season-grain
                # endpoint once per season. That asymmetry is the fan-out.
                for season in (selected_seasons if endpoint.needs_season else [None]):
                    tasks.append(ExtractTask(name, endpoint, league, season))
        return tasks

    def _select_endpoints(self, names: list[str] | None) -> list[tuple[str, Endpoint]]:
        if not names:
            return list(self.endpoints.items())
        unknown = sorted(set(names) - set(self.endpoints))
        if unknown:
            raise ValueError(f"unknown endpoint(s) {unknown}; configured: {sorted(self.endpoints)}")
        return [(name, self.endpoints[name]) for name in names]

    def _select_leagues(self, league_ids: list[int] | None) -> list[League]:
        if not league_ids:
            return list(self.leagues)
        known = {league.id: league for league in self.leagues}
        unknown = sorted(set(league_ids) - set(known))
        if unknown:
            raise ValueError(f"unknown league id(s) {unknown}; configured: {sorted(known)}")
        return [known[league_id] for league_id in league_ids]

    def _select_seasons(self, seasons: list[str] | None) -> list[str]:
        if not seasons:
            return list(self.seasons)
        unknown = sorted(set(seasons) - set(self.seasons))
        if unknown:
            raise ValueError(f"unknown season(s) {unknown}; configured: {self.seasons}")
        return list(seasons)


@dataclass(frozen=True)
class Settings:
    """Environment-provided settings. Secrets and destinations only."""

    api_key: str
    gcp_project_id: str | None
    gcs_raw_bucket: str | None
    local_raw_dir: Path

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv(REPO_ROOT / ".env")  # no-op if the file is absent
        return cls(
            # "3" is TheSportsDB's public test key - a usable default keeps the
            # project runnable on a fresh clone with no credentials at all.
            api_key=os.getenv("TSDB_API_KEY", "3"),
            gcp_project_id=os.getenv("GCP_PROJECT_ID") or None,
            gcs_raw_bucket=os.getenv("GCS_RAW_BUCKET") or None,
            # The object path already begins with "raw/", so this is the root
            # the bucket is being stood in for - keeping both layouts identical.
            local_raw_dir=Path(os.getenv("LOCAL_RAW_DIR", str(REPO_ROOT / "data"))),
        )

    @property
    def default_destination(self) -> Literal["gcs", "local"]:
        """Land in GCS when a bucket is configured, otherwise on disk."""
        return "gcs" if self.gcs_raw_bucket else "local"
