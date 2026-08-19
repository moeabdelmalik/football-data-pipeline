from __future__ import annotations

import pytest

from elt.extract.client import RateLimiter, TheSportsDBClient
from elt.util.config import Settings, SourceConfig

BASE = "https://api.test/json"


@pytest.fixture
def config() -> SourceConfig:
    """A miniature source config - fast, and independent of the real one."""
    return SourceConfig.model_validate(
        {
            "source": "thesportsdb",
            "base_url": BASE,
            "request": {
                "min_interval_seconds": 0.0,     # no throttling in tests
                "backoff_initial_seconds": 0.0,  # no real sleeping between retries
                "max_attempts": 3,
            },
            "endpoints": {
                "teams": {"path": "teams.php", "root_key": "teams", "grain": "league",
                          "primary_key": "idTeam", "table": "tsdb_teams",
                          "params": {"id": "{league_id}"}},
                "events": {"path": "events.php", "root_key": "events", "grain": "league_season",
                           "primary_key": "idEvent", "table": "tsdb_events",
                           "params": {"id": "{league_id}", "s": "{season}"}},
            },
            "leagues": [
                {"id": 4328, "name": "English Premier League", "country": "England"},
                {"id": 4331, "name": "German Bundesliga", "country": "Germany"},
            ],
            "seasons": ["2024-2025", "2025-2026"],
        }
    )


@pytest.fixture
def client(config: SourceConfig) -> TheSportsDBClient:
    # A rate limiter that records instead of sleeping keeps the suite instant.
    return TheSportsDBClient("KEY", config, rate_limiter=RateLimiter(0.0, sleep=lambda _: None))


@pytest.fixture
def make_settings(tmp_path):
    """Build Settings for a test, overriding only what the test cares about.

    A factory rather than literals scattered across the suite: adding a field
    to Settings then costs one line here instead of breaking every test file.
    """

    def _make(**overrides):
        defaults = dict(
            api_key="KEY",
            gcp_project_id="my-project",
            gcp_region="US",
            gcs_raw_bucket="my-bucket",
            local_raw_dir=tmp_path,
            bq_dataset_raw="sports_raw",
        )
        return Settings(**{**defaults, **overrides})

    return _make
