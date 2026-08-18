from __future__ import annotations

import pytest

from elt.extract.client import RateLimiter, TheSportsDBClient
from elt.util.config import SourceConfig

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
                          "params": {"id": "{league_id}"}},
                "events": {"path": "events.php", "root_key": "events", "grain": "league_season",
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
