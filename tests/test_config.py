from __future__ import annotations

import pytest
from pydantic import ValidationError

from elt.util.config import SourceConfig


def test_real_config_matches_the_documented_scope():
    """The committed config must stay in step with requirements.md section 3."""
    config = SourceConfig.load()
    assert len(config.leagues) == 5
    assert config.seasons == ["2024-2025", "2025-2026", "2026-2027"]
    # 5 leagues x 3 seasons of events, plus 5 league-grain team calls.
    assert len(config.plan()) == 20
    assert len(config.plan(endpoints=["events"])) == 15
    assert len(config.plan(endpoints=["teams"])) == 5


def test_plan_fans_out_by_grain(config):
    assert len(config.plan(endpoints=["events"])) == 4   # 2 leagues x 2 seasons
    assert len(config.plan(endpoints=["teams"])) == 2    # 2 leagues, season is None
    assert all(task.season is None for task in config.plan(endpoints=["teams"]))


def test_plan_filters_narrow_to_one_backfill_unit(config):
    tasks = config.plan(endpoints=["events"], league_ids=[4328], seasons=["2024-2025"])
    assert [t.key for t in tasks] == ["events/4328/2024-2025"]
    assert tasks[0].params == {"id": "4328", "s": "2024-2025"}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"endpoints": ["nope"]},
        {"league_ids": [9999]},
        {"seasons": ["1999-2000"]},
    ],
)
def test_plan_rejects_values_not_in_config(config, kwargs):
    """Fail fast on a typo, rather than silently extracting nothing."""
    with pytest.raises(ValueError):
        config.plan(**kwargs)


def test_season_format_is_validated():
    with pytest.raises(ValidationError):
        SourceConfig.model_validate(
            {"source": "s", "base_url": "u", "endpoints": {}, "leagues": [], "seasons": ["2024"]}
        )
