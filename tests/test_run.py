from __future__ import annotations

import json
from datetime import date

import responses

from elt.extract.run import main, run, summarise


@responses.activate
def test_run_lands_one_file_per_task(tmp_path, config, make_settings):
    responses.get("https://api.test/json/KEY/events.php", json={"events": [{"idEvent": "1"}]})

    outcomes = run(
        config=config, settings=make_settings(gcs_raw_bucket=None), destination="local",
        endpoints=["events"], ingest_date=date(2026, 8, 18),
    )

    assert len(outcomes) == 4                      # 2 leagues x 2 seasons
    assert all(o.ok for o in outcomes)
    assert len(list(tmp_path.rglob("*.ndjson"))) == 4

    landed = tmp_path / (
        "raw/thesportsdb/events/ingest_date=2026-08-18"
        "/league_id=4328/season=2024-2025/data.ndjson"
    )
    record = json.loads(landed.read_text())
    assert record["idEvent"] == "1"
    assert record["_season"] == "2024-2025"


@responses.activate
def test_one_failing_task_does_not_stop_the_others(tmp_path, config, make_settings):
    """Independence: a single bad league x season must not cost a full re-run."""
    responses.get("https://api.test/json/KEY/events.php", status=404)   # first task fails
    responses.get("https://api.test/json/KEY/events.php", json={"events": [{"idEvent": "1"}]})

    outcomes = run(
        config=config, settings=make_settings(gcs_raw_bucket=None), destination="local",
        endpoints=["events"], ingest_date=date(2026, 8, 18),
    )

    assert [o.ok for o in outcomes] == [False, True, True, True]
    assert "404" in outcomes[0].error
    assert "FAILED   : 1" in summarise(outcomes)


@responses.activate
def test_rerunning_a_task_is_idempotent(tmp_path, config, make_settings):
    """Success criterion 3: run it twice, get identical output."""
    responses.get("https://api.test/json/KEY/events.php",
                  json={"events": [{"idEvent": "1"}, {"idEvent": "2"}]})

    kwargs = dict(
        config=config, settings=make_settings(gcs_raw_bucket=None), destination="local",
        endpoints=["events"], league_ids=[4328], seasons=["2024-2025"],
        ingest_date=date(2026, 8, 18),
    )
    run(**kwargs)
    first = {p: p.read_text() for p in tmp_path.rglob("*.ndjson")}
    run(**kwargs)
    second = {p: p.read_text() for p in tmp_path.rglob("*.ndjson")}

    assert len(first) == 1
    assert list(first) == list(second)          # same paths, no new files
    assert [len(v.splitlines()) for v in second.values()] == [2]   # not doubled


@responses.activate
def test_empty_result_still_writes_a_file(tmp_path, config, make_settings):
    """An empty file is a fact: 'we asked, there was nothing'. Absence is not."""
    responses.get("https://api.test/json/KEY/events.php", json={"events": None})

    outcomes = run(
        config=config, settings=make_settings(gcs_raw_bucket=None), destination="local",
        endpoints=["events"], league_ids=[4328], seasons=["2024-2025"],
        ingest_date=date(2026, 8, 18),
    )
    assert outcomes[0].ok and outcomes[0].record_count == 0
    assert len(list(tmp_path.rglob("*.ndjson"))) == 1


def test_dry_run_makes_no_http_calls_and_writes_nothing(tmp_path, config, caplog, make_settings):
    with responses.RequestsMock():   # any HTTP call would raise here
        outcomes = run(
            config=config, settings=make_settings(gcs_raw_bucket=None), destination="local", dry_run=True,
        )
    assert outcomes == []
    assert list(tmp_path.rglob("*")) == []


@responses.activate
def test_main_exit_code_reflects_failure(tmp_path, monkeypatch, config, make_settings):
    monkeypatch.setenv("LOCAL_RAW_DIR", str(tmp_path))
    monkeypatch.setenv("TSDB_API_KEY", "KEY")
    monkeypatch.setattr("elt.extract.run.SourceConfig.load", lambda _path: config)
    responses.get("https://api.test/json/KEY/events.php", status=404)

    code = main(["--endpoints", "events", "--leagues", "4328", "--seasons", "2024-2025", "--dest", "local"])
    assert code == 1
