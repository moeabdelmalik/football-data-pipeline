from __future__ import annotations

import pytest
import requests
import responses

from elt.extract.client import FatalError, RateLimiter, RetryableError

EVENTS_URL = "https://api.test/json/KEY/events.php"
PARAMS = {"id": "4328", "s": "2024-2025"}


def endpoint(config):
    return config.endpoints["events"]


@responses.activate
def test_fetch_returns_rows_under_the_root_key(client, config):
    responses.get(EVENTS_URL, json={"events": [{"idEvent": "1"}, {"idEvent": "2"}]})
    rows = client.fetch(endpoint(config), PARAMS)
    assert [r["idEvent"] for r in rows] == ["1", "2"]


@responses.activate
def test_null_root_key_is_an_empty_result_not_an_error(client, config):
    """TheSportsDB answers 'nothing here' with {"events": null} - that is data."""
    responses.get(EVENTS_URL, json={"events": None})
    assert client.fetch(endpoint(config), PARAMS) == []


@responses.activate
def test_missing_root_key_is_fatal(client, config):
    """A changed response shape must fail loudly, not load zero rows quietly."""
    responses.get(EVENTS_URL, json={"something_else": []})
    with pytest.raises(FatalError, match="root key 'events' missing"):
        client.fetch(endpoint(config), PARAMS)


@responses.activate
def test_server_error_is_retried_then_succeeds(client, config):
    responses.get(EVENTS_URL, status=503)
    responses.get(EVENTS_URL, status=503)
    responses.get(EVENTS_URL, json={"events": [{"idEvent": "1"}]})
    assert len(client.fetch(endpoint(config), PARAMS)) == 1
    assert len(responses.calls) == 3


@responses.activate
def test_retries_give_up_after_max_attempts(client, config):
    responses.get(EVENTS_URL, status=500)
    with pytest.raises(RetryableError):
        client.fetch(endpoint(config), PARAMS)
    assert len(responses.calls) == 3  # max_attempts from the fixture


@responses.activate
def test_rate_limit_response_is_retried(client, config):
    responses.get(EVENTS_URL, status=429)
    responses.get(EVENTS_URL, json={"events": []})
    assert client.fetch(endpoint(config), PARAMS) == []
    assert len(responses.calls) == 2


@responses.activate
def test_client_error_is_not_retried(client, config):
    """Retrying a 404 four more times just delays a config bug being noticed."""
    responses.get(EVENTS_URL, status=404)
    with pytest.raises(FatalError):
        client.fetch(endpoint(config), PARAMS)
    assert len(responses.calls) == 1


@responses.activate
def test_timeout_is_retried(client, config):
    responses.get(EVENTS_URL, body=requests.Timeout("too slow"))
    responses.get(EVENTS_URL, json={"events": []})
    assert client.fetch(endpoint(config), PARAMS) == []
    assert len(responses.calls) == 2


@responses.activate
def test_html_error_page_served_as_200_is_retried(client, config):
    responses.get(EVENTS_URL, body="<html>over capacity</html>", status=200)
    responses.get(EVENTS_URL, json={"events": []})
    assert client.fetch(endpoint(config), PARAMS) == []


@responses.activate
def test_api_key_is_embedded_in_the_url_path(client, config):
    responses.get(EVENTS_URL, json={"events": []})
    client.fetch(endpoint(config), PARAMS)
    assert "/json/KEY/events.php" in responses.calls[0].request.url


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_rate_limiter_waits_only_the_remaining_interval():
    clock = FakeClock()
    limiter = RateLimiter(1.5, clock=clock.time, sleep=clock.sleep)

    limiter.wait()          # first call never waits
    assert clock.slept == []

    clock.now += 0.5        # 0.5s already spent elsewhere
    limiter.wait()
    assert clock.slept == [1.0]  # only the remaining 1.0s is paid

    clock.now += 99.0       # plenty of time has passed
    limiter.wait()
    assert clock.slept == [1.0]  # no extra sleep
