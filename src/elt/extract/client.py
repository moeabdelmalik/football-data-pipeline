"""HTTP client for TheSportsDB.

Everything here exists to answer one question: what happens when the API
misbehaves? It is undocumented, unversioned, free, and publishes no rate
limits (CONSTRAINT-5), so the client assumes fragility:

* **Throttle before every call** - be a good citizen, don't get blocked.
* **Retry only what is worth retrying** - a 500 or a timeout is bad luck and
  will probably succeed next time. A 404 is a bug in our config and retrying
  it four more times just wastes four more seconds and hides the error.
* **Distinguish "no rows" from "broken"** - the API answers an empty result
  with ``{"events": null}``, which is a legitimate answer, not a failure.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

import requests
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from elt.util.config import Endpoint, RequestPolicy, SourceConfig

log = logging.getLogger(__name__)


class ExtractionError(RuntimeError):
    """Base class for extraction failures."""


class RetryableError(ExtractionError):
    """Transient: a later attempt may well succeed (5xx, 429, timeout)."""


class FatalError(ExtractionError):
    """Permanent: retrying cannot help (404, bad key, schema drift)."""


class RateLimiter:
    """Enforce a minimum interval between calls.

    Uses a monotonic clock (never jumps backwards on an NTP correction) and
    measures the gap since the last call, so time already spent inside a slow
    response counts towards the interval instead of being paid twice.
    """

    def __init__(
        self,
        min_interval_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.min_interval = max(0.0, min_interval_seconds)
        self._clock = clock
        self._sleep = sleep
        self._last_call: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._last_call is not None:
            remaining = self.min_interval - (now - self._last_call)
            if remaining > 0:
                log.debug("throttling for %.2fs", remaining)
                self._sleep(remaining)
        self._last_call = self._clock()


class TheSportsDBClient:
    """Fetches JSON rows from one endpoint, with throttling and retries."""

    def __init__(
        self,
        api_key: str,
        config: SourceConfig,
        *,
        session: requests.Session | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.api_key = api_key
        self.config = config
        self.policy: RequestPolicy = config.request
        self.session = session or requests.Session()
        self.rate_limiter = rate_limiter or RateLimiter(self.policy.min_interval_seconds)

    @property
    def base_url(self) -> str:
        # TheSportsDB authenticates by embedding the key in the URL path.
        return f"{self.config.base_url.rstrip('/')}/{self.api_key}"

    def url_for(self, endpoint: Endpoint) -> str:
        return f"{self.base_url}/{endpoint.path}"

    def fetch(self, endpoint: Endpoint, params: dict[str, str]) -> list[dict[str, Any]]:
        """Return the rows under the endpoint's root key. May be empty."""
        url = self.url_for(endpoint)

        # Imperative retry loop rather than the @retry decorator: the policy is
        # read from config at runtime, so it cannot be baked in at import time.
        retrying = Retrying(
            stop=stop_after_attempt(self.policy.max_attempts),
            wait=wait_exponential(
                multiplier=self.policy.backoff_initial_seconds,
                max=self.policy.backoff_max_seconds,
            ),
            retry=retry_if_exception_type(RetryableError),
            reraise=True,  # surface the real error, not tenacity's RetryError
        )
        payload = retrying(self._get_once, url, params)
        return self._extract_rows(payload, endpoint, url, params)

    def _get_once(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        """A single attempt. Classifies every failure as retryable or fatal."""
        self.rate_limiter.wait()
        log.debug("GET %s params=%s", url, params)

        try:
            response = self.session.get(url, params=params, timeout=self.policy.timeout_seconds)
        except (requests.Timeout, requests.ConnectionError) as exc:
            raise RetryableError(f"network error calling {url}: {exc}") from exc

        status = response.status_code
        if status == 429 or status >= 500:
            raise RetryableError(f"HTTP {status} from {url} params={params}")
        if status >= 400:
            raise FatalError(f"HTTP {status} from {url} params={params} - check config/API key")

        try:
            payload = response.json()
        except ValueError as exc:
            # The API serves an HTML error page when overloaded, which arrives
            # as a 200 with an unparseable body. Transient - worth retrying.
            raise RetryableError(f"non-JSON body from {url}: {response.text[:200]!r}") from exc

        if not isinstance(payload, dict):
            raise FatalError(f"expected a JSON object from {url}, got {type(payload).__name__}")
        return payload

    def _extract_rows(
        self,
        payload: dict[str, Any],
        endpoint: Endpoint,
        url: str,
        params: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Unwrap the single root key, treating null as a legitimate empty set.

        The distinction matters:
        * key present, value ``null`` -> the API is telling us there are no
          rows (a season with no fixtures loaded yet). Normal. Return [].
        * key absent entirely        -> the response shape has changed under
          us. That is schema drift and must fail loudly, not silently load 0
          rows and let a downstream league table quietly go empty.
        """
        if endpoint.root_key not in payload:
            raise FatalError(
                f"root key '{endpoint.root_key}' missing from {url} params={params}; "
                f"got keys {sorted(payload)} - the API contract may have changed"
            )

        rows = payload[endpoint.root_key]
        if rows is None:
            log.info("no rows for %s params=%s", endpoint.path, params)
            return []
        if not isinstance(rows, list):
            raise FatalError(f"expected a list under '{endpoint.root_key}', got {type(rows).__name__}")

        non_objects = [row for row in rows if not isinstance(row, dict)]
        if non_objects:
            raise FatalError(f"expected JSON objects under '{endpoint.root_key}', got {non_objects[:1]}")
        return rows
