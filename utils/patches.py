"""
NBA API Patches

Replaces nba_api's ``NBAHTTP.send_api_request`` with a curl_cffi request that
impersonates Chrome (TLS fingerprint) and sends the Chrome-131 header set from
``utils.nba_cdn`` — the combination stats.nba.com and cdn.nba.com both accept.
``Host`` is derived from the request URL, so the one patch serves both hosts:

- ``nba_api.stats``  (NBAStatsHTTP → https://stats.nba.com/stats/...)
- ``nba_api.live``   (NBALiveHTTP  → https://cdn.nba.com/static/json/liveData/...)
  NBALiveHTTP subclasses NBAHTTP without overriding send_api_request, so the
  live scoreboard/boxscore calls inherit the patch automatically.

An optional residential proxy (``settings.nba_api_proxy_url``) is used when set;
cloud egress IPs are sometimes blocked by stats.nba.com.

stats.nba.com intermittently answers HTTP 500 with an empty body; the same
request usually succeeds when asked again. nba_api would hand that body to
``json.loads`` and raise a JSONDecodeError nothing retries, so for that host a
5xx, an empty body or a body that is not JSON is re-asked here a few times and,
if it stays bad, raised as a ``RetryableError`` (``ServerError`` for a 5xx,
``NetworkError`` otherwise) with the status code in the message — which is what
the extractors' ``@with_retry`` retries. The re-asks sit below
``nba_api_circuit``, so the circuit counts a call that stayed bad, not each
flaky response. 4xx is passed through untouched, and so is cdn.nba.com: the
live extractor reads an empty box score body as "no data yet".

The re-asks share one budget (``STATS_BUDGET``, requests and pauses together)
rather than a fresh timeout each, so answers that are slow as well as bad are
not multiplied by four: a call takes no longer than its first request or the
budget, whichever is longer.

This module must be imported early in application startup (see main.py) so the
patch is applied before any nba_api call is made.
"""

import time
from urllib.parse import urlsplit

from curl_cffi import requests
from nba_api.library.http import NBAHTTP

from core.resilience import NetworkError, ServerError
from core.settings import settings
from utils.nba_cdn import nba_cdn_headers

# curl_cffi 0.7.x supports impersonation targets up to "chrome124".
IMPERSONATE = "chrome124"

STATS_HOST = "stats.nba.com"
# Requests per send_api_request call for a stats.nba.com response that is a
# 5xx or not JSON, and the pause before the first re-ask (doubles each time).
STATS_ATTEMPTS = 4
STATS_RETRY_DELAY = 1.0
# Seconds one send_api_request call may spend on a response that stays bad,
# requests and pauses together. A re-ask's timeout is what is left of it, and
# with less than STATS_MIN_TIMEOUT left after the pause no re-ask is made.
STATS_BUDGET = 20.0
STATS_MIN_TIMEOUT = 1.0


def _stats_failure(status_code: int, text: str, endpoint: str, valid_json: bool) -> Exception | None:
    """The retryable error for a stats.nba.com response, or None when it is usable (or a 4xx)."""
    if status_code >= 500:
        body = "an empty body" if not text.strip() else "a body"
        return ServerError(
            f"stats.nba.com returned HTTP {status_code} with {body} for {endpoint}",
            status_code=status_code,
        )
    if status_code >= 400 or valid_json:
        return None
    kind = "an empty body" if not text.strip() else "a non-JSON body"
    return NetworkError(f"stats.nba.com returned HTTP {status_code} with {kind} for {endpoint}")


def browser_impersonation_request(
    self,
    endpoint,
    parameters,
    referer=None,
    proxy=None,
    headers=None,
    timeout=None,
    raise_exception_on_error=False,
):
    """
    Replacement for NBAHTTP.send_api_request that uses curl_cffi
    with browser impersonation to avoid NBA API blocking.
    """
    base_url = self.base_url.format(endpoint=endpoint)

    # Library defaults first (they carry x-nba-stats-origin / x-nba-stats-token
    # for stats.nba.com), then the browser set wins for everything it names,
    # then per-call overrides.
    request_headers = dict(self.headers or {})
    request_headers.update(nba_cdn_headers(urlsplit(base_url).netloc))
    if headers:
        request_headers.update(headers)
    if referer:
        request_headers["Referer"] = referer

    # Clean 'None' values - standard requests drops None values automatically,
    # but curl_cffi sends them as the string "None". Filter them out.
    clean_params = {k: v for k, v in parameters.items() if v is not None}

    proxy_url = proxy or settings.nba_api_proxy_url
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

    is_stats = urlsplit(base_url).netloc == STATS_HOST
    attempts = STATS_ATTEMPTS if is_stats else 1

    request_timeout = timeout or 30
    started = time.monotonic()
    failure = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                base_url,
                params=clean_params,
                headers=request_headers,
                timeout=request_timeout,
                impersonate=IMPERSONATE,
                proxies=proxies,
            )
        except requests.exceptions.Timeout as e:
            if failure is None:
                raise
            # A re-ask that ran out of time: report the answer that stayed bad.
            raise failure from e

        data = self.nba_response(
            response=response.text,
            status_code=response.status_code,
            url=base_url,
        )
        failure = (
            _stats_failure(response.status_code, response.text, endpoint, data.valid_json())
            if is_stats
            else None
        )
        if failure is None:
            break
        delay = STATS_RETRY_DELAY * 2 ** (attempt - 1)
        # What the next request would have left of the budget after the pause.
        left = STATS_BUDGET - (time.monotonic() - started) - delay
        if attempt == attempts or left < STATS_MIN_TIMEOUT:
            raise failure
        time.sleep(delay)
        request_timeout = min(timeout or 30, left)

    if raise_exception_on_error and not data.valid_json():
        raise Exception("InvalidResponse: Response is not in a valid JSON format.")
    return data


def apply_nba_api_patch():
    """Apply the browser impersonation patch to nba_api."""
    NBAHTTP.send_api_request = browser_impersonation_request


# Apply the patch immediately when this module is imported
apply_nba_api_patch()
