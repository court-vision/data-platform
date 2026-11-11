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
budget, whichever is longer. Each re-ask is logged (``stats_reask``), so a
night of flaky answers that all came good is still visible.

cdn.nba.com requests go over kept connections (``_kept_get``). The live-stats
pipeline reads the scoreboard and every box score of the night each 30 s, and
with a connection of its own every one of those requests opened a proxy tunnel
and a TLS session first (0.3-0.9 s a file on 2026-11-09, handshakes included).
nba_api's own ``send_api_request`` keeps one ``requests.Session``; this
replacement had dropped it. Only the connection is kept: each request still
goes out with the same headers and no cookies. ``NBA_CDN_KEEP_CONNECTIONS=false``
goes back to a connection per request.

This module must be imported early in application startup (see main.py) so the
patch is applied before any nba_api call is made.
"""

import threading
import time
from urllib.parse import urlsplit

from curl_cffi import requests
from nba_api.library.http import NBAHTTP

from core.logging import get_logger
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

CDN_HOST = "cdn.nba.com"
# Idle sessions kept per proxy. The live pipeline's box score readers
# (settings.live_box_score_workers) and a scoreboard read fit with room to
# spare; a session that comes back to a full shelf is closed.
CDN_KEPT = 16

# Sessions not in use, most recently used last, by the proxy they go through
# (a curl handle keeps the proxy it was last given). Each holds one curl handle
# and its open connection. A request takes one for itself, so a handle is never
# used by two threads at once, and takes the one used last, so a quiet night
# keeps reusing the same warm connections rather than cycling through cold ones.
_kept: dict[str | None, list] = {}
_kept_lock = threading.Lock()


def _new_session():
    """A session with one curl handle of its own (not one per thread: `_kept_get` hands it to one request at a time)."""
    return requests.Session(use_thread_local_curl=False)


def _close(session) -> None:
    try:
        session.close()
    except Exception:
        pass


def _kept_get(url: str, proxy_url: str | None, **kwargs):
    """GET `url` on a kept connection through `proxy_url`; the session goes back on the shelf when it answers.

    A request that raises closes its session: whatever state the connection was
    left in, the retry starts on a new one. libcurl itself drops a kept
    connection the other end has closed, or one idle past its
    CURLOPT_MAXAGE_CONN (118 s), and connects again within the same request.
    """
    with _kept_lock:
        idle = _kept.get(proxy_url)
        session = idle.pop() if idle else None
    if session is None:
        session = _new_session()
    try:
        response = session.get(url, **kwargs)
        # Only the connection is kept. A session would also send back whatever
        # cookies the last answer set; a request of its own never had any, and
        # each one here still goes out as that request did.
        session.cookies.clear()
    except BaseException:
        _close(session)
        raise
    with _kept_lock:
        idle = _kept.setdefault(proxy_url, [])
        if len(idle) < CDN_KEPT:
            idle.append(session)
            session = None
    if session is not None:
        _close(session)
    return response


def close_kept_sessions() -> None:
    """Close every idle kept session (tests; a session in use closes itself if it fails)."""
    with _kept_lock:
        sessions = [session for idle in _kept.values() for session in idle]
        _kept.clear()
    for session in sessions:
        _close(session)


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

    host = urlsplit(base_url).netloc
    is_stats = host == STATS_HOST
    attempts = STATS_ATTEMPTS if is_stats else 1
    kept = host == CDN_HOST and settings.nba_cdn_keep_connections

    request_timeout = timeout or 30
    started = time.monotonic()
    failure = None

    for attempt in range(1, attempts + 1):
        request = dict(
            params=clean_params,
            headers=request_headers,
            timeout=request_timeout,
            impersonate=IMPERSONATE,
            proxies=proxies,
        )
        try:
            if kept:
                response = _kept_get(base_url, proxy_url, **request)
            else:
                response = requests.get(base_url, **request)
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
        get_logger("nba_api").warning(
            "stats_reask",
            endpoint=endpoint,
            status_code=response.status_code,
            attempt=attempt,
            wait=delay,
            error=str(failure),
        )
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
