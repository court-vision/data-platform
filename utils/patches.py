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

This module must be imported early in application startup (see main.py) so the
patch is applied before any nba_api call is made.
"""

from urllib.parse import urlsplit

from curl_cffi import requests
from nba_api.library.http import NBAHTTP

from core.settings import settings
from utils.nba_cdn import nba_cdn_headers

# curl_cffi 0.7.x supports impersonation targets up to "chrome124".
IMPERSONATE = "chrome124"


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

    response = requests.get(
        base_url,
        params=clean_params,
        headers=request_headers,
        timeout=timeout or 30,
        impersonate=IMPERSONATE,
        proxies=proxies,
    )

    data = self.nba_response(
        response=response.text,
        status_code=response.status_code,
        url=base_url,
    )
    if raise_exception_on_error and not data.valid_json():
        raise Exception("InvalidResponse: Response is not in a valid JSON format.")
    return data


def apply_nba_api_patch():
    """Apply the browser impersonation patch to nba_api."""
    NBAHTTP.send_api_request = browser_impersonation_request


# Apply the patch immediately when this module is imported
apply_nba_api_patch()
