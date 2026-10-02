"""
`utils.patches`: what the curl_cffi replacement for nba_api's request does with
a bad stats.nba.com answer.

stats.nba.com answers HTTP 500 with an empty body often enough that a
14-season backfill took 60 pipeline runs: nba_api raised a JSONDecodeError and
nothing retried it. The patch re-asks, and a response that stays bad becomes a
`RetryableError` carrying the status code, which the extractors' `@with_retry`
retries. 4xx is not retried, and cdn.nba.com (the live endpoints) is untouched —
the live box score reads an empty body as "no data yet".
"""

import json

import pytest
from circuitbreaker import CircuitBreakerMonitor
from nba_api.live.nba.library.http import NBALiveHTTP
from nba_api.stats.library.http import NBAStatsHTTP

from core.resilience import NetworkError, RetryableError, ServerError
from pipelines.extractors.nba_api import NBAApiExtractor
from utils import patches

pytestmark = pytest.mark.unit

OK_BODY = json.dumps({
    "resource": "playerindex",
    "parameters": {},
    "resultSets": [{"name": "PlayerIndex", "headers": ["PERSON_ID"], "rowSet": [[1], [2]]}],
})


class FakeResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


@pytest.fixture
def stats(monkeypatch):
    """Queue (status, body) answers for requests.get; records the URLs asked and the pauses taken."""

    class Wire:
        def __init__(self):
            self.answers = []
            self.urls = []
            self.sleeps = []

        def get(self, url, **kwargs):
            self.urls.append(url)
            # The last answer repeats, so "always 500" is one entry.
            status, text = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
            return FakeResponse(status, text)

    wire = Wire()
    monkeypatch.setattr(patches.requests, "get", wire.get)
    # `patches.time` is the time module itself, so this also takes the wait out
    # of tenacity's backoff in the extractor tests below.
    monkeypatch.setattr(patches.time, "sleep", wire.sleeps.append)
    return wire


@pytest.fixture
def closed_circuit():
    """nba_api_circuit is process-wide state: start closed, leave closed."""
    breaker = CircuitBreakerMonitor.get("nba_api")
    breaker._CircuitBreaker__call_succeeded()
    yield breaker
    breaker._CircuitBreaker__call_succeeded()


def _send(endpoint="playerindex"):
    return NBAStatsHTTP().send_api_request(endpoint=endpoint, parameters={"Season": "2026-27"})


def test_good_response_is_one_request(stats):
    stats.answers = [(200, OK_BODY)]
    assert _send().get_dict()["resource"] == "playerindex"
    assert len(stats.urls) == 1 and stats.sleeps == []


def test_empty_500_is_asked_again(stats):
    stats.answers = [(500, ""), (500, ""), (200, OK_BODY)]
    assert _send().get_dict()["resource"] == "playerindex"
    assert len(stats.urls) == 3
    assert stats.sleeps == [1.0, 2.0]


def test_500_that_stays_is_a_retryable_error_with_the_status(stats):
    stats.answers = [(500, "")]
    with pytest.raises(ServerError) as exc:
        _send("leaguedashplayerstats")
    assert isinstance(exc.value, RetryableError)
    assert exc.value.status_code == 500
    assert "HTTP 500" in str(exc.value) and "empty body" in str(exc.value)
    assert "leaguedashplayerstats" in str(exc.value)
    assert len(stats.urls) == patches.STATS_ATTEMPTS
    assert stats.sleeps == [1.0, 2.0, 4.0]


def test_5xx_with_a_json_body_is_still_a_failure(stats):
    stats.answers = [(503, '{"message": "unavailable"}')]
    with pytest.raises(ServerError, match="HTTP 503"):
        _send()


@pytest.mark.parametrize("body,kind", [("", "an empty body"), ("   \n", "an empty body"), ("<html>blocked</html>", "a non-JSON body")])
def test_200_that_is_not_json_is_a_network_error(stats, body, kind):
    stats.answers = [(200, body)]
    with pytest.raises(NetworkError) as exc:
        _send()
    assert "HTTP 200" in str(exc.value) and kind in str(exc.value)
    assert len(stats.urls) == patches.STATS_ATTEMPTS


@pytest.mark.parametrize("status,body", [(400, "Season is required"), (403, ""), (404, "<html>not found</html>"), (429, "")])
def test_4xx_is_not_retried(stats, status, body):
    stats.answers = [(status, body)]
    data = _send()
    assert len(stats.urls) == 1 and stats.sleeps == []
    # Handed to nba_api as before: its own JSON error, which with_retry leaves alone.
    with pytest.raises(json.JSONDecodeError):
        data.get_dict()


def test_cdn_host_is_left_alone(stats):
    """The live box score's empty body means "no data yet" to the extractor, not an outage."""
    stats.answers = [(500, "")]
    data = NBALiveHTTP().send_api_request(endpoint="boxscore/boxscore_0022500001.json", parameters={})
    assert len(stats.urls) == 1 and stats.sleeps == []
    assert not data.valid_json()


class TestThroughTheExtractor:
    """The patch under a real extractor method: @with_retry outside, nba_api_circuit inside."""

    def test_a_burst_of_500s_is_ridden_out_without_touching_the_circuit(self, stats, closed_circuit):
        stats.answers = [(500, "")] * 3 + [(200, OK_BODY)]
        rows = NBAApiExtractor().get_player_index("2026-27")
        assert [r["PERSON_ID"] for r in rows] == [1, 2]
        assert len(stats.urls) == 4
        assert closed_circuit.failure_count == 0 and closed_circuit.closed

    def test_with_retry_retries_a_call_that_stayed_bad(self, stats, closed_circuit):
        stats.answers = [(500, "")] * patches.STATS_ATTEMPTS + [(200, OK_BODY)]
        rows = NBAApiExtractor().get_player_index("2026-27")
        assert len(rows) == 2
        assert len(stats.urls) == patches.STATS_ATTEMPTS + 1
        # One failed call, then a success: the count is back to zero.
        assert closed_circuit.failure_count == 0 and closed_circuit.closed

    def test_an_outage_fails_the_call_and_counts_once_per_attempt(self, stats, closed_circuit):
        from core.settings import settings

        stats.answers = [(500, "")]
        with pytest.raises(ServerError, match="HTTP 500"):
            NBAApiExtractor().get_season_totals("2025-26")
        assert len(stats.urls) == patches.STATS_ATTEMPTS * settings.retry_max_attempts
        # 3 failed calls (12 bad responses) is still under the threshold of 5.
        assert closed_circuit.failure_count == settings.retry_max_attempts
        assert closed_circuit.closed

    def test_a_4xx_fails_at_once(self, stats, closed_circuit):
        stats.answers = [(400, "Season is required")]
        with pytest.raises(json.JSONDecodeError):
            NBAApiExtractor().get_player_index("2026-27")
        assert len(stats.urls) == 1
        assert closed_circuit.failure_count == 0
