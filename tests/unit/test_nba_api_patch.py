"""
`utils.patches`: what the curl_cffi replacement for nba_api's request does with
a bad stats.nba.com answer.

stats.nba.com answers HTTP 500 with an empty body often enough that a
14-season backfill took 60 pipeline runs: nba_api raised a JSONDecodeError and
nothing retried it. The patch re-asks, and a response that stays bad becomes a
`RetryableError` carrying the status code, which the extractors' `@with_retry`
retries. 4xx is not retried, and cdn.nba.com (the live endpoints) is untouched —
the live box score reads an empty body as "no data yet".

The re-asks share one budget: answers that are slow as well as bad must not be
multiplied by four under a trigger that runs inside its HTTP request.
"""

import json

import pytest
from circuitbreaker import CircuitBreakerMonitor
from nba_api.live.nba.library.http import NBALiveHTTP
from nba_api.stats.library.http import NBAStatsHTTP
from structlog.testing import capture_logs

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
    """
    Queue (status, body) answers for requests.get; records the URLs asked, the
    timeouts given and the pauses taken, on a clock that only they move.
    """

    class Wire:
        def __init__(self):
            self.answers = []
            self.urls = []
            self.timeouts = []
            self.sleeps = []
            self.latency = 0.0  # seconds every answer takes
            self.started = self.now = 1000.0

        @property
        def elapsed(self):
            return self.now - self.started

        def get(self, url, **kwargs):
            self.urls.append(url)
            self.timeouts.append(kwargs["timeout"])
            if self.latency > kwargs["timeout"]:
                self.now += kwargs["timeout"]
                raise patches.requests.exceptions.Timeout("curl: (28) Operation timed out")
            self.now += self.latency
            # The last answer repeats, so "always 500" is one entry.
            status, text = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
            return FakeResponse(status, text)

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            self.now += seconds

    wire = Wire()
    monkeypatch.setattr(patches.requests, "get", wire.get)
    # `patches.time` is the time module itself, so this also takes the wait out
    # of tenacity's backoff in the extractor tests below.
    monkeypatch.setattr(patches.time, "sleep", wire.sleep)
    monkeypatch.setattr(patches.time, "monotonic", lambda: wire.now)
    return wire


@pytest.fixture
def closed_circuit():
    """nba_api_circuit is process-wide state: start closed, leave closed."""
    breaker = CircuitBreakerMonitor.get("nba_api")
    breaker._CircuitBreaker__call_succeeded()
    yield breaker
    breaker._CircuitBreaker__call_succeeded()


def _send(endpoint="playerindex", **kwargs):
    return NBAStatsHTTP().send_api_request(endpoint=endpoint, parameters={"Season": "2026-27"}, **kwargs)


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


class TestTheReaskLog:
    """A bad answer that came good on a re-ask would otherwise leave no trace."""

    def test_each_reask_is_logged_with_what_was_wrong(self, stats):
        stats.answers = [(500, ""), (200, "<html>blocked</html>"), (200, OK_BODY)]
        with capture_logs() as logs:
            _send("leaguedashplayerstats")
        reasks = [entry for entry in logs if entry["event"] == "stats_reask"]
        assert [(e["attempt"], e["status_code"], e["wait"]) for e in reasks] == [(1, 500, 1.0), (2, 200, 2.0)]
        assert all(e["log_level"] == "warning" and e["endpoint"] == "leaguedashplayerstats" for e in reasks)
        assert "HTTP 500 with an empty body" in reasks[0]["error"]
        assert "non-JSON body" in reasks[1]["error"]

    def test_a_good_answer_logs_nothing(self, stats):
        stats.answers = [(200, OK_BODY)]
        with capture_logs() as logs:
            _send()
        assert logs == []

    def test_the_request_that_gives_up_is_not_logged_as_a_reask(self, stats):
        """with_retry logs the error that is raised; a re-ask line would promise a request that is not made."""
        stats.answers = [(500, "")]
        with capture_logs() as logs, pytest.raises(ServerError):
            _send()
        assert [entry["attempt"] for entry in logs] == [1, 2, 3]


class TestTheBudget:
    """One call's re-asks share STATS_BUDGET seconds, requests and pauses together."""

    def test_fast_bad_answers_get_every_reask(self, stats):
        stats.answers = [(500, "")]
        stats.latency = 0.5
        with pytest.raises(ServerError):
            _send()
        assert len(stats.urls) == patches.STATS_ATTEMPTS
        assert stats.sleeps == [1.0, 2.0, 4.0]

    def test_slow_bad_answers_are_not_asked_again_past_the_budget(self, stats):
        stats.answers = [(502, "<html>Bad Gateway</html>")]
        stats.latency = 9.0
        with pytest.raises(ServerError, match="HTTP 502"):
            _send()
        # 9 s + 1 s pause + 9 s: a third request could not start inside 20 s.
        assert len(stats.urls) == 2 and stats.sleeps == [1.0]
        assert stats.elapsed <= patches.STATS_BUDGET

    def test_a_reask_gets_what_is_left_of_the_budget_not_a_fresh_timeout(self, stats):
        stats.answers = [(500, "")]
        stats.latency = 6.0
        with pytest.raises(ServerError) as exc:
            _send()
        assert stats.timeouts == [30, 13.0, 5.0]
        # The third request was cut off at the deadline; the error is still the
        # answer that stayed bad, which with_retry retries and the circuit counts.
        assert exc.value.status_code == 500
        assert isinstance(exc.value.__cause__, patches.requests.exceptions.Timeout)
        assert stats.elapsed == patches.STATS_BUDGET

    def test_a_first_answer_slower_than_the_budget_is_not_asked_again(self, stats):
        stats.answers = [(500, "")]
        stats.latency = 25.0
        with pytest.raises(ServerError):
            _send()
        assert len(stats.urls) == 1 and stats.sleeps == []

    def test_no_reask_is_made_with_under_a_second_left(self, stats):
        """curl reads a timeout that rounds to 0 ms as "no timeout"."""
        stats.answers = [(500, "")]
        stats.latency = 18.5
        with pytest.raises(ServerError):
            _send()
        assert len(stats.urls) == 1 and stats.sleeps == []

    def test_a_shorter_timeout_from_the_caller_is_kept(self, stats):
        stats.answers = [(500, ""), (500, ""), (200, OK_BODY)]
        _send(timeout=5)
        assert stats.timeouts == [5, 5, 5]

    def test_a_timeout_on_the_first_request_is_raised_as_it_is(self, stats):
        stats.answers = [(200, OK_BODY)]
        stats.latency = 31.0
        with pytest.raises(patches.requests.exceptions.Timeout):
            _send()
        assert len(stats.urls) == 1 and stats.sleeps == []

    def test_the_cdn_request_keeps_its_own_timeout(self, stats):
        stats.answers = [(200, "{}")]
        NBALiveHTTP().send_api_request(endpoint="scoreboard/todaysScoreboard_00.json", parameters={}, timeout=10)
        assert stats.timeouts == [10]


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

    def test_an_outage_of_slow_answers_is_bounded_by_the_budget(self, stats, closed_circuit):
        from core.settings import settings

        stats.answers = [(500, "")]
        stats.latency = 10.0
        with pytest.raises(ServerError, match="HTTP 500"):
            NBAApiExtractor().get_player_index("2026-27")
        # Two requests per attempt, not four: 3 x 20 s plus with_retry's 2 s + 4 s.
        assert len(stats.urls) == 2 * settings.retry_max_attempts
        assert stats.elapsed == 3 * patches.STATS_BUDGET + 6
