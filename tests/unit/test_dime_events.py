"""
What came in, handed to Dime (the Dime autopilot plan, step 6): `BackendClient.post_events`
(POST /v1/internal/jobs/checkins/events) and the two pipelines that call it, the
availability pipeline with its held verdict changes and the news feed with its new items.
The backend owns the events and the check-ins; nothing here touches the database.
"""

import json
from datetime import date
from types import SimpleNamespace

import pytest
import requests
import responses

from core.settings import settings
from pipelines.availability import AvailabilityPipeline
from pipelines.context import PipelineContext
from pipelines.news_feed import NewsFeedPipeline
from services.backend_client import POST_EVENTS_PATH, BackendClient, CheckinStarted, EventsPost

pytestmark = pytest.mark.unit

BASE = "http://api.railway.internal:8080"
URL = BASE + POST_EVENTS_PATH
TOKEN = "pipeline-secret-token"
NIGHT = date(2026, 11, 10)


def _client():
    return BackendClient(base_url=BASE, token=TOKEN, timeout_seconds=45.0)


def _envelope(**data):
    body = {"off": False, "teams": 2, "facts": 3, "recorded": 2, "started": [], **data}
    return {"status": "success", "message": "ok", "data": body}


def test_disabled_without_base_url():
    post = BackendClient(base_url=None, token=TOKEN, timeout_seconds=45.0).post_events(news=[1])
    assert (post.ok, post.reason) == (False, "backend_not_configured")


@responses.activate
def test_200_parses_what_was_started_and_sends_the_contract():
    started = {"checkin_id": 41, "team_id": 23, "kind": "event", "date": "2026-11-10"}
    responses.post(URL, json=_envelope(started=[started]), status=200)

    post = _client().post_events(availability=[(1628973, NIGHT)], news=[7, 9], correlation_id="corr-3")

    assert post.ok and not post.off and (post.teams, post.facts, post.recorded) == (2, 3, 2)
    assert post.started == [CheckinStarted(checkin_id=41, team_id=23, kind="event", date="2026-11-10")]
    request = responses.calls[0].request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.headers["X-Correlation-ID"] == "corr-3"
    assert json.loads(request.body) == {
        "availability": [{"player_id": 1628973, "game_date": "2026-11-10"}],
        "news": [7, 9],
    }


@responses.activate
def test_events_switched_off_is_an_ok_answer():
    responses.post(URL, json=_envelope(off=True, teams=0, facts=0, recorded=0), status=200)
    post = _client().post_events(news=[1])
    assert post.ok and post.off and post.recorded == 0


@responses.activate
@pytest.mark.parametrize("status,body,reason", [
    (500, {"message": "boom"}, "http_500"),
    (401, {"detail": "bad token"}, "unauthorized"),
    (200, {"status": "success", "data": {"started": "nope"}}, "malformed_response"),
])
def test_a_failed_post_is_not_ok_and_never_raises(status, body, reason):
    responses.post(URL, json=body, status=status)
    post = _client().post_events(news=[1])
    assert (post.ok, post.reason) == (False, reason)
    assert len(responses.calls) == 1


@responses.activate
def test_a_connection_error_is_not_ok():
    responses.post(URL, body=requests.ConnectionError("refused"))
    assert _client().post_events(news=[1]).reason == "ConnectionError"


# ---- the callers -----------------------------------------------------------------------


class FakeClient:
    def __init__(self):
        self.posts: list[dict] = []

    def post_events(self, **kwargs):
        self.posts.append(kwargs)
        return EventsPost(ok=True, recorded=1)


def _ctx(name: str) -> PipelineContext:
    return PipelineContext(pipeline_name=name)


def _change(pid: int) -> SimpleNamespace:
    return SimpleNamespace(player_id=pid)


def test_the_availability_pipeline_posts_each_held_player_once(monkeypatch):
    monkeypatch.setattr(settings, "post_dime_events", True)
    pipeline = AvailabilityPipeline()
    pipeline.backend_client = FakeClient()

    pipeline._post_events(_ctx("availability"), NIGHT, [_change(3), _change(1), _change(3)])

    assert [p["availability"] for p in pipeline.backend_client.posts] == [[(1, NIGHT), (3, NIGHT)]]


def test_nothing_is_posted_with_the_posts_switched_off(monkeypatch):
    monkeypatch.setattr(settings, "post_dime_events", False)
    availability, news = AvailabilityPipeline(), NewsFeedPipeline()
    availability.backend_client = news.backend_client = FakeClient()

    availability._post_events(_ctx("availability"), NIGHT, [_change(1)])
    news._post_events(_ctx("news_feed"), [5])

    assert availability.backend_client.posts == []


def test_the_news_feed_posts_its_new_items(monkeypatch):
    monkeypatch.setattr(settings, "post_dime_events", True)
    pipeline = NewsFeedPipeline()
    pipeline.backend_client = FakeClient()

    pipeline._post_events(_ctx("news_feed"), [5, 8])

    assert [p["news"] for p in pipeline.backend_client.posts] == [[5, 8]]
