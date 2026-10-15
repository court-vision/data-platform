"""
The ESPN injuries extractor's parser against a real payload (fetched
2026-10-15, preseason; trimmed of logos and extra links), then fed through the
availability resolver.
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from pipelines.extractors.espn_injuries import nba_tricode, parse_espn_injuries
from pipelines.transformers.availability import GameContext, Report, resolve

pytestmark = pytest.mark.unit

PAYLOAD = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "espn_injuries_2026-10-15.json").read_text()
)


@pytest.fixture(scope="module")
def parsed():
    return parse_espn_injuries(PAYLOAD)


def by_name(entries, name):
    return next(e for e in entries if e.name == name)


def test_every_entry_parses(parsed):
    entries, skipped = parsed
    assert (len(entries), skipped) == (135, 0)
    assert {e.status for e in entries} == {"out", "questionable"}
    assert sum(e.status == "out" for e in entries) == 22


def test_a_day_to_day_entry(parsed):
    gilbert = by_name(parsed[0], "Keshon Gilbert")
    assert (gilbert.espn_id, gilbert.team, gilbert.status, gilbert.fantasy_status) == (4585618, "ATL", "questionable", "GTD")
    assert (gilbert.reason, gilbert.expected_return) == ("Knee", date(2026, 10, 16))
    assert gilbert.source_time == datetime(2026, 10, 12, 21, 36, tzinfo=timezone.utc)
    assert gilbert.detail.startswith("Gilbert (knee) won't play in Monday's preseason game")


def test_teams_are_nba_tricodes(parsed):
    teams = {e.team for e in parsed[0]}
    assert {"GSW", "NOP", "NYK", "SAS", "UTA", "WAS"} <= teams
    assert not teams & {"GS", "NO", "NY", "SA", "UTAH", "WSH"}
    assert nba_tricode("LAL") == "LAL" and nba_tricode(None) is None


def test_out_for_season_is_out_with_a_far_return_date(parsed):
    ofs = [e for e in parsed[0] if e.fantasy_status == "OFS"]
    assert len(ofs) == 2 and all(e.status == "out" and e.expected_return == date(2027, 7, 1) for e in ofs)


def test_entries_without_an_id_or_a_known_status_are_counted_not_dropped():
    payload = {"injuries": [{"injuries": [
        {"id": "1", "status": "Day-To-Day", "athlete": {"displayName": "No Id", "links": []}},
        {"id": "2", "status": "Suspended", "athlete": {"links": [{"href": "https://www.espn.com/nba/player/_/id/9/x"}]}},
        {"id": "3", "status": "Out", "athlete": {"headshot": {"href": "https://a.espncdn.com/i/headshots/nba/players/full/77.png"}}},
    ]}]}
    entries, skipped = parse_espn_injuries(payload)
    assert [e.espn_id for e in entries] == [77] and skipped == 2


def test_the_monday_blurb_makes_gilbert_out_for_monday_only(parsed):
    """Day-To-Day plus "won't play in Monday's ... game" -> out on Monday 10/12, questionable on Friday."""
    g = by_name(parsed[0], "Keshon Gilbert")
    report = Report(source="espn_injuries", status=g.status, observed_at=g.source_time, source_time=g.source_time,
                    reason=g.reason, detail=g.detail, expected_return=g.expected_return)
    assert resolve([report], GameContext(game_date=date(2026, 10, 12))).verdict == "out"
    assert resolve([report], GameContext(game_date=date(2026, 10, 16))).verdict == "questionable"
