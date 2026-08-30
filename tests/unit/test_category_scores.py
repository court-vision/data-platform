"""Category snapshot transformer + stat vocabulary parity with the fantasy points formula."""

import json
from pathlib import Path

import pytest

from pipelines.transformers import calculate_fantasy_points
from pipelines.transformers.category_scores import (
    espn_category_scores,
    espn_scoring_format,
    yahoo_category_scores,
)
from utils.stat_vocab import DEFAULT_POINT_WEIGHTS, ESPN_ID_TO_KEY, YAHOO_ID_TO_KEY

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.mark.unit
def test_default_weights_reproduce_calculate_fantasy_points():
    """Pinned against the formula written out literally.

    calculate_fantasy_points now *derives from* DEFAULT_POINT_WEIGHTS
    (cv-core), so comparing the two would be circular — this asserts both
    against the sum the five hand-written copies used to compute.
    """
    s = {"pts": 20, "reb": 10, "ast": 5, "stl": 2, "blk": 1, "tov": 3, "fgm": 8, "fga": 15, "fg3m": 3, "ftm": 5, "fta": 6}
    literal = (s["pts"] + s["reb"] + 2 * s["ast"] + 4 * (s["stl"] + s["blk"])
               - 2 * s["tov"] + s["fg3m"] + (2 * s["fgm"] - s["fga"]) + (s["ftm"] - s["fta"]))
    weighted = sum(DEFAULT_POINT_WEIGHTS[k] * v for k, v in s.items())
    assert literal == weighted == calculate_fantasy_points(s) == 49


@pytest.mark.unit
def test_id_maps_cover_nine_cat():
    for key in ("pts", "reb", "ast", "stl", "blk", "tov", "fg3m", "fg_pct", "ft_pct"):
        assert key in ESPN_ID_TO_KEY.values() and key in YAHOO_ID_TO_KEY.values()


@pytest.mark.unit
def test_espn_snapshot_from_captured_matchup():
    m = json.loads((FIXTURES / "espn_matchup_h2h_points.json").read_text())
    entry = m["schedule"][0]
    assert espn_scoring_format({"scoringSettings": {"scoringType": "H2H_POINTS"}}) == "points"
    assert espn_scoring_format({"scoringSettings": {"scoringType": "H2H_CATEGORY"}}) == "categories"
    snap = espn_category_scores(entry["home"], entry["away"])
    assert snap["scoring_format"] == "categories"
    assert snap["you"]["pts"] > 0 and snap["opp"]["pts"] > 0
    assert 0 < snap["you"]["fg_pct"] < 1                      # derived from fgm/fga
    assert (snap["wins"], snap["losses"], snap["ties"]) == (0, 0, 0)
    assert espn_category_scores({"totalPoints": 1}, {"totalPoints": 2}) is None


@pytest.mark.unit
def test_yahoo_snapshot_counts_stat_winners():
    def team(key, pts, fg):
        return {"team": [[{"team_key": key}], {"team_stats": {"stats": [
            {"stat": {"stat_id": "15", "value": str(pts)}}, {"stat": {"stat_id": "9004003", "value": fg}}]}}]}
    matchup = {
        "stat_winners": [{"stat_winner": {"stat_id": "15", "winner_team_key": "a"}},
                         {"stat_winner": {"stat_id": "7", "is_tied": 1}},
                         {"stat_winner": {"stat_id": "18", "winner_team_key": "b"}}],
        "0": {"teams": {"0": team("a", 500, "200/400"), "1": team("b", 450, "180/360"), "count": 2}},
    }
    snap = yahoo_category_scores(matchup, "a")
    assert snap["you"]["pts"] == 500 and snap["you"]["fg_pct"] == 0.5 and snap["opp"]["fga"] == 360
    assert (snap["wins"], snap["losses"], snap["ties"]) == (1, 1, 1)
    assert yahoo_category_scores({"0": {"teams": {}}}, "a") is None
