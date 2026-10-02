"""
The projections editor's pure parts: trying an edit on, what is sent for
valuation, the page's rows, the preview, and the "is the board reading this?"
count. Inputs are built by hand — two veterans and a rookie — so every number
can be checked on paper; the database read and the routes have their own tests.
"""

from datetime import date, datetime

import pytest

from db.models.nba import ProjectionAdjustment
from schemas.projections import AdjustmentChange
from services import projection_editor as editor
from services.projection_inputs import ProjectionInputs
from services.projection_model import LINE_KEYS, Coefficients, EspnLine, HistoryRow
from services.valuation_client import StandardRank, Valuation

pytestmark = pytest.mark.unit

FLAT = Coefficients(curve={}, gp_beta=[0.72, 0, 0, 0, 0, 0, 0, 0, 0], weights=(7, 2, 1),
                    reg_minutes=0.0, reg_games=0.0, gp_shrink=0.0, espn_weight=0.5, version="test-1")
PER_MIN = {"pts": 0.6, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
           "fgm": 0.22, "fga": 0.46, "fg3m": 0.07, "fg3a": 0.19, "ftm": 0.09, "fta": 0.11,
           "oreb": 0.04, "dreb": 0.16}
STAR, VET, ROOKIE = 1, 2, 3


def _history(pid, mpg=30.0, gp=70):
    minutes = gp * mpg
    totals = {k: v * minutes for k, v in PER_MIN.items()} | {"dd2": 7.0, "td3": 0.0}
    return HistoryRow(player_id=pid, season=2025, gp=gp, minutes=minutes, totals=totals, age=27.0, from_year=2018)


def _espn(pts, minutes=34.0, games=72.0):
    return EspnLine(per_game={k: 1.0 for k in LINE_KEYS} | {"pts": pts}, minutes=minutes, games=games)


def _record(pid, **fields):
    base = dict(id=40 + pid, player=pid, season="2026-27", kind="role", note="why", author="jp",
                created_at=datetime(2026, 10, 1, 12, 0))
    base.update(fields)
    return ProjectionAdjustment(**base)


def _inputs(records=None) -> ProjectionInputs:
    # Phoenix plays twice in January, once in March.
    per_day = {date(2027, 1, 5): frozenset({"PHX"}), date(2027, 1, 9): frozenset({"PHX", "DEN"}),
               date(2027, 3, 2): frozenset({"PHX"})}
    return ProjectionInputs(
        season="2026-27", target=2026,
        history={STAR: [_history(STAR, mpg=34.0)], VET: [_history(VET, mpg=24.0)]},
        espn={STAR: _espn(27.0), ROOKIE: _espn(15.0, minutes=28.0, games=70.0)},
        espn_as_of=date(2026, 9, 30),
        current={STAR: "DEN", VET: "PHX"},
        roster={STAR, VET, ROOKIE},
        per_day=per_day, coeffs=FLAT,
        records=records or {},
    )


def _valuation(order_points, order_cats=None) -> Valuation:
    order_cats = order_cats or order_points
    ranks = {
        pid: StandardRank(player_id=pid, points_rank=order_points.index(pid) + 1, points_value=30.0,
                          category_rank=order_cats.index(pid) + 1, category_value=28.0)
        for pid in order_points
    }
    return Valuation(available=True, ranks=ranks, league_size=12, rounds=13, playoff_weight=2.0,
                     playoff_weeks=(20, 21, 22, 23))


# ---- trying an edit on -------------------------------------------------------------------


def test_a_change_resolves_like_a_stored_adjustment():
    inputs = _inputs()
    change = AdjustmentChange(minutes=30.0, games=60, usage=1.1, rates={"blk": 1.2})
    adj = editor.resolve_change(inputs, VET, change)
    assert (adj.id, adj.minutes, adj.games, adj.usage, adj.rates) == (None, 30.0, 60.0, 1.1, {"blk": 1.2})
    assert adj.games_available is None

    # A return date is the games his team has left on the calendar from that day.
    back = editor.resolve_change(inputs, VET, AdjustmentChange(return_date=date(2027, 1, 9)))
    assert back.games_available == 2.0
    # No team on file, nothing to count; and no change at all is no adjustment.
    assert editor.resolve_change(inputs, ROOKIE, AdjustmentChange(return_date=date(2027, 1, 9))).games_available is None
    assert editor.resolve_change(inputs, VET, None) is None
    assert editor.resolve_change(inputs, VET, AdjustmentChange()) is None


def test_an_override_replaces_or_removes_one_players_live_adjustment():
    inputs = _inputs({VET: _record(VET, minutes=32.0)})
    by_id = lambda lines: {b.player_id: b for b in lines}      # noqa: E731

    live = by_id(editor.breakdowns(inputs))
    assert live[VET].final.minutes == 32.0 and live[VET].blended.minutes == pytest.approx(24.0)

    tried = by_id(editor.breakdowns(
        inputs, override=(VET, editor.resolve_change(inputs, VET, AdjustmentChange(minutes=28.0)))))
    assert tried[VET].final.minutes == 28.0
    assert tried[STAR].final.line() == live[STAR].final.line()         # nobody else moves

    without = by_id(editor.breakdowns(inputs, override=(VET, None)))
    assert without[VET].final.minutes == pytest.approx(24.0)
    # The inputs themselves are untouched: the next read still sees the live row.
    assert by_id(editor.breakdowns(inputs))[VET].final.minutes == 32.0


def test_the_pool_is_sent_as_the_pipeline_stores_it():
    inputs = _inputs()
    lines = editor.breakdowns(inputs)
    players = {p["player_id"]: p for p in editor.valuation_players(lines, inputs.current)}

    star = next(b for b in lines if b.player_id == STAR)
    assert players[STAR]["line"] == star.final.line()                  # two decimals, min included
    assert set(players[STAR]["line"]) == set(LINE_KEYS) | {"min"}
    assert players[STAR]["games"] == int(round(star.final.games)) and isinstance(players[STAR]["games"], int)
    assert players[STAR]["team"] == "DEN" and players[ROOKIE]["team"] is None
    assert players[STAR]["dd_rate"] == round(star.final.dd_rate, 4) == 0.1


# ---- the page ----------------------------------------------------------------------------


def test_the_page_shows_four_lines_ranks_and_the_live_adjustment():
    record = _record(VET, minutes=32.0, games=64, rates={"blk": 1.1}, source_url="https://example.test/a")
    state = editor.EditorState(
        inputs=_inputs({VET: record}),
        players={STAR: ("Star", "G"), VET: ("Vet", "F"), ROOKIE: ("Rookie", "C")},
        market={STAR: (1, 3), ROOKIE: (40, None)},
        published_as_of=date(2026, 10, 1),
    )
    lines = editor.breakdowns(state.inputs)
    now = datetime(2026, 10, 1, 15, 0)

    data = editor.build_view(state, lines, _valuation([STAR, ROOKIE, VET], [VET, STAR, ROOKIE]), now)

    assert (data.season, data.coefficients_version, data.espn_weight) == ("2026-27", "test-1", 0.5)
    assert (data.espn_as_of, data.published_as_of, data.fetched_at) == (date(2026, 9, 30), date(2026, 10, 1), now)
    assert data.ranks_available and data.league.league_size == 12 and data.league.playoff_weeks == [20, 21, 22, 23]
    assert data.kinds[0] == "year2" and "injury_return" in data.kinds
    # Best standard-points rank first.
    assert [row.name for row in data.players] == ["Star", "Rookie", "Vet"]

    star, rookie, vet = data.players
    assert (star.ranks.points, star.ranks.categories) == (1, 2)
    assert (star.espn_ranks.points, star.espn_ranks.categories) == (1, 3)
    assert star.statistical.min == 34.0 and star.espn.pts == 27.0 and star.espn.games == 72.0
    assert star.blended.pts == star.final.pts and star.adjustment is None
    assert (star.team, star.position, star.age, star.seasons, star.espn_weight) == ("DEN", "G", 28.0, [2025], 0.5)

    # A rookie has no history: ESPN's line is his line, and nobody ranks him in categories.
    assert rookie.statistical is None and rookie.espn_weight == 1.0 and rookie.team is None
    assert rookie.final.pts == 15.0 and rookie.espn_ranks.categories is None

    # No ESPN line: the blend is the statistical line, and the adjustment sits on top.
    assert vet.espn is None and vet.espn_weight == 0.0 and vet.espn_ranks.points is None
    assert vet.blended.min == vet.statistical.min == 24.0
    assert (vet.final.min, vet.final.games) == (32.0, 64.0)
    assert vet.final.blk == pytest.approx(vet.blended.blk * (32 / 24) * 1.1, abs=0.01)
    assert (vet.adjustment.id, vet.adjustment.state, vet.adjustment.minutes) == (42, "live", 32.0)
    assert vet.adjustment.rates == {"blk": 1.1} and vet.adjustment.source_url == "https://example.test/a"


def test_without_the_backend_the_page_still_has_its_lines():
    state = editor.EditorState(inputs=_inputs(), players={STAR: ("Star", "G"), VET: ("Vet", "F"), ROOKIE: ("Rookie", "C")})
    data = editor.build_view(state, editor.breakdowns(state.inputs), Valuation.unavailable("timeout"))

    assert not data.ranks_available and data.ranks_reason == "timeout" and data.league is None
    assert [row.name for row in data.players] == ["Rookie", "Star", "Vet"]      # name order instead
    assert all(row.ranks.points is None and row.ranks.categories is None for row in data.players)
    assert all(row.final.pts > 0 for row in data.players)


def test_unpublished_counts_what_the_board_is_not_reading():
    state = editor.EditorState(inputs=_inputs())
    lines = editor.breakdowns(state.inputs)
    stored = {b.player_id: (b.final.line(), int(round(b.final.games))) for b in lines}

    state.published = dict(stored)
    assert editor.unpublished_count(state, lines) == 0

    # One line edited since, one player never published, one published and gone.
    state.published = {
        STAR: ({**stored[STAR][0], "pts": stored[STAR][0]["pts"] + 0.5}, stored[STAR][1]),
        VET: stored[VET],
        99: stored[VET],
    }
    assert editor.unpublished_count(state, lines) == 3
    # Games count too, and a rounding step in a stat does not.
    state.published = {**stored, STAR: (stored[STAR][0], stored[STAR][1] - 1)}
    assert editor.unpublished_count(state, lines) == 1
    state.published = {**stored, STAR: ({**stored[STAR][0], "pts": stored[STAR][0]["pts"] + 0.004}, stored[STAR][1])}
    assert editor.unpublished_count(state, lines) == 0


# ---- the preview -------------------------------------------------------------------------


def test_a_preview_is_the_same_player_before_and_after():
    inputs = _inputs()
    before = editor.breakdowns(inputs)
    change = editor.resolve_change(inputs, VET, AdjustmentChange(minutes=36.0, games=75))
    after = editor.breakdowns(inputs, override=(VET, change))

    preview = editor.build_preview(
        VET, before, _valuation([STAR, ROOKIE, VET]), after, _valuation([STAR, VET, ROOKIE], [VET, STAR, ROOKIE]),
    )

    assert preview.player_id == VET and preview.ranks_available
    assert (preview.before.final.min, preview.after.final.min) == (24.0, 36.0)
    assert preview.after.final.games == 75.0
    assert preview.after.final.pts == pytest.approx(preview.before.final.pts * 1.5, abs=0.01)
    assert (preview.before.ranks.points, preview.after.ranks.points) == (3, 2)
    assert (preview.before.ranks.categories, preview.after.ranks.categories) == (3, 1)


def test_a_preview_without_ranks_says_why_and_an_unprojected_player_has_none():
    inputs = _inputs()
    lines = editor.breakdowns(inputs)
    preview = editor.build_preview(VET, lines, _valuation([STAR, ROOKIE, VET]), lines, Valuation.unavailable("http_503"))
    assert not preview.ranks_available and preview.ranks_reason == "http_503"
    assert preview.before.ranks.points == 3 and preview.after.ranks.points is None
    assert editor.build_preview(404, lines, Valuation(available=True), lines, Valuation(available=True)) is None


# ---- versions ----------------------------------------------------------------------------


def test_an_entry_says_whether_it_is_still_live():
    live = _record(VET, minutes=32.0)
    old = _record(VET, id=7, minutes=30.0, superseded_by=42)
    gone = _record(VET, id=5, games=50, retired_at=datetime(2026, 9, 20))

    assert [editor.adjustment_entry(r).state for r in (live, old, gone)] == ["live", "superseded", "retired"]
    entry = editor.adjustment_entry(_record(VET, usage=0.95, return_date=date(2027, 1, 5)))
    assert (entry.usage, entry.return_date, entry.minutes, entry.rates) == (0.95, date(2027, 1, 5), None, None)
