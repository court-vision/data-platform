"""The Court Vision projection's pure parts: the statistical line, ESPN blend,
adjustments, the roster rule, and fitting/backtesting on synthetic history.

Every player below is built by hand so each number can be checked on paper;
the committed coefficients file is checked for shape, not for its values.
"""

import json
from datetime import date

import pytest

from services.projection_inputs import COEFFICIENTS_PATH, adjustment_of, per_day_calendar
from pipelines.season_history import history_record
from services.projection_fit import backtest, fit_curve, fit_games, league_drift
from services.projection_model import (
    LINE_KEYS,
    RATE_KEYS,
    Adjustment,
    Coefficients,
    EspnLine,
    HistoryRow,
    adjust,
    blend,
    bucket,
    games_fraction,
    project,
    project_breakdown,
    season_label,
    season_start,
    statistical_line,
    team_games_after,
)

pytestmark = pytest.mark.unit

FLAT = Coefficients(curve={}, gp_beta=[0.72, 0, 0, 0, 0, 0, 0, 0, 0], weights=(7, 2, 1),
                    reg_minutes=0.0, reg_games=0.0, gp_shrink=0.0, espn_weight=0.5)


def _row(pid, season, gp=70, mpg=30.0, per_min=None, age=27.0, from_year=2018, **extra):
    per_min = per_min or {"pts": 0.6, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
                          "fgm": 0.22, "fga": 0.46, "fg3m": 0.07, "fg3a": 0.19, "ftm": 0.09, "fta": 0.11,
                          "oreb": 0.04, "dreb": 0.16}
    minutes = gp * mpg
    totals = {k: v * minutes for k, v in per_min.items()}
    totals.update({"dd2": extra.get("dd2", 10.0), "td3": extra.get("td3", 1.0)})
    return HistoryRow(player_id=pid, season=season, gp=gp, minutes=minutes, totals=totals,
                      age=age, from_year=from_year)


CELL = {k: 0.1 for k in RATE_KEYS} | {"MPG": 20.0}


class TestBuckets:
    def test_experience_first_then_age(self):
        assert bucket(1, 20.0) == "exp1"
        assert bucket(3, 22.0) == "exp3"
        assert bucket(4, 24.0) == "age24"
        assert bucket(None, 40.0) == "age37"
        assert bucket(None, None) == "age27"

    def test_season_labels(self):
        assert season_start("2025-26") == 2025
        assert season_label(2025) == "2025-26"
        assert season_label(2099) == "2099-00"


class TestStatisticalLine:
    def test_flat_curve_no_regression_reproduces_the_history(self):
        p = statistical_line([_row(1, 2025)], 2026, FLAT, CELL)
        assert p.minutes == pytest.approx(30.0)
        assert p.per_game["pts"] == pytest.approx(18.0)
        assert p.games == pytest.approx(0.72 * 82)

    def test_recent_seasons_weigh_more(self):
        old = _row(1, 2023, mpg=20.0)
        new = _row(1, 2025, mpg=34.0)
        p = statistical_line([old, new], 2026, FLAT, CELL)
        assert 32.0 < p.minutes < 34.0         # 7:1 toward the newer season

    def test_regression_pulls_a_thin_sample_to_its_cell(self):
        coeffs = Coefficients(curve={}, gp_beta=FLAT.gp_beta, reg_minutes=600.0, reg_games=0.0)
        thin = statistical_line([_row(1, 2025, gp=10, mpg=15.0)], 2026, coeffs, CELL)
        thick = statistical_line([_row(2, 2025, gp=80, mpg=35.0)], 2026, coeffs, CELL)
        cell_rate = CELL["pts"]
        thin_rate = thin.per_game["pts"] / thin.minutes
        thick_rate = thick.per_game["pts"] / thick.minutes
        assert abs(thin_rate - cell_rate) < abs(thick_rate - cell_rate)

    def test_the_curve_ages_every_season_forward(self):
        curve = {"exp1": {k: 1.1 for k in RATE_KEYS} | {"MPG": 3.0}}
        coeffs = Coefficients(curve=curve, gp_beta=FLAT.gp_beta, reg_minutes=0.0, reg_games=0.0)
        rookie = _row(1, 2025, from_year=2025, age=20.0)
        p = statistical_line([rookie], 2026, coeffs, CELL)
        assert p.minutes == pytest.approx(33.0)
        assert p.per_game["pts"] / p.minutes == pytest.approx(0.6 * 1.1)

    def test_no_evidence_is_none(self):
        zero = Coefficients(curve={}, gp_beta=FLAT.gp_beta, weights=(1, 0, 0), reg_minutes=0.0, reg_games=0.0)
        assert statistical_line([_row(1, 2023)], 2026, zero, CELL) is None

    def test_double_double_rates_come_from_history(self):
        p = statistical_line([_row(1, 2025, gp=70, dd2=35.0, td3=7.0)], 2026, FLAT, CELL)
        assert p.dd_rate == pytest.approx(0.5)
        assert p.td_rate == pytest.approx(0.1)


class TestGames:
    BETA = [0.2, 0.42, 0.0, 0.14, 0.08, 0.08, 0.06, -0.007, 0.01]

    def test_a_lost_season_hurts_less_once_shrunk(self):
        rows = [_row(1, 2025, gp=10), _row(1, 2024, gp=78), _row(1, 2023, gp=76)]
        linear = games_fraction(rows, 2026, 27.0, Coefficients(curve={}, gp_beta=self.BETA, gp_shrink=0.0))
        shrunk = games_fraction(rows, 2026, 27.0, Coefficients(curve={}, gp_beta=self.BETA, gp_shrink=0.5))
        assert linear < shrunk < 0.72 + 0.1

    def test_clipped_before_shrinking(self):
        beta = [5.0, 0, 0, 0, 0, 0, 0, 0, 0]
        f = games_fraction([_row(1, 2025)], 2026, 27.0, Coefficients(curve={}, gp_beta=beta, gp_shrink=0.0))
        assert f == 0.98


class TestBlendAndAdjust:
    def _stat(self):
        return statistical_line([_row(1, 2025)], 2026, FLAT, CELL)

    def test_blend_halves_toward_espn(self):
        espn = EspnLine(per_game={k: 0.0 for k in LINE_KEYS} | {"pts": 24.0}, minutes=36.0, games=70.0)
        p = blend(self._stat(), espn, 1, 0.5)
        assert p.minutes == pytest.approx(33.0)
        assert p.games == pytest.approx((70.0 + 0.72 * 82) / 2)
        assert p.per_game["pts"] / p.minutes == pytest.approx((24.0 / 36.0 + 0.6) / 2)
        assert p.per_game["oreb"] > 0          # ESPN has no oreb; the statistical rate stands

    def test_rookie_takes_espns_line(self):
        espn = EspnLine(per_game={k: 1.0 for k in LINE_KEYS}, minutes=25.0, games=None)
        p = blend(None, espn, 9, 0.5)
        assert p.minutes == 25.0 and p.per_game["pts"] == 1.0
        assert p.components["espn_weight"] == 1.0

    def test_rookie_games_are_blended_with_what_lottery_rookies_actually_play(self):
        espn = EspnLine(per_game={k: 1.0 for k in LINE_KEYS}, minutes=25.0, games=72.0)
        p = blend(None, espn, 9, 0.5, rookie_games=64.0)
        assert p.games == pytest.approx(68.0)
        assert blend(None, EspnLine(per_game={}, minutes=25.0, games=None), 9, 0.5, rookie_games=64.0).games == 64.0

    def test_project_uses_the_rookie_prior_not_the_veteran_one(self):
        espn = {3: EspnLine(per_game={k: 1.0 for k in LINE_KEYS}, minutes=20.0, games=72.0)}
        coeffs = Coefficients(curve={}, gp_beta=FLAT.gp_beta, gp_mean=0.60, rookie_gp_mean=0.80)
        out = project(2026, {}, espn, {}, roster={3}, coeffs=coeffs)
        assert out[0].games == pytest.approx(0.5 * 72.0 + 0.5 * 0.80 * 82)

    def test_neither_is_nothing(self):
        assert blend(None, None, 1, 0.5) is None

    def test_minutes_target_rescales_at_constant_production(self):
        p = adjust(self._stat(), Adjustment(id=4, minutes=24.0))
        assert p.minutes == 24.0
        assert p.per_game["pts"] == pytest.approx(0.6 * 24.0)
        assert p.components["adjustment_id"] == 4

    def test_usage_keeps_the_shooting_percentages(self):
        before = self._stat()
        after = adjust(before, Adjustment(usage=1.2))
        assert after.per_game["pts"] == pytest.approx(before.per_game["pts"] * 1.2)
        assert after.per_game["fgm"] / after.per_game["fga"] == pytest.approx(
            before.per_game["fgm"] / before.per_game["fga"])
        assert after.per_game["reb"] == pytest.approx(before.per_game["reb"])

    def test_return_date_caps_games_at_his_share_of_what_is_left(self):
        p = adjust(self._stat(), Adjustment(games_available=41.0))
        assert p.games == pytest.approx(41.0 * 0.72)

    def test_explicit_games_still_cannot_exceed_what_is_left(self):
        assert adjust(self._stat(), Adjustment(games=60.0, games_available=30.0)).games == 30.0
        assert adjust(self._stat(), Adjustment(games=25.0, games_available=30.0)).games == 25.0


class TestProject:
    def test_roster_decides_who_is_projected(self):
        history = {1: [_row(1, 2025)], 2: [_row(2, 2025)]}          # 2 retired
        espn = {3: EspnLine(per_game={k: 1.0 for k in LINE_KEYS}, minutes=20.0, games=60.0)}  # rookie
        out = project(2026, history, espn, {}, roster={1, 3}, coeffs=FLAT)
        assert sorted(p.player_id for p in out) == [1, 3]

    def test_seasons_outside_the_window_are_ignored(self):
        history = {1: [_row(1, 2020)]}
        assert project(2026, history, {}, {}, roster={1}, coeffs=FLAT) == []

    def test_adjustment_applies_last(self):
        history = {1: [_row(1, 2025)]}
        out = project(2026, history, {}, {1: Adjustment(games=40.0)}, roster={1}, coeffs=FLAT)
        assert out[0].games == 40.0
        assert out[0].components["version"] == FLAT.version


class TestBreakdown:
    """The four lines the editor shows side by side."""

    ESPN = EspnLine(per_game={k: 1.0 for k in LINE_KEYS} | {"pts": 24.0}, minutes=34.0, games=72.0)

    def test_every_stage_is_kept_and_the_published_line_is_the_last(self):
        history = {1: [_row(1, 2025)]}
        adj = {1: Adjustment(id=9, minutes=36.0, games=60)}
        (b,) = project_breakdown(2026, history, {1: self.ESPN}, adj, {1}, FLAT)

        assert b.statistical.minutes == pytest.approx(30.0)        # history alone
        assert b.espn is self.ESPN
        assert b.blended.minutes == pytest.approx(32.0)            # halfway to ESPN's 34
        assert b.blended.games == pytest.approx((72 + 0.72 * 82) / 2)
        assert (b.final.minutes, b.final.games) == (36.0, 60.0)    # the adjustment, last
        assert b.final.components["adjustment_id"] == 9
        # `project` is the published line of the same computation.
        (p,) = project(2026, history, {1: self.ESPN}, adj, {1}, FLAT)
        assert p.line() == b.final.line() and p.games == b.final.games

    def test_the_statistical_line_shown_is_not_the_object_later_stages_write_to(self):
        """With no ESPN line the blend *is* the statistical line, and the
        adjustment then scales it. The editor's copy must still say what
        history alone said."""
        history = {1: [_row(1, 2025)]}
        (b,) = project_breakdown(2026, history, {}, {1: Adjustment(usage=2.0)}, {1}, FLAT)

        assert b.espn is None
        assert b.statistical.per_game["pts"] == pytest.approx(18.0)
        assert b.final.per_game["pts"] == pytest.approx(36.0)
        assert b.statistical is not b.blended
        assert "espn_weight" not in b.statistical.components and b.blended.components["espn_weight"] == 0.0

    def test_a_rookie_has_no_statistical_line(self):
        (b,) = project_breakdown(2026, {}, {7: self.ESPN}, {}, {7}, FLAT)
        assert b.statistical is None and b.espn is self.ESPN
        assert b.blended.per_game["pts"] == 24.0 and b.final is b.blended


class TestFit:
    def _league(self):
        rows = []
        for pid in range(40):
            for season in range(2016, 2026):
                age = 21.0 + (pid % 15) + (season - 2016)
                rows.append(_row(pid, season, gp=60 + (pid % 20), mpg=20.0 + (pid % 10), age=age,
                                 from_year=season - int(age - 20)))
        return rows

    def test_curve_has_experience_and_age_buckets(self):
        curve = fit_curve(self._league(), through=2025)
        assert any(k.startswith("age") for k in curve)
        assert all(0.5 < v["pts"] < 2.0 for v in curve.values())

    def test_flat_league_has_no_drift(self):
        drift = league_drift(self._league())
        assert all(abs(v) < 1e-9 for season in drift.values() for v in season.values())

    def test_games_fit_is_a_coefficient_per_feature(self):
        beta = fit_games(self._league(), through=2025, first=2019)
        assert len(beta) == 9

    def test_backtest_runs_and_scores(self):
        rows = self._league()
        coeffs = Coefficients(curve=fit_curve(rows, 2022), gp_beta=fit_games(rows, 2022, 2019))
        result = backtest(rows, [2023, 2024, 2025], coeffs)
        assert result.fpts_pg_mae >= 0 and result.targets == (2023, 2024, 2025)


def test_committed_coefficients_load_and_cover_every_bucket():
    doc = json.loads(COEFFICIENTS_PATH.read_text())
    coeffs = Coefficients.from_json(doc)
    assert {"exp1", "exp2", "exp3"} <= set(coeffs.curve)
    assert {f"age{a}" for a in range(24, 36)} <= set(coeffs.curve)
    assert len(coeffs.gp_beta) == 9
    assert tuple(coeffs.weights) == (7.0, 2.0, 1.0)
    assert doc["backtest"]["model"]["fpts_pg_mae"] < doc["backtest"]["last_season_line"]["fpts_pg_mae"]


class TestPipelineHelpers:
    def test_history_record_maps_totals_and_usage(self):
        total = {"PLAYER_ID": 203999, "PLAYER_NAME": "Nikola Jokic", "TEAM_ABBREVIATION": "DEN",
                 "AGE": 30.0, "GP": 65, "MIN": 2261.4, "PTS": 1914, "REB": 810, "AST": 646,
                 "STL": 90, "BLK": 45, "TOV": 216, "FGM": 700, "FGA": 1200, "FG3M": 120, "FG3A": 300,
                 "FTM": 390, "FTA": 480, "OREB": 180, "DREB": 630, "DD2": 55, "TD3": 30}
        rec = history_record("2025-26", total, {"USG_PCT": 0.3021}, 2015)
        assert rec["player_id"] == 203999 and rec["season"] == "2025-26"
        assert rec["gp"] == 65 and rec["min"] == 2261.4 and rec["dd2"] == 55
        assert rec["usg_pct"] == 0.302 and rec["from_year"] == 2015

    def test_history_record_skips_players_without_games(self):
        assert history_record("2025-26", {"PLAYER_ID": 1, "GP": 0}, None, None) is None
        assert history_record("2025-26", {"GP": 5}, None, None) is None

    def test_team_games_after_a_return_date(self):
        per_day = {date(2027, 1, 1): frozenset({"MIA", "BOS"}), date(2027, 1, 3): frozenset({"MIA"}),
                   date(2026, 12, 30): frozenset({"MIA"})}
        assert team_games_after(date(2027, 1, 1), per_day, "MIA") == 2
        assert team_games_after(date(2027, 1, 1), per_day, None) is None

    def test_real_calendar_parses_and_adjustment_resolves_a_return_date(self):
        per_day = per_day_calendar("2026-27")
        if not per_day:
            pytest.skip("2026-27 calendar not on disk")
        assert sum(1 for teams in per_day.values() if "PHX" in teams) >= 80

        class Rec:
            id = 7
            minutes = None
            games = None
            return_date = date(2027, 1, 1)
            usage = None
            rates = None

        adj = adjustment_of(Rec(), "PHX", per_day)
        assert 30 <= adj.games_available <= 50


class TestSeedRows:
    def _parse(self, **row):
        from scripts.seed_projection_adjustments import parse_row

        base = {"player": "X", "kind": "role", "minutes": "", "games": "", "return_date": "",
                "usage": "", "note": "why", "source_url": ""}
        base.update(row)
        return parse_row(base)

    def test_numbers_and_dates_parse(self):
        f = self._parse(minutes="30", games="68", return_date="2027-01-05", usage="0.95")
        assert f == {"kind": "role", "note": "why", "source_url": None, "minutes": 30.0, "games": 68,
                     "return_date": date(2027, 1, 5), "usage": 0.95}

    @pytest.mark.parametrize("row", [dict(kind="guess", games="60"), dict(), dict(games="60", note=" ")])
    def test_bad_rows_are_refused(self, row):
        with pytest.raises(ValueError):
            self._parse(**row)

    def test_the_committed_seed_parses(self):
        import csv
        from pathlib import Path

        from scripts.seed_projection_adjustments import parse_row

        path = Path(__file__).resolve().parents[2] / "seeds" / "projection_adjustments_2026_27.csv"
        rows = list(csv.DictReader(path.open(newline="")))
        assert len(rows) >= 40
        for row in rows:
            parse_row(row)


class TestSeedIdempotence:
    def _live(self, **over):
        from decimal import Decimal
        from types import SimpleNamespace

        base = dict(kind="role", minutes=Decimal("30.0"), games=None, return_date=None,
                    usage=Decimal("0.950"), rates=None, note="why", source_url=None)
        base.update(over)
        return SimpleNamespace(**base)

    def test_an_identical_row_is_left_alone(self):
        from scripts.seed_projection_adjustments import same_as_live

        fields = {"kind": "role", "minutes": 30.0, "usage": 0.95, "note": "why", "source_url": None}
        assert same_as_live(self._live(), fields)

    @pytest.mark.parametrize("change", [
        {"usage": 0.93}, {"minutes": 31.0}, {"games": 60}, {"note": "another reason"},
        {"rates": {"ast": 0.9}}, {"kind": "trade"},
    ])
    def test_any_changed_field_is_a_new_version(self, change):
        from scripts.seed_projection_adjustments import same_as_live

        fields = {"kind": "role", "minutes": 30.0, "usage": 0.95, "note": "why", "source_url": None}
        fields.update(change)
        assert not same_as_live(self._live(), fields)

    def test_no_live_row_is_always_a_write(self):
        from scripts.seed_projection_adjustments import same_as_live

        assert not same_as_live(None, {"kind": "role", "games": 60, "note": "x"})
