"""
The consistency checks against real rows.

One small world that holds together (a game, its two players, their season,
rolling, team and live rows), then one thing broken at a time: each test says
which checks must notice and that no other check does.

The checks are replayed with the window pinned to the world's game night
(`build_consistency_checks(through=...)`), so the tests do not depend on today.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from db.base import db
from db.models.data_quality_check import DataQualityCheck
from db.models.data_quality_run import DataQualityRun
from db.models.nba.games import Game
from db.models.nba.live_player_stats import LivePlayerStats
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_rolling_stats import PlayerRollingStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.players import Player
from db.models.nba.team_stats import TeamStats
from services.consistency_checks import CONSISTENCY_CHECKS, build_consistency_checks
from services.data_quality_service import DataQualityService

pytestmark = pytest.mark.integration

NIGHT = date(2026, 3, 10)
SEASON = "2025-26"
GAME = "0022500900"
CUP_FINAL = "0062500001"
CURRY, JAMES, ROOKIE = 201939, 2544, 1649999

LINE = dict(reb=5, ast=6, stl=2, blk=0, tov=3, fgm=10, fga=20, fg3m=5, fg3a=12, ftm=5, fta=6)
# What each player's season row held before the window: totals the game log
# in this database knows nothing about. Old history, which the checks let be.
BEFORE = dict(gp=50, pts=1000, reb=250, ast=300, stl=60, blk=10, tov=150,
              fgm=350, fga=700, fg3m=200, fg3a=500, ftm=100, fta=110)


@pytest.fixture(scope="module", autouse=True)
def quality_tables(integration_db):
    db.create_tables([DataQualityRun, DataQualityCheck], safe=True)


def _season_row(player_id: int, team: str, as_of: date, games: int, pts: int) -> PlayerSeasonStats:
    """A season row `games` games (of LINE, `pts` points in all) past BEFORE."""
    totals = {stat: BEFORE[stat] + games * value for stat, value in LINE.items()}
    return PlayerSeasonStats.create(
        player_id=player_id, team_id=team, as_of_date=as_of, season=SEASON,
        gp=BEFORE["gp"] + games, pts=BEFORE["pts"] + pts, fpts=0, min=0, **totals,
    )


def _played(player_id: int, team: str, pts: int, night: date = NIGHT, game_id: str | None = GAME):
    return PlayerGameStats.create(
        player_id=player_id, team_id=team, game_date=night, game_id=game_id,
        fpts=40, pts=pts, min=34, **LINE,
    )


@pytest.fixture
def world(integration_db):
    """LAL 28 @ GSW 30 on NIGHT, and every table agreeing about it."""
    Game.create(
        game_id=GAME, game_date=NIGHT, season=SEASON,
        home_team_id="GSW", away_team_id="LAL", status="final",
        home_score=30, away_score=28,
    )
    for player_id, name, team, pts in ((CURRY, "Stephen Curry", "GSW", 30), (JAMES, "LeBron James", "LAL", 28)):
        Player.create(id=player_id, name=name, name_normalized=name.lower())
        _season_row(player_id, team, NIGHT - timedelta(days=9), games=0, pts=0)
        _played(player_id, team, pts)
        _season_row(player_id, team, NIGHT, games=1, pts=pts)
        for window in (7, 14, 30):
            PlayerRollingStats.create(
                player_id=player_id, team_id=team, as_of_date=NIGHT, window_days=window, gp=1,
                fpts=40, pts=pts, min=34, fg_pct=0.5, fg3_pct=0.4167, ft_pct=0.8333, **LINE,
            )
        LivePlayerStats.create(
            player_id=player_id, game_id=GAME, game_date=NIGHT, game_status=3, min=34, pts=pts,
        )
    TeamStats.create(team_id="GSW", as_of_date=NIGHT, season=SEASON, gp=1, w=1, l=0)
    TeamStats.create(team_id="LAL", as_of_date=NIGHT, season=SEASON, gp=1, w=0, l=1)


def failing(through: date = NIGHT) -> dict[str, int]:
    """check name -> offending rows, for the checks that find any."""
    found = {}
    for check in build_consistency_checks(through=through):
        count = db.execute_sql(check.sql).fetchone()[0]
        if count:
            found[check.name] = count
    return found


def sample(name: str, through: date = NIGHT) -> list[dict]:
    check = next(c for c in build_consistency_checks(through=through) if c.name == name)
    cursor = db.execute_sql(check.sample_sql)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


GAMES = "player_season_stats_games_keep_pace_with_game_log"
TOTALS = "player_season_stats_totals_keep_pace_with_game_log"
ROLLING_ROW = "player_rolling_stats_row_for_every_game_played"
ROLLING_MATCH = "player_rolling_stats_match_game_log"
LINK = "player_game_stats_game_link_agrees"
SCORE = "games_score_matches_player_points"
FINAL = "games_final_after_game_night"
FINAL_OTHER = "games_outside_regular_season_final_after_game_night"
TEAM = "team_stats_record_matches_schedule"
LIVE = "live_players_have_game_log"


class TestAWorldThatHoldsTogether:
    def test_no_check_finds_anything(self, world):
        assert failing() == {}

    def test_old_history_is_let_be(self, world):
        # Each season row is 50 games ahead of a game log that holds one game.
        # That gap is the same before the window as after it: not a failure.
        assert failing() == {}
        Game.update(home_score=31).where(Game.game_id == GAME).execute()
        assert set(failing()) == {SCORE}, "the world can still be broken"

    def test_an_empty_database_passes(self, integration_db):
        assert failing() == {}

    def test_the_live_rule_runs_as_written(self, world):
        # The catalogue's own SQL (NOW()-based window): nothing here is recent
        # enough to judge, so every check executes and counts nothing.
        for check in CONSISTENCY_CHECKS:
            assert db.execute_sql(check.sql).fetchone()[0] == 0, check.name
            assert db.execute_sql(check.sample_sql).fetchall() == []


class TestSeasonTotalsAgainstTheGameLog:
    def test_a_game_the_season_totals_never_absorbed(self, world):
        # The late-game lag: the game row is in, the season row for the night is not.
        PlayerSeasonStats.delete().where(
            PlayerSeasonStats.player_id == CURRY, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()

        assert failing() == {GAMES: 1}
        row, = sample(GAMES)
        assert (row["player"], row["season_gp"], row["games_logged"]) == ("Stephen Curry", 50, 1)
        # One game behind where the two stood before the window.
        assert row["gap"] - row["gap_before"] == -1

    def test_it_is_gone_once_the_season_row_catches_up(self, world):
        PlayerSeasonStats.update(as_of_date=NIGHT + timedelta(days=1)).where(
            PlayerSeasonStats.player_id == CURRY, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()

        assert failing(NIGHT) == {GAMES: 1}
        assert GAMES not in failing(NIGHT + timedelta(days=1))

    def test_a_game_the_log_never_got(self, world):
        # Season games played moved by two, the log holds one.
        PlayerSeasonStats.update(gp=BEFORE["gp"] + 2).where(
            PlayerSeasonStats.player_id == JAMES, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()

        assert failing() == {GAMES: 1}
        row, = sample(GAMES)
        assert row["player"] == "LeBron James" and row["gap"] - row["gap_before"] == 1

    def test_a_player_with_a_game_row_and_no_season_row_at_all(self, world):
        Player.create(id=ROOKIE, name="New Guy", name_normalized="new guy")
        _played(ROOKIE, "GSW", pts=0)

        assert failing()[GAMES] == 1
        row, = sample(GAMES)
        assert (row["player"], row["season_row"], row["season_gp"]) == ("New Guy", None, 0)

    def test_same_games_different_stats(self, world):
        # A stat correction the game log missed: one more point, one more free throw.
        PlayerSeasonStats.update(
            pts=PlayerSeasonStats.pts + 1, ftm=PlayerSeasonStats.ftm + 1
        ).where(
            PlayerSeasonStats.player_id == CURRY, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()

        assert failing() == {TOTALS: 1}
        row, = sample(TOTALS)
        assert row["season_moved_vs_game_log"] == "pts +1, ftm +1"

    def test_a_missing_game_is_reported_once_not_as_a_stat_difference_too(self, world):
        PlayerSeasonStats.delete().where(
            PlayerSeasonStats.player_id == CURRY, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()
        assert TOTALS not in failing()

    def test_a_night_is_not_judged_before_it_is_due(self, world):
        PlayerSeasonStats.delete().where(PlayerSeasonStats.as_of_date == NIGHT).execute()
        assert failing(NIGHT - timedelta(days=1)) == {}

    def test_a_gap_that_opened_before_the_window_has_aged_out(self, world):
        PlayerSeasonStats.delete().where(
            PlayerSeasonStats.player_id == CURRY, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()
        assert GAMES in failing(NIGHT + timedelta(days=6))
        assert GAMES not in failing(NIGHT + timedelta(days=8))

    def test_the_cup_final_is_in_the_game_log_and_in_no_season_total(self, world):
        # The night before: a game row every player in it gets, which the
        # season totals never count. Rolling averages do average it.
        eve = NIGHT - timedelta(days=1)
        Game.create(
            game_id=CUP_FINAL, game_date=eve, season=SEASON,
            home_team_id="GSW", away_team_id="LAL", status="final", home_score=30, away_score=28,
        )
        _played(CURRY, "GSW", pts=30, night=eve, game_id=CUP_FINAL)
        _played(JAMES, "LAL", pts=28, night=eve, game_id=CUP_FINAL)
        PlayerRollingStats.update(gp=2).execute()

        assert failing() == {}
        # And on the morning after it, before either player's next game.
        PlayerGameStats.delete().where(PlayerGameStats.game_date == NIGHT).execute()
        assert GAMES not in failing(eve) and TOTALS not in failing(eve)

    def test_the_cup_final_does_not_bring_back_a_gap_that_has_aged_out(self, world):
        PlayerSeasonStats.delete().where(
            PlayerSeasonStats.player_id == CURRY, PlayerSeasonStats.as_of_date == NIGHT
        ).execute()
        later = NIGHT + timedelta(days=8)
        assert GAMES not in failing(later)
        # His only game row in the window: not a game the season totals moved for.
        Game.create(
            game_id=CUP_FINAL, game_date=later, season=SEASON,
            home_team_id="GSW", away_team_id="LAL", status="final", home_score=30, away_score=28,
        )
        _played(CURRY, "GSW", pts=30, night=later, game_id=CUP_FINAL)
        assert GAMES not in failing(later) and TOTALS not in failing(later)

    def test_a_season_row_written_a_game_short_is_not_a_failure_a_week_later(self, integration_db):
        # A back-to-back whose games both arrive late. A season row is written
        # when the games played move: the 14th's row exists because the 13th's
        # game arrived, and lacks the 14th's own. The 15th's run catches up.
        Player.create(id=CURRY, name="Stephen Curry", name_normalized="stephen curry")
        played = [date(2026, 1, day) for day in (8, 10, 13, 14, 17, 19, 21)]
        for night in played:
            _played(CURRY, "GSW", pts=30, night=night, game_id=None)
        written = {8: 1, 10: 2, 14: 3, 15: 4, 17: 5, 19: 6, 21: 7}  # day -> games in the row
        for day, games in written.items():
            PlayerSeasonStats.create(
                player_id=CURRY, team_id="GSW", as_of_date=date(2026, 1, day), season=SEASON,
                gp=games, pts=30 * games, fpts=0, min=0,
                **{stat: games * value for stat, value in LINE.items()},
            )

        # The lag itself is reported on both mornings it is real.
        for day in (13, 14):
            row, = sample(GAMES, through=date(2026, 1, day))
            assert (row["gap"], row["gap_before"]) == (-1, 0), day
        # A week on, the 14th's row is the baseline: one game short, while the
        # totals and the log now agree exactly.
        a_week_on = failing(date(2026, 1, 21))
        assert GAMES not in a_week_on and TOTALS not in a_week_on


class TestRollingAveragesAgainstTheGameLog:
    def test_a_window_that_was_not_recomputed(self, world):
        PlayerRollingStats.delete().where(
            PlayerRollingStats.player_id == CURRY, PlayerRollingStats.window_days == 30
        ).execute()

        assert failing() == {ROLLING_ROW: 1}
        row, = sample(ROLLING_ROW)
        assert (row["player"], row["windows_refreshed"]) == ("Stephen Curry", 2)

    def test_an_average_the_game_log_no_longer_supports(self, world):
        PlayerRollingStats.update(pts=29.5).where(
            PlayerRollingStats.player_id == CURRY, PlayerRollingStats.window_days == 7
        ).execute()

        assert failing() == {ROLLING_MATCH: 1}
        row, = sample(ROLLING_MATCH)
        assert (row["window_days"], float(row["rolling_pts"]), float(row["game_log_pts"])) == (7, 29.5, 30.0)

    def test_rounding_is_not_a_difference(self, world):
        # 30, 30 and 31 points average 30.333...: stored to 2 places, compared
        # with Postgres's own average.
        _played(CURRY, "GSW", pts=30, night=NIGHT - timedelta(days=1), game_id=None)
        _played(CURRY, "GSW", pts=31, night=NIGHT - timedelta(days=2), game_id=None)
        PlayerRollingStats.update(gp=3, pts=30.33).where(PlayerRollingStats.player_id == CURRY).execute()
        assert ROLLING_MATCH not in failing()

        PlayerRollingStats.update(pts=30.34).where(PlayerRollingStats.player_id == CURRY).execute()
        assert failing()[ROLLING_MATCH] == 3

    def test_only_the_newest_rolling_rows_are_judged(self, world):
        for row in PlayerRollingStats.select().where(PlayerRollingStats.player_id == CURRY):
            PlayerRollingStats.create(
                player_id=CURRY, team_id="GSW", as_of_date=NIGHT - timedelta(days=2),
                window_days=row.window_days, gp=9, fpts=1, pts=1, reb=1, ast=1, stl=1, blk=1,
                tov=1, min=1, fgm=1, fga=1, fg_pct=1, fg3m=1, fg3a=1, fg3_pct=1, ftm=1, fta=1, ft_pct=1,
            )
        assert failing() == {}


class TestTheGameLogAgainstTheSchedule:
    def test_a_game_row_linked_to_no_game(self, world):
        PlayerGameStats.update(game_id=None).where(PlayerGameStats.player_id == CURRY).execute()

        # The link check names the row; the score check misses its 30 points.
        assert failing() == {LINK: 1, SCORE: 1}
        assert sample(LINK)[0]["game_id"] is None

    def test_a_game_row_on_a_team_that_did_not_play_that_game(self, world):
        PlayerGameStats.update(team_id="BOS").where(PlayerGameStats.player_id == CURRY).execute()

        assert failing() == {LINK: 1, SCORE: 1}
        assert sample(LINK)[0]["linked_game"] == "LAL @ GSW"

    def test_a_score_its_players_do_not_add_up_to(self, world):
        Game.update(home_score=44).where(Game.game_id == GAME).execute()

        assert failing() == {SCORE: 1}
        row, = sample(SCORE)
        assert (row["team"], row["score"], row["player_points"], row["player_rows"]) == ("GSW", 44, 30, 1)

    def test_a_final_game_with_no_player_rows(self, world):
        Game.create(
            game_id="0022500901", game_date=NIGHT, season=SEASON,
            home_team_id="BOS", away_team_id="NYK", status="final", home_score=101, away_score=99,
        )
        assert failing() == {SCORE: 2}

    def test_a_game_outside_the_regular_season_is_not_held_to_the_game_log(self, world):
        Game.create(
            game_id="0042500101", game_date=NIGHT, season=SEASON,
            home_team_id="BOS", away_team_id="NYK", status="final", home_score=101, away_score=99,
        )
        assert failing() == {}

    def test_a_game_still_scheduled_after_its_night(self, world):
        Game.create(
            game_id="0022500902", game_date=NIGHT - timedelta(days=1), season=SEASON,
            home_team_id="BOS", away_team_id="NYK", status="scheduled",
        )
        assert failing() == {FINAL: 1}
        assert sample(FINAL)[0]["game"] == "NYK @ BOS"

    def test_a_playoff_game_still_scheduled_is_reported_apart_from_the_regular_season(self, world):
        # The nightly schedule pipeline fetches regular-season results only.
        Game.create(
            game_id="0042500101", game_date=NIGHT - timedelta(days=1), season=SEASON,
            home_team_id="BOS", away_team_id="NYK", status="scheduled",
        )
        assert failing() == {FINAL_OTHER: 1}
        assert sample(FINAL_OTHER)[0]["game"] == "NYK @ BOS"

    def test_a_game_later_than_the_window_may_still_be_scheduled(self, world):
        Game.create(
            game_id="0022500903", game_date=NIGHT + timedelta(days=1), season=SEASON,
            home_team_id="BOS", away_team_id="NYK", status="scheduled",
        )
        assert failing() == {}


class TestTeamStatsAgainstTheSchedule:
    def test_team_stats_a_game_behind(self, world):
        TeamStats.update(gp=0, w=0).where(TeamStats.team_id == "GSW").execute()

        assert failing() == {TEAM: 1}
        row, = sample(TEAM)
        assert (row["team"], row["team_stats_gp"], row["final_games"]) == ("GSW", 0, 1)
        assert (row["team_stats_wins"], row["wins_in_schedule"]) == (0, 1)

    def test_team_stats_that_moved_without_a_game(self, world):
        TeamStats.update(gp=2).where(TeamStats.team_id == "LAL").execute()
        assert failing() == {TEAM: 1}

    def test_a_win_the_schedule_scores_as_a_loss(self, world):
        TeamStats.update(w=1, l=0).where(TeamStats.team_id == "LAL").execute()
        assert failing() == {TEAM: 1}

    def test_only_each_teams_newest_row_is_judged(self, world):
        # Two nights ago GSW's row was wrong; last night's is right.
        TeamStats.create(team_id="GSW", as_of_date=NIGHT - timedelta(days=2), season=SEASON, gp=7, w=7, l=0)
        assert failing() == {}

    def test_a_team_whose_row_is_old_but_has_played_since(self, world):
        TeamStats.update(as_of_date=NIGHT - timedelta(days=1), gp=0, w=0).where(
            TeamStats.team_id == "GSW"
        ).execute()
        assert failing() == {TEAM: 1}


class TestLiveAgainstSettled:
    def test_a_player_tracked_live_who_never_got_a_game_row(self, world):
        Player.create(id=ROOKIE, name="New Guy", name_normalized="new guy")
        LivePlayerStats.create(
            player_id=ROOKIE, game_id=GAME, game_date=NIGHT, game_status=3, min=2, pts=3, fpts=4,
        )

        assert failing() == {LIVE: 1}
        row, = sample(LIVE)
        assert (row["player"], row["live_minutes"], row["live_points"]) == ("New Guy", 2, 3)

    def test_a_live_night_not_yet_due_is_left_alone(self, world):
        PlayerGameStats.delete().execute()
        assert LIVE not in failing(NIGHT - timedelta(days=1))

    def test_a_playoff_game_is_not_expected_in_the_game_log(self, world):
        Player.create(id=ROOKIE, name="New Guy", name_normalized="new guy")
        LivePlayerStats.create(
            player_id=ROOKIE, game_id="0042500101", game_date=NIGHT, game_status=3, min=20, pts=9,
        )
        assert failing() == {}


class TestAFailedCheckKeepsItsOffendingRows:
    def _run(self, name: str) -> dict:
        service = DataQualityService()
        # The service's catalogue judges the nights before today; pin this one
        # check to the world's night instead.
        service._checks = {c.name: c for c in build_consistency_checks(through=NIGHT)}
        run = service.run_checks(check_names=[name], triggered_by="test")
        outcome, = service.get_run(str(run.id))["checks"]
        return outcome

    def test_the_sample_is_stored_beside_the_count(self, world):
        Game.update(home_score=44).where(Game.game_id == GAME).execute()

        outcome = self._run(SCORE)
        assert (outcome["status"], outcome["severity"], outcome["failures"]) == ("failed", "warning", 1)
        assert outcome["details"] == {
            "failures": 1,
            "sample": [{
                "game_id": GAME, "game_date": "2026-03-10", "team": "GSW",
                "score": 44, "player_points": 30, "player_rows": 1,
            }],
        }

    def test_at_most_five_rows_are_kept(self, world):
        for i in range(7):
            Player.create(id=ROOKIE + i, name=f"New Guy {i}", name_normalized=f"new guy {i}")
            LivePlayerStats.create(player_id=ROOKIE + i, game_id=GAME, game_date=NIGHT, min=2)

        outcome = self._run(LIVE)
        assert outcome["failures"] == 7 and len(outcome["details"]["sample"]) == 5

    def test_a_passing_check_keeps_nothing(self, world):
        outcome = self._run(SCORE)
        assert (outcome["status"], outcome["details"]) == ("passed", None)

    def test_a_warning_does_not_alert(self, world, monkeypatch):
        sent = []
        monkeypatch.setattr(
            "services.data_quality_service.get_alert_service",
            lambda: type("Alerts", (), {"notify": lambda self, event: sent.append(event)})(),
        )
        Game.update(home_score=44).where(Game.game_id == GAME).execute()
        assert self._run(SCORE)["status"] == "failed" and sent == []
