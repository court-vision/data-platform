"""
Post-game readiness: season totals and team stats wait for the night's games.

Both pipelines read a season dashboard from the NBA stats API (LeagueLeaders,
LeagueDashTeamStats), and both dashboards trail the night's last games. A run
that takes the dashboard as it stands succeeds, is deduped for the night, and
leaves the late teams a game behind until tomorrow's batch. These cover the
check that holds each pipeline back: who played is taken from the night's game
log, and every one of them has to show the game.

The database is faked at each pipeline's own query helpers — these are tests
of the decision, not of Peewee. The queries themselves run against Postgres in
`tests/integration/test_post_game_aggregate_pipelines.py`.
"""

from collections import Counter
from datetime import date

import pytest

from db.models.nba import Player, PlayerSeasonStats
from db.models.nba.team_stats import TeamStats
from pipelines.base import DataNotReady
from pipelines.context import PipelineContext
from pipelines.player_season_stats import PlayerSeasonStatsPipeline
from pipelines.team_stats import TeamStatsPipeline

NIGHT = date(2026, 4, 8)
SEASON = "2025-26"

EARLY, LATE = 1, 2  # a player from the 7 PM game, one from the 10 PM game


def _leader(player_id: int, gp: int) -> dict:
    return {
        "PLAYER_ID": player_id, "PLAYER": f"Player {player_id}", "TEAM": "PHX", "GP": gp,
        "PTS": 100, "REB": 40, "AST": 30, "STL": 8, "BLK": 4, "TOV": 15,
        "FGM": 35, "FGA": 80, "FG3M": 12, "FG3A": 30, "FTM": 18, "FTA": 22, "MIN": 300,
    }


@pytest.fixture
def season(monkeypatch):
    """The season pipeline with its reads stubbed and its writes captured.

    `stored` is each player's GP on his newest season row, `played` who has a
    game row for the night, `written` who already has a season row dated it.
    `before_tip_off` is his GP on the rows written before the night began:
    the same as `stored` unless a test says a backfill ran during the slate.
    """
    pipeline = PlayerSeasonStatsPipeline()
    pipeline.stored = {EARLY: 63, LATE: 63}
    pipeline.played = {EARLY, LATE}
    pipeline.written = set()
    pipeline.before_tip_off = None
    pipeline.upserts = []

    pipeline.espn_extractor.get_player_data = lambda: {}
    pipeline._latest_gp = lambda season: dict(pipeline.stored)
    pipeline._played_on = lambda game_date: set(pipeline.played)
    pipeline._written_for = lambda game_date, season: set(pipeline.written)
    pipeline._gp_before_tip_off = lambda game_date, season, player_ids: dict(
        pipeline.stored if pipeline.before_tip_off is None else pipeline.before_tip_off
    )
    pipeline.logged_before = {}
    pipeline._games_logged_before = lambda game_date, season, player_ids: dict(pipeline.logged_before)

    monkeypatch.setattr(Player, "upsert_player", classmethod(lambda cls, **kw: None))
    monkeypatch.setattr(
        PlayerSeasonStats,
        "upsert_season_stats",
        classmethod(lambda cls, **kw: pipeline.upserts.append(kw)),
    )
    return pipeline


def _leaders(pipeline, gp_by_player: dict[int, int]) -> None:
    pipeline.nba_extractor.get_league_leaders = lambda *_: [
        _leader(player_id, gp) for player_id, gp in gp_by_player.items()
    ]


def _ctx(name: str, **kwargs) -> PipelineContext:
    return PipelineContext(name, **({"nba_date": NIGHT} | kwargs))


@pytest.mark.unit
class TestSeasonStatsWaitForTheNight:
    def test_a_partial_update_is_not_ready(self, season):
        """The early game is in, the late one is not: the case that used to pass."""
        _leaders(season, {EARLY: 64, LATE: 63})

        # DataNotReady, so the run alerts as waiting rather than as a crash.
        with pytest.raises(DataNotReady) as raised:
            season.execute(_ctx("player_season_stats"))

        assert "Data not ready yet — will retry" in str(raised.value)
        assert "1 of 2 players" in str(raised.value)
        # Nothing is written for the night, the early player included: a
        # partial write is what a later run would mistake for a finished one.
        assert season.upserts == []

    def test_nothing_moved_is_not_ready(self, season):
        _leaders(season, {EARLY: 63, LATE: 63})

        with pytest.raises(RuntimeError, match="2 of 2 players"):
            season.execute(_ctx("player_season_stats"))

        assert season.upserts == []

    def test_a_player_missing_from_the_response_is_behind(self, season):
        """Opening night: a late-game player has no totals at all yet."""
        season.stored = {}
        _leaders(season, {EARLY: 1})

        with pytest.raises(RuntimeError, match="1 of 2 players"):
            season.execute(_ctx("player_season_stats"))

    def test_a_player_with_no_season_row_must_outcount_his_games_in_the_log(self, season):
        """No row to compare against, so an entry alone is not a new game."""
        season.stored = {EARLY: 63}
        season.logged_before = {LATE: 1}
        _leaders(season, {EARLY: 64, LATE: 1})

        with pytest.raises(RuntimeError, match="1 of 2 players"):
            season.execute(_ctx("player_season_stats"))

        _leaders(season, {EARLY: 64, LATE: 2})
        season.execute(_ctx("player_season_stats"))
        assert {u["player_id"]: u["stats"]["gp"] for u in season.upserts} == {EARLY: 64, LATE: 2}

    def test_writes_once_everyone_who_played_has_moved(self, season):
        _leaders(season, {EARLY: 64, LATE: 64})
        ctx = _ctx("player_season_stats")

        season.execute(ctx)

        assert {u["player_id"] for u in season.upserts} == {EARLY, LATE}
        assert all(u["as_of_date"] == NIGHT and u["stats"]["gp"] == 64 for u in season.upserts)
        assert ctx.records_processed == 2

    def test_players_who_did_not_play_are_not_waited_for(self, season):
        rested = 3
        season.stored[rested] = 50
        _leaders(season, {EARLY: 64, LATE: 64, rested: 50})

        season.execute(_ctx("player_season_stats"))

        assert {u["player_id"] for u in season.upserts} == {EARLY, LATE}

    def test_a_rerun_after_the_night_was_written_is_ready(self, season):
        """Nothing moves the second time, because the first run wrote it.

        The old check read "no increments, but game rows exist" as not ready,
        which failed every re-run of a night that had already succeeded.
        """
        season.stored = {EARLY: 64, LATE: 64}
        season.written = {EARLY, LATE}
        _leaders(season, {EARLY: 64, LATE: 64})

        season.execute(_ctx("player_season_stats"))

        assert season.upserts == []

    def test_a_backfill_is_not_held(self, season):
        """The API has no as-of date: today's totals cannot be judged against an old night."""
        _leaders(season, {EARLY: 64, LATE: 63})

        season.execute(_ctx("player_season_stats", date_override=NIGHT))

        assert {u["player_id"] for u in season.upserts} == {EARLY}

    def test_a_backfill_during_the_slate_does_not_hold_the_night(self, season):
        """`?date=` for an earlier night, run while tonight's games were landing.

        The backfill wrote the early player's totals, tonight's game included,
        under the old date. He never changes again and has no row dated
        tonight, so "no change since his newest row" held the run on every
        poll with the API fully caught up, and nothing was written for anyone.
        """
        season.stored = {EARLY: 65, LATE: 64}          # what the backfill wrote
        season.before_tip_off = {EARLY: 63, LATE: 63}  # what was there before the slate
        _leaders(season, {EARLY: 65, LATE: 65})

        season.execute(_ctx("player_season_stats"))

        assert {u["player_id"] for u in season.upserts} == {LATE}

    def test_a_backfill_during_the_slate_does_not_excuse_a_player_who_has_not_moved(self, season):
        """Judged against the rows from before the slate, he still has to show a game."""
        season.stored = {EARLY: 65, LATE: 63}
        season.before_tip_off = {EARLY: 63, LATE: 63}
        _leaders(season, {EARLY: 65, LATE: 63})

        with pytest.raises(RuntimeError, match="1 of 2 players"):
            season.execute(_ctx("player_season_stats"))

        assert season.upserts == []

    def test_a_player_the_backfill_wrote_for_the_first_time_has_caught_up(self, season):
        """No row from before the slate at all: his being in the response is the new game."""
        season.stored = {EARLY: 1, LATE: 0}
        season.before_tip_off = {LATE: 0}
        _leaders(season, {EARLY: 1, LATE: 1})

        season.execute(_ctx("player_season_stats"))

        assert {u["player_id"] for u in season.upserts} == {LATE}

    def test_without_a_tip_off_time_the_newest_row_is_the_last_word(self, season):
        """The schedule has no start time for the night: nothing to date "before" from."""
        season.stored = {EARLY: 65, LATE: 64}
        season._gp_before_tip_off = lambda game_date, season, player_ids: None
        _leaders(season, {EARLY: 65, LATE: 65})

        with pytest.raises(RuntimeError, match="1 of 2 players"):
            season.execute(_ctx("player_season_stats"))

    def test_without_a_game_log_there_is_nothing_to_hold_against(self, season):
        """Why the batch must not run this before the night's game log.

        With no game rows the check passes on whatever the API has. The
        dependency on `player_game_stats` is what covers that, and it only
        does so because the batch reads it as "succeeded tonight"
        (`tests/api/test_post_game_batch.py`).
        """
        season.played = set()
        _leaders(season, {EARLY: 64, LATE: 63})

        season.execute(_ctx("player_season_stats"))

        assert {u["player_id"] for u in season.upserts} == {EARLY}


# ---------------------------------------------------------------------------
# Team stats
# ---------------------------------------------------------------------------


def _team(abbr: str, gp: int) -> dict:
    return {"TEAM_ABBREVIATION": abbr, "TEAM_NAME": abbr, "GP": gp, "W": gp // 2, "L": gp - gp // 2}


@pytest.fixture
def teams(monkeypatch):
    """The team pipeline with its reads stubbed and its writes captured.

    BOS and NYK played the early game, PHX and LAL the late one, DEN was off.
    `before` is each team's final regular-season games before the night.
    """
    pipeline = TeamStatsPipeline()
    pipeline.played = {"BOS", "NYK", "PHX", "LAL"}
    pipeline.before = Counter({"BOS": 78, "NYK": 78, "PHX": 79, "LAL": 78, "DEN": 79})
    pipeline.upserts = []

    pipeline._teams_played = lambda game_date, season: set(pipeline.played)
    pipeline._final_games_before = lambda game_date, season: Counter(pipeline.before)

    monkeypatch.setattr(
        TeamStats,
        "upsert_team_stats",
        classmethod(lambda cls, **kw: pipeline.upserts.append(kw)),
    )
    return pipeline


def _dashboard(pipeline, gp_by_team: dict[str, int]) -> None:
    pipeline.nba_extractor.get_team_stats = lambda *_: [
        _team(abbr, gp) for abbr, gp in gp_by_team.items()
    ]


CAUGHT_UP = {"BOS": 79, "NYK": 79, "PHX": 80, "LAL": 79, "DEN": 79}


@pytest.mark.unit
class TestTeamStatsWaitForTheNight:
    def test_declares_what_it_needs_first(self):
        """The game log says who played; the schedule is what the record is counted from."""
        assert set(TeamStatsPipeline.config.depends_on) == {"player_game_stats", "game_schedule"}

    def test_a_team_missing_its_late_game_is_not_ready(self, teams):
        _dashboard(teams, CAUGHT_UP | {"PHX": 79, "LAL": 78})

        with pytest.raises(DataNotReady) as raised:
            teams.execute(_ctx("team_stats"))

        message = str(raised.value)
        assert "Data not ready yet — will retry" in message
        assert "2 team(s)" in message and "LAL" in message and "PHX" in message
        # Raised before the first write, the caught-up teams included.
        assert teams.upserts == []

    def test_writes_once_every_team_that_played_shows_the_game(self, teams):
        _dashboard(teams, CAUGHT_UP)
        ctx = _ctx("team_stats")

        teams.execute(ctx)

        assert {u["team_id"] for u in teams.upserts} == set(CAUGHT_UP)
        assert all(u["as_of_date"] == NIGHT for u in teams.upserts)
        assert ctx.records_processed == 5

    def test_a_team_that_did_not_play_is_not_checked(self, teams):
        """DEN's count is whatever the schedule says; only the night's teams are held."""
        teams.before["DEN"] = 82
        _dashboard(teams, CAUGHT_UP)

        teams.execute(_ctx("team_stats"))

        assert len(teams.upserts) == 5

    def test_a_schedule_missing_a_game_does_not_hold_the_stats(self, teams):
        """More games than the schedule knows of is the schedule's gap, not a stale dashboard."""
        teams.before["BOS"] = 77
        _dashboard(teams, CAUGHT_UP)

        teams.execute(_ctx("team_stats"))

        assert len(teams.upserts) == 5

    def test_a_team_absent_from_the_response_is_behind(self, teams):
        _dashboard(teams, {k: v for k, v in CAUGHT_UP.items() if k != "LAL"})

        with pytest.raises(RuntimeError, match=r"1 team\(s\).*LAL"):
            teams.execute(_ctx("team_stats"))

        assert teams.upserts == []

    def test_an_empty_response_on_a_game_night_is_not_ready(self, teams):
        """Used to be a quiet success with nothing written."""
        _dashboard(teams, {})

        with pytest.raises(RuntimeError, match=r"4 team\(s\)"):
            teams.execute(_ctx("team_stats"))

    def test_an_empty_response_with_no_games_is_a_quiet_success(self, teams):
        teams.played = set()
        _dashboard(teams, {})

        teams.execute(_ctx("team_stats"))

        assert teams.upserts == []

    def test_a_backfill_is_not_held(self, teams):
        _dashboard(teams, CAUGHT_UP | {"PHX": 79, "LAL": 78})

        teams.execute(_ctx("team_stats", date_override=NIGHT))

        assert len(teams.upserts) == 5
