"""PlayerGameLogs rows -> stored lines and games (pipelines/game_logs.py), and
the game-id rules both models derive season and season type from."""

from datetime import date

import pytest

from db.models.nba.games import season_for, season_type_for
from pipelines.game_history import build_rows, postseason_window
from pipelines.game_logs import SEASON_TYPE_PARAMS, box_score_stats, games_from_logs, parse_game_date, team_id
from pipelines.player_advanced_stats import _fraction_of_100


def _line(game_id="0022500001", team="GSW", matchup="GSW vs. LAL", player_id=201939, pts=30, minutes=34.2, **extra):
    row = {
        "GAME_ID": game_id, "GAME_DATE": "2025-10-21T00:00:00", "MATCHUP": matchup,
        "PLAYER_ID": player_id, "PLAYER_NAME": f"Player {player_id}", "TEAM_ABBREVIATION": team,
        "MIN": minutes, "PTS": pts, "REB": 5, "AST": 6, "STL": 2, "BLK": 1, "TOV": 3,
        "FGM": 10, "FGA": 20, "FG3M": 5, "FG3A": 12, "FTM": 5, "FTA": 6,
    }
    row.update(extra)
    return row


@pytest.mark.unit
class TestGameIds:
    @pytest.mark.parametrize("game_id,season_type", [
        ("0012500001", "preseason"), ("0022500123", "regular"), ("0032500001", "all_star"),
        ("0042500401", "playoffs"), ("0052500101", "play_in"), ("0062500001", "cup_final"),
        ("0092500001", None), ("", None), (None, None),
    ])
    def test_season_type_comes_from_the_prefix(self, game_id, season_type):
        assert season_type_for(game_id) == season_type

    @pytest.mark.parametrize("game_id,season", [
        ("0022500123", "2025-26"), ("0041200101", "2012-13"), ("0029900001", "1999-00"),
        ("0029600001", "1996-97"), ("0020800001", "2008-09"), ("0092500001", None), ("002", None),
    ])
    def test_season_comes_from_the_two_digits_after_the_prefix(self, game_id, season):
        assert season_for(game_id) == season


@pytest.mark.unit
class TestLines:
    def test_a_line_carries_fantasy_points_and_the_new_columns(self):
        line = box_score_stats(_line(OREB=1, DREB=4, PF=2, PLUS_MINUS=-7))
        assert line["min"] == 34 and line["pts"] == 30
        assert (line["oreb"], line["dreb"], line["pf"], line["plus_minus"]) == (1, 4, 2, -7)
        assert line["fpts"] > 0

    def test_a_payload_without_the_new_columns_leaves_them_out(self):
        # The model's upsert then leaves whatever is stored alone.
        assert "oreb" not in box_score_stats(_line())

    @pytest.mark.parametrize("minutes", [0, None, "", float("nan")])
    def test_a_player_who_did_not_play_has_no_line(self, minutes):
        assert box_score_stats(_line(minutes=minutes)) is None

    def test_a_sub_minute_appearance_is_kept_with_zero_minutes(self):
        assert box_score_stats(_line(minutes=0.67))["min"] == 0

    def test_the_2012_13_hornets_are_todays_pelicans(self):
        assert team_id("NOH") == "NOP" and team_id(" bos ") == "BOS" and team_id(None) is None

    @pytest.mark.parametrize("raw", ["2025-10-21T00:00:00", "2025-10-21", "OCT 21, 2025", date(2025, 10, 21)])
    def test_game_dates_in_every_format_sent(self, raw):
        assert parse_game_date(raw) == date(2025, 10, 21)


@pytest.mark.unit
class TestGamesFromLogs:
    def test_home_away_and_score_come_from_the_lines(self):
        rows = [
            _line(team="GSW", matchup="GSW vs. LAL", player_id=1, pts=30),
            _line(team="GSW", matchup="GSW vs. LAL", player_id=2, pts=12),
            _line(team="LAL", matchup="LAL @ GSW", player_id=3, pts=25),
        ]
        games, unreadable = games_from_logs(rows)
        assert unreadable == []
        assert games == [{
            "game_id": "0022500001", "game_date": date(2025, 10, 21), "season": "2025-26",
            "season_type": "regular", "home_team": "GSW", "away_team": "LAL",
            "home_score": 42, "away_score": 25, "status": "final",
        }]

    def test_the_opponent_is_read_from_the_matchup_and_aliased(self):
        games, _ = games_from_logs([_line(game_id="0021200001", team="BOS", matchup="BOS @ NOH")])
        assert (games[0]["home_team"], games[0]["away_team"]) == ("NOP", "BOS")
        assert games[0]["home_score"] == 0 and games[0]["season"] == "2012-13"

    @pytest.mark.parametrize("home_matchup,away_matchup", [
        ("SAS vs. IND", "IND vs. SAS"),   # both claim home (Paris, 2025-01-23)
        ("SAS @ IND", "IND @ SAS"),       # both claim away
    ])
    def test_a_neutral_site_game_takes_the_schedules_sides(self, home_matchup, away_matchup):
        rows = [_line(game_id="0022400621", team="SAS", matchup=home_matchup, player_id=1, pts=140),
                _line(game_id="0022400621", team="IND", matchup=away_matchup, player_id=2, pts=110)]
        games, unreadable = games_from_logs(rows, {"0022400621": ("SAS", "IND")})
        assert unreadable == []
        assert [(g["home_team"], g["home_score"], g["away_team"], g["away_score"]) for g in games] == [
            ("SAS", 140, "IND", 110)
        ]

    def test_without_a_schedule_row_a_neutral_game_is_in_alphabetical_order(self):
        rows = [_line(game_id="0022400621", team="SAS", matchup="SAS vs. IND", player_id=1, pts=140),
                _line(game_id="0022400621", team="IND", matchup="IND vs. SAS", player_id=2, pts=110)]
        games, _ = games_from_logs(rows)
        assert (games[0]["home_team"], games[0]["home_score"], games[0]["away_score"]) == ("IND", 110, 140)

    def test_a_game_whose_teams_cannot_be_read_is_reported(self):
        games, unreadable = games_from_logs([_line(matchup="GSW v LAL")])
        assert games == [] and unreadable == ["0022500001"]


@pytest.mark.unit
class TestBuildRows:
    def test_lines_take_their_games_date_season_and_type(self):
        rows = [
            _line(game_id="0042500101", team="OKC", matchup="OKC vs. MEM", player_id=1, OREB=2),
            _line(game_id="0042500101", team="MEM", matchup="MEM @ OKC", player_id=2),
        ]
        games, lines, players, skipped = build_rows(rows, {"OKC", "MEM"}, run_id=None)
        assert len(games) == 1 and len(lines) == 2 and not +skipped
        assert {(line["season"], line["season_type"]) for line in lines} == {("2025-26", "playoffs")}
        assert lines[0]["oreb"] == 2 and lines[1]["oreb"] is None
        assert set(players) == {1, 2}

    def test_preseason_and_all_star_lines_are_counted_not_stored(self):
        rows = [_line(game_id="0012500001", player_id=1), _line(game_id="0032500001", player_id=2),
                _line(game_id="0022500001", team="LAL", matchup="LAL @ GSW", player_id=3)]
        games, lines, _, skipped = build_rows(rows, {"GSW", "LAL"}, run_id=None)
        assert [g["game_id"] for g in games] == ["0022500001"] and [l["player"] for l in lines] == [3]
        assert (skipped["preseason"], skipped["all_star"]) == (1, 1)

    def test_a_repeated_line_is_stored_once(self):
        """Two lines for one (player, game) in a batch would fail the whole upsert."""
        rows = [_line(player_id=1), _line(player_id=1, pts=31), _line(team="LAL", matchup="LAL @ GSW", player_id=2)]
        _, lines, _, skipped = build_rows(rows, {"GSW", "LAL"}, run_id=None)
        assert [line["player"] for line in lines] == [1, 2] and skipped["duplicate_line"] == 1

    def test_unknown_teams_and_players_who_did_not_play_are_counted_not_stored(self):
        rows = [
            _line(team="GSW", matchup="GSW vs. XYZ", player_id=1),
            _line(game_id="0022500002", team="BOS", matchup="BOS vs. NYK", player_id=2, minutes=0),
            _line(game_id="0022500002", team="NYK", matchup="NYK @ BOS", player_id=3),
        ]
        games, lines, _, skipped = build_rows(rows, {"GSW", "BOS", "NYK"}, run_id=None)
        assert [g["game_id"] for g in games] == ["0022500002"]
        assert [line["player"] for line in lines] == [3]
        assert skipped["game_unknown_team"] == 1 and skipped["no_game"] == 1 and skipped["did_not_play"] == 1


@pytest.mark.unit
class TestPostseasonWindow:
    LAST = date(2027, 4, 11)

    @pytest.mark.parametrize("night,expected", [
        (date(2027, 4, 11), False),   # the last regular-season night itself
        (date(2027, 4, 14), True),    # play-in
        (date(2027, 6, 20), True),    # finals
        (date(2027, 6, 26), False),   # past the window
    ])
    def test_only_after_the_last_regular_season_night(self, night, expected):
        assert postseason_window(self.LAST, night) is expected

    def test_no_regular_season_yet_means_no_postseason(self):
        assert postseason_window(None, date(2026, 10, 12)) is False


@pytest.mark.unit
def test_the_regular_season_is_asked_for_by_name():
    """No SeasonType returns every type for a whole season (production, 2026-10-13)."""
    assert SEASON_TYPE_PARAMS == {"regular": "Regular Season", "play_in": "PlayIn", "playoffs": "Playoffs"}


@pytest.mark.unit
def test_turnover_ratio_is_stored_as_a_fraction_like_its_siblings():
    assert _fraction_of_100(6.9) == 0.069 and _fraction_of_100("10.75") == 0.1075
    assert _fraction_of_100(None) is None and _fraction_of_100("n/a") is None
