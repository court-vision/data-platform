"""
What the post-game readiness checks share.

The season dashboards (LeagueLeaders, LeagueDashTeamStats) count regular-season
games only, and they trail the night's last games. The pipelines that read them
hold themselves back until every player or team in the night's game log shows
the game, so the game log has to be read the way the dashboards count.
"""

from datetime import date

from peewee import fn

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats

# NBA game ids start with the kind of game: 001 preseason, 002 regular season,
# 004 playoffs, 006 the Cup final (which counts in no one's totals).
REGULAR_SEASON_PREFIX = "002"


def regular_season_game_rows(game_date: date):
    """Filter for that night's game-log rows the season dashboards count.

    The game log is fetched with no season type, so it carries whatever was
    played: the Cup final is in it (18 rows for 2025-12-16, ids `006`), and a
    dashboard never moves for it. A check that waited on those players would
    hold the pipeline all night.

    A row without a game id is one whose game the schedule did not have when
    it was written, so its kind has to come from the night. Regular-season
    and other games do not share a date: the row is counted unless the
    schedule has a game of another kind that night. A playoff game the
    schedule had not caught up with is then left out with the rest of its
    night, rather than holding the pipelines for a game no dashboard counts.
    """
    other_kind_that_night = Game.select().where(
        (Game.game_date == game_date)
        & ~Game.game_id.startswith(REGULAR_SEASON_PREFIX)
    )
    return PlayerGameStats.game_id.startswith(REGULAR_SEASON_PREFIX) | (
        PlayerGameStats.game_id.is_null() & ~fn.EXISTS(other_kind_that_night)
    )
