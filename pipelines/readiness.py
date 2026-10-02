"""
What the post-game readiness checks share.

The season dashboards (LeagueLeaders, LeagueDashTeamStats) count regular-season
games only, and they trail the night's last games. The pipelines that read them
hold themselves back until every player or team in the night's game log shows
the game, so the game log has to be read the way the dashboards count.
"""

from db.models.nba.player_game_stats import PlayerGameStats

# NBA game ids start with the kind of game: 001 preseason, 002 regular season,
# 004 playoffs, 006 the Cup final (which counts in no one's totals).
REGULAR_SEASON_PREFIX = "002"


def regular_season_game_rows():
    """Filter for game-log rows the season dashboards count.

    The game log does carry the Cup final (18 rows for 2025-12-16, ids `006`),
    and a dashboard never moves for it: a check that waited on those players
    would hold the pipeline all night. A row without a game id is one whose
    game the schedule did not have when it was written; the log is fetched as
    regular season, so it is counted.
    """
    return PlayerGameStats.game_id.is_null() | PlayerGameStats.game_id.startswith(
        REGULAR_SEASON_PREFIX
    )
