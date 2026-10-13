"""
PlayerGameLogs rows -> nba.player_game_stats and nba.games rows.

Shared by the nightly box-score pipeline (one date) and the game-history
pipeline (a whole season and season type), so both store a line the same way.

The history load builds its nba.games rows from the same payload rather than
from a second endpoint: each line carries GAME_ID, GAME_DATE and MATCHUP
("LAL vs. BOS" at home, "LAL @ BOS" away), and a team's score is the sum of its
players' points. Games and box scores therefore always agree, and every line's
game exists before the line is written (nba.player_game_stats.game_id is a
foreign key).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

from db.models.nba.games import season_for, season_type_for
from pipelines.transformers import calculate_fantasy_points, minutes_to_int, normalize_name
from pipelines.transformers.minutes import seconds_played

# Our season types -> PlayerGameLogs' SeasonType parameter. Every stored row is
# labelled from its own game id (games.season_type_for), never from this.
# "regular" is named: for a whole season, no SeasonType returns every type,
# preseason included (the first production run, 2026-10-13). The nightly
# pipeline's per-date call sends none, which on a regular-season night is that
# night's games and the Cup final.
SEASON_TYPE_PARAMS: dict[str, str] = {
    "regular": "Regular Season",
    "play_in": "PlayIn",
    "playoffs": "Playoffs",
}
POSTSEASON_TYPES = ("play_in", "playoffs")

# What the history load stores. Preseason and All-Star lines never reach these
# tables (game_start_times skips preseason games for the same reason).
LOADED_SEASON_TYPES = ("regular", "cup_final", "play_in", "playoffs")

# The first season the history load accepts: TEAM_ALIASES covers every
# abbreviation from here on, and older seasons need more (SEA, NJN, VAN, ...).
GAME_HISTORY_FIRST_YEAR = 2012

# Abbreviations the NBA used in 2012-13 onward that nba.teams no longer has.
# Same franchise, so the line is stored under today's team.
TEAM_ALIASES: dict[str, str] = {
    "NOH": "NOP",  # New Orleans Hornets until 2012-13
}

STAT_COLUMNS: dict[str, str] = {
    "pts": "PTS", "reb": "REB", "ast": "AST", "stl": "STL", "blk": "BLK", "tov": "TOV",
    "fgm": "FGM", "fga": "FGA", "fg3m": "FG3M", "fg3a": "FG3A", "ftm": "FTM", "fta": "FTA",
}
# Columns added in migration 0031. Optional: a payload without one stores NULL.
EXTRA_COLUMNS: dict[str, str] = {
    "oreb": "OREB", "dreb": "DREB", "pf": "PF", "plus_minus": "PLUS_MINUS",
}


def team_id(abbrev: Any) -> Optional[str]:
    """Today's abbreviation for a team, or None when the payload has none."""
    if abbrev is None:
        return None
    text = str(abbrev).strip().upper()
    if not text or text == "NAN":
        return None
    return TEAM_ALIASES.get(text, text)


def _int(value: Any) -> Optional[int]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return int(round(number))


def box_score_stats(row: Mapping[str, Any]) -> Optional[dict]:
    """The stored stat line for one PlayerGameLogs row, or None if he did not play.

    A sub-minute appearance is a game played and is kept with `min` 0
    (pipelines/transformers/minutes.py).
    """
    minutes = row.get("MIN")
    if minutes is None or minutes == "" or seconds_played(minutes) <= 0:
        return None
    stats = {col: _int(row.get(src)) or 0 for col, src in STAT_COLUMNS.items()}
    line = {
        "fpts": calculate_fantasy_points(stats),
        "min": minutes_to_int(minutes),
        **stats,
    }
    for col, src in EXTRA_COLUMNS.items():
        if src in row:
            line[col] = _int(row.get(src))
    return line


def parse_game_date(value: Any) -> Optional[date]:
    """GAME_DATE as sent: "2025-10-21T00:00:00", "2025-10-21" or "OCT 21, 2025"."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        if "," in text:
            return datetime.strptime(text.title(), "%b %d, %Y").date()
        return datetime.strptime(text.split("T")[0], "%Y-%m-%d").date()
    except ValueError:
        return None


def _opponent(matchup: str) -> tuple[Optional[str], Optional[bool]]:
    """(opponent, is_home) from "LAL vs. BOS" / "LAL @ BOS"; (None, None) if unreadable."""
    if " vs. " in matchup:
        return team_id(matchup.split(" vs. ", 1)[1]), True
    if " @ " in matchup:
        return team_id(matchup.split(" @ ", 1)[1]), False
    return None, None


def _sides(teams: set[str], claims: dict[str, set[bool]], known: Optional[tuple[str, str]]) -> Optional[tuple[str, str]]:
    """(home, away) for a game's two teams, or None if there aren't exactly two.

    The lines usually agree ("LAL vs. BOS" and "BOS @ LAL"). At a neutral site
    (Mexico City, Paris, Berlin, London, the Cup semifinals in Las Vegas) both
    teams' lines can claim the same side. Then the schedule's sides are used
    when nba.games has the game, and otherwise the teams in alphabetical order:
    there is no home team to read, and the scores still go to the right team.
    """
    if len(teams) != 2:
        return None
    home_claims = {t for t in teams if claims.get(t) == {True}}
    away_claims = {t for t in teams if claims.get(t) == {False}}
    if len(home_claims) == 1 and not (away_claims & home_claims) and len(away_claims) <= 1:
        home = next(iter(home_claims))
        return home, next(iter(teams - {home}))
    if len(away_claims) == 1 and not home_claims:
        away = next(iter(away_claims))
        return next(iter(teams - {away})), away
    if known is not None and set(known) == teams:
        return known
    first, second = sorted(teams)
    return first, second


def games_from_logs(
    rows: Iterable[Mapping[str, Any]],
    known_sides: Optional[Mapping[str, tuple[str, str]]] = None,
) -> tuple[list[dict], list[str]]:
    """nba.games rows (final, scored) for every game the lines cover.

    `known_sides` is game id -> (home, away) as nba.games has it, for games
    whose lines disagree on who was home (see _sides).

    Returns (games, unreadable_game_ids): a game without exactly two teams, a
    date or a season is left out, and its id is reported.
    """
    by_game: dict[str, dict] = {}
    for row in rows:
        game_id = str(row.get("GAME_ID") or "").strip()
        team = team_id(row.get("TEAM_ABBREVIATION"))
        if not game_id or team is None:
            continue
        game = by_game.setdefault(game_id, {"date": None, "teams": set(), "claims": {}, "points": {}})
        game["date"] = game["date"] or parse_game_date(row.get("GAME_DATE"))
        opponent, is_home = _opponent(str(row.get("MATCHUP") or ""))
        game["teams"].add(team)
        if opponent:
            game["teams"].add(opponent)
        if is_home is not None:
            game["claims"].setdefault(team, set()).add(is_home)
        game["points"][team] = game["points"].get(team, 0) + (_int(row.get("PTS")) or 0)

    games: list[dict] = []
    unreadable: list[str] = []
    for game_id, game in sorted(by_game.items()):
        sides = _sides(game["teams"], game["claims"], (known_sides or {}).get(game_id))
        season = season_for(game_id)
        if sides is None or game["date"] is None or season is None:
            unreadable.append(game_id)
            continue
        home, away = sides
        games.append({
            "game_id": game_id,
            "game_date": game["date"],
            "season": season,
            "season_type": season_type_for(game_id),
            "home_team": home,
            "away_team": away,
            "home_score": game["points"].get(home, 0),
            "away_score": game["points"].get(away, 0),
            "status": "final",
        })
    return games, unreadable


def player_identity(row: Mapping[str, Any]) -> Optional[tuple[int, str, str]]:
    """(player_id, name, name_normalized) for a line, or None without an id."""
    player_id = _int(row.get("PLAYER_ID"))
    if player_id is None:
        return None
    name = str(row.get("PLAYER_NAME") or f"Player {player_id}")[:100]
    return player_id, name, normalize_name(name)[:100]
