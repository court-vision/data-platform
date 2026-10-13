"""
Game History Pipeline

Whole seasons of box scores and games into nba.player_game_stats and
nba.games: regular season, play-in and playoffs, one PlayerGameLogs call per
season and season type. Two uses:

- the history load (manual, POST /pipelines/game-history): `options.seasons`
  and optionally `options.season_types`, e.g. every season from 2012-13
  (docs/ASK_PLAN.md);
- the postseason, nightly (the `playoffs` cron job): no options = this season's
  play-in and playoffs, reloaded whole on every run once the regular season is
  over, so a game the log had not caught up with is stored the next night.
  Outside that window it does nothing.

Games are built from the same lines (game_logs.games_from_logs), so every line's
game exists before the line is written. Rows are labelled from their own game
ids. A re-run updates rows in place: the unique key is (player_id, game_id).

Players a season brings in who are not in nba.players yet (anyone gone before
2025-26) are inserted with no ESPN id and no position.
"""

from __future__ import annotations

import time
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Optional

from peewee import EXCLUDED, Case, fn

from core.settings import settings
from db.base import db
from db.models.nba import NBATeam, Player, PlayerGameStats
from db.models.nba.games import Game, season_type_for
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor
from pipelines.game_logs import (
    LOADED_SEASON_TYPES,
    POSTSEASON_TYPES,
    SEASON_TYPE_PARAMS,
    box_score_stats,
    games_from_logs,
    player_identity,
    team_id,
)

PAUSE_BETWEEN_CALLS = 1.5  # seconds, as season_history: polite to stats.nba.com
BATCH_SIZE = 1000
# The postseason runs about 65 days after the last regular-season game.
POSTSEASON_DAYS = 75

# Rewritten on a re-run: everything but the key and the first-written stamp.
LINE_PRESERVE = [
    PlayerGameStats.team, PlayerGameStats.game_date, PlayerGameStats.season, PlayerGameStats.season_type,
    PlayerGameStats.fpts, PlayerGameStats.min,
    PlayerGameStats.pts, PlayerGameStats.reb, PlayerGameStats.ast, PlayerGameStats.stl,
    PlayerGameStats.blk, PlayerGameStats.tov,
    PlayerGameStats.fgm, PlayerGameStats.fga, PlayerGameStats.fg3m, PlayerGameStats.fg3a,
    PlayerGameStats.ftm, PlayerGameStats.fta,
    PlayerGameStats.oreb, PlayerGameStats.dreb, PlayerGameStats.pf, PlayerGameStats.plus_minus,
    PlayerGameStats.pipeline_run_id, PlayerGameStats.updated_at,
]


def _score_of(team_column):
    """On conflict: the incoming score of the team the stored row has in this column."""
    return Case(
        None,
        ((team_column == EXCLUDED.home_team_id, EXCLUDED.home_score),),
        EXCLUDED.away_score,
    )


def postseason_window(last_regular: Optional[date], night: date) -> bool:
    """Whether `night` falls in the postseason that follows a regular season ending on `last_regular`."""
    if last_regular is None:
        return False
    return last_regular < night <= last_regular + timedelta(days=POSTSEASON_DAYS)


def build_rows(
    rows: Iterable[Mapping[str, Any]],
    known_teams: set[str],
    run_id=None,
) -> tuple[list[dict], list[dict], dict[int, tuple[str, str]], Counter]:
    """(games, lines, players, skipped) for one season and season type's payload.

    A line is kept only when its game, team and player can all be stored; the
    rest are counted in `skipped` by reason.
    """
    rows = list(rows)
    skipped: Counter = Counter()

    games, unreadable = games_from_logs(rows)
    skipped["unreadable_game"] += len(unreadable)
    storable_games = []
    for game in games:
        if game["season_type"] not in LOADED_SEASON_TYPES:
            continue  # its lines are counted below, by season type
        if game["home_team"] in known_teams and game["away_team"] in known_teams:
            storable_games.append(game)
        else:
            skipped["game_unknown_team"] += 1
    by_id = {g["game_id"]: g for g in storable_games}

    now = datetime.utcnow()
    lines: list[dict] = []
    players: dict[int, tuple[str, str]] = {}
    seen: set[tuple[int, str]] = set()
    for row in rows:
        game_id = str(row.get("GAME_ID") or "").strip()
        season_type = season_type_for(game_id)
        if season_type not in LOADED_SEASON_TYPES:
            skipped[season_type or "unknown_season_type"] += 1
            continue
        if game_id not in by_id:
            skipped["no_game"] += 1
            continue
        team = team_id(row.get("TEAM_ABBREVIATION"))
        if team not in known_teams:
            skipped["unknown_team"] += 1
            continue
        identity = player_identity(row)
        if identity is None:
            skipped["no_player"] += 1
            continue
        line = box_score_stats(row)
        if line is None:
            skipped["did_not_play"] += 1
            continue
        player_id, name, normalized = identity
        # One line per player per game: a repeat would make the batch's upsert
        # touch one row twice, which Postgres refuses for the whole batch.
        if (player_id, game_id) in seen:
            skipped["duplicate_line"] += 1
            continue
        seen.add((player_id, game_id))
        players.setdefault(player_id, (name, normalized))
        game = by_id[game_id]
        lines.append({
            "player": player_id,
            "team": team,
            "game": game_id,
            # The game's own date and season, so a line can't disagree with it.
            "game_date": game["game_date"],
            "season": game["season"],
            "season_type": game["season_type"],
            "oreb": None, "dreb": None, "pf": None, "plus_minus": None,
            **line,
            "pipeline_run_id": run_id,
            "created_at": now,
            "updated_at": now,
        })
    return storable_games, lines, players, skipped


class GameHistoryPipeline(BasePipeline):
    """Load whole seasons of games and box scores, or this season's postseason nightly."""

    config = PipelineConfig(
        name="game_history",
        display_name="Game History",
        description="Whole seasons of box scores and games (regular season, play-in, playoffs); "
                    "nightly during the postseason",
        target_table="nba.player_game_stats",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="game-history",
        cron_job="playoffs",
        timeout_seconds=1800,
    )

    def __init__(self):
        super().__init__()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        options = ctx.options or {}
        seasons = list(options.get("seasons") or [])
        season_types = list(options.get("season_types") or [])

        nightly = not seasons
        if nightly:
            season = settings.nba_season
            night = ctx.game_date()
            last_regular = self._last_regular_night(season)
            if not postseason_window(last_regular, night):
                ctx.log.info("game_history_idle", season=season, night=str(night),
                             last_regular=str(last_regular) if last_regular else None)
                return
            seasons, season_types = [season], list(POSTSEASON_TYPES)

        season_types = season_types or list(SEASON_TYPE_PARAMS)
        unknown = [t for t in season_types if t not in SEASON_TYPE_PARAMS]
        if unknown:
            raise ValueError(f"Unknown season types {unknown}; expected some of {list(SEASON_TYPE_PARAMS)}")

        known_teams = {t.id for t in NBATeam.select(NBATeam.id)}
        ctx.log.info("game_history_start", seasons=seasons, season_types=season_types)

        failed: list[str] = []
        for season in seasons:
            for season_type in season_types:
                time.sleep(PAUSE_BETWEEN_CALLS)
                try:
                    rows = self.nba_extractor.get_season_game_logs(season, SEASON_TYPE_PARAMS[season_type])
                except Exception as exc:  # one bad call must not lose the rest of a long load
                    ctx.log.warning("game_history_fetch_failed", season=season, season_type=season_type,
                                    error=f"{type(exc).__name__}: {exc}")
                    ctx.increment_failed(1, f"fetch_failed:{season}:{season_type}")
                    failed.append(f"{season} {season_type}")
                    continue
                self._store(ctx, season, season_type, rows, known_teams)

        # The nightly run has no one reading its result: a failed call fails the
        # run, so it alerts (pipeline_failed) instead of passing as partial.
        if nightly and failed:
            raise RuntimeError(f"PlayerGameLogs failed for {', '.join(failed)}")

    @staticmethod
    def _last_regular_night(season: str) -> Optional[date]:
        return (
            Game.select(fn.MAX(Game.game_date))
            .where((Game.season == season) & (Game.season_type == "regular"))
            .scalar()
        )

    def _store(self, ctx: PipelineContext, season: str, season_type: str, rows: list[dict], known_teams: set[str]) -> None:
        games, lines, players, skipped = build_rows(rows, known_teams, ctx.run_id)
        labels = Counter(line["season_type"] for line in lines)

        existing = {
            p.id for p in Player.select(Player.id).where(Player.id.in_(list(players)))
        } if players else set()
        new_players = [
            {"id": pid, "name": name, "name_normalized": normalized}
            for pid, (name, normalized) in players.items()
            if pid not in existing
        ]

        with db.atomic():
            for i in range(0, len(new_players), BATCH_SIZE):
                Player.insert_many(new_players[i : i + BATCH_SIZE]).on_conflict_ignore().execute()
            for i in range(0, len(games), BATCH_SIZE):
                (
                    Game.insert_many(games[i : i + BATCH_SIZE])
                    .on_conflict(
                        conflict_target=[Game.game_id],
                        # A played game is final with the score its lines add up
                        # to; the date, teams and start time a schedule wrote stay.
                        # Scores go by team: at a neutral site (Mexico City, the
                        # Cup in Las Vegas) the box scores and the schedule can
                        # name different home teams.
                        update={
                            Game.home_score: _score_of(Game.home_team),
                            Game.away_score: _score_of(Game.away_team),
                            Game.status: "final",
                            Game.season_type: EXCLUDED.season_type,
                            Game.updated_at: datetime.utcnow(),
                        },
                    )
                    .execute()
                )
            for i in range(0, len(lines), BATCH_SIZE):
                (
                    PlayerGameStats.insert_many(lines[i : i + BATCH_SIZE])
                    .on_conflict(
                        conflict_target=[PlayerGameStats.player, PlayerGameStats.game],
                        preserve=LINE_PRESERVE,
                    )
                    .execute()
                )

        ctx.increment_records(len(lines))
        for reason, count in skipped.items():
            if count:
                ctx.increment_skipped(count, f"{season_type}:{reason}")
        ctx.log.info(
            "game_history_written",
            season=season,
            season_type=season_type,
            fetched=len(rows),
            lines=len(lines),
            games=len(games),
            new_players=len(new_players),
            labels=dict(labels),
            skipped={k: v for k, v in skipped.items() if v},
        )
