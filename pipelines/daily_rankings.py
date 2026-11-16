"""
Daily Rankings Pipeline — a ranking stored every morning (the Live rankings
plan, step 1).

Court Vision's rankings were only ever computed when a board was opened, so a
rank had no history: nothing could say how it moved, when ESPN last changed its
own, or which of the two was right about what happened next. Each morning of
the regular season this stores, in nba.daily_rankings:

- **Court Vision's two standard boards** (`source='cv'`, boards `points` and
  `categories`), exactly as the Draft lab shows them to a room with no league.
  The backend owns the valuation, so they are one call
  (`ValuationClient.daily_boards`, POST /v1/internal/jobs/rankings/daily) and
  this platform keeps no second copy of it. Since step 2 the boards are built
  on the morning's live projection, which this platform writes just before
  (`pipelines/live_projection.py`, the first link of the same trigger); each
  row's `model` says so, or names the blend the backend falls back to.
- **ESPN's two ranks that morning** (`source='espn'`): draftRanksByRankType
  STANDARD on the points board, ROTO on the categories one, from the same
  public-league read the preseason market snapshot uses. ESPN keeps updating
  those ranks in season, a few weeks apart; a row a day is what shows when.

The day is the backend's: `as_of` is the fantasy day its boards count games
from, and both sources are stored under it. One board a day. The morning's is
kept: a later tick that finds the day's Court Vision boards stored writes only
what is still missing (ESPN's, after a failed read), and only a forced run
(`?force=true`, the dashboard's Run button) rebuilds the day.

Nothing is stored for a day that is over. The backend's day turns at 3 AM ET,
so from midnight until then its `as_of` is still yesterday: a run in those
hours, forced or not, would file boards that have seen last night's games
under the morning before them. It stores nothing and says so.

It waits for last night. The boards read each player's season line, so until
the post-game batch has written the night's box scores into
nba.player_season_stats the run raises `DataNotReady` and the next hourly tick
tries again. The newest date on a season line is not enough: that batch writes
player by player, so one that is still running, or stopped midway, has moved
the date for some players and not for others, and the run waits for it too
(`season_stats_unsettled`). So does a run whose boards were read while the
batch was writing: the batch's newest run is noted before the read
(`batch_mark`) and has to be the same one, finished, after it. From
`WAIT_UNTIL_HOUR_ET` on it stops waiting and stores what there is, so a night
whose box scores never land still has a board.

Everything of one source is written in one transaction: a reader resolving "the
latest board" never finds half of one.

Counters: a row written is a processed record; an ESPN-ranked player with no
nba.players row is a skip; a backend or an ESPN that could not be read is one
failed record with its reason, and then fails the run, so cron-runner's next
tick retries it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import pytz
from peewee import fn

from core.logging import get_correlation_id
from db.base import db
from db.models.nba.daily_rankings import (
    BOARD_CATEGORIES,
    BOARD_POINTS,
    BOARDS,
    SOURCE_CV,
    SOURCE_ESPN,
    DailyRanking,
)
from db.models.nba.games import Game
from db.models.nba.players import Player
from db.models.pipeline_run import PipelineRun
from pipelines.base import BasePipeline, DataNotReady
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor
from pipelines.readiness import REGULAR_SEASON_PREFIX
from services.valuation_client import DailyBoards, valuation_client_from_settings

EASTERN = pytz.timezone("US/Eastern")

# From this hour (ET) the run no longer waits for last night's box scores.
WAIT_UNTIL_HOUR_ET = 10

# The batch that writes the season lines the boards read
SEASON_STATS_PIPELINE = "player_season_stats"

# ESPN's rank and auction value for each board, as `parse_draft_market_players` names them
ESPN_FIELDS = {BOARD_POINTS: ("overall_rank", "auction_value"), BOARD_CATEGORIES: ("roto_rank", "roto_auction_value")}

# ESPN's "nothing wrong with him" statuses: stored as no flag, like the boards' own
ESPN_HEALTHY = frozenset({"ACTIVE", "NORMAL"})


class DailyRankingsFailed(RuntimeError):
    """A source could not be read: the run is recorded as failed, so the next tick retries it."""


@dataclass(frozen=True)
class Stored:
    """What a day already holds, per source."""

    cv: int
    espn: int

    @property
    def complete(self) -> bool:
        return self.cv > 0 and self.espn > 0


def stored_on(day: date) -> Stored:
    return Stored(cv=DailyRanking.stored(day, SOURCE_CV), espn=DailyRanking.stored(day, SOURCE_ESPN))


def today_et(now: Optional[datetime] = None) -> date:
    """The Eastern calendar date: what the trigger's gate looks for a board under.

    The boards' own day is the backend's fantasy day, which is the same date
    from 3 AM ET on, the hours this job is scheduled in.
    """
    return (now or datetime.now(pytz.utc)).astimezone(EASTERN).date()


def now_eastern() -> datetime:
    return datetime.now(EASTERN)


def last_game_day(before: date, season: str) -> Optional[date]:
    """The newest day before `before` with a regular-season game on the schedule."""
    return (
        Game.select(fn.MAX(Game.game_date))
        .where((Game.season == season) & (Game.game_date < before)
               & Game.game_id.startswith(REGULAR_SEASON_PREFIX))
        .scalar()
    )


@dataclass(frozen=True)
class BatchMark:
    """A batch's newest run, whatever its status: which one, and how it stands.

    Taken before reading what the batch writes, and compared after
    (`batch_unsettled`), it tells a read the batch wrote under from a clean one.
    """

    run_id: Optional[str]                   # None: the batch has never run
    status: Optional[str]                   # running, success or failed


def batch_mark(pipeline: str) -> BatchMark:
    """The newest run of `pipeline` as the run records have it."""
    run = (
        PipelineRun.select(PipelineRun.id, PipelineRun.status)
        .where(PipelineRun.pipeline_name == pipeline)
        .order_by(PipelineRun.started_at.desc())
        .limit(1)
        .first()
    )
    return BatchMark(str(run.id), run.status) if run is not None else BatchMark(None, None)


def batch_unsettled(pipeline: str, label: str, before: Optional[BatchMark] = None) -> Optional[str]:
    """Why what `pipeline` writes may be half-written, or None when it is whole.

    The post-game batches write one row at a time with no transaction around
    the night, so the newest date appears with the first row. What they write
    is whole when no run is going and the newest one succeeded (a retry after a
    run that stopped midway writes the rows that run did not reach).

    A newest run still marked running is not whole however old it is:
    `PipelineRun.is_running` stops counting one after two hours, but a run that
    hung, or whose end was never recorded, left the night as unfinished as one
    that failed, until a later run gets through.

    `before` is the mark taken before the caller read what the batch writes.
    A newest run that is not that one, or no longer stands as it did, started
    or finished during the read, which may then hold rows of both nights.
    """
    if PipelineRun.is_running(pipeline):
        return f"the {label} batch is still writing"
    newest = batch_mark(pipeline)
    if newest.status == "running":
        return f"the last {label} batch never finished, so what it writes may be half-written"
    if newest.status == "failed":
        return f"the last {label} batch failed, so what it writes may be half-written"
    if before is not None and newest != before:
        return f"the {label} batch was writing while this run read what it writes"
    return None


def season_stats_unsettled(before: Optional[BatchMark] = None) -> Optional[str]:
    """Why the season lines the boards read may be half-written, or None.
    `before`: the batch's mark from before the boards were read."""
    return batch_unsettled(SEASON_STATS_PIPELINE, "season-stats", before)


def waiting_on(
    boards: DailyBoards,
    last_night: Optional[date],
    now_et: datetime,
    unsettled: Optional[str] = None,
) -> Optional[str]:
    """Why the boards should not be stored yet, or None when they can be.

    They can once the season lines carry the last game day's box scores and
    the batch that writes them is done (`unsettled`, from
    `season_stats_unsettled`, is why it is not), or once the morning has gone
    on long enough that waiting costs the day's board.
    """
    if last_night is None or now_et.hour >= WAIT_UNTIL_HOUR_ET:
        return None
    if boards.season_as_of is None or boards.season_as_of < last_night:
        through = boards.season_as_of.isoformat() if boards.season_as_of else "no game yet"
        return f"season lines run through {through}; the box scores of {last_night.isoformat()} have not landed"
    return unsettled


def cv_rows(boards: DailyBoards, board: str) -> list[dict]:
    """One of the backend's boards as nba.daily_rankings rows."""
    rows = []
    for player in boards.players:
        place = player.points if board == BOARD_POINTS else player.categories
        rows.append({
            "player_id": player.player_id, "rank": place.rank, "value": place.value, "score": place.score,
            "games": place.games, "season_gp": player.season_gp, "injury_status": player.injury_status,
            "line_source": player.line_source, "model": boards.model,
        })
    return rows


def espn_rows(market: list[dict], player_ids: dict[int, int], board: str) -> tuple[list[dict], int]:
    """ESPN's rank on one board as rows, and how many ranked players had no nba.players row.

    `market` is the extractor's parse, `player_ids` ESPN id -> NBA id. A player
    ESPN does not rank on this board has no row; one it lists twice keeps his
    better rank.
    """
    rank_field, value_field = ESPN_FIELDS[board]
    best: dict[int, dict] = {}
    unresolved = 0
    for row in market:
        rank = row.get(rank_field)
        if rank is None or int(rank) < 1:
            continue
        player_id = player_ids.get(row["espn_id"])
        if player_id is None:
            unresolved += 1
            continue
        status = row.get("injury_status")
        entry = {
            "player_id": player_id, "rank": int(rank), "value": row.get(value_field),
            "injury_status": None if (not status or str(status).upper() in ESPN_HEALTHY) else str(status)[:20],
        }
        if player_id not in best or entry["rank"] < best[player_id]["rank"]:
            best[player_id] = entry
    return sorted(best.values(), key=lambda r: (r["rank"], r["player_id"])), unresolved


class DailyRankingsPipeline(BasePipeline):
    """Store Court Vision's two standard boards and ESPN's ranks for the day."""

    config = PipelineConfig(
        name="daily_rankings",
        display_name="Daily Rankings",
        description="Stores Court Vision's two standard boards and ESPN's ranks every morning of the season",
        target_table="nba.daily_rankings",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="daily-rankings",
        cron_job="daily-rankings",
        timeout_seconds=180,
        force_on_run=True,
    )

    def __init__(self):
        super().__init__()
        self.valuation_client = valuation_client_from_settings()
        self.espn_extractor = ESPNExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        force = bool((ctx.options or {}).get("force"))

        # Before the read, so a season-stats batch that writes under it shows afterwards
        season_stats = batch_mark(SEASON_STATS_PIPELINE)
        boards = self.valuation_client.daily_boards(correlation_id=get_correlation_id())
        if not boards.available or boards.as_of is None:
            reason = boards.reason or "backend_unavailable"
            ctx.increment_failed(1, f"backend:{reason}")
            raise DailyRankingsFailed(f"the backend could not be asked for the boards ({reason})")
        if not boards.in_season:
            ctx.log.info("daily_rankings_out_of_season", as_of=str(boards.as_of))
            return

        day = boards.as_of
        today = today_et()
        if day != today:
            # Midnight to 3 AM ET: the backend is still on yesterday, whose games are in
            ctx.increment_skipped(1, "day_over")
            ctx.log.warning("daily_rankings_day_over", as_of=str(day), today=str(today), forced=force)
            return

        held = stored_on(day)
        failures: list[str] = []

        if held.cv and not force:
            ctx.log.info("daily_rankings_cv_kept", as_of=str(day), rows=held.cv)
        else:
            reason = waiting_on(boards, last_game_day(day, boards.season), now_eastern(),
                                season_stats_unsettled(season_stats))
            if reason and not force:
                raise DataNotReady(reason)
            with db.atomic():
                written = sum(
                    DailyRanking.replace_day(day, boards.season, SOURCE_CV, board, cv_rows(boards, board), ctx.run_id)
                    for board in BOARDS
                )
            ctx.increment_records(written)
            ctx.log.info("daily_rankings_cv_stored", as_of=str(day), players=len(boards.players), rows=written,
                         model=boards.model, season_as_of=str(boards.season_as_of) if boards.season_as_of else None,
                         live_as_of=str(boards.live_as_of) if boards.live_as_of else None)

        if held.espn and not force:
            ctx.log.info("daily_rankings_espn_kept", as_of=str(day), rows=held.espn)
        else:
            error = self._store_espn(ctx, day, boards.season)
            if error:
                failures.append(error)

        if failures:
            raise DailyRankingsFailed("; ".join(failures))

    def _store_espn(self, ctx: PipelineContext, day: date, season: str) -> Optional[str]:
        """Read and store ESPN's two ranks for `day`. Returns why it could not, or None."""
        try:
            market = self.espn_extractor.get_draft_market_data()
        except Exception as exc:  # the extractor retries; what is left is an outage
            ctx.increment_failed(1, f"espn:{type(exc).__name__}")
            ctx.log.warning("daily_rankings_espn_failed", error=str(exc)[:300])
            return f"ESPN's ranks could not be read ({type(exc).__name__})"
        if market is None:
            ctx.increment_failed(1, "espn:league_not_rolled")
            return "ESPN's ranks could not be read (the public league is not on this season)"

        espn_ids = [row["espn_id"] for row in market]
        player_ids = {
            p.espn_id: p.id
            for p in Player.select(Player.id, Player.espn_id).where(Player.espn_id.in_(espn_ids))
        } if espn_ids else {}

        boards = {board: espn_rows(market, player_ids, board) for board in BOARDS}
        if not any(rows for rows, _ in boards.values()):
            ctx.increment_failed(1, "espn:no_ranks")
            return "ESPN's ranks could not be read (the payload ranks nobody)"
        with db.atomic():
            written = sum(
                DailyRanking.replace_day(day, season, SOURCE_ESPN, board, rows, ctx.run_id)
                for board, (rows, _) in boards.items()
            )
        ctx.increment_records(written)
        unresolved = max(missing for _, missing in boards.values())
        if unresolved:
            ctx.increment_skipped(unresolved, "unresolved_player")
        ctx.log.info("daily_rankings_espn_stored", as_of=str(day), rows=written, unresolved=unresolved,
                     points=len(boards[BOARD_POINTS][0]), categories=len(boards[BOARD_CATEGORIES][0]))
        return None
