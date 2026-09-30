"""
Table freshness for the dashboard.

The question the old dashboard could not answer: is the data current? A
pipeline card says when the pipeline last ran; this says what date the table
it writes actually runs through, and when it was last written.

Where the facts come from:

- The registry says which table each pipeline writes (`PipelineConfig.
  target_table`), so a new pipeline appears here by being registered.
- The table's Peewee model says which column is its business date (`game_date`,
  `as_of_date`, `snapshot_date`, ...) and which is its write timestamp. A test
  checks that every registry target resolves to a model with both, so a config
  naming a table that does not exist fails there and not on the page.
- The schedule says what to expect: the game days in `nba.games` (any status)
  and their first tip-offs. Those rows are written for the whole season ahead
  by `game_start_times` (its own cron, with a static fallback), never by the
  nightly batch being judged, so a batch-wide failure cannot move the mark.
  Before the regular season nothing nightly is due, so those tables are
  `idle`, not `stale`; after it they stay judged through its last night.

The judgement (`judge`) and the due dates (`due_dates`) are pure functions of
dates, so they are tested without a database; `build_freshness` is the only
thing that queries.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, Iterable, Literal, Mapping, Optional

import pytz
from peewee import DateField, DateTimeField, Model

from core.logging import get_logger
from core.settings import settings
from db.base import BaseModel, db
from pipelines import PIPELINE_REGISTRY
from pipelines.config import PipelineCategory
from schemas.dashboard import FreshnessData, TableFreshness, TableWriter
from services.schedule_service import SeasonPhase, get_season_bounds, get_season_phase

log = get_logger("freshness")

FreshnessState = Literal["fresh", "stale", "idle", "empty", "unjudged", "error"]

# The column that says what date a table runs through, by preference. Matched
# by exact name so `birthdate` or `expected_return` never count.
DATE_COLUMNS = ("game_date", "as_of_date", "snapshot_date", "report_date",
                "notification_date", "nba_date", "date")

# The column that says when a row was last written, by preference.
WRITE_COLUMNS = ("updated_at", "last_updated", "captured_at", "created_at", "sent_at")

# Pipelines that write only when there is something to say. A quiet night
# leaves their table untouched, so its age says nothing about their health.
# lineup_alerts: only when a lineup needs attention. breakout_detection: only
# when a prominent player is out (`no_prominent_injuries_today` returns
# without a row).
CONDITIONAL_WRITERS = frozenset({"lineup_alerts", "breakout_detection"})

# Most time-critical first: the category shown for a table with two writers
# (nba.games: game_schedule nightly, game_start_times on its own cron).
CATEGORY_PRIORITY = (
    PipelineCategory.LIVE,
    PipelineCategory.PRE_GAME,
    PipelineCategory.POST_GAME,
    PipelineCategory.SCHEDULED,
)

# Last night's post-game batch is due by this hour ET the next morning (the
# ESPN flip is ~2:30 AM ET; the pipelines' own game-date cutoff is 6 AM ET).
POST_GAME_DEADLINE_HOUR_ET = 6

EASTERN = pytz.timezone("US/Eastern")

# Model modules that `db.models` itself does not import.
_MODEL_MODULES = (
    "db.models",
    "db.models.nba",
    "db.models.pipeline_run",
    "db.models.notifications",
    "db.models.stats.cumulative_player_stats",
    "db.models.stats.daily_matchup_score",
    "db.models.stats.daily_player_stats",
)


# ---- targets ---------------------------------------------------------------


@dataclass(frozen=True)
class TableTarget:
    """One table a registered pipeline writes, with the columns that date it."""

    table: str                       # "nba.player_game_stats"
    pipelines: tuple[str, ...]       # registry keys, registry order
    category: PipelineCategory       # the most time-critical writer's
    model: Optional[type[Model]]     # None: no model declares this table
    date_column: Optional[str]
    write_column: Optional[str]


def _all_models() -> dict[str, type[Model]]:
    """Every concrete model, keyed "schema.table"."""
    for name in _MODEL_MODULES:
        importlib.import_module(name)
    found: dict[str, type[Model]] = {}
    pending: list[type[Model]] = list(BaseModel.__subclasses__())
    while pending:
        cls = pending.pop()
        pending.extend(cls.__subclasses__())
        meta = cls._meta
        table = getattr(meta, "table_name", None)
        if not table:
            continue
        found.setdefault(f"{meta.schema or 'public'}.{table}", cls)
    return found


def _pick(model: type[Model], names: Iterable[str], kinds: tuple[type, ...]) -> Optional[str]:
    columns = {f.column_name: f for f in model._meta.sorted_fields}
    for name in names:
        field = columns.get(name)
        if field is not None and isinstance(field, kinds):
            return name
    return None


def targets() -> list[TableTarget]:
    """The registry's target tables, in registry order, one entry per table."""
    models = _all_models()
    by_table: dict[str, list[str]] = {}
    for name, cls in PIPELINE_REGISTRY.items():
        by_table.setdefault(cls.config.target_table, []).append(name)

    out: list[TableTarget] = []
    for table, pipelines in by_table.items():
        categories = [PIPELINE_REGISTRY[p].config.category for p in pipelines]
        category = next((c for c in CATEGORY_PRIORITY if c in categories), categories[0])
        model = models.get(table)
        out.append(TableTarget(
            table=table,
            pipelines=tuple(pipelines),
            category=category,
            model=model,
            # peewee's DateField and DateTimeField are siblings, so a timestamp
            # never passes as a business date.
            date_column=_pick(model, DATE_COLUMNS, (DateField,)) if model else None,
            write_column=_pick(model, WRITE_COLUMNS, (DateTimeField,)) if model else None,
        ))
    return out


# ---- the rule ---------------------------------------------------------------


@dataclass(frozen=True)
class Clock:
    """What time it is, for the rule: computed once per request, passed in."""

    now_et: datetime       # naive, Eastern
    today: date            # the ET calendar date
    settled_through: date  # last game date whose post-game deadline has passed
    phase: SeasonPhase     # where the season is today (display; the rule uses `Due`)


def clock(now: Optional[datetime] = None, season: Optional[str] = None) -> Clock:
    now_et = (now or datetime.now(timezone.utc)).astimezone(EASTERN).replace(tzinfo=None)
    today = now_et.date()
    # A game date is settled once the next morning's deadline has passed.
    settled_through = (now_et - timedelta(hours=POST_GAME_DEADLINE_HOUR_ET)).date() - timedelta(days=1)
    return Clock(now_et=now_et, today=today, settled_through=settled_through,
                 phase=get_season_phase(today, season))


@dataclass(frozen=True)
class Due:
    """The game date each nightly cadence should have run through by now.

    None: nothing is due yet (before the regular season's first settled night,
    or a calendar with no games). After the season ends this stays at its last
    night, so a final night that never landed is reported, not forgotten.
    """

    post_game: Optional[date]  # last regular-season game day whose morning deadline has passed
    pre_game: Optional[date]   # last regular-season game day whose first tip-off has passed


def due_dates(
    clk: Clock,
    first_tips: Mapping[date, Optional[time]],
    in_regular_season: Callable[[date], bool],
) -> Due:
    """
    Read the due dates off the schedule.

    `first_tips` is every game day on or before today with the day's earliest
    tip-off (ET), None when the start times are not known yet. Preseason days
    are on the calendar too; only regular-season days can be due.

    Post-game: the last game day that is settled (its 6 AM ET next-morning
    deadline has passed). Pre-game: a run for a game day is due at that day's
    first tip-off — the injury report should be in by then — so yesterday's
    game day until today's tip, today's from then on. A game day without
    known start times is not due until it is over.
    """
    game_days = sorted(day for day in first_tips if in_regular_season(day))

    post_game = max((day for day in game_days if day <= clk.settled_through), default=None)

    def pre_game_due(day: date) -> bool:
        if day < clk.today:
            return True
        if day > clk.today:
            return False
        tip = first_tips.get(day)
        return tip is not None and clk.now_et.time() >= tip

    pre_game = max((day for day in game_days if pre_game_due(day)), default=None)
    return Due(post_game=post_game, pre_game=pre_game)


def judge(
    target: TableTarget,
    *,
    latest_date: Optional[date],
    latest_written_at: Optional[datetime],
    due: Due,
) -> tuple[FreshnessState, Optional[date]]:
    """
    One word for a table, and the date it was expected to run through.

    - `unjudged`: no nightly cadence to hold it to — a scheduled or live
      pipeline, a conditional writer, or a table with no business date. What
      it holds is shown, not judged.
    - `idle`: nothing is due yet for its cadence (see `Due`).
    - `empty`: a nightly table with nothing in it while something is due.
    - `fresh` / `stale`: against the game day its cadence is due through —
      post-game tables the last settled night, pre-game tables the last day
      whose first tip-off has passed.
    """
    if target.category == PipelineCategory.POST_GAME:
        expected = due.post_game
    elif target.category == PipelineCategory.PRE_GAME:
        expected = due.pre_game
    else:
        return "unjudged", None
    if any(p in CONDITIONAL_WRITERS for p in target.pipelines) or target.date_column is None:
        return "unjudged", None
    if expected is None:
        return "idle", None
    if latest_date is None and latest_written_at is None:
        return "empty", None
    if latest_date is None:
        return "stale", expected
    return ("stale" if latest_date < expected else "fresh"), expected


# ---- queries ---------------------------------------------------------------


def _first_tips(through: date, season: str) -> dict[date, Optional[time]]:
    """Every game day of the season on or before `through`, with its earliest
    tip-off (ET). Any status: the schedule, not the results."""
    rows = db.execute_sql(
        "SELECT game_date, min(start_time_et) FROM nba.games "
        "WHERE season = %s AND game_date <= %s GROUP BY game_date",
        (season, through),
    ).fetchall()
    return {day: tip for day, tip in rows}


def _next_game_date(today: date) -> Optional[date]:
    row = db.execute_sql(
        "SELECT min(game_date) FROM nba.games WHERE game_date >= %s", (today,),
    ).fetchone()
    return row[0] if row else None


def _row_estimates() -> dict[str, int]:
    """Planner statistics, one query for every table: cheap, approximate."""
    rows = db.execute_sql(
        "SELECT schemaname || '.' || relname, n_live_tup FROM pg_stat_user_tables "
        "WHERE schemaname IN ('nba', 'stats_s2', 'usr')"
    ).fetchall()
    return {table: int(count) for table, count in rows}


def _latest(target: TableTarget) -> tuple[Optional[date], Optional[datetime]]:
    """max(date column), max(write column). Both indexed-or-small; one statement."""
    parts = []
    for column in (target.date_column, target.write_column):
        parts.append(f'max("{column}")' if column else "NULL")
    schema, table = target.table.split(".", 1)
    with db.atomic():  # a failure here must not poison the next table's query
        row = db.execute_sql(f'SELECT {", ".join(parts)} FROM "{schema}"."{table}"').fetchone()
    latest_date, latest_written = row if row else (None, None)
    if isinstance(latest_date, datetime):  # a DateField backed by a timestamp column
        latest_date = latest_date.date()
    return latest_date, latest_written


def build_freshness(now: Optional[datetime] = None) -> FreshnessData:
    """Every target table, judged. Runs synchronously: wrap in run_in_db_thread."""
    now = now or datetime.now(timezone.utc)
    clk = clock(now)
    bounds = get_season_bounds()
    due = due_dates(
        clk,
        _first_tips(clk.today, settings.nba_season),
        lambda day: bounds.opening_night <= day <= bounds.regular_season_end,
    )
    next_game = _next_game_date(clk.today)
    try:
        estimates = _row_estimates()
    except Exception as exc:  # statistics are a nicety, never a failure
        log.warning("freshness_row_estimates_failed", error=str(exc))
        estimates = {}

    tables: list[TableFreshness] = []
    for target in targets():
        writers = [
            TableWriter(name=p, display_name=PIPELINE_REGISTRY[p].config.display_name)
            for p in target.pipelines
        ]
        base = dict(
            table=target.table,
            pipelines=writers,
            category=target.category.value,
            date_column=target.date_column,
            write_column=target.write_column,
            rows_estimate=estimates.get(target.table),
        )
        if target.model is None:
            tables.append(TableFreshness(**base, state="error", error="no model declares this table"))
            continue
        try:
            latest_date, latest_written = _latest(target)
        except Exception as exc:
            log.warning("freshness_query_failed", table=target.table, error=str(exc))
            tables.append(TableFreshness(**base, state="error", error=type(exc).__name__))
            continue
        state, expected = judge(
            target, latest_date=latest_date, latest_written_at=latest_written, due=due,
        )
        tables.append(TableFreshness(
            **base,
            latest_date=latest_date,
            latest_written_at=latest_written,
            expected_date=expected,
            state=state,
        ))

    return FreshnessData(
        tables=tables,
        season=settings.nba_season,
        phase=clk.phase,
        today=clk.today,
        settled_through=clk.settled_through,
        post_game_due=due.post_game,
        pre_game_due=due.pre_game,
        next_game_date=next_game,
        fetched_at=now,
    )
