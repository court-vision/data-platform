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
- The season calendar and `nba.games` say what to expect. Outside the regular
  season nothing nightly is due, so those tables are `idle`, not `stale`.

The judgement (`judge`) is a pure function of dates, so it is tested without a
database; `build_freshness` is the only thing that queries.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Literal, Optional

import pytz
from peewee import DateField, DateTimeField, Model

from core.logging import get_logger
from core.settings import settings
from db.base import BaseModel, db
from pipelines import PIPELINE_REGISTRY
from pipelines.config import PipelineCategory
from schemas.dashboard import FreshnessData, TableFreshness, TableWriter
from services.schedule_service import SeasonPhase, get_season_phase

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
CONDITIONAL_WRITERS = frozenset({"lineup_alerts"})

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

    today: date            # the ET calendar date
    settled_through: date  # last game date whose post-game deadline has passed
    phase: SeasonPhase


def clock(now: Optional[datetime] = None, season: Optional[str] = None) -> Clock:
    now_et = (now or datetime.now(timezone.utc)).astimezone(EASTERN)
    today = now_et.date()
    # A game date is settled once the next morning's deadline has passed.
    settled_through = (now_et - timedelta(hours=POST_GAME_DEADLINE_HOUR_ET)).date() - timedelta(days=1)
    return Clock(today=today, settled_through=settled_through, phase=get_season_phase(today, season))


def judge(
    target: TableTarget,
    *,
    latest_date: Optional[date],
    latest_written_at: Optional[datetime],
    last_game_date: Optional[date],
    clock: Clock,
) -> tuple[FreshnessState, Optional[date]]:
    """
    One word for a table, and the date it was expected to run through.

    - `unjudged`: no nightly cadence to hold it to — a scheduled or live
      pipeline, a conditional writer, or a table with no business date. What
      it holds is shown, not judged.
    - `idle`: a nightly table outside the regular season, or before the first
      settled game of one. Nothing is due.
    - `empty`: a nightly table with nothing in it while something is due.
    - `fresh` / `stale`: in season, against the last settled game date (the
      last date with a final game whose next-morning deadline has passed).
      Pre-game tables are held to the same date: their run for that day
      preceded its games.
    """
    if target.category not in (PipelineCategory.POST_GAME, PipelineCategory.PRE_GAME):
        return "unjudged", None
    if any(p in CONDITIONAL_WRITERS for p in target.pipelines) or target.date_column is None:
        return "unjudged", None
    if clock.phase != "regular" or last_game_date is None:
        return "idle", None
    if latest_date is None and latest_written_at is None:
        return "empty", None
    if latest_date is None:
        return "stale", last_game_date
    return ("stale" if latest_date < last_game_date else "fresh"), last_game_date


# ---- queries ---------------------------------------------------------------


def _last_game_date(through: date) -> Optional[date]:
    row = db.execute_sql(
        "SELECT max(game_date) FROM nba.games WHERE status = 'final' AND game_date <= %s",
        (through,),
    ).fetchone()
    return row[0] if row else None


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
    last_game = _last_game_date(clk.settled_through)
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
            target, latest_date=latest_date, latest_written_at=latest_written,
            last_game_date=last_game, clock=clk,
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
        last_game_date=last_game,
        next_game_date=next_game,
        fetched_at=now,
    )
