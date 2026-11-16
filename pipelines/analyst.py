"""
Analyst Pipeline: the morning's reads, in shadow (the Live rankings plan, step 4).

The live projection cannot read. Each morning, once it and the day's boards
are stored, this asks an AI analyst to read the stored news about a short list
of players and to write down, as inputs to the projection, what a box score
cannot say: how long he is out, whether a role is temporary, whether he is
limited on his way back.

One run, in order:

1. **The screen** (`services.analyst_screen`): fixed rules pick the players
   whose news might say such a thing. A player whose facts are the ones his
   last completed read saw is not read again, and a morning reads at most
   `ANALYST_MAX_READS` players, the strongest reasons first.
2. **The reads**: one call to the backend per player
   (`services.analyst_client`), a few at a time. The backend owns the model
   call and the month's budget; with the budget used up, or the analyst
   switched off there, nothing more is asked.
3. **The check** (`services.analyst_notes`): a proposal that cites no stored
   item, or sits outside its bounds, is refused. One that passes becomes the
   analyst's note for the player (nba.projection_adjustments, scope
   `analyst`). Every read is logged in nba.analyst_reads with why he was
   picked, what it came to and what it cost.
4. **The shadow projection**: the morning's projection built again with the
   analyst's notes applied, stored beside the live one under the source
   `cv_live_shadow`. A note written by hand wins over the analyst's wherever
   a player has both.
5. **The shadow boards**: the backend's two standard boards built on that
   projection, stored in nba.daily_rankings under the source `cv_shadow`.

Nothing a user sees reads any of it. The live projection reads the `season`
scope alone, so in shadow the analyst's notes move only the second projection
and the second boards, which is what the two are graded on before it goes
live. The projections editor shows the morning as a digest.

It has no cron job of its own: the daily-rankings trigger starts it in the
background once the day's projection and boards are stored, and each later
tick that morning starts it again until a run has succeeded. A run that could
not ask for its reads, or could not store its boards, fails, and the next tick
picks up where it stopped: the reads that came to an answer are not asked
again.

Counters: a read logged and a shadow row written are processed records; a
candidate left unread (unchanged, over the cap, out of budget) is a skip; a
read that did not complete is a failed record.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import pytz

from core.logging import get_correlation_id
from core.settings import settings
from db.base import db
from db.models.nba import AnalystRead, PlayerProjection, ProjectionAdjustment
from db.models.nba.analyst_reads import OUTCOME_DECLINED, OUTCOME_FAILED
from db.models.nba.daily_rankings import BOARDS, SOURCE_CV, DailyRanking
from db.models.nba.projection_adjustments import SCOPE_ANALYST
from db.models.pipeline_run import PipelineRun
from pipelines.base import BasePipeline
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.daily_rankings import cv_rows, now_eastern, today_et
from pipelines.live_projection import DAY_TURNS_HOUR_ET, build_rows, store, stored_on
from services import analyst_inputs, analyst_notes
from services.analyst_client import UNAVAILABLE, ReadResult, analyst_client_from_settings
from services.analyst_screen import Candidate, screen
from services.live_projection_inputs import LiveInputs, load_coefficients, load_inputs, note_of, noted_inputs
from services.live_projection_model import BoxLine, build_morning, project
from services.valuation_client import valuation_client_from_settings

NAME = "analyst"
SHADOW_SOURCE = "cv_live_shadow"            # nba.player_projections: the live projection with its notes applied
SHADOW_BOARDS = "cv_shadow"                 # nba.daily_rankings: the boards built on that
# The shadow projection is read once, by the boards built on it and the digest beside
# them; the boards are what is graded and are kept for good
SHADOW_KEEP_DAYS = 7
READ_WORKERS = 4
# This many reads in a row that could not be asked end the morning's reads
FAILURES_STOP = 3
EASTERN = pytz.timezone("US/Eastern")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class AnalystFailed(RuntimeError):
    """The run could not finish: it is recorded as failed, so the next tick takes it up again."""


@dataclass
class Planned:
    """One read about to be asked for."""

    candidate: Candidate
    body: dict
    owner_note_id: Optional[int]


def succeeded_on(day: date) -> bool:
    """Whether a run of the analyst has succeeded since `day` began, Eastern."""
    start = EASTERN.localize(datetime.combine(day, time(0))).astimezone(pytz.utc).replace(tzinfo=None)
    return (
        PipelineRun.select()
        .where((PipelineRun.pipeline_name == NAME) & (PipelineRun.status == "success")
               & (PipelineRun.started_at >= start))
        .exists()
    )


def due_reason(season: str, day: date) -> Optional[str]:
    """Why the daily-rankings trigger should not start the analyst now, or None when it should."""
    if not stored_on(season, day):
        return "this morning's live projection is not stored yet"
    if not DailyRanking.stored(day, SOURCE_CV):
        return "this morning's boards are not stored yet"
    if PipelineRun.is_running(NAME):
        return "the analyst is already running"
    if succeeded_on(day):
        return "the analyst has run this morning"
    return None


def shadow_inputs(inputs: LiveInputs) -> tuple[LiveInputs, int]:
    """`inputs` with the analyst's live notes laid under the notes written by hand,
    and how many of the analyst's are read.

    A player with a note written by hand that is in force is read off that
    one alone: the owner's edits win. Where the owner's note has ended or is
    past its review date it says nothing this morning, and the analyst's is read.
    """
    owners = {pid for pid, found in inputs.standings.items() if found.in_force}
    analyst = {
        record.player_id: note_of(record)
        for record in ProjectionAdjustment.active_for(inputs.season, SCOPE_ANALYST)
        if record.player_id not in owners
    }
    merged = {**inputs.notes, **analyst}
    return replace(inputs, **noted_inputs(inputs.known_before_notes, merged, inputs.as_of, inputs.last_played)), len(analyst)


def prune_shadow(season: str, day: date) -> int:
    return (
        PlayerProjection.delete()
        .where((PlayerProjection.season == season) & (PlayerProjection.source == SHADOW_SOURCE)
               & (PlayerProjection.as_of_date < day - timedelta(days=SHADOW_KEEP_DAYS)))
        .execute()
    )


class AnalystPipeline(BasePipeline):
    """Read the morning's short list, and store the projection and boards its notes would make."""

    config = PipelineConfig(
        name=NAME,
        display_name="Analyst (shadow)",
        description="Reads the news about a short list of players each morning and prices its notes on a second, "
                    "unseen projection and boards",
        target_table="nba.analyst_reads",
        category=PipelineCategory.SCHEDULED,
        trigger_slug="analyst",
        cron_job="daily-rankings",      # started in the background once the day's boards are stored
        timeout_seconds=900,
    )

    def __init__(self):
        super().__init__()
        self.client = analyst_client_from_settings()
        self.valuation_client = valuation_client_from_settings()

    def execute(self, ctx: PipelineContext) -> None:
        season = settings.nba_season
        day = today_et()
        now_et = now_eastern()
        if now_et.hour < DAY_TURNS_HOUR_ET:
            ctx.increment_skipped(1, "day_over")
            ctx.log.warning("analyst_day_over", today=str(day), hour_et=now_et.hour)
            return
        if not stored_on(season, day):
            # Its reads are of what the live projection assumes: without this morning's there is nothing to read against
            ctx.increment_skipped(1, "no_live_projection")
            ctx.log.info("analyst_no_live_projection", as_of=str(day))
            return

        month = self.client.month(correlation_id=get_correlation_id())
        if not month.available:
            ctx.increment_failed(1, f"backend:{month.reason}")
            raise AnalystFailed(f"the backend could not be asked about the analyst ({month.reason})")
        if not month.enabled:
            ctx.increment_skipped(1, "off")
            ctx.log.info("analyst_off", as_of=str(day))
            return

        inputs = load_inputs(season, day)
        if not inputs.lines or not any(inputs.ahead.values()):
            ctx.log.info("analyst_out_of_season", as_of=str(day), lines=len(inputs.lines))
            return
        coeffs = load_coefficients()
        morning = build_morning(inputs.lines, inputs.priors, inputs.ahead, day, teams=inputs.teams, ages=inputs.ages)
        live = project(morning, coeffs, inputs.known, inputs.role_notes)

        cut_short = self._reads(ctx, inputs, morning, live, room=month.room)

        shadow, applied = shadow_inputs(inputs)
        rows = build_rows(shadow, morning, coeffs, ctx.run_id, SHADOW_SOURCE)
        store(rows, season, day, SHADOW_SOURCE)
        pruned = prune_shadow(season, day)
        ctx.increment_records(len(rows))
        ctx.log.info("analyst_shadow_projection_stored", as_of=str(day), players=len(rows), notes_applied=applied,
                     pruned=pruned)

        self._boards(ctx, season, day)
        if cut_short:
            raise AnalystFailed(cut_short)

    # ---- the reads ---------------------------------------------------------------------------

    def _reads(self, ctx: PipelineContext, inputs: LiveInputs, morning, live, *, room: bool) -> Optional[str]:
        """Screen, read and settle. Returns why the reads were cut short, or None."""
        gathered = analyst_inputs.gather(inputs, utc_now())
        candidates = screen(analyst_inputs.subjects(gathered, inputs, morning, live))

        by_player: dict[int, list[BoxLine]] = {}
        wanted = {c.player_id for c in candidates}
        for line in inputs.lines:
            if line.player_id in wanted:
                by_player.setdefault(line.player_id, []).append(line)

        limit = max(int(settings.analyst_max_reads), 0)
        planned: list[Planned] = []
        unchanged = over = 0
        for candidate in candidates:
            pid = candidate.player_id
            body = analyst_inputs.packet(candidate, gathered, inputs, morning, live, by_player.get(pid, ()))
            owner = gathered.owner_notes.get(pid)
            own = analyst_inputs.own_state(gathered.analyst_notes.get(pid), gathered.as_of, inputs.last_played)
            seen = analyst_inputs.fingerprint(body, own, owner.id if owner is not None else None)
            if gathered.fingerprints.get(pid) == seen:
                unchanged += 1
            elif len(planned) >= limit:
                over += 1
            else:
                planned.append(Planned(candidate, body, owner.id if owner is not None else None))
        if unchanged:
            ctx.increment_skipped(unchanged, "unchanged")
        if over:
            ctx.increment_skipped(over, "over_cap")
        ctx.log.info("analyst_screened", as_of=str(gathered.as_of), candidates=len(candidates),
                     unchanged=unchanged, over_cap=over, to_read=len(planned), budget_room=room)
        if not planned:
            return None
        if not room:
            ctx.increment_skipped(len(planned), "budget")
            ctx.log.warning("analyst_budget_reached", unread=len(planned))
            return None

        results = self._ask(planned)
        counts: dict[str, int] = {}
        cost = 0
        for plan, result in zip(planned, results):
            if result is None or result.outcome in ("budget", "off"):
                ctx.increment_skipped(1, result.outcome if result is not None else "not_asked")
                continue
            outcome = self._settle(ctx, plan, result, gathered, inputs)
            counts[outcome] = counts.get(outcome, 0) + 1
            cost += result.cost_micros
        ctx.log.info("analyst_reads_done", as_of=str(gathered.as_of), cost_micros=cost, **counts)

        asked = [r for r in results if r is not None]
        tail = 0
        for result in reversed(asked):
            if result.outcome != UNAVAILABLE:
                break
            tail += 1
        if tail >= FAILURES_STOP or (asked and tail == len(asked)):
            return f"the backend could not be asked for the last {tail} reads ({asked[-1].detail})"
        return None

    def _ask(self, planned: list[Planned]) -> list[Optional[ReadResult]]:
        """Ask for every planned read, a few at a time, in order. A read comes back
        None when it was never asked: the budget ran out, the analyst was switched
        off, or the backend stopped answering."""
        correlation_id = get_correlation_id()
        stop = threading.Event()
        lock = threading.Lock()
        failures = [0]

        def one(plan: Planned) -> Optional[ReadResult]:
            if stop.is_set():
                return None
            result = self.client.read(plan.body, correlation_id=correlation_id)
            with lock:
                if result.outcome == UNAVAILABLE:
                    failures[0] += 1
                    if failures[0] >= FAILURES_STOP:
                        stop.set()
                else:
                    failures[0] = 0
                    if result.outcome in ("budget", "off"):
                        stop.set()
            return result

        with ThreadPoolExecutor(max_workers=READ_WORKERS, thread_name_prefix="analyst-read") as pool:
            return list(pool.map(one, planned))

    def _settle(self, ctx: PipelineContext, plan: Planned, result: ReadResult,
                gathered: analyst_inputs.Gathered, inputs: LiveInputs) -> str:
        """Log one read, and write the note it came to. Returns the read's outcome."""
        pid = plan.candidate.player_id
        own = gathered.analyst_notes.get(pid)
        row = {
            "season": gathered.season, "as_of_date": gathered.as_of, "player": pid,
            "reasons": [r.as_dict() for r in plan.candidate.reasons],
            "model": result.model, "prompt_version": result.prompt_version,
            "input_tokens": result.input_tokens, "output_tokens": result.output_tokens,
            "cost_micros": result.cost_micros, "pipeline_run_id": ctx.run_id,
        }
        own_after = analyst_inputs.own_state(own, gathered.as_of, inputs.last_played)

        if result.outcome != "read":
            if result.outcome == "declined":
                outcome, summary = OUTCOME_DECLINED, "The model declined the request."
            elif result.outcome == UNAVAILABLE:
                outcome, summary = OUTCOME_FAILED, f"The backend could not be asked ({result.detail})."
            else:
                outcome, summary = OUTCOME_FAILED, f"The answer was not a proposal: {result.detail or 'unreadable'}."
            AnalystRead.create(
                **row, outcome=outcome, summary=summary, note=own.id if own is not None else None,
                fingerprint=analyst_inputs.fingerprint(plan.body, own_after, plan.owner_note_id),
            )
            if outcome == OUTCOME_FAILED:
                ctx.increment_failed(1, "unavailable" if result.outcome == UNAVAILABLE else "malformed")
            else:
                ctx.increment_records(1)
            return outcome

        judgment = analyst_notes.judge(
            result.proposal, plan.body, gathered.items.get(pid, ()), gathered.as_of, own, inputs.last_played,
        )
        with db.atomic():
            outcome, note_id = analyst_notes.settle(judgment, pid, gathered.season, own)
            if note_id is None:
                own_after = None
            elif own is None or note_id != own.id:
                own_after = analyst_inputs.own_state(
                    ProjectionAdjustment.get_by_id(note_id), gathered.as_of, inputs.last_played,
                )
            AnalystRead.create(
                **row, outcome=outcome, summary=judgment.summary, note=note_id, disagrees=judgment.disagrees,
                proposal=result.proposal,
                fingerprint=analyst_inputs.fingerprint(plan.body, own_after, plan.owner_note_id),
            )
        ctx.increment_records(1)
        return outcome

    # ---- the shadow boards ---------------------------------------------------------------------

    def _boards(self, ctx: PipelineContext, season: str, day: date) -> None:
        """Store the backend's two boards built on this morning's shadow projection."""
        boards = self.valuation_client.daily_boards(correlation_id=get_correlation_id(), shadow=True)
        if not boards.available or boards.as_of is None:
            reason = boards.reason or "backend_unavailable"
            ctx.increment_failed(1, f"backend:{reason}")
            raise AnalystFailed(f"the backend could not be asked for the shadow boards ({reason})")
        if not boards.in_season:
            ctx.log.info("analyst_boards_out_of_season", as_of=str(boards.as_of))
            return
        if boards.as_of != day or boards.live_as_of != day:
            # Yesterday's shadow projection, or the blend the backend falls back to: not this morning's notes
            ctx.increment_failed(1, "boards_not_on_shadow")
            raise AnalystFailed(
                f"the shadow boards were not built on this morning's shadow projection "
                f"(boards for {boards.as_of}, on the projection of {boards.live_as_of}, model {boards.model})"
            )
        with db.atomic():
            written = sum(
                DailyRanking.replace_day(day, boards.season or season, SHADOW_BOARDS, board, cv_rows(boards, board),
                                         ctx.run_id)
                for board in BOARDS
            )
        ctx.increment_records(written)
        ctx.log.info("analyst_shadow_boards_stored", as_of=str(day), players=len(boards.players), rows=written,
                     model=boards.model)
