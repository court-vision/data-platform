"""
Projections Editor

The API behind the dashboard's /projections page: Court Vision's projection
with the lines it was built from, and the curated adjustments on top of it.
Everything takes the pipeline bearer token.

Routes:
    GET    /v1/dashboard/projections                          — every projected player, four lines and ranks
    POST   /v1/dashboard/projections/preview                  — one player's line and ranks under an edit
    PUT    /v1/dashboard/projections/{player_id}/adjustment   — save a new version, then republish
    DELETE /v1/dashboard/projections/{player_id}/adjustment   — retire the live one, then republish
    GET    /v1/dashboard/projections/{player_id}/adjustments  — every version, newest first

    POST   /v1/dashboard/projections/note/preview             — one player's live line under a season note
    PUT    /v1/dashboard/projections/{player_id}/note         — save a season note, then rebuild the live projection
    DELETE /v1/dashboard/projections/{player_id}/note         — retire it, then rebuild
    GET    /v1/dashboard/projections/{player_id}/news         — his recent stored news: what a note can cite

    GET    /v1/dashboard/projections/analyst                  — the analyst's morning digest (in shadow)

A save writes one row to nba.projection_adjustments (append-only: it
supersedes the live row of its scope). A preseason adjustment then runs
cv-projection, so the snapshot the draft board reads is the one the page
showed in its preview. A season note (the Live rankings plan, step 3) leaves
the preseason projection alone and rebuilds the day's live projection, which
is what the board is priced on in season.

The analyst (the Live rankings plan, step 4) writes notes of its own, in a
scope of its own, which in shadow reach only a second projection and second
boards. Its digest is read here; nothing on this page writes for it.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query, Security

from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from core.settings import settings
from datetime import datetime

from db.base import run_in_db_thread
from db.models.nba import ProjectionAdjustment
from db.models.nba.projection_adjustments import SCOPE_SEASON
from pipelines import run_pipeline
from schemas.analyst import AnalystDigestResponse
from schemas.common import ApiStatus
from schemas.projections import (
    AdjustmentHistory,
    AdjustmentHistoryResponse,
    AdjustmentSave,
    AdjustmentSaved,
    AdjustmentSavedResponse,
    NotePreviewRequest,
    NotePreviewResponse,
    NoteSave,
    NoteSaved,
    NoteSavedResponse,
    PlayerNews,
    PlayerNewsResponse,
    PreviewRequest,
    PreviewResponse,
    ProjectionsResponse,
)
from services import analyst_digest
from services import live_projection_inputs as live_inputs
from services import projection_editor as editor
from services import projection_notes as notes
from services.analyst_client import analyst_client_from_settings
from services.valuation_client import Valuation, valuation_client_from_settings

router = APIRouter(
    prefix="/dashboard/projections",
    tags=["projections"],
    dependencies=[Security(verify_pipeline_token)],
)
log = get_logger("projections_api")

_PLAYER_ID = Path(..., ge=1, description="NBA player id (nba.players.id)")


def _valuation(players: list[dict]) -> Valuation:
    """Ask the backend to rank a pool. Blocking: call it off the event loop."""
    return valuation_client_from_settings(settings).standard(players)


@router.get("", response_model=ProjectionsResponse)
async def get_projections() -> ProjectionsResponse:
    """
    Every projected player with the four lines side by side — statistical,
    ESPN, blend, final — his live adjustment, and where the final line ranks in
    the standard league next to ESPN's own ranks.

    The lines are computed here from the same inputs cv-projection reads, so
    the page reflects an adjustment the moment it is saved; `unpublished` says
    how many of them the published snapshot has not caught up with.
    """
    state = await run_in_db_thread(editor.load_state, settings.nba_season)

    def build():
        lines = editor.breakdowns(state.inputs)
        valuation = _valuation(editor.valuation_players(lines, state.inputs.current))
        return editor.build_view(state, lines, valuation)

    data = await asyncio.to_thread(build)
    adjusted = sum(1 for row in data.players if row.adjustment is not None)
    return ProjectionsResponse(
        status="success",
        message=f"{len(data.players)} players, {adjusted} adjusted",
        data=data,
    )


@router.post("/preview", response_model=PreviewResponse)
async def preview_adjustment(req: PreviewRequest) -> PreviewResponse:
    """
    One player's final line and standard ranks as they stand, and as they
    would be under the adjustment in the body (or with none at all). Nothing is
    written. The whole pool is re-valued, because a rank is a place among
    everybody.
    """
    state = await run_in_db_thread(editor.load_state, settings.nba_season)

    def build():
        inputs = state.inputs
        before = editor.breakdowns(inputs)
        change = editor.resolve_change(inputs, req.player_id, req.adjustment)
        after = editor.breakdowns(inputs, override=(req.player_id, change))
        before_valuation = _valuation(editor.valuation_players(before, inputs.current))
        after_valuation = _valuation(editor.valuation_players(after, inputs.current))
        return editor.build_preview(req.player_id, before, before_valuation, after, after_valuation)

    preview = await asyncio.to_thread(build)
    if preview is None:
        raise HTTPException(status_code=404, detail=f"Player {req.player_id} is not a projected player")
    return PreviewResponse(status="success", message="Preview computed; nothing was saved", data=preview)


async def _republish(player_id: int) -> AdjustmentSaved:
    """Run cv-projection after a write, so the board reads what was just saved.

    A failed republish does not undo the write: the adjustment is recorded, the
    page says the snapshot is behind, and the next run (or the Republish
    button) catches it up.
    """
    result = await run_pipeline("cv_projection", options={"force": True})
    published = result.status in (ApiStatus.SUCCESS, ApiStatus.SUCCESS.value)
    if not published:
        log.warning("projection_republish_failed", player_id=player_id, message=result.message, error=result.error)
    return AdjustmentSaved(player_id=player_id, published=published, pipeline=result)


@router.put("/{player_id}/adjustment", response_model=AdjustmentSavedResponse)
async def save_adjustment(body: AdjustmentSave, player_id: int = _PLAYER_ID) -> AdjustmentSavedResponse:
    """
    Make the body the player's live adjustment for the season. The previous
    version is kept and marked superseded. Then republish the projection.
    """
    season = settings.nba_season

    def write() -> Optional[ProjectionAdjustment]:
        if not editor.player_exists(player_id):
            return None
        return ProjectionAdjustment.record(
            player_id, season,
            kind=body.kind, minutes=body.minutes, games=body.games, return_date=body.return_date,
            usage=body.usage, rates=body.rates, note=body.note, source_url=body.source_url,
            author=body.author,
        )

    record = await run_in_db_thread(write)
    if record is None:
        raise HTTPException(status_code=404, detail=f"No player {player_id}")
    log.info("projection_adjustment_saved", player_id=player_id, adjustment_id=record.id,
             kind=body.kind, author=body.author)

    saved = await _republish(player_id)
    saved.adjustment = editor.adjustment_entry(record)
    return AdjustmentSavedResponse(
        status="success",
        message="Adjustment saved and the projection republished" if saved.published
        else "Adjustment saved; the projection was not republished",
        data=saved,
    )


@router.delete("/{player_id}/adjustment", response_model=AdjustmentSavedResponse)
async def retire_adjustment(player_id: int = _PLAYER_ID) -> AdjustmentSavedResponse:
    """
    Withdraw the player's live adjustment without a replacement (the row is
    kept, marked retired), then republish the projection.
    """
    season = settings.nba_season
    retired = await run_in_db_thread(ProjectionAdjustment.retire, player_id, season)
    if not retired:
        raise HTTPException(status_code=404, detail=f"Player {player_id} has no live adjustment for {season}")
    log.info("projection_adjustment_retired", player_id=player_id)

    saved = await _republish(player_id)
    return AdjustmentSavedResponse(
        status="success",
        message="Adjustment retired and the projection republished" if saved.published
        else "Adjustment retired; the projection was not republished",
        data=saved,
    )


@router.get("/{player_id}/adjustments", response_model=AdjustmentHistoryResponse)
async def get_adjustment_history(player_id: int = _PLAYER_ID) -> AdjustmentHistoryResponse:
    """Every version of the player's adjustment this season, newest first:
    the live one, the ones it superseded, and any that were retired."""
    season = settings.nba_season
    versions = await run_in_db_thread(editor.adjustment_history, player_id, season)
    return AdjustmentHistoryResponse(
        status="success",
        message=f"{len(versions)} version(s)",
        data=AdjustmentHistory(player_id=player_id, season=season, versions=versions),
    )


# ---- notes in season -----------------------------------------------------------------------


def _eastern_now() -> datetime:
    """Now, as Eastern wall-clock time: when a note saved this moment was written."""
    return live_inputs.eastern(datetime.utcnow())


@router.post("/note/preview", response_model=NotePreviewResponse)
async def preview_note(req: NotePreviewRequest) -> NotePreviewResponse:
    """
    One player's live projection this morning as it stands, and as it would be
    under the season note in the body (or with none at all): the line in a
    game he plays, the games he is expected to play from here, and the
    stretches of the calendar the two are averaged over. Nothing is written.

    It prices the line, not a rank: the board's rank is worked out by the
    backend on its next read, under its own league.
    """
    season = settings.nba_season
    inputs = await run_in_db_thread(live_inputs.load_inputs, season, notes.today_et())
    preview = await asyncio.to_thread(
        notes.preview, inputs, req.player_id, req.note, written=_eastern_now(), review_date=req.review_date
    )
    if preview is None:
        raise HTTPException(
            status_code=404,
            detail=f"Player {req.player_id} is not in the live projection: no game this season and no preseason line",
        )
    return NotePreviewResponse(status="success", message="Preview computed; nothing was saved", data=preview)


async def _rebuild_live(player_id: int) -> NoteSaved:
    """Rebuild the day's live projection after a write, so the board reads the
    note at once. A rebuild that did not happen (before the day turns, out of
    season, or a failure) does not undo the write: the next morning's run
    reads the note anyway."""
    result = await run_pipeline("live_projection", options={"force": True})
    ok = result.status in (ApiStatus.SUCCESS, ApiStatus.SUCCESS.value)
    rebuilt = ok and bool(result.records_processed)
    if not rebuilt:
        log.warning("live_projection_rebuild_skipped", player_id=player_id, message=result.message, error=result.error)
    return NoteSaved(player_id=player_id, rebuilt=rebuilt, pipeline=result)


@router.put("/{player_id}/note", response_model=NoteSavedResponse)
async def save_note(body: NoteSave, player_id: int = _PLAYER_ID) -> NoteSavedResponse:
    """
    Make the body the player's live season note. His previous note is kept and
    marked superseded; his preseason adjustment is not touched. Then rebuild
    the day's live projection.

    The note is read again on its review date (two weeks on when the body does
    not say): past that day it is no longer applied.
    """
    season = settings.nba_season
    today = notes.today_et()
    if body.depends_on_player_id == player_id:
        raise HTTPException(status_code=422, detail="A note cannot depend on the player it is about")
    if body.review_date is not None and body.review_date < today:
        raise HTTPException(status_code=422, detail="The review date has already passed")

    def write():
        if not editor.player_exists(player_id):
            return "player", None
        if body.depends_on_player_id is not None and not editor.player_exists(body.depends_on_player_id):
            return "teammate", None
        evidence, missing = notes.evidence_for(body.evidence_news_ids)
        if missing:
            return "evidence", missing
        record = ProjectionAdjustment.record(player_id, season, **notes.note_fields(body, today, evidence))
        return "saved", record

    outcome, value = await run_in_db_thread(write)
    if outcome == "player":
        raise HTTPException(status_code=404, detail=f"No player {player_id}")
    if outcome == "teammate":
        raise HTTPException(status_code=422, detail=f"No player {body.depends_on_player_id} to depend on")
    if outcome == "evidence":
        raise HTTPException(status_code=422, detail=f"No stored news item {', '.join(str(i) for i in value)}")
    record = value
    log.info("projection_note_saved", player_id=player_id, adjustment_id=record.id, kind=body.kind,
             author=body.author, review_date=str(record.review_date))

    saved = await _rebuild_live(player_id)
    history = await run_in_db_thread(editor.adjustment_history, player_id, season)
    saved.note = next((entry for entry in history if entry.id == record.id), None)
    return NoteSavedResponse(
        status="success",
        message="Note saved and the live projection rebuilt" if saved.rebuilt
        else "Note saved; the live projection reads it on its next run",
        data=saved,
    )


@router.delete("/{player_id}/note", response_model=NoteSavedResponse)
async def retire_note(player_id: int = _PLAYER_ID) -> NoteSavedResponse:
    """
    Withdraw the player's live season note without a replacement (the row is
    kept, marked retired), then rebuild the day's live projection.
    """
    season = settings.nba_season
    retired = await run_in_db_thread(ProjectionAdjustment.retire, player_id, season, SCOPE_SEASON)
    if not retired:
        raise HTTPException(status_code=404, detail=f"Player {player_id} has no live season note for {season}")
    log.info("projection_note_retired", player_id=player_id)

    saved = await _rebuild_live(player_id)
    return NoteSavedResponse(
        status="success",
        message="Note retired and the live projection rebuilt" if saved.rebuilt
        else "Note retired; the live projection drops it on its next run",
        data=saved,
    )


@router.get("/{player_id}/news", response_model=PlayerNewsResponse)
async def get_player_news(player_id: int = _PLAYER_ID) -> PlayerNewsResponse:
    """The player's stored news of the last three weeks, newest first: the
    items a season note can cite as its evidence."""
    items = await run_in_db_thread(notes.recent_news, player_id)
    return PlayerNewsResponse(
        status="success",
        message=f"{len(items)} item(s)",
        data=PlayerNews(player_id=player_id, items=items),
    )


# ---- the analyst, in shadow ----------------------------------------------------------------


@router.get("/analyst", response_model=AnalystDigestResponse)
async def get_analyst_digest(
    day: Optional[date] = Query(None, description="The morning to show. Omit for the newest with reads."),
) -> AnalystDigestResponse:
    """
    The analyst's morning: every read with why the screen picked the player,
    what the read came to, the note it left with the stored items it rests on,
    and what it says against a note written by hand. Beside each, the player
    on the live projection and boards and on the second ones, which have the
    analyst's notes applied and which nothing else reads. `moved` is the
    widest moves between the two points boards that morning, and `spend` the
    month's bill for reads from the backend's ledger.

    In shadow this is the only place the analyst's work shows. To act on a
    read, save a note of your own from it: a note written by hand is the one
    applied wherever a player has both.
    """
    season = settings.nba_season
    month = await asyncio.to_thread(analyst_client_from_settings(settings).month)
    digest = await run_in_db_thread(analyst_digest.build, season, day, month, notes.today_et())
    reads = len(digest.reads)
    return AnalystDigestResponse(
        status="success",
        message=f"{reads} read(s) on {digest.as_of.isoformat()}" if digest.as_of else "The analyst has not run yet",
        data=digest,
    )
