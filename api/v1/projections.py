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

A save writes one row to nba.projection_adjustments (append-only: it
supersedes the live row) and runs cv-projection, so the snapshot the draft
board reads is the one the page showed in its preview.
"""

from __future__ import annotations

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Security

from core.logging import get_logger
from core.pipeline_auth import verify_pipeline_token
from core.settings import settings
from db.base import run_in_db_thread
from db.models.nba import ProjectionAdjustment
from pipelines import run_pipeline
from schemas.common import ApiStatus
from schemas.projections import (
    AdjustmentHistory,
    AdjustmentHistoryResponse,
    AdjustmentSave,
    AdjustmentSaved,
    AdjustmentSavedResponse,
    PreviewRequest,
    PreviewResponse,
    ProjectionsResponse,
)
from services import projection_editor as editor
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
