"""
AI Spend

The API behind the dashboard's /ai-spend page: what Dime's chat, its check-ins and
the live rankings' analyst have spent, and the monthly caps they run under, which
the page can change. The backend owns all of it (the spend ledger, the caps and
every model call); these routes ask it with the shared pipeline token and pass its
answer through. Everything takes the pipeline bearer token.

Routes:
    GET /v1/dashboard/ai-spend       — this month against each cap, and the last N UTC days
    PUT /v1/dashboard/ai-spend/caps  — set or clear one line's cap, the whole app's, or one user's on a line
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Query, Security
from pydantic import ValidationError

from core.logging import get_correlation_id
from core.pipeline_auth import verify_pipeline_token
from core.settings import settings
from schemas.ai_spend import (
    AiCapData,
    AiCapRequest,
    AiCapResponse,
    AiSpendReport,
    AiSpendResponse,
)
from services.ai_spend_client import AiSpendClient, BackendRefused, BackendUnavailable

router = APIRouter(
    prefix="/dashboard/ai-spend",
    tags=["ai spend"],
    dependencies=[Security(verify_pipeline_token)],
)

MAX_DAYS = 90


def _client() -> AiSpendClient:
    return AiSpendClient(settings.backend_internal_url, settings.pipeline_api_token.get_secret_value())


def _unavailable(exc: BackendUnavailable) -> HTTPException:
    return HTTPException(status_code=502, detail=f"The backend could not be asked ({exc.reason})")


def _malformed() -> HTTPException:
    # A 200 whose data is not the report (or the cap) is the backend's fault, not
    # this server's: a 502, like any other answer that can't be read
    return _unavailable(BackendUnavailable("malformed_response"))


@router.get("", response_model=AiSpendResponse)
async def get_ai_spend(
    days: int = Query(30, ge=1, le=MAX_DAYS, description="How many UTC days back, today included"),
) -> AiSpendResponse:
    """
    This month's spend on each line (Dime's desk, its check-ins, the analyst)
    against its cap and against the whole app's, the ledger over the window by day, feature, user and team,
    what each call's record says it used (model and tokens), and every user with
    spend or a cap.
    """
    try:
        data = await asyncio.to_thread(_client().report, days, get_correlation_id())
    except BackendUnavailable as exc:
        raise _unavailable(exc) from exc
    except BackendRefused as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    try:
        report = AiSpendReport.model_validate(data)
    except ValidationError as exc:
        raise _malformed() from exc
    return AiSpendResponse(status="success", message=f"AI spend over {days} days", data=report)


@router.put("/caps", response_model=AiCapResponse)
async def put_ai_cap(req: AiCapRequest) -> AiCapResponse:
    """
    Set a line's monthly cap, the whole app's (`line` app), or one user's on the desk
    or autopilot line, from the next turn, check-in or read on. `monthly_usd` null clears it, and the backend's
    default applies again. `author` is the name typed on the page.
    """
    body = {
        "line": req.line,
        "user_id": req.user_id,
        "monthly_usd": None if req.monthly_usd is None else str(req.monthly_usd),
        "author": (req.author or "").strip() or None,
    }
    try:
        data = await asyncio.to_thread(_client().set_cap, body, get_correlation_id())
    except BackendUnavailable as exc:
        raise _unavailable(exc) from exc
    except BackendRefused as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc
    try:
        cap = AiCapData.model_validate(data)
    except ValidationError as exc:
        # The cap may well be saved: the page re-reads the report either way
        raise _malformed() from exc
    what = ("whole app's cap" if req.line == "app" else f"{req.line} cap") + (
        f" for user {req.user_id}" if req.user_id is not None else "")
    message = f"Cleared the {what}" if req.monthly_usd is None else f"Set the {what} to ${req.monthly_usd}"
    return AiCapResponse(status="success", message=message, data=cap)
