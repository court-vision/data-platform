"""
AI spend page schemas: what Dime and the analyst spend, and the caps they run under.

The backend owns the spend ledger, the caps and every model call; this platform
only shows them. `GET /v1/dashboard/ai-spend` passes the backend's report through
(backend/services/ai/spend_report.py, `GET /v1/internal/jobs/ai/spend`) and
`PUT /v1/dashboard/ai-spend/caps` passes a cap change to it. The shapes here
match the backend's (backend/schemas/ai_spend.py); money is in millionths of a
dollar (micros) except a cap's `monthly_usd`, the dollars typed on the page.
Days are UTC, the calendar the monthly caps reset on.

    line     desk (Dime's chat, its draft room and replies in a check-in),
             autopilot (the check-ins Dime starts itself), analyst (the live
             rankings' morning reads)
    feature  chat | draft | checkin_reply | checkin | analyst | unlinked (desk
             spend whose chat was deleted, or whose turn was never written)
    app      the whole app's cap, over every line together (backend migration
             0056): a cap only, never a ledger row's line

`GET /v1/dashboard/ai-spend/bill` passes the backend's bill check through
(backend/services/ai/bill.py, `GET /v1/internal/jobs/ai/bill`): what Anthropic bills
the organization (its Usage and Cost Analytics API, read by the backend with its
Admin API key) beside the ledger, by UTC day and by API key.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import Field

from schemas.common import ApiModel

AiLine = Literal["desk", "autopilot", "analyst"]
AiCapLine = Literal["desk", "autopilot", "analyst", "app"]
AiFeature = Literal["chat", "draft", "checkin_reply", "checkin", "analyst", "unlinked"]


class AiCapView(ApiModel):
    """One cap as it stands."""

    cap_micros: Optional[int] = Field(None, description="In force; null: no cap of the user's own on this line")
    default_cap_micros: Optional[int] = Field(None, description="What applies with no cap set; null: none")
    cap_set: bool = Field(False, description="Set from this page")
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None


class AiSpendLine(AiCapView):
    """One line's month so far."""

    line: AiLine
    spent_micros: int = Field(..., description="This month, settled")
    open_micros: int = Field(..., description="This month, reserved and not settled: running now, or died "
                                              "mid-call and counted at its ceiling")


class AiSpendApp(AiCapView):
    """The whole app's month so far against its cap: every line together."""

    spent_micros: int = Field(..., description="This month on every line, settled")
    open_micros: int = Field(..., description="This month on every line, reserved and not settled")


class AiSpendRow(ApiModel):
    """The ledger over the window, per UTC day, line, feature, user and team."""

    day: date
    line: AiLine
    feature: AiFeature
    detail: Optional[str] = Field(None, description="A check-in's kind: morning, weekly, pre_lock, night, event")
    user_id: Optional[int] = None
    team_id: Optional[int] = None
    cost_micros: int = Field(..., description="Settled and open together, as the caps count it")
    open_micros: int
    entries: int


class AiUsageRow(ApiModel):
    """What each call's record says it used, per UTC day, feature and model."""

    day: date
    feature: AiFeature
    model: str
    records: int = Field(..., description="Turns, or analyst reads")
    model_calls: int
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int
    cost_micros: int


class AiUserLine(AiCapView):
    spent_micros: int = Field(..., description="This month, settled and open")


class AiSpendUser(ApiModel):
    user_id: int
    email: str
    allowed: bool = Field(..., description="On the AI desk's allowlist (AI_AGENT_USER_IDS)")
    desk: AiUserLine
    autopilot: AiUserLine


class AiSpendTeam(ApiModel):
    team_id: int
    user_id: int
    name: str


class AiSpendReport(ApiModel):
    as_of: datetime
    month_start: date
    since: date = Field(..., description="First UTC day of the window")
    days: int
    turn_max_micros: int = Field(..., description="What one desk turn or check-in reserves before it starts")
    read_max_micros: int = Field(..., description="What one analyst read reserves before it starts")
    lines: list[AiSpendLine]
    app: Optional[AiSpendApp] = Field(None, description="The whole app's cap; null from a backend that has none")
    rows: list[AiSpendRow]
    usage: list[AiUsageRow]
    users: list[AiSpendUser]
    teams: list[AiSpendTeam]


class AiSpendResponse(ApiModel):
    """Response for GET /v1/dashboard/ai-spend."""

    status: str
    message: str
    data: AiSpendReport


class AiCapRequest(ApiModel):
    """Set a line's monthly cap (no `user_id`), the whole app's (`line` app), or one
    user's on the desk or autopilot line. `monthly_usd` null clears it: the backend's
    default applies again."""

    line: AiCapLine
    user_id: Optional[int] = Field(None, ge=1)
    monthly_usd: Optional[Decimal] = Field(None, ge=0, le=1000, max_digits=6, decimal_places=2)
    author: Optional[str] = Field(None, max_length=80)


class AiCapData(AiCapView):
    line: AiCapLine
    user_id: Optional[int] = None


class AiCapResponse(ApiModel):
    """Response for PUT /v1/dashboard/ai-spend/caps."""

    status: str
    message: str
    data: AiCapData


# ---- The bill check ----


class AiBillMonth(ApiModel):
    """One calendar month (UTC): what Anthropic bills against what the ledger priced."""

    month: date = Field(..., description="Its first day")
    billed_micros: int = Field(..., description="Anthropic's cost for the whole organization, before credits")
    ours_micros: Optional[int] = Field(None, description="Of it, on the backend's own API key; null: not found")
    ledger_micros: int = Field(..., description="The ledger's settled spend")
    open_micros: int = Field(0, description="The ledger's reservations still open")


class AiBillDay(ApiModel):
    day: date
    billed_micros: int
    ours_micros: Optional[int] = None
    ledger_micros: int
    open_micros: int = 0


class AiBillKey(ApiModel):
    api_key_id: Optional[str] = Field(None, description="Null: usage made without a key (the Console)")
    name: Optional[str] = None
    hint: Optional[str] = Field(None, description="Anthropic's partial hint of the key")
    ours: bool = Field(False, description="The key the backend sends its model calls with")
    billed_micros: int


class AiBill(ApiModel):
    state: str = Field(..., description="ok | no_key | refused | unavailable | unreachable")
    message: Optional[str] = Field(None, description="Why the check couldn't run")
    read_at: datetime = Field(..., description="When the backend read Anthropic")
    refreshed_at: Optional[datetime] = Field(None, description="Anthropic's numbers are complete up to here")
    months: list[AiBillMonth] = Field(default_factory=list, description="Last month, then this month")
    days: list[AiBillDay] = Field(default_factory=list, description="Every UTC day of both months to today")
    keys: list[AiBillKey] = Field(default_factory=list, description="This month, largest first")


class AiBillResponse(ApiModel):
    """Response for GET /v1/dashboard/ai-spend/bill."""

    status: str
    message: str
    data: AiBill
