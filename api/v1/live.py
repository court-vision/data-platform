"""
Live Game Scheduling - Cron-Runner Read Endpoint

Read-only endpoint for the cron-runner's live polling loop to determine
when to wake up and start polling for live game stats.

No authentication required (data is non-sensitive game schedule info).
"""

from datetime import datetime, timedelta, date

import pytz
from fastapi import APIRouter

from core.logging import get_logger

router = APIRouter(prefix="/live", tags=["Live"])
log = get_logger("live_api")


def _get_nba_date() -> date:
    """Return today's NBA game date in ET (before 6am = yesterday)."""
    eastern = pytz.timezone("US/Eastern")
    now_et = datetime.now(eastern)
    if now_et.hour < 6:
        return (now_et - timedelta(days=1)).date()
    return now_et.date()


@router.get("/schedule/today")
async def get_today_schedule() -> dict:
    """
    Get game scheduling info for today's live polling.

    Returns the first tip-off time so the cron-runner's live loop can
    sleep until 30 minutes before the first game rather than relying on
    a hardcoded cron start time. Reads from the precomputed game schedule
    in the DB (populated by GameStartTimesPipeline).
    """
    game_date = _get_nba_date()
    log.debug("schedule_today_request", game_date=str(game_date))

    from db.models.nba.games import Game

    games = Game.get_games_on_date(game_date)
    if not games:
        return {
            "status": "success",
            "message": f"No games scheduled for {game_date}",
            "data": {
                "has_games": False,
                "game_date": str(game_date),
                "first_game_et": None,
                "wake_at_et": None,
            },
        }

    earliest_time = Game.get_earliest_game_time_on_date(game_date)
    if not earliest_time:
        # Games exist but start times aren't loaded — start immediately
        return {
            "status": "success",
            "message": f"Games scheduled for {game_date} but start times unavailable",
            "data": {
                "has_games": True,
                "game_date": str(game_date),
                "first_game_et": None,
                "wake_at_et": None,
            },
        }

    eastern = pytz.timezone("US/Eastern")
    first_game_naive = datetime.combine(game_date, earliest_time)
    first_game_et = eastern.localize(first_game_naive)
    wake_at_et = first_game_et - timedelta(minutes=30)

    return {
        "status": "success",
        "message": f"First game at {earliest_time} ET on {game_date}",
        "data": {
            "has_games": True,
            "game_date": str(game_date),
            "first_game_et": first_game_et.isoformat(),
            "wake_at_et": wake_at_et.isoformat(),
        },
    }
