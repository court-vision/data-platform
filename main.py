"""
Data Platform API Server

FastAPI server for triggering data pipeline tasks via HTTP requests.
Includes token-based authentication for security.

Usage:
    uvicorn main:app --host 0.0.0.0 --port 8001

Environment Variables:
    PIPELINE_API_TOKEN - Required secret token for authentication
    DATABASE_URL - PostgreSQL connection for stats_s2 schema
    BACKEND_DATABASE_URL - PostgreSQL connection for usr schema (matchup scores)
"""

import os
import traceback
from datetime import datetime
from functools import wraps
from typing import Callable

import pytz
from fastapi import FastAPI, HTTPException, Security, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from db.base import init_db

# Initialize FastAPI app
app = FastAPI(
    title="Data Platform API",
    description="API for triggering data pipeline tasks",
    version="1.0.0",
)

# Security scheme
security = HTTPBearer()

# Get API token from environment
API_TOKEN = os.getenv("PIPELINE_API_TOKEN")


def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:
    """Verify the bearer token matches our secret."""
    if not API_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Server misconfigured: PIPELINE_API_TOKEN not set",
        )

    if credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication token",
        )

    return credentials.credentials


# Response models
class PipelineResponse(BaseModel):
    status: str
    message: str
    started_at: str
    completed_at: str | None = None
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
    timestamp: str


# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database connection on server startup."""
    try:
        init_db()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint (no auth required)."""
    central_tz = pytz.timezone("US/Central")
    now = datetime.now(central_tz)
    return HealthResponse(status="healthy", timestamp=now.isoformat())


def run_pipeline(pipeline_func: Callable, pipeline_name: str) -> PipelineResponse:
    """Execute a pipeline function and return standardized response."""
    central_tz = pytz.timezone("US/Central")
    started_at = datetime.now(central_tz)

    try:
        pipeline_func()
        completed_at = datetime.now(central_tz)
        return PipelineResponse(
            status="success",
            message=f"{pipeline_name} completed successfully",
            started_at=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
        )
    except Exception as e:
        completed_at = datetime.now(central_tz)
        return PipelineResponse(
            status="error",
            message=f"{pipeline_name} failed",
            started_at=started_at.isoformat(),
            completed_at=completed_at.isoformat(),
            error=f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}",
        )


@app.post("/pipelines/daily-player-stats", response_model=PipelineResponse)
async def trigger_daily_player_stats(token: str = Security(verify_token)):
    """
    Trigger the daily player stats pipeline.

    Fetches yesterday's game stats from NBA API and ESPN ownership data,
    then inserts into daily_player_stats table.
    """
    from tasks.daily_player_stats import main as daily_player_stats_main

    return run_pipeline(daily_player_stats_main, "Daily Player Stats")


@app.post("/pipelines/cumulative-player-stats", response_model=PipelineResponse)
async def trigger_cumulative_player_stats(token: str = Security(verify_token)):
    """
    Trigger the cumulative player stats pipeline.

    Updates season totals and rankings for players who played yesterday.
    """
    from tasks.cumulative_player_stats import main as cumulative_player_stats_main

    return run_pipeline(cumulative_player_stats_main, "Cumulative Player Stats")


@app.post("/pipelines/daily-matchup-scores", response_model=PipelineResponse)
async def trigger_daily_matchup_scores(token: str = Security(verify_token)):
    """
    Trigger the daily matchup scores pipeline.

    Fetches current matchup scores for all saved teams and records
    daily snapshots for visualization.
    """
    from tasks.daily_matchup_scores import main as daily_matchup_scores_main

    return run_pipeline(daily_matchup_scores_main, "Daily Matchup Scores")


@app.post("/pipelines/all", response_model=dict)
async def trigger_all_pipelines(token: str = Security(verify_token)):
    """
    Trigger all pipelines in sequence.

    Runs: daily-player-stats -> cumulative-player-stats -> daily-matchup-scores
    """
    from tasks.daily_player_stats import main as daily_player_stats_main
    from tasks.cumulative_player_stats import main as cumulative_player_stats_main
    from tasks.daily_matchup_scores import main as daily_matchup_scores_main

    results = {}

    results["daily_player_stats"] = run_pipeline(
        daily_player_stats_main, "Daily Player Stats"
    )
    results["cumulative_player_stats"] = run_pipeline(
        cumulative_player_stats_main, "Cumulative Player Stats"
    )
    results["daily_matchup_scores"] = run_pipeline(
        daily_matchup_scores_main, "Daily Matchup Scores"
    )

    return results


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
