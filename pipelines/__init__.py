"""
Pipeline Registry and Exports

Provides a unified registry of all available pipelines and helper functions
for running them by name or category.
"""

from datetime import date
from typing import Optional, Type

from core.logging import get_logger
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.player_game_stats import PlayerGameStatsPipeline
from pipelines.player_season_stats import PlayerSeasonStatsPipeline
from pipelines.player_advanced_stats import PlayerAdvancedStatsPipeline
from pipelines.player_ownership import PlayerOwnershipPipeline
from pipelines.player_rolling_stats import PlayerRollingStatsPipeline
from pipelines.team_stats import TeamStatsPipeline
from pipelines.game_schedule import GameSchedulePipeline
from pipelines.espn_injury_status import ESPNInjuryStatusPipeline
from pipelines.breakout_detection import BreakoutDetectionPipeline
from pipelines.lineup_alerts import LineupAlertsPipeline
from pipelines.live_game_stats import LiveGameStatsPipeline
from pipelines.player_profiles import PlayerProfilesPipeline
from pipelines.game_start_times import GameStartTimesPipeline
from pipelines.daily_matchup_scores import DailyMatchupScoresPipeline
from pipelines.playoff_bracket import PlayoffBracketPipeline
from schemas.pipeline import PipelineResult
from schemas.common import ApiStatus


# Unified registry of all pipelines.
# Registration order is dependency-safe: dependencies listed before dependents.
# Category-based endpoints use get_pipelines_by_category() to filter this list.
PIPELINE_REGISTRY: dict[str, Type[BasePipeline]] = {
    # POST_GAME — run after games complete
    "player_game_stats": PlayerGameStatsPipeline,
    "player_ownership": PlayerOwnershipPipeline,
    "player_season_stats": PlayerSeasonStatsPipeline,
    "player_rolling_stats": PlayerRollingStatsPipeline,
    "player_advanced_stats": PlayerAdvancedStatsPipeline,
    "team_stats": TeamStatsPipeline,
    "game_schedule": GameSchedulePipeline,
    "daily_matchup_scores": DailyMatchupScoresPipeline,
    # PRE_GAME — run before games start (timed relative to first tip-off)
    "espn_injury_status": ESPNInjuryStatusPipeline,
    "breakout_detection": BreakoutDetectionPipeline,
    "lineup_alerts": LineupAlertsPipeline,
    # LIVE — run every ~60s during active game windows
    "live_game_stats": LiveGameStatsPipeline,
    # SCHEDULED — hardcoded Railway cron; each has its own service trigger
    "player_profiles": PlayerProfilesPipeline,
    "game_start_times": GameStartTimesPipeline,
    "playoff_bracket": PlayoffBracketPipeline,
}


def get_pipelines_by_category(category: PipelineCategory) -> list[Type[BasePipeline]]:
    """Return all pipelines matching a category, in dependency-safe registry order."""
    return [cls for cls in PIPELINE_REGISTRY.values() if cls.config.category == category]


def get_pipeline(name: str) -> BasePipeline:
    """
    Get a pipeline instance by name.

    Args:
        name: Pipeline name (e.g., "player_game_stats")

    Returns:
        Instantiated pipeline

    Raises:
        KeyError: If pipeline name not found
    """
    if name not in PIPELINE_REGISTRY:
        available = ", ".join(PIPELINE_REGISTRY.keys())
        raise KeyError(f"Unknown pipeline '{name}'. Available: {available}")

    return PIPELINE_REGISTRY[name]()


async def run_pipeline(
    name: str,
    date_override: Optional[date] = None,
    options: Optional[dict] = None,
    nba_date: Optional[date] = None,
) -> PipelineResult:
    """
    Run a pipeline by name.

    Args:
        name: Pipeline name
        date_override: If provided, the pipeline uses this date instead of
                       computing from the current time. Useful for backfills.
        options: Per-run options exposed to the pipeline as ctx.options.
        nba_date: The triggering batch's NBA game date (not a backfill marker).

    Returns:
        PipelineResult with status and details
    """
    pipeline = get_pipeline(name)
    return await pipeline.run(
        date_override=date_override, options=options, nba_date=nba_date
    )


async def run_all_pipelines(date_override: Optional[date] = None) -> dict[str, PipelineResult]:
    """
    Run all non-SCHEDULED pipelines in sequence.

    Intended for backfills and manual use — not part of the regular production
    schedule (which uses category-specific endpoints instead).

    Args:
        date_override: If provided, all pipelines use this date instead of
                       computing from the current time. Useful for backfills.

    Returns:
        Dict mapping pipeline name to PipelineResult
    """
    log = get_logger("pipeline").bind(operation="run_all")

    pipeline_names = [
        name for name, cls in PIPELINE_REGISTRY.items()
        if cls.config.category != PipelineCategory.SCHEDULED
    ]

    log.info("all_pipelines_started", count=len(pipeline_names))

    results = {}
    for i, name in enumerate(pipeline_names, 1):
        log.info("running_pipeline", pipeline=name, step=f"{i}/{len(pipeline_names)}")
        results[name] = await run_pipeline(name, date_override=date_override)

    success_count = sum(1 for r in results.values() if r.status == ApiStatus.SUCCESS)
    log.info(
        "all_pipelines_completed",
        success_count=success_count,
        total_count=len(results),
    )

    return results


def list_pipelines() -> list[dict]:
    """
    List all available pipelines with their configurations.

    Returns:
        List of pipeline info dicts
    """
    return [cls.get_info() for cls in PIPELINE_REGISTRY.values()]


__all__ = [
    # Base classes
    "BasePipeline",
    "PipelineConfig",
    "PipelineCategory",
    "PipelineContext",
    # Pipelines
    "PlayerGameStatsPipeline",
    "PlayerSeasonStatsPipeline",
    "PlayerAdvancedStatsPipeline",
    "PlayerOwnershipPipeline",
    "PlayerRollingStatsPipeline",
    "TeamStatsPipeline",
    "GameSchedulePipeline",
    "ESPNInjuryStatusPipeline",
    "BreakoutDetectionPipeline",
    "LineupAlertsPipeline",
    "LiveGameStatsPipeline",
    "PlayerProfilesPipeline",
    "GameStartTimesPipeline",
    "DailyMatchupScoresPipeline",
    "PlayoffBracketPipeline",
    # Registry
    "PIPELINE_REGISTRY",
    "get_pipelines_by_category",
    "get_pipeline",
    "run_pipeline",
    "run_all_pipelines",
    "list_pipelines",
]
