"""
Pipeline Configuration

Immutable configuration dataclass for pipeline metadata.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class PipelineCategory(str, Enum):
    """Which trigger group a pipeline belongs to.

    PRE_GAME  — runs before today's games (POST /pipelines/pre-game)
    LIVE      — runs every ~60s during active games (POST /pipelines/live-stats)
    POST_GAME — runs after last night's games complete (POST /pipelines/post-game)
    SCHEDULED — runs on a fixed, hardcoded cron schedule (own Railway service)
    """

    PRE_GAME = "pre_game"
    LIVE = "live"
    POST_GAME = "post_game"
    SCHEDULED = "scheduled"


@dataclass(frozen=True)
class PipelineConfig:
    """
    Immutable configuration for a pipeline.

    Attributes:
        name: Internal name used for tracking (e.g., "daily_player_stats")
        display_name: Human-readable name (e.g., "Daily Player Stats")
        description: What this pipeline does
        target_table: Primary table this pipeline writes to
        category: Which trigger group this pipeline belongs to
        pre_game_window_minutes: For PRE_GAME pipelines — how many minutes before
            first tip-off this pipeline becomes eligible. None falls back to
            settings.pre_game_window_minutes (global default: 150 min).
        max_retries: Override retry count (None uses settings default)
        retry_base_delay: Override base delay (None uses settings default)
        retry_max_delay: Override max delay (None uses settings default)
        timeout_seconds: Maximum time for pipeline execution
        allow_concurrent: Whether multiple instances can run simultaneously
        depends_on: Pipeline names that must complete first
    """

    name: str
    display_name: str
    description: str
    target_table: str
    category: PipelineCategory

    # Pre-game window override (only meaningful for PRE_GAME pipelines).
    # None falls back to settings.pre_game_window_minutes.
    pre_game_window_minutes: Optional[int] = None

    # Retry configuration (override defaults from settings)
    max_retries: Optional[int] = None
    retry_base_delay: Optional[float] = None
    retry_max_delay: Optional[float] = None

    # Execution constraints
    timeout_seconds: int = 600
    allow_concurrent: bool = False

    # Dependencies (other pipeline names that must complete first)
    depends_on: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self):
        """Validate configuration."""
        if not self.name:
            raise ValueError("Pipeline name is required")
        if not self.target_table:
            raise ValueError("Pipeline target_table is required")
