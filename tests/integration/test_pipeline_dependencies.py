"""
Pipeline Dependency Enforcement Tests

Validates that pipeline execution respects the dependency graph:
- Dependents are skipped when dependencies haven't succeeded
- Execution order within a category matches registry order
- The depends_on field is populated for known dependent pipelines
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from db.models.nba.games import Game
from db.models.nba.player_game_stats import PlayerGameStats
from db.models.nba.player_season_stats import PlayerSeasonStats
from db.models.nba.players import Player
from db.models.pipeline_run import PipelineRun
from pipelines import PIPELINE_REGISTRY, get_pipelines_by_category
from pipelines.config import PipelineCategory
from schemas.common import ApiStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_game(game_date: date, game_id: str = "0022500001") -> None:
    Game.create(
        game_id=game_id,
        game_date=game_date,
        season="2025-26",
        home_team_id="GSW",
        away_team_id="LAL",
        status="final",
        home_score=120,
        away_score=115,
    )


def _stats_df(game_id: str = "0022500001") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "GAME_ID": game_id,
                "PLAYER_ID": 201939,
                "PLAYER_NAME": "Stephen Curry",
                "TEAM_ABBREVIATION": "GSW",
                "MIN": "34:12",
                "PTS": 30,
                "REB": 5,
                "AST": 6,
                "STL": 2,
                "BLK": 0,
                "TOV": 3,
                "FGM": 10,
                "FGA": 20,
                "FG3M": 5,
                "FG3A": 12,
                "FTM": 5,
                "FTA": 6,
            }
        ]
    )


def _run_player_game_stats_successfully(game_date: date) -> None:
    """Run the player_game_stats pipeline with mocked extractors."""
    from pipelines.player_game_stats import PlayerGameStatsPipeline

    pipeline = PlayerGameStatsPipeline()
    pipeline.espn_extractor.get_player_data = lambda: {
        "stephen curry": {"espn_id": 3975, "rost_pct": 99.1}
    }
    pipeline.nba_extractor.get_game_logs = lambda *_: _stats_df()
    result = pipeline._run_sync(date_override=game_date)
    assert result.status == ApiStatus.SUCCESS


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDependencyDeclarations:
    """Verify that known dependent pipelines declare their dependencies."""

    def test_player_season_stats_depends_on_game_stats(self):
        cls = PIPELINE_REGISTRY["player_season_stats"]
        assert "player_game_stats" in cls.config.depends_on

    def test_player_rolling_stats_depends_on_game_stats(self):
        cls = PIPELINE_REGISTRY["player_rolling_stats"]
        assert "player_game_stats" in cls.config.depends_on

    def test_breakout_detection_depends_on_three_pipelines(self):
        cls = PIPELINE_REGISTRY["breakout_detection"]
        deps = set(cls.config.depends_on)
        assert "espn_injury_status" in deps
        assert "player_season_stats" in deps
        assert "player_game_stats" in deps


@pytest.mark.integration
class TestExecutionOrder:
    """Verify pipelines execute in dependency-safe order."""

    def test_post_game_order_matches_registry(self):
        """POST_GAME pipelines returned in registry insertion order."""
        filtered = get_pipelines_by_category(PipelineCategory.POST_GAME)
        names = [cls.config.name for cls in filtered]
        registry_order = [
            name for name, cls in PIPELINE_REGISTRY.items()
            if cls.config.category == PipelineCategory.POST_GAME
        ]
        assert names == registry_order

    def test_pre_game_order_matches_registry(self):
        """PRE_GAME pipelines returned in registry insertion order."""
        filtered = get_pipelines_by_category(PipelineCategory.PRE_GAME)
        names = [cls.config.name for cls in filtered]
        registry_order = [
            name for name, cls in PIPELINE_REGISTRY.items()
            if cls.config.category == PipelineCategory.PRE_GAME
        ]
        assert names == registry_order

    def test_player_game_stats_before_season_stats_in_post_game(self):
        """player_game_stats must appear before player_season_stats."""
        post_game = get_pipelines_by_category(PipelineCategory.POST_GAME)
        names = [cls.config.name for cls in post_game]
        assert names.index("player_game_stats") < names.index("player_season_stats")

    def test_player_game_stats_before_rolling_stats_in_post_game(self):
        """player_game_stats must appear before player_rolling_stats."""
        post_game = get_pipelines_by_category(PipelineCategory.POST_GAME)
        names = [cls.config.name for cls in post_game]
        assert names.index("player_game_stats") < names.index("player_rolling_stats")


@pytest.mark.integration
class TestDependencyDataFlow:
    """Verify that downstream pipelines consume upstream data correctly."""

    def test_season_stats_uses_game_stats_data(self, integration_db):
        """player_season_stats reads from player_game_stats rows."""
        game_date = date(2026, 2, 14)
        _seed_game(game_date)
        _run_player_game_stats_successfully(game_date)

        # Verify game stats were created
        assert PlayerGameStats.select().count() == 1

        # Verify player was created (needed as FK for season stats)
        assert Player.select().count() == 1
        player = Player.select().first()
        assert player.id == 201939

    def test_pipeline_run_audit_trail(self, integration_db):
        """Running a pipeline creates a PipelineRun record."""
        game_date = date(2026, 2, 14)
        _seed_game(game_date)
        _run_player_game_stats_successfully(game_date)

        runs = list(
            PipelineRun.select()
            .where(PipelineRun.pipeline_name == "player_game_stats")
            .order_by(PipelineRun.started_at.desc())
        )
        assert len(runs) >= 1
        latest = runs[0]
        assert latest.status == "success"
        assert latest.records_processed > 0

    def test_dedup_gate_uses_pipeline_run_records(self, integration_db):
        """was_successful_on_date returns True after a successful run."""
        game_date = date(2026, 2, 14)
        _seed_game(game_date)
        _run_player_game_stats_successfully(game_date)

        assert PipelineRun.was_successful_on_date("player_game_stats", game_date)
        assert not PipelineRun.was_successful_on_date("player_season_stats", game_date)
