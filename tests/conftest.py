import os
from datetime import date, time

import pytest
from freezegun import freeze_time


# Ensure required env vars exist before importing app modules.
os.environ.setdefault("DATABASE_URL", "postgresql://cv:cv@localhost:5432/cv_test")
os.environ.setdefault("PIPELINE_API_TOKEN", "test-token")
# Deterministic season for the suite (settings would derive it from today's date)
os.environ.setdefault("NBA_SEASON", "2025-26")


# ---------------------------------------------------------------------------
# Time-freezing fixtures for testing self-gating logic
# ---------------------------------------------------------------------------


@pytest.fixture
def freeze_post_game_window():
    """
    Freeze time to a post-game window: 1:00 AM ET on March 5, 2026.
    NBA date = March 4 (before 6 AM ET).
    Latest game started at 10 PM ET → estimated end 12:30 AM → window open.
    """
    with freeze_time("2026-03-05T06:00:00Z"):  # 1:00 AM ET (UTC-5)
        yield date(2026, 3, 4)  # yields the NBA date


@pytest.fixture
def freeze_pre_game_window():
    """
    Freeze time to a pre-game window: 5:30 PM ET on March 4, 2026.
    NBA date = March 4 (after 6 AM ET).
    First game at 7:30 PM ET → 150 min window opens at 5:00 PM.
    """
    with freeze_time("2026-03-04T22:30:00Z"):  # 5:30 PM ET (UTC-5)
        yield date(2026, 3, 4)


@pytest.fixture
def freeze_live_game_time():
    """
    Freeze time to during games: 9:00 PM ET on March 4, 2026.
    NBA date = March 4. Games in progress.
    """
    with freeze_time("2026-03-05T02:00:00Z"):  # 9:00 PM ET (UTC-5)
        yield date(2026, 3, 4)


@pytest.fixture
def freeze_no_game_morning():
    """
    Freeze time to 10:00 AM ET on March 4, 2026.
    NBA date = March 4. Well before any game windows.
    """
    with freeze_time("2026-03-04T15:00:00Z"):  # 10:00 AM ET (UTC-5)
        yield date(2026, 3, 4)


# ---------------------------------------------------------------------------
# Game schedule data for testing
# ---------------------------------------------------------------------------


SAMPLE_GAME_SCHEDULES = {
    "no_games": [],
    "single_early_game": [
        {
            "game_id": "0022500100",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "GSW",
            "away_team_id": "LAL",
            "status": "scheduled",
            "start_time_et": time(19, 0),  # 7:00 PM
        },
    ],
    "typical_night": [
        {
            "game_id": "0022500101",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "GSW",
            "away_team_id": "LAL",
            "status": "scheduled",
            "start_time_et": time(19, 30),  # 7:30 PM
        },
        {
            "game_id": "0022500102",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "BOS",
            "away_team_id": "MIA",
            "status": "scheduled",
            "start_time_et": time(20, 0),  # 8:00 PM
        },
        {
            "game_id": "0022500103",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "DEN",
            "away_team_id": "PHX",
            "status": "scheduled",
            "start_time_et": time(22, 0),  # 10:00 PM
        },
    ],
    "games_completed": [
        {
            "game_id": "0022500101",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "GSW",
            "away_team_id": "LAL",
            "status": "final",
            "start_time_et": time(19, 30),
            "home_score": 120,
            "away_score": 115,
        },
        {
            "game_id": "0022500102",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "BOS",
            "away_team_id": "MIA",
            "status": "final",
            "start_time_et": time(20, 0),
            "home_score": 108,
            "away_score": 102,
        },
        {
            "game_id": "0022500103",
            "game_date": date(2026, 3, 4),
            "season": "2025-26",
            "home_team_id": "DEN",
            "away_team_id": "PHX",
            "status": "final",
            "start_time_et": time(22, 0),
            "home_score": 115,
            "away_score": 110,
        },
    ],
}

