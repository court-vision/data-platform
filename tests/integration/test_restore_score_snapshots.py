"""scripts/restore_score_snapshots.py: opening night's saved score histories go back in once."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from db.models.nba.live_game_score_snapshots import LiveGameScoreSnapshot
from scripts.restore_score_snapshots import RESTORE_RUN_ID, load_rows, restore


@pytest.mark.integration
def test_the_saved_file_holds_each_opening_night_game_from_the_tip_to_the_final():
    rows = load_rows()

    assert {r["game_id"] for r in rows} == {"0022600001", "0022600002", "0022600003"}
    assert all(r["game_date"] == date(2026, 10, 20) and r["game_status"] in (2, 3) for r in rows)
    finals = {r["game_id"]: (r["away_score"], r["home_score"]) for r in rows if r["game_status"] == 3}
    assert finals == {"0022600001": (113, 121), "0022600002": (106, 100), "0022600003": (116, 104)}


@pytest.mark.integration
def test_a_dry_run_writes_nothing_and_a_second_apply_skips_what_is_there(integration_db):
    rows = load_rows()

    assert sum(restore(rows).values()) == len(rows)
    assert LiveGameScoreSnapshot.select().count() == 0

    assert sum(restore(rows, apply=True).values()) == len(rows)
    stored = LiveGameScoreSnapshot.select().where(LiveGameScoreSnapshot.pipeline_run_id == RESTORE_RUN_ID)
    assert stored.count() == len(rows)

    assert restore(rows, apply=True) == {"0022600001": 0, "0022600002": 0, "0022600003": 0}
    assert LiveGameScoreSnapshot.select().count() == len(rows)


@pytest.mark.integration
def test_a_game_the_pipeline_kept_is_left_alone(integration_db):
    LiveGameScoreSnapshot.create(
        game_id="0022600003", game_date=date(2026, 10, 20), home_team="SAS", away_team="OKC",
        home_score=104, away_score=116, period=4, game_status=3, captured_at=datetime(2026, 10, 21, 4, 46),
    )

    done = restore(load_rows(), apply=True)

    assert done["0022600003"] == 0 and done["0022600001"] > 0 and done["0022600002"] > 0
    assert LiveGameScoreSnapshot.select().where(LiveGameScoreSnapshot.game_id == "0022600003").count() == 1
