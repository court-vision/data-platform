"""
`SnapshotStore` against Postgres: a day's write is one transaction, a replace
reports drift against the old rows, and the children go with the header.
"""

from datetime import date

import pytest

from db.models.lineup_snapshots import LineupSnapshot, LineupSnapshotPlayer
from pipelines.lineup_snapshots import LeagueKey, SnapshotStore, TeamDayRecord
from pipelines.transformers.lineup_snapshots import SnapshotPlayer, TeamLineup

KEY = LeagueKey(provider_league_id="993431466", season=2027)
DAY = date(2026, 12, 20)


def player(pid, slot, name="P"):
    return SnapshotPlayer(player_id=pid, name=f"{name}{pid}", pro_team="BOS", default_position_id=1,
                          lineup_slot_id=slot, eligible_slot_ids=(0, 5, 11, 12), injured=False,
                          injury_status="ACTIVE", applied_total=10.5)


def record(team_id, players, opponent=None):
    return TeamDayRecord(
        lineup=TeamLineup(provider_team_id=team_id, team_name=f"Team {team_id}", applied_stat_total=42.0,
                          players=tuple(players)),
        matchup_period_id=9, opponent_provider_team_id=opponent,
    )


@pytest.mark.integration
def test_write_then_replace_reports_drift_and_leaves_no_orphans(integration_db):
    store = SnapshotStore()
    first = store.write_day(KEY, 60, DAY, [record(1, [player(1, 0), player(2, 12)], opponent=2),
                                           record(2, [player(3, 11)], opponent=1)],
                            replace=False, source="pipeline", pipeline_run_id=None)
    assert first.stored == 2 and first.drift == []
    assert store.newest_period(KEY) == 60
    assert LineupSnapshotPlayer.select().count() == 3

    # a second nightly write of the same day is a no-op
    again = store.write_day(KEY, 60, DAY, [record(1, [player(1, 0)])], replace=False, source="pipeline",
                            pipeline_run_id=None)
    assert again.stored == 0 and LineupSnapshotPlayer.select().count() == 3

    # a backfill replaces the day: player 2 dropped, player 1 moved to the bench
    replaced = store.write_day(KEY, 60, DAY, [record(1, [player(1, 12)])], replace=True, source="backfill",
                               pipeline_run_id=None)
    assert replaced.stored == 1
    assert replaced.drift == [{"provider_team_id": 1, "scoring_period_id": 60, "players_before": 2,
                               "players_after": 1, "changed": 3}]
    header = LineupSnapshot.get(LineupSnapshot.provider_team_id == 1)
    assert header.source == "backfill" and header.player_count == 1 and header.nba_date == DAY
    assert [(p.player_id, p.lineup_slot_id, p.eligible_slot_ids) for p in header.players] == [(1, 12, [0, 5, 11, 12])]
    assert LineupSnapshotPlayer.select().count() == 2          # team 1's new row + team 2's untouched row
    assert LineupSnapshot.select().count() == 2


@pytest.mark.integration
def test_newest_period_is_per_league(integration_db):
    store = SnapshotStore()
    other = LeagueKey(provider_league_id="1", season=2027)
    store.write_day(KEY, 5, DAY, [record(1, [player(1, 0)])], replace=False, source="pipeline", pipeline_run_id=None)
    store.write_day(other, 9, DAY, [record(1, [player(1, 0)])], replace=False, source="pipeline", pipeline_run_id=None)
    assert store.newest_period(KEY) == 5 and store.newest_period(other) == 9
    assert store.newest_period(LeagueKey(provider_league_id="1", season=2026)) is None
