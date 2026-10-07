"""
`pipelines.transformers.lineup_snapshots` against trimmed captures of a public
ESPN league (993431466, season 2026): one `mTeam,mRoster&scoringPeriodId=60`
read (three teams kept) and one `mTeam,mSettings,mMatchup` read. Owner SWIDs
were stripped from both.
"""

import json
from pathlib import Path

import pytest

from pipelines.transformers.lineup_snapshots import (
    LeagueDiscovery,
    day_points,
    parse_league_day,
    parse_league_discovery,
    parse_team_lineup,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.fixture(scope="module")
def rosters_payload():
    return json.loads((FIXTURES / "espn_league_rosters_period.json").read_text())


@pytest.fixture(scope="module")
def discovery_payload():
    return json.loads((FIXTURES / "espn_league_discovery.json").read_text())


@pytest.mark.unit
class TestParseLeagueDay:
    def test_every_team_in_the_payload_with_its_day(self, rosters_payload):
        day = parse_league_day(rosters_payload)
        assert day.scoring_period_id == 60
        assert [(t.provider_team_id, t.team_name, len(t.players)) for t in day.teams] == [
            (1, "Flaggin it", 14),
            (2, "Steve's Smart Team", 13),
            (3, "Matthew's Monstrous Team", 14),
        ]
        # the day's points over the active slots, summed from the period's stat lines —
        # team 2 had two bench players score (38 + 56) that do not count
        assert [t.applied_stat_total for t in day.teams] == [145.0, 132.0, 163.0]

    def test_player_fields(self, rosters_payload):
        team = parse_league_day(rosters_payload).teams[0]
        cade = next(p for p in team.players if p.player_id == 4432166)
        assert cade.name == "Cade Cunningham"
        assert cade.pro_team == "DET"
        assert cade.lineup_slot_id == 11            # UT that day; he was PG on day 90
        assert cade.default_position_id == 1
        assert 0 in cade.eligible_slot_ids and 11 in cade.eligible_slot_ids
        assert cade.injured is False and cade.injury_status == "ACTIVE"   # the player object wins over the entry's NORMAL
        bam = next(p for p in team.players if p.player_id == 4066261)
        assert bam.lineup_slot_id == 3 and bam.applied_total == 29.0     # his day-60 line, not the 59.0 pool total
        assert cade.applied_total is None                                 # no game on day 60 (his line is day 59)
        assert {p.lineup_slot_id for p in team.players} == {0, 1, 2, 3, 4, 5, 6, 11, 12, 13}

    def test_pro_team_corrections_and_free_agents(self):
        team = parse_team_lineup({
            "id": 9, "name": "  Spaced  ",
            "roster": {"entries": [
                {"playerId": 1, "lineupSlotId": 12, "playerPoolEntry": {"player": {"id": 1, "fullName": "A", "proTeamId": 20}}},
                {"playerId": 2, "lineupSlotId": 13, "playerPoolEntry": {"player": {"id": 2, "fullName": "B", "proTeamId": 0}}},
                {"playerId": 3, "lineupSlotId": 11},                      # no player object: dropped
            ]},
        })
        assert team.team_name == "Spaced"
        assert [(p.player_id, p.pro_team, p.lineup_slot_id) for p in team.players] == [(1, "PHI", 12), (2, "FA", 13)]
        assert team.applied_stat_total is None

    def test_day_points_take_only_the_periods_actual_line(self):
        player = {"stats": [
            {"scoringPeriodId": 60, "statSplitTypeId": 5, "statSourceId": 1, "appliedTotal": 99.0},  # projected
            {"scoringPeriodId": 59, "statSplitTypeId": 5, "statSourceId": 0, "appliedTotal": 48.0},  # yesterday
            {"scoringPeriodId": 0, "statSplitTypeId": 0, "statSourceId": 0, "appliedTotal": 3226.0}, # season
            {"scoringPeriodId": 60, "statSplitTypeId": 5, "statSourceId": 0, "appliedTotal": 29.0},
        ]}
        assert day_points(player, 60) == 29.0
        assert day_points(player, 61) is None
        assert day_points(player, None) is None
        assert day_points({}, 60) is None

    def test_a_team_without_a_name_falls_back(self):
        assert parse_team_lineup({"id": 4, "abbrev": "ABC"}).team_name == "ABC"
        assert parse_team_lineup({"id": 4}).team_name == "Team 4"


@pytest.mark.unit
class TestParseLeagueDiscovery:
    def test_status_and_week_map(self, discovery_payload):
        disc = parse_league_discovery(discovery_payload)
        assert (disc.latest_scoring_period, disc.final_scoring_period, disc.current_matchup_period) == (175, 167, 21)
        # a one-week matchup, and the two-week playoff rounds
        assert disc.week_to_matchup_period[9] == 9
        assert disc.week_to_matchup_period[20] == 20 and disc.week_to_matchup_period[21] == 20
        assert disc.week_to_matchup_period[22] == 21 and disc.week_to_matchup_period[23] == 21
        assert disc.team_names[1] == "Flaggin it"

    def test_opponents_both_ways(self, discovery_payload):
        disc = parse_league_discovery(discovery_payload)
        assert disc.matchup_for(9, 1) == (9, 9)
        assert disc.matchup_for(9, 9) == (9, 1)
        assert disc.matchup_for(21, 1) == (20, disc.opponents[(20, 1)])

    def test_unknowns_are_none(self, discovery_payload):
        disc = parse_league_discovery(discovery_payload)
        assert disc.matchup_for(None, 1) == (None, None)
        assert disc.matchup_for(99, 1) == (None, None)
        assert disc.matchup_for(9, 999) == (9, None)

    def test_preseason_status_reads_as_no_day(self):
        disc = parse_league_discovery({"status": {"latestScoringPeriod": 0}, "schedule": [
            {"matchupPeriodId": 1, "home": {"teamId": 3}},   # a bye: no away side
        ]})
        assert disc == LeagueDiscovery(latest_scoring_period=None, final_scoring_period=None,
                                       current_matchup_period=None)
