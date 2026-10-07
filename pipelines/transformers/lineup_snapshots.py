"""
Pure parsers for the ESPN league payloads the `lineup_snapshots` pipeline reads.

`parse_league_day` turns `?view=mTeam&view=mRoster&scoringPeriodId=N` — ESPN's
roster of EVERY team in the league as of day N, membership and slots alike —
into one `TeamLineup` per team. `parse_league_discovery` turns the day-less
`mTeam,mSettings,mMatchup` read into what the pipeline needs to decide which
days are finished and who played whom. Field derivations match the backend's
`services/lineup_read_service.py::parse_espn_lineup`.

A player's points for the day are his stat line for that period
(`player.stats[]` with `scoringPeriodId == N`, `statSplitTypeId` 5 = one scoring
period, `statSourceId` 0 = actual), present only when he played. ESPN's
`playerPoolEntry.appliedStatTotal` and `roster.appliedStatTotal` are NOT the
day's numbers — they sum whatever recent lines the payload carries — so the
team's day total is computed here from the active slots' lines.

`scheduleSettings.matchupPeriods` maps a matchup period to the calendar WEEK
ids it spans (`{"20": [20, 21]}`), the same relation `get_espn_matchup_dates`
relies on; it is inverted here so a week number (our calendar's
`matchup_number`) resolves to ESPN's matchup period.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from utils.espn_helpers import PRO_TEAM_MAP

TEAM_ABBREV_CORRECTIONS = {"PHL": "PHI", "PHO": "PHX"}
ACTIVE_SLOT_IDS = frozenset(range(0, 12))   # 12 = BE, 13 = IR
STAT_SPLIT_SCORING_PERIOD = 5
STAT_SOURCE_ACTUAL = 0


def _int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class SnapshotPlayer:
    player_id: int
    name: str
    pro_team: str
    default_position_id: Optional[int]
    lineup_slot_id: int
    eligible_slot_ids: tuple[int, ...]
    injured: bool
    injury_status: Optional[str]
    applied_total: Optional[float]


@dataclass(frozen=True)
class TeamLineup:
    provider_team_id: int
    team_name: str
    applied_stat_total: Optional[float]
    players: tuple[SnapshotPlayer, ...]


@dataclass(frozen=True)
class LeagueDay:
    scoring_period_id: Optional[int]       # ESPN's echo of the day requested
    teams: tuple[TeamLineup, ...]


@dataclass(frozen=True)
class LeagueDiscovery:
    latest_scoring_period: Optional[int]   # ESPN's today; 0/absent before opening night → None
    final_scoring_period: Optional[int]
    current_matchup_period: Optional[int]
    week_to_matchup_period: dict[int, int] = field(default_factory=dict)
    opponents: dict[tuple[int, int], int] = field(default_factory=dict)   # (matchup period, team) -> opponent
    team_names: dict[int, str] = field(default_factory=dict)

    def matchup_for(self, week: Optional[int], team_id: int) -> tuple[Optional[int], Optional[int]]:
        """(matchup_period_id, opponent_team_id) for a team in a calendar week; Nones when unknown."""
        if week is None:
            return None, None
        matchup_period = self.week_to_matchup_period.get(int(week))
        if matchup_period is None:
            return None, None
        return matchup_period, self.opponents.get((matchup_period, int(team_id)))


def _entry_player(entry: dict) -> dict:
    pool = entry.get("playerPoolEntry") or {}
    return pool.get("player") or entry.get("player") or {}


def day_points(player: dict, scoring_period_id: Optional[int]) -> Optional[float]:
    """The player's actual points for one scoring period, None when he has no line for it."""
    if scoring_period_id is None:
        return None
    for line in player.get("stats") or []:
        if (
            _int(line.get("scoringPeriodId")) == scoring_period_id
            and _int(line.get("statSplitTypeId")) == STAT_SPLIT_SCORING_PERIOD
            and _int(line.get("statSourceId")) == STAT_SOURCE_ACTUAL
        ):
            return _float(line.get("appliedTotal"))
    return None


def parse_team_lineup(team: dict, scoring_period_id: Optional[int] = None) -> TeamLineup:
    roster = team.get("roster") or {}
    players: list[SnapshotPlayer] = []
    for entry in roster.get("entries") or []:
        player = _entry_player(entry)
        if not player:
            continue
        abbrev = PRO_TEAM_MAP.get(player.get("proTeamId", 0), "FA")
        players.append(SnapshotPlayer(
            player_id=int(player.get("id") or entry.get("playerId") or 0),
            name=player.get("fullName") or "Unknown",
            pro_team=TEAM_ABBREV_CORRECTIONS.get(abbrev, abbrev),
            default_position_id=_int(player.get("defaultPositionId")),
            lineup_slot_id=int(entry.get("lineupSlotId", 0) or 0),
            eligible_slot_ids=tuple(int(s) for s in player.get("eligibleSlots") or []),
            injured=bool(player.get("injured", False)),
            injury_status=player.get("injuryStatus") or entry.get("injuryStatus"),
            applied_total=day_points(player, scoring_period_id),
        ))
    team_id = int(team.get("id"))
    name = (team.get("name") or "").strip() or (team.get("abbrev") or "").strip() or f"Team {team_id}"
    active_lines = [p.applied_total for p in players if p.lineup_slot_id in ACTIVE_SLOT_IDS and p.applied_total is not None]
    return TeamLineup(
        provider_team_id=team_id,
        team_name=name,
        applied_stat_total=round(sum(active_lines), 2) if active_lines else None,
        players=tuple(players),
    )


def parse_league_day(payload: dict) -> LeagueDay:
    """Every team's lineup in a `mTeam,mRoster` read for one scoring period."""
    scoring_period_id = _int(payload.get("scoringPeriodId"))
    teams = tuple(
        parse_team_lineup(team, scoring_period_id)
        for team in payload.get("teams") or []
        if team.get("id") is not None
    )
    return LeagueDay(scoring_period_id=scoring_period_id, teams=teams)


def parse_league_discovery(payload: dict) -> LeagueDiscovery:
    """Status, the week → matchup-period map and the schedule's pairings."""
    status = payload.get("status") or {}
    schedule_settings = (payload.get("settings") or {}).get("scheduleSettings") or {}

    week_to_matchup: dict[int, int] = {}
    for matchup_period, weeks in (schedule_settings.get("matchupPeriods") or {}).items():
        for week in weeks or []:
            week_to_matchup[int(week)] = int(matchup_period)

    opponents: dict[tuple[int, int], int] = {}
    for matchup in payload.get("schedule") or []:
        matchup_period = _int(matchup.get("matchupPeriodId"))
        home = _int((matchup.get("home") or {}).get("teamId"))
        away = _int((matchup.get("away") or {}).get("teamId"))
        if matchup_period is None or home is None or away is None:
            continue  # a bye has no away side
        opponents[(matchup_period, home)] = away
        opponents[(matchup_period, away)] = home

    team_names = {
        int(team["id"]): (team.get("name") or "").strip()
        for team in payload.get("teams") or []
        if team.get("id") is not None
    }
    return LeagueDiscovery(
        latest_scoring_period=_int(status.get("latestScoringPeriod")) or None,
        final_scoring_period=_int(status.get("finalScoringPeriod")) or None,
        current_matchup_period=_int(status.get("currentMatchupPeriod")) or None,
        week_to_matchup_period=week_to_matchup,
        opponents=opponents,
        team_names=team_names,
    )
