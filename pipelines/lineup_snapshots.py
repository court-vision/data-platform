"""
Lineup Snapshots Pipeline — every ESPN team's lineup for each finished scoring period.

ESPN keeps a per-day history of every roster: `?scoringPeriodId=N` with the
`mRoster` view answers with each team's membership and lineup slots AS OF day
N, for the whole league in one read. This pipeline materializes that into
usr.lineup_snapshots once the day is over, so a past day of a matchup can show
the rosters that actually counted (bench told apart from starters) instead of
today's — for the user's own team and for the opponent, who has no usr.teams
row of his own.

One capture per league, not per team: ESPN teams in usr.teams are grouped by
(league_id, year) and read with the first member's cookies that work (public
leagues need none). The day to capture is decided by ESPN itself —
`status.latestScoringPeriod` (L) is today, so days ≤ L-1 are finished — and
every finished day not yet stored within the last `GAP_FILL_PERIODS` is
captured in ascending order, so a missed night (an early Sunday slate closing
the post-game window before 02:00 CT, an outage, the All-Star break) heals
itself on the next run. A night where ESPN has not rolled past the game date
yet raises `DataNotReady`, and the post-game batch retries at its next poll.

Timing: POST_GAME with `depends_on=("daily_matchup_scores",)` — that
pipeline's ESPN gate is what waits for ESPN's nightly flip, and an unmet
dependency is a silent per-poll skip. Not `espn_gated` itself: the gate's
watermarks are daily_matchup_scores' rows, so a second gated pipeline would
re-run on every poll of a WITHHOLD night and only retry at the 02:30 CT
fallback otherwise.

`?date=` re-captures exactly that NBA date's ESPN day for every league and
replaces what is stored; any difference is logged as `lineup_snapshot_drift`
— the running evidence that ESPN never rewrites a finished day.

Counters: a team-day stored is a processed record; business outcomes (a league
whose cookies all fail, a Yahoo team, a day outside the regular season) are
skipped records with a reason; a league whose read or write raised is a failed
record. Each day's write is one transaction, so a redeploy mid-run never leaves
half a day behind.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, time
from typing import Optional

from peewee import fn

from core.crypto import CredentialDecryptionError
from core.settings import settings
from db.base import db
from db.models.lineup_snapshots import LineupSnapshot, LineupSnapshotPlayer
from db.models.teams import Team
from pipelines.base import BasePipeline, DataNotReady
from pipelines.config import PipelineCategory, PipelineConfig
from pipelines.context import PipelineContext
from pipelines.extractors.espn import ESPNExtractor, ProviderAuthError
from pipelines.transformers.lineup_snapshots import (
    LeagueDay,
    LeagueDiscovery,
    TeamLineup,
    parse_league_day,
    parse_league_discovery,
)
from services import credential_service
from services import schedule_service

# How far back a nightly run looks for finished days it has not stored. The
# All-Star break is ~7 game-less nights (no post-game batch), an early Sunday
# slate can close the window before 02:00 CT; 14 covers both with room.
GAP_FILL_PERIODS = 14
DISCOVERY_VIEWS = ("mTeam", "mSettings", "mMatchup")
ROSTER_VIEWS = ("mTeam", "mRoster")
ACTIVE_SLOT_IDS = frozenset(range(0, 12))  # 12 = BE, 13 = IR


@dataclass(frozen=True)
class LeagueKey:
    provider_league_id: str
    season: int

    @property
    def league_id(self) -> int:
        return int(self.provider_league_id)


@dataclass
class LeagueGroup:
    key: LeagueKey
    teams: list[tuple[Team, dict]]   # (row, unhydrated league_info) for every CV team in the league


@dataclass(frozen=True)
class TeamDayRecord:
    lineup: TeamLineup
    matchup_period_id: Optional[int]
    opponent_provider_team_id: Optional[int]


@dataclass
class WriteResult:
    stored: int = 0
    drift: list[dict] | None = None

    def __post_init__(self):
        self.drift = self.drift or []


# ------------------------------- storage ------------------------------- #


class SnapshotStore:
    """The table writes, behind one object so the pipeline's tests can fake them."""

    @staticmethod
    def _league_rows(key: LeagueKey):
        return (
            (LineupSnapshot.provider == "espn")
            & (LineupSnapshot.provider_league_id == key.provider_league_id)
            & (LineupSnapshot.season == key.season)
        )

    def stored_periods(self, key: LeagueKey, low: int, high: int) -> set[int]:
        """The league's days in [low, high] that have rows."""
        query = (
            LineupSnapshot.select(LineupSnapshot.scoring_period_id)
            .where(self._league_rows(key) & LineupSnapshot.scoring_period_id.between(low, high))
            .distinct()
        )
        return {period for (period,) in query.tuples()}

    def write_day(
        self,
        key: LeagueKey,
        scoring_period_id: int,
        nba_date: date,
        records: list[TeamDayRecord],
        *,
        replace: bool,
        source: str,
        pipeline_run_id,
    ) -> WriteResult:
        """Store every team's lineup for one day in one transaction.

        A team-day that already exists is left alone unless `replace`, in which
        case the old rows are compared (player, slot) against the new ones and
        reported in `drift` before being replaced.
        """
        result = WriteResult()
        with db.atomic():
            for record in records:
                lineup = record.lineup
                existing = (
                    LineupSnapshot.select()
                    .where(
                        self._league_rows(key)
                        & (LineupSnapshot.provider_team_id == lineup.provider_team_id)
                        & (LineupSnapshot.scoring_period_id == scoring_period_id)
                    )
                    .first()
                )
                if existing is not None:
                    if not replace:
                        continue
                    old = {(p.player_id, p.lineup_slot_id) for p in existing.players}
                    new = {(p.player_id, p.lineup_slot_id) for p in lineup.players}
                    result.drift.append({
                        "provider_team_id": lineup.provider_team_id,
                        "scoring_period_id": scoring_period_id,
                        "players_before": len(old),
                        "players_after": len(new),
                        "changed": len(old ^ new),
                    })
                    existing.delete_instance()  # children go with it (ON DELETE CASCADE)

                header = LineupSnapshot.create(
                    provider="espn",
                    provider_league_id=key.provider_league_id,
                    season=key.season,
                    provider_team_id=lineup.provider_team_id,
                    team_name=lineup.team_name[:120],
                    scoring_period_id=scoring_period_id,
                    nba_date=nba_date,
                    matchup_period_id=record.matchup_period_id,
                    opponent_provider_team_id=record.opponent_provider_team_id,
                    applied_stat_total=lineup.applied_stat_total,
                    player_count=len(lineup.players),
                    source=source,
                    pipeline_run_id=pipeline_run_id,
                )
                rows = [
                    {
                        "snapshot": header.id,
                        "player_id": p.player_id,
                        "player_name": p.name[:120],
                        "pro_team": p.pro_team,
                        "default_position_id": p.default_position_id,
                        "lineup_slot_id": p.lineup_slot_id,
                        "eligible_slot_ids": list(p.eligible_slot_ids),
                        "injured": p.injured,
                        "injury_status": p.injury_status,
                        "applied_total": p.applied_total,
                    }
                    for p in lineup.players
                ]
                if rows:
                    LineupSnapshotPlayer.insert_many(rows).execute()
                result.stored += 1
        return result

    @staticmethod
    def fantasy_points_logged(nba_date: date, espn_player_ids: list[int]) -> Optional[float]:
        """Sum of our game log's fpts for these ESPN players on that date; None when unknown."""
        from db.models.nba.player_game_stats import PlayerGameStats
        from db.models.nba.players import Player

        if not espn_player_ids:
            return None
        value = (
            PlayerGameStats.select(fn.SUM(PlayerGameStats.fpts))
            .join(Player, on=(PlayerGameStats.player == Player.id))
            .where((Player.espn_id.in_(espn_player_ids)) & (PlayerGameStats.game_date == nba_date))
            .scalar()
        )
        return float(value) if value is not None else None


# ------------------------------- the pipeline ------------------------------- #


def _load_teams() -> list[Team]:
    return list(Team.select())


class LineupSnapshotsPipeline(BasePipeline):
    """
    1. Group the saved ESPN teams by league (current season only)
    2. Per league, read ESPN's status with the first cookies that work
    3. Capture every finished day not yet stored (or exactly the ?date= day)
    4. Write each day's lineups for every team in the league in one transaction
    """

    config = PipelineConfig(
        name="lineup_snapshots",
        display_name="Lineup Snapshots",
        description="Every ESPN team's lineup for each finished scoring period",
        target_table="usr.lineup_snapshots",
        category=PipelineCategory.POST_GAME,
        trigger_slug="lineup-snapshots",
        # daily_matchup_scores' ESPN gate is what waits for ESPN's nightly flip;
        # an unmet dependency is a silent per-poll skip.
        depends_on=("daily_matchup_scores",),
        # ESPN rolls the day ~2 AM CT. Cheap insurance for a forced run.
        earliest_run_time_cst=time(2, 0),
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()
        self.store = SnapshotStore()

    # ---- execute ----

    def execute(self, ctx: PipelineContext) -> None:
        leagues, skipped = self._leagues()
        for reason, count in skipped.items():
            ctx.increment_skipped(count, reason)
        ctx.log.info("lineup_snapshots_leagues", leagues=len(leagues), skipped=skipped)
        if not leagues:
            return

        backfill = ctx.date_override is not None
        target = schedule_service.season_day(ctx.game_date())
        if target is None:
            ctx.log.info("lineup_snapshots_outside_regular_season", game_date=str(ctx.game_date()))
            ctx.increment_skipped(len(leagues), "outside_regular_season")
            return

        waiting = 0
        processed_leagues = 0
        observed_totals = False
        for league in leagues:
            try:
                found = self._discover(ctx, league)
                if found is None:
                    ctx.increment_skipped(1, "league_unreadable")
                    continue
                credentials, discovery = found
                latest = discovery.latest_scoring_period
                if not latest:
                    ctx.log.info("lineup_snapshots_no_scoring_period", league_id=league.key.league_id)
                    ctx.increment_skipped(1, "no_scoring_period")
                    continue

                if backfill:
                    if target >= latest:
                        ctx.log.info("lineup_snapshots_day_not_finished", league_id=league.key.league_id,
                                     target=target, latest=latest)
                        ctx.increment_skipped(1, "day_not_finished")
                        continue
                    periods = [target]
                elif latest <= target:
                    ctx.log.info("lineup_snapshots_waiting_for_espn", league_id=league.key.league_id,
                                 target=target, latest=latest)
                    waiting += 1
                    continue
                else:
                    periods = self._missing_periods(league.key, latest, discovery.final_scoring_period)

                before = ctx.records_processed
                for period in periods:
                    payload = self.espn_extractor.get_league(
                        league_id=league.key.league_id,
                        espn_s2=credentials.get("espn_s2", ""),
                        swid=credentials.get("swid", ""),
                        year=league.key.season,
                        views=ROSTER_VIEWS,
                        scoring_period_id=period,
                    )
                    day = parse_league_day(payload)
                    stored = self._store_day(ctx, league.key, period, day, discovery, replace=backfill)
                    if stored and not observed_totals:
                        observed_totals = self._observe_applied_totals(ctx, league.key, period, day)
                ctx.log.info(
                    "lineup_snapshots_league_done",
                    league_id=league.key.league_id,
                    periods=periods,
                    team_days=ctx.records_processed - before,
                )
                processed_leagues += 1
            except Exception as exc:
                ctx.log.warning(
                    "lineup_snapshots_league_error",
                    league_id=league.key.league_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                ctx.increment_failed(1, type(exc).__name__)

        if waiting and ctx.records_processed == 0:
            raise DataNotReady(
                f"ESPN has not advanced past day {target} for {waiting} league(s); the next poll retries"
            )
        if processed_leagues == 0 and ctx.records_failed > 0 and ctx.records_failed >= len(leagues):
            raise RuntimeError(f"0 of {len(leagues)} leagues processed — ESPN may be unavailable")

    # ---- leagues and credentials ----

    @staticmethod
    def _leagues() -> tuple[list[LeagueGroup], dict[str, int]]:
        """Saved ESPN teams grouped by league for the configured season; skip counts by reason."""
        groups: dict[LeagueKey, LeagueGroup] = {}
        skipped: dict[str, int] = {}
        for team in _load_teams():
            try:
                league_info = json.loads(team.league_info or "{}")
            except (TypeError, ValueError):
                skipped["league_info_unreadable"] = skipped.get("league_info_unreadable", 0) + 1
                continue
            provider = league_info.get("provider") or "espn"
            if provider != "espn":
                skipped["provider_unsupported"] = skipped.get("provider_unsupported", 0) + 1
                continue
            league_id = league_info.get("league_id")
            if not league_id:
                skipped["league_id_missing"] = skipped.get("league_id_missing", 0) + 1
                continue
            year = int(league_info.get("year") or settings.espn_year)
            if year != settings.espn_year:
                skipped["season_mismatch"] = skipped.get("season_mismatch", 0) + 1
                continue
            key = LeagueKey(provider_league_id=str(league_id), season=year)
            groups.setdefault(key, LeagueGroup(key=key, teams=[])).teams.append((team, league_info))
        return list(groups.values()), skipped

    def _discover(self, ctx: PipelineContext, league: LeagueGroup) -> Optional[tuple[dict, LeagueDiscovery]]:
        """The league's status, read with the first member team's cookies ESPN accepts."""
        for team, league_info in league.teams:
            try:
                credentials = credential_service.hydrate(team, dict(league_info))
            except CredentialDecryptionError:
                ctx.log.warning("lineup_snapshots_credentials_unreadable", team_id=team.team_id)
                continue
            try:
                payload = self.espn_extractor.get_league(
                    league_id=league.key.league_id,
                    espn_s2=credentials.get("espn_s2", ""),
                    swid=credentials.get("swid", ""),
                    year=league.key.season,
                    views=DISCOVERY_VIEWS,
                )
            except ProviderAuthError as exc:
                ctx.log.warning("lineup_snapshots_credentials_refused", team_id=team.team_id,
                                league_id=league.key.league_id, status_code=exc.status_code)
                continue
            return credentials, parse_league_discovery(payload)
        ctx.log.warning("lineup_snapshots_league_unreadable", league_id=league.key.league_id,
                        teams=len(league.teams))
        return None

    # ---- which days ----

    def _missing_periods(self, key: LeagueKey, latest: int, final: Optional[int]) -> list[int]:
        """Finished days not yet stored, ascending: every day in
        [max(1, L-14) .. min(L-1, final)] with no rows.

        Every day of the window is checked, not just those after the newest
        stored one: the stored days need not be contiguous (a `?date=` capture
        can land past a gap), and a gap must still heal within the lookback.
        """
        high = latest - 1
        if final is None:
            try:
                final = schedule_service.season_day(schedule_service.get_season_bounds().regular_season_end)
            except Exception:
                final = None
        if final is not None:
            high = min(high, final)
        low = max(1, latest - GAP_FILL_PERIODS)
        if low > high:
            return []
        stored = self.store.stored_periods(key, low, high)
        return [period for period in range(low, high + 1) if period not in stored]

    # ---- write ----

    def _store_day(
        self,
        ctx: PipelineContext,
        key: LeagueKey,
        period: int,
        day: LeagueDay,
        discovery: LeagueDiscovery,
        *,
        replace: bool,
    ) -> int:
        nba_date = schedule_service.date_for_espn_scoring_period(period)
        week = schedule_service.get_current_matchup(nba_date)
        week_number = week["matchup_number"] if week else None
        records = []
        for lineup in day.teams:
            matchup_period, opponent = discovery.matchup_for(week_number, lineup.provider_team_id)
            records.append(TeamDayRecord(lineup=lineup, matchup_period_id=matchup_period,
                                         opponent_provider_team_id=opponent))
        result = self.store.write_day(
            key, period, nba_date, records,
            replace=replace, source="backfill" if replace else "pipeline", pipeline_run_id=ctx.run_id,
        )
        for drift in result.drift:
            if drift["changed"]:
                ctx.log.warning("lineup_snapshot_drift", league_id=key.league_id, **drift)
            else:
                ctx.log.info("lineup_snapshot_recaptured_identical", league_id=key.league_id, **drift)
        ctx.increment_records(result.stored)
        ctx.log.info("lineup_snapshots_day_stored", league_id=key.league_id, scoring_period_id=period,
                     nba_date=str(nba_date), teams=result.stored, replaced=replace)
        return result.stored

    def _observe_applied_totals(self, ctx: PipelineContext, key: LeagueKey, period: int, day: LeagueDay) -> bool:
        """Log ESPN's team total for the day beside our game log's sum for the
        same active players — an observation, never a judgement (the league's
        scoring may not be the default formula)."""
        team = next((t for t in day.teams if t.applied_stat_total is not None), None)
        if team is None:
            return False
        try:
            nba_date = schedule_service.date_for_espn_scoring_period(period)
            active = [p.player_id for p in team.players if p.lineup_slot_id in ACTIVE_SLOT_IDS]
            logged = self.store.fantasy_points_logged(nba_date, active)
            ctx.log.info(
                "lineup_snapshot_applied_total_check",
                league_id=key.league_id, provider_team_id=team.provider_team_id, scoring_period_id=period,
                espn_applied_total=team.applied_stat_total, game_log_fpts=logged, active_players=len(active),
            )
        except Exception as exc:  # the observation must never fail the run
            ctx.log.debug("lineup_snapshot_applied_total_check_failed", error=str(exc))
        return True
