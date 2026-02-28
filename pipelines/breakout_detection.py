"""
Breakout Streamer Detection Pipeline

Identifies players likely to see increased minutes/production due to a
prominent teammate being injured. Writes to nba.breakout_candidates.

Algorithm — hybrid depth-chart + position-validated history:

1. Find "prominent injured players" — starters with avg >= 28 min/game,
   currently listed as Out or Doubtful.

2. For each injured starter, build a POSITION DEPTH CHART for their
   position group on that team: all rotation players (>=12 min/g) at
   that position, ranked by avg_min descending. The injured player sits
   at the top; everyone below them is a candidate.

3. For each candidate, find POSITION-VALIDATED OPPORTUNITY GAMES:
   a. Games where the candidate played significantly above their avg
      (>= season_avg * 1.25, floor of 20 min) — their "high-usage games"
   b. Filter to only games where at least one position-group peer on
      the same team was absent from the box score (guarantees the
      candidate was genuinely covering a rotation gap, not just garbage
      time in a blowout).
   This naturally subsumes the player-specific absence signal: if the
   currently injured player was already out before, those games pass
   the position-peer filter automatically.

4. Score using four components:
   - depth_rank_bonus: #2 in depth chart >> #4
   - opportunity_boost: performance delta in validated opportunity games
   - production_score: current avg fpts baseline
   - headroom_bonus: room to grow in minutes

Why 28 min/game as the prominent-player threshold?
58% of a position's available time (~48 min game). Their absence
redistributes ~10-15 extra minutes, enough to push a 20 min/game
player to 30+ with proportionally higher fantasy output.
"""

from datetime import date, timedelta

from peewee import fn

from db.models.nba import (
    Player,
    PlayerGameStats,
    PlayerSeasonStats,
    PlayerInjury,
    Game,
    BreakoutCandidate,
)
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig
from pipelines.context import PipelineContext


# Min avg minutes for a player to be considered "prominent" (starter-level)
PROMINENT_MIN_THRESHOLD = 28.0
# Min games played to have a reliable avg (avoids small sample aberrations)
PROMINENT_MIN_GP = 20
# Min avg minutes for a teammate to be in the depth chart
TEAMMATE_MIN_THRESHOLD = 12.0
# Min games played for depth chart eligibility
TEAMMATE_MIN_GP = 10
# High-usage game threshold: candidate played this much above their avg
HIGH_USAGE_MULTIPLIER = 1.25
# Floor for high-usage games (eliminates DNP-like edge cases)
HIGH_USAGE_MIN_FLOOR = 20.0
# Min validated opportunity games to use the historical signal
MIN_OPP_SAMPLES = 2

# Depth rank score bonuses (diminishing returns beyond #3)
DEPTH_RANK_BONUSES = {1: 35, 2: 25, 3: 15, 4: 8}

# Position group adjacency: who absorbs whose minutes
# Maps a position → set of positions in the same rotation group
POSITION_ADJACENCY: dict[str, set[str]] = {
    "PG": {"PG", "SG", "G"},
    "SG": {"SG", "PG", "SF", "G"},
    "SF": {"SF", "SG", "PF", "F"},
    "PF": {"PF", "SF", "C", "F"},
    "C":  {"C", "PF"},
    "G":  {"PG", "SG", "G"},
    "F":  {"SF", "PF", "F"},
}


class BreakoutDetectionPipeline(BasePipeline):
    """
    Detect breakout candidates via position depth charts + validated history.

    Depends on:
    - espn_injury_status (populates nba.player_injuries)
    - player_season_stats (avg minutes and production baselines)
    - player_game_stats (historical box scores for opportunity game lookup)
    - game_schedule (team game date lookup for absence detection)
    """

    config = PipelineConfig(
        name="breakout_detection",
        display_name="Breakout Detection",
        description="Identifies players likely to benefit from a prominent teammate's injury",
        target_table="nba.breakout_candidates",
        depends_on=("espn_injury_status", "player_season_stats", "player_game_stats"),
    )

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the breakout detection pipeline."""
        if ctx.date_override:
            as_of_date = ctx.date_override
        else:
            now_cst = ctx.started_at
            if now_cst.hour < 6:
                as_of_date = (now_cst - timedelta(days=1)).date()
            else:
                as_of_date = now_cst.date()

        ctx.log.info("breakout_detection_start", as_of_date=str(as_of_date))

        injured_starters = self._get_prominent_injured_players(as_of_date)
        ctx.log.info("prominent_injured_found", count=len(injured_starters))

        if not injured_starters:
            ctx.log.info("no_prominent_injuries_today")
            return

        latest_stats_date = self._get_latest_stats_date()
        if not latest_stats_date:
            ctx.log.warning("no_season_stats_available")
            return

        total_candidates = 0

        for injured in injured_starters:
            player_id = injured["player_id"]
            team_id = injured["team_id"]
            injured_avg_min = injured["avg_min"]
            injured_position = injured["position"]

            ctx.log.info(
                "processing_injured_player",
                player=injured["name"],
                team=team_id,
                avg_min=round(injured_avg_min, 1),
                status=injured["status"],
                position=injured_position,
            )

            # Build position depth chart for this team + position group
            # The injured player is the implicit #1; we rank everyone below them
            depth_chart = self._build_position_depth_chart(
                team_id=team_id,
                injured_player_id=player_id,
                injured_position=injured_position,
                latest_stats_date=latest_stats_date,
            )

            if not depth_chart:
                ctx.log.debug(
                    "no_depth_chart_candidates",
                    player=injured["name"],
                    team=team_id,
                )
                continue

            ctx.log.debug(
                "depth_chart_built",
                player=injured["name"],
                depth_size=len(depth_chart),
            )

            # The full set of position-group peers (including the injured player)
            # used to validate opportunity games: absence of ANY of them counts
            all_position_peer_ids = {player_id} | {c["player_id"] for c in depth_chart}

            for candidate in depth_chart:
                candidate_id = candidate["player_id"]
                candidate_avg_min = candidate["avg_min"]

                # Find position-validated opportunity games for this candidate
                opp_min, opp_fpts, opp_count = self._get_position_validated_opportunity_stats(
                    candidate_player_id=candidate_id,
                    candidate_avg_min=candidate_avg_min,
                    team_id=team_id,
                    position_peer_ids=all_position_peer_ids - {candidate_id},
                    as_of_date=as_of_date,
                )

                depth_rank = candidate["depth_rank"]
                projected_boost = self._estimate_min_boost(
                    injured_avg_min=injured_avg_min,
                    candidate_avg_min=candidate_avg_min,
                    opp_min=opp_min,
                    depth_rank=depth_rank,
                    depth_size=len(depth_chart),
                )

                score = self._calculate_breakout_score(
                    candidate_avg_min=candidate_avg_min,
                    candidate_avg_fpts=candidate["avg_fpts"],
                    injured_avg_min=injured_avg_min,
                    depth_rank=depth_rank,
                    opp_min=opp_min,
                    opp_fpts=opp_fpts,
                    opp_count=opp_count,
                )

                BreakoutCandidate.upsert(
                    injured_player_id=player_id,
                    injured_avg_min=round(injured_avg_min, 1),
                    injury_status=injured["status"],
                    expected_return=injured.get("expected_return"),
                    beneficiary_player_id=candidate_id,
                    team_id=team_id,
                    depth_rank=depth_rank,
                    beneficiary_avg_min=round(candidate_avg_min, 1),
                    beneficiary_avg_fpts=round(candidate["avg_fpts"], 1),
                    projected_min_boost=round(projected_boost, 1),
                    opp_min_avg=round(opp_min, 1) if opp_min is not None else None,
                    opp_fpts_avg=round(opp_fpts, 1) if opp_fpts is not None else None,
                    opp_game_count=opp_count,
                    breakout_score=round(score, 1),
                    as_of_date=as_of_date,
                    pipeline_run_id=ctx.run_id,
                )
                ctx.increment_records()
                total_candidates += 1

        ctx.log.info(
            "breakout_detection_complete",
            injured_starters=len(injured_starters),
            total_candidates=total_candidates,
        )

    # -------------------------------------------------------------------------
    # Core helpers
    # -------------------------------------------------------------------------

    def _get_prominent_injured_players(self, as_of_date: date) -> list[dict]:
        """
        Find starters (>=28 min/g, >=20 gp) currently listed Out or Doubtful.
        """
        # Latest injury record per player on or before as_of_date
        injury_subq = (
            PlayerInjury.select(
                PlayerInjury.player,
                fn.MAX(PlayerInjury.report_date).alias("max_date"),
            )
            .where(PlayerInjury.report_date <= as_of_date)
            .group_by(PlayerInjury.player)
        )

        latest_injuries = list(
            PlayerInjury.select()
            .join(
                injury_subq,
                on=(
                    (PlayerInjury.player == injury_subq.c.player_id)
                    & (PlayerInjury.report_date == injury_subq.c.max_date)
                ),
            )
            .where(PlayerInjury.status.in_(["Out", "Doubtful"]))
        )

        if not latest_injuries:
            return []

        injured_player_ids = [inj.player_id for inj in latest_injuries]
        injury_by_player_id = {inj.player_id: inj for inj in latest_injuries}

        latest_date = (
            PlayerSeasonStats.select(fn.MAX(PlayerSeasonStats.as_of_date))
            .scalar()
        )
        if not latest_date:
            return []

        season_stats = list(
            PlayerSeasonStats.select(PlayerSeasonStats, Player)
            .join(Player)
            .where(
                (PlayerSeasonStats.player.in_(injured_player_ids))
                & (PlayerSeasonStats.as_of_date == latest_date)
                & (PlayerSeasonStats.gp >= PROMINENT_MIN_GP)
            )
        )

        results = []
        for stats in season_stats:
            avg_min = stats.min / stats.gp if stats.gp > 0 else 0
            if avg_min < PROMINENT_MIN_THRESHOLD:
                continue

            injury = injury_by_player_id.get(stats.player_id)
            results.append({
                "player_id": stats.player_id,
                "name": stats.player.name,
                "team_id": stats.team_id,
                "position": stats.player.position or "",
                "avg_min": avg_min,
                "avg_fpts": stats.fpts / stats.gp if stats.gp > 0 else 0,
                "gp": stats.gp,
                "status": injury.status if injury else "Out",
                "expected_return": injury.expected_return if injury else None,
            })

        return results

    def _get_latest_stats_date(self) -> date | None:
        return (
            PlayerSeasonStats.select(fn.MAX(PlayerSeasonStats.as_of_date))
            .scalar()
        )

    def _build_position_depth_chart(
        self,
        team_id: str,
        injured_player_id: int,
        injured_position: str,
        latest_stats_date: date,
    ) -> list[dict]:
        """
        Return rotation players at the injured player's position group,
        sorted by avg_min descending, with depth_rank assigned (1-based).

        The injured player is excluded — they're the implicit #1.
        Depth rank 1 here means "first backup" (the player who would be
        promoted to starter role).
        """
        adjacent_positions = POSITION_ADJACENCY.get(injured_position.upper(), set())
        if not adjacent_positions:
            # Unknown position: include all rotation players on the team
            adjacent_positions = {"PG", "SG", "SF", "PF", "C", "G", "F"}

        teammates_stats = list(
            PlayerSeasonStats.select(PlayerSeasonStats, Player)
            .join(Player)
            .where(
                (PlayerSeasonStats.team == team_id)
                & (PlayerSeasonStats.as_of_date == latest_stats_date)
                & (PlayerSeasonStats.player != injured_player_id)
                & (PlayerSeasonStats.gp >= TEAMMATE_MIN_GP)
            )
        )

        candidates = []
        for stats in teammates_stats:
            avg_min = stats.min / stats.gp if stats.gp > 0 else 0
            if avg_min < TEAMMATE_MIN_THRESHOLD:
                continue

            position_upper = (stats.player.position or "").upper()
            in_group = (
                not injured_position       # injured position unknown → include all
                or not position_upper      # candidate position unknown → include
                or position_upper in adjacent_positions
            )
            if not in_group:
                continue

            candidates.append({
                "player_id": stats.player_id,
                "name": stats.player.name,
                "position": stats.player.position or "",
                "avg_min": avg_min,
                "avg_fpts": stats.fpts / stats.gp if stats.gp > 0 else 0,
                "gp": stats.gp,
            })

        # Sort by avg_min descending — highest minutes player is next in line
        candidates.sort(key=lambda c: c["avg_min"], reverse=True)

        # Assign depth rank (1 = first backup, 2 = second backup, etc.)
        for rank, candidate in enumerate(candidates, start=1):
            candidate["depth_rank"] = rank

        return candidates

    def _get_position_validated_opportunity_stats(
        self,
        candidate_player_id: int,
        candidate_avg_min: float,
        team_id: str,
        position_peer_ids: set[int],
        as_of_date: date,
        lookback_days: int = 120,
    ) -> tuple[float | None, float | None, int]:
        """
        Return (avg_min, avg_fpts, game_count) for position-validated
        opportunity games.

        A "position-validated opportunity game" is a game where:
        1. The candidate played >= max(season_avg * 1.25, 20) minutes
        2. At least one position-group peer on the same team was absent
           from the box score (team played but peer has no stats row)

        This filters blowout garbage time while capturing any positional
        absence — not just the currently injured player specifically.

        Returns (None, None, 0) if fewer than MIN_OPP_SAMPLES valid games.
        """
        if not position_peer_ids:
            return None, None, 0

        start_date = as_of_date - timedelta(days=lookback_days)
        min_threshold = max(candidate_avg_min * HIGH_USAGE_MULTIPLIER, HIGH_USAGE_MIN_FLOOR)

        # Step 1: Find candidate's high-usage games on this team
        high_usage_games = list(
            PlayerGameStats.select(
                PlayerGameStats.game_date,
                PlayerGameStats.min,
                PlayerGameStats.fpts,
            ).where(
                (PlayerGameStats.player == candidate_player_id)
                & (PlayerGameStats.team == team_id)
                & (PlayerGameStats.game_date >= start_date)
                & (PlayerGameStats.game_date < as_of_date)
                & (PlayerGameStats.min >= min_threshold)
            )
        )

        if not high_usage_games:
            return None, None, 0

        game_dates = [g.game_date for g in high_usage_games]

        # Step 2: For each high-usage date, find which position peers DID play
        peers_played = list(
            PlayerGameStats.select(
                PlayerGameStats.player,
                PlayerGameStats.game_date,
            ).where(
                (PlayerGameStats.player.in_(list(position_peer_ids)))
                & (PlayerGameStats.game_date.in_(game_dates))
            )
        )

        # Build {game_date: set of peer player_ids who played}
        peers_by_date: dict[date, set[int]] = {}
        for stat in peers_played:
            d = stat.game_date
            if d not in peers_by_date:
                peers_by_date[d] = set()
            peers_by_date[d].add(stat.player_id)

        # Step 3: Validate — keep only games where at least one peer was absent
        validated = [
            g for g in high_usage_games
            if position_peer_ids - peers_by_date.get(g.game_date, set())
            # i.e., at least one peer in position_peer_ids did NOT play
        ]

        if len(validated) < MIN_OPP_SAMPLES:
            return None, None, 0

        avg_min = sum(g.min for g in validated) / len(validated)
        avg_fpts = sum(g.fpts for g in validated) / len(validated)
        return avg_min, avg_fpts, len(validated)

    # -------------------------------------------------------------------------
    # Scoring helpers
    # -------------------------------------------------------------------------

    def _estimate_min_boost(
        self,
        injured_avg_min: float,
        candidate_avg_min: float,
        opp_min: float | None,
        depth_rank: int,
        depth_size: int,
    ) -> float:
        """
        Estimate extra minutes the candidate will absorb.

        Prefer opportunity-game signal when available. Otherwise estimate
        proportionally: the #1 backup absorbs more of the minutes vacuum
        than the #3 backup.
        """
        if opp_min is not None:
            return max(0.0, opp_min - candidate_avg_min)

        # Proportional fallback: share of injured player's minutes based on rank
        # Rank 1 gets ~35%, rank 2 gets ~25%, rank 3+ gets ~15%
        share = max(0.35 - (depth_rank - 1) * 0.10, 0.10)
        return injured_avg_min * share

    def _calculate_breakout_score(
        self,
        candidate_avg_min: float,
        candidate_avg_fpts: float,
        injured_avg_min: float,
        depth_rank: int,
        opp_min: float | None,
        opp_fpts: float | None,
        opp_count: int,
    ) -> float:
        """
        Composite breakout score (higher = stronger candidate).

        Components:
        - depth_rank_bonus (0-35): position in depth chart — #2 >> #4
        - opportunity_boost (0-40): performance delta in validated opportunity games
          confidence-weighted by sample size (more games = more reliable)
        - production_score (0-~30): current avg fpts baseline
        - headroom_bonus (0-~12): room to grow in minutes
        """
        # 1. Depth rank bonus
        rank_bonus = DEPTH_RANK_BONUSES.get(depth_rank, 4)

        # 2. Opportunity game boost
        if opp_min is not None and opp_fpts is not None:
            fpts_delta = opp_fpts - candidate_avg_fpts
            min_delta = opp_min - candidate_avg_min
            # Weight by sample confidence: cap at 1.0 after 5 games
            confidence = min(opp_count / 5.0, 1.0)
            raw_boost = (fpts_delta * 1.5 + min_delta * 0.8) * confidence
            opp_boost = min(max(raw_boost, 0.0), 40.0)
        else:
            opp_boost = 0.0

        # 3. Current production baseline
        production_score = candidate_avg_fpts * 0.5

        # 4. Minutes headroom (more room to grow = higher ceiling)
        headroom = max(36.0 - candidate_avg_min, 0.0)
        headroom_bonus = headroom * 0.4

        return rank_bonus + opp_boost + production_score + headroom_bonus
