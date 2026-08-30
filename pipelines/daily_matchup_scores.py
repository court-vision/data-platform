"""
Daily Matchup Scores Pipeline

Fetches current matchup scores for all saved teams and records daily snapshots.
"""

import json
from datetime import time
from typing import Optional

import pytz

from core.settings import settings
from db.models.teams import Team
from services import credential_service
from db.models.stats.daily_matchup_score import DailyMatchupScore
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor, YahooExtractor
from services.schedule_service import get_current_matchup


WRITE = "write"          # store the snapshot, watermark included
WITHHOLD = "withhold"    # store the totals but no watermark; next poll confirms
SKIP = "skip"            # store nothing this poll; the gate retries


def _totals_differ(a, b) -> bool:
    """Compare two per-category totals dicts by value (JSONB floats vs floats)."""
    keys = set(a) | set(b)
    return any(float(a.get(k, 0) or 0) != float(b.get(k, 0) or 0) for k in keys)


def _category_totals(category_scores):
    """(you, opp) totals dicts, or None when absent/malformed."""
    if not category_scores:
        return None
    you, opp = category_scores.get("you"), category_scores.get("opp")
    if not isinstance(you, dict) or not isinstance(opp, dict):
        return None
    return you, opp


def _is_zero_seed(score, opponent_score, category_scores) -> bool:
    """A snapshot that claims nothing: 0-0, and zero category totals if present."""
    if float(score) != 0.0 or float(opponent_score) != 0.0:
        return False
    totals = _category_totals(category_scores)
    if totals is None:
        return True
    you, opp = totals
    return not _totals_differ(you, {}) and not _totals_differ(opp, {})


def _movement(stored_score, stored_opp, stored_categories, new_score, new_opp, new_categories):
    """Did ESPN's materialized numbers move since the stored reference?

    True/False when comparable; None when the two snapshots cannot be compared
    (e.g. the league's scoring format flipped between polls).

    Category leagues are judged on their per-category totals, not on
    current_score: the extractor stores categories WON there, a small integer
    that routinely stays put overnight while every underlying total moved — so
    win-counts as the movement signal would classify a fully materialized
    batch as "not landed" and starve the night's snapshot.
    """
    new_totals = _category_totals(new_categories)
    stored_totals = _category_totals(stored_categories)
    if new_totals is not None:
        if stored_totals is None:
            return None
        moved = (
            _totals_differ(new_totals[0], stored_totals[0])
            or _totals_differ(new_totals[1], stored_totals[1])
        )
        # Win counts moving without totals moving shouldn't happen, but if it
        # does, ESPN materialized *something*.
        return moved or (
            float(new_score) != float(stored_score)
            or float(new_opp) != float(stored_opp)
        )
    if stored_totals is not None:
        return None
    return float(new_score) != float(stored_score) or float(new_opp) != float(stored_opp)


def watermark_decision(
    stored_period,
    stored_score,
    stored_opponent_score,
    stored_categories,
    new_period,
    new_source,
    new_score,
    new_opponent_score,
    new_categories,
    days_since_stored: int = 0,
    stored_exists: bool = True,
) -> str:
    """Should this snapshot be stored, and may it carry its watermark?

    The watermark design rests on one pairing: a snapshot at watermark B covers
    scores through day B-1 (docs/PENDING_PROD_CHECKS.md #4). ESPN reports both
    fields in one response, but if it ever advances `latestScoringPeriod`
    before its batch materializes `totalPoints`, storing that pairing freezes
    an overstated claim into the baseline and a day of score silently vanishes
    from the live overlay. The write side therefore demands *evidence* before
    advancing the watermark: totals movement from a same-period reference.

    Three outcomes:
    - WRITE: store everything. The movement (or a claim-free zero seed, or the
      idle-matchup timeout) confirms the pairing.
    - WITHHOLD: store the totals with **no watermark** (`scoring_period_id`
      NULL, source "unknown"). Used when there is nothing to compare against —
      an unseeded period with nonzero totals, or a multi-period gap where
      movement only proves the *oldest* missing batch landed, not all of them.
      The read path treats an unusable watermark as "prefer a slightly stale
      score to a double-counted one" (matchup_window `legacy_date_rule`), and
      the stored totals become the reference the next poll confirms against.
    - SKIP: store nothing; the gate's next poll retries.

    Residual, accepted: a gap of 3+ periods needs one confirming poll per
    missing batch, and a batch landing in the exact poll interval between two
    others can still be conflated — each WITHHOLD round shrinks the claim's
    error by a day, and the read path under- rather than over-counts
    throughout.
    """
    if new_source != "provider" or new_period is None:
        return WRITE  # calendar/unknown watermarks are derived, not ESPN's claim

    if not stored_exists:
        if _is_zero_seed(new_score, new_opponent_score, new_categories):
            return WRITE  # a 0-0 day-0 seed claims nothing
        return WITHHOLD  # no reference to confirm against — totals yes, claim no

    moved = _movement(
        stored_score, stored_opponent_score, stored_categories,
        new_score, new_opponent_score, new_categories,
    )

    if stored_period is None:
        # The stored row is a previous WITHHOLD: its totals are the reference.
        if moved is True:
            return WRITE
        if moved is None:
            return WITHHOLD  # incomparable reference — refresh it
        return WRITE if days_since_stored >= 2 else SKIP

    if new_period <= stored_period:
        return WRITE  # same-day refresh; nothing new is being claimed

    if moved is None:
        return WITHHOLD
    if moved is False:
        # Advanced period, frozen totals: the batch has not landed. The 2-day
        # out is the genuinely idle matchup, whose totals legitimately never
        # move (both rosters scoreless).
        return WRITE if days_since_stored >= 2 else SKIP

    # Totals moved. That confirms the *oldest* missing batch landed — which is
    # everything, when only one period was missing.
    if new_period - stored_period == 1:
        return WRITE
    return WITHHOLD


class DailyMatchupScoresPipeline(BasePipeline):
    """
    Fetch current matchup scores for all saved teams and record daily snapshots.

    This pipeline:
    1. Determines the current matchup period
    2. Fetches all saved user teams
    3. For each team, fetches matchup data from ESPN or Yahoo based on provider
    4. Records daily score snapshots for visualization
    """

    config = PipelineConfig(
        name="daily_matchup_scores",
        display_name="Daily Matchup Scores",
        description="Fetches current matchup scores for all saved teams",
        target_table="stats_s2.daily_matchup_score",
        # ESPN matchup data (totalPoints + lineup slots) isn't ready until ESPN's
        # nightly batch (~2 AM ET), which runs after games end (~midnight–1 AM ET).
        # espn_gated=True tells the post-game endpoint to hold this pipeline until
        # latestScoringPeriod has advanced, with a 2:30 AM ET time fallback.
        category=PipelineCategory.POST_GAME,
        espn_gated=True,
        # ESPN publishes updated matchup scores (totalPoints, lineup slots) at
        # ~2 AM CST nightly. Gate execution until then to avoid writing stale
        # scores that would need to be overwritten anyway.
        earliest_run_time_cst=time(2, 0),
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()
        self.yahoo_extractor = YahooExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the daily matchup scores pipeline."""
        central_tz = pytz.timezone("US/Central")
        today = ctx.started_at.date()

        # Get current matchup info
        matchup_info = self._get_current_matchup_info(today)
        if not matchup_info:
            ctx.log.info("no_active_matchup")
            return

        ctx.log.info(
            "matchup_info",
            matchup_period=matchup_info["matchup_number"],
            day_index=matchup_info["day_index"],
        )

        # Get all saved teams
        teams = list(Team.select())
        ctx.log.info("teams_found", count=len(teams))

        for team in teams:
            try:
                league_info = credential_service.hydrate(team, json.loads(team.league_info))
                team_name = league_info.get("team_name", "")
                provider = league_info.get("provider", "espn")  # Default to ESPN for backward compatibility

                if provider == "yahoo":
                    matchup_data = self._fetch_yahoo_matchup(
                        ctx, team, league_info, team_name, matchup_info["matchup_number"]
                    )
                else:
                    matchup_data = self._fetch_espn_matchup(
                        league_info, team_name, matchup_info["matchup_number"]
                    )

                if matchup_data:
                    # Use provider's matchup period if available (Yahoo may differ
                    # from local schedule numbering), otherwise use local schedule's
                    effective_matchup_period = matchup_data.get(
                        "matchup_period", matchup_info["matchup_number"]
                    )

                    # Compute day_of_matchup from the actual matchup start date.
                    # For 2-week playoff rounds the ESPN matchup period spans two
                    # local schedule weeks, so the extractor returns the real start
                    # date. This gives continuous indices (0-13) across the full
                    # round instead of resetting to 0 in the second week.
                    matchup_start = matchup_data.get("matchup_start")
                    day_of_matchup = (
                        (today - matchup_start).days
                        if matchup_start
                        else matchup_info["day_index"]
                    )

                    # Write-side pairing guard: never let the stored
                    # watermark outrun the totals it claims to cover.
                    stored = (
                        DailyMatchupScore.select()
                        .where(
                            (DailyMatchupScore.team_id == team.team_id)
                            & (DailyMatchupScore.matchup_period == effective_matchup_period)
                        )
                        .order_by(DailyMatchupScore.date.desc())
                        .first()
                    )
                    decision = watermark_decision(
                        stored.scoring_period_id if stored else None,
                        stored.current_score if stored else 0,
                        stored.opponent_current_score if stored else 0,
                        stored.category_scores if stored else None,
                        matchup_data.get("scoring_period_id"),
                        matchup_data.get("scoring_period_source", "unknown"),
                        matchup_data["current_score"],
                        matchup_data["opponent_current_score"],
                        matchup_data.get("category_scores"),
                        days_since_stored=(today - stored.date).days if stored else 0,
                        stored_exists=stored is not None,
                    )
                    if decision == SKIP:
                        ctx.log.info(
                            "watermark_awaiting_totals",
                            team_id=team.team_id,
                            stored_period=stored.scoring_period_id if stored else None,
                            espn_period=matchup_data.get("scoring_period_id"),
                        )
                        ctx.increment_skipped(1, "totals_not_materialized")
                        continue
                    if decision == WITHHOLD:
                        # Store the totals, claim nothing: the read path treats
                        # a null watermark as "don't overlay", and this row is
                        # the reference the next poll's movement check uses.
                        ctx.log.info(
                            "watermark_withheld",
                            team_id=team.team_id,
                            stored_period=stored.scoring_period_id if stored else None,
                            espn_period=matchup_data.get("scoring_period_id"),
                        )
                        matchup_data["scoring_period_id"] = None
                        matchup_data["scoring_period_source"] = "unknown"

                    # Upsert daily score
                    record = {
                        "team_id": team.team_id,
                        "team_name": matchup_data["team_name"],
                        "matchup_period": effective_matchup_period,
                        "opponent_team_name": matchup_data["opponent_team_name"],
                        "date": today,
                        "day_of_matchup": day_of_matchup,
                        "current_score": matchup_data["current_score"],
                        "opponent_current_score": matchup_data["opponent_current_score"],
                        "scoring_period_id": matchup_data.get("scoring_period_id"),
                        "scoring_period_source": matchup_data.get("scoring_period_source", "unknown"),
                        "category_scores": matchup_data.get("category_scores"),
                        "pipeline_run_id": ctx.run_id,
                    }

                    DailyMatchupScore.insert(record).on_conflict(
                        conflict_target=[
                            DailyMatchupScore.team_id,
                            DailyMatchupScore.matchup_period,
                            DailyMatchupScore.date,
                        ],
                        update={
                            "current_score": record["current_score"],
                            "opponent_current_score": record["opponent_current_score"],
                            "team_name": record["team_name"],
                            "opponent_team_name": record["opponent_team_name"],
                            "scoring_period_id": record["scoring_period_id"],
                            "scoring_period_source": record["scoring_period_source"],
                            "category_scores": record["category_scores"],
                            "pipeline_run_id": record["pipeline_run_id"],
                        },
                    ).execute()
                    ctx.increment_records()

                    ctx.log.debug(
                        "team_score_recorded",
                        team=team_name,
                        provider=provider,
                        score=matchup_data["current_score"],
                        opponent_score=matchup_data["opponent_current_score"],
                    )
                else:
                    ctx.increment_skipped(1, "no_matchup_data")

            except Exception as e:
                ctx.log.warning(
                    "team_processing_error",
                    team_id=team.team_id,
                    error=str(e),
                )
                ctx.increment_failed(1, type(e).__name__)
                continue

        if teams and ctx.records_processed == 0:
            raise RuntimeError(
                f"0 of {len(teams)} teams processed — ESPN/Yahoo API may be unavailable or returning no matchup data"
            )

    def _fetch_espn_matchup(
        self,
        league_info: dict,
        team_name: str,
        matchup_period: int,
    ) -> Optional[dict]:
        """Fetch matchup data from ESPN."""
        return self.espn_extractor.get_matchup_data(
            league_id=league_info["league_id"],
            team_name=team_name,
            espn_s2=league_info.get("espn_s2", ""),
            swid=league_info.get("swid", ""),
            year=league_info.get("year", settings.espn_year),
            matchup_period=matchup_period,
        )

    def _fetch_yahoo_matchup(
        self,
        ctx: PipelineContext,
        team: Team,
        league_info: dict,
        team_name: str,
        matchup_period: int,
    ) -> Optional[dict]:
        """Fetch matchup data from Yahoo and handle token refresh."""
        yahoo_team_key = league_info.get("yahoo_team_key")
        if not yahoo_team_key:
            ctx.log.warning("yahoo_team_key_missing", team=team_name)
            return None

        matchup_data, new_tokens = self.yahoo_extractor.get_matchup_data(
            team_key=yahoo_team_key,
            team_name=team_name,
            access_token=league_info.get("yahoo_access_token", ""),
            refresh_token=league_info.get("yahoo_refresh_token"),
            token_expiry=league_info.get("yahoo_token_expiry"),
            matchup_period=matchup_period,
        )

        # If tokens were refreshed, persist them back to the database
        if new_tokens:
            self._update_yahoo_tokens(ctx, team, league_info, new_tokens)

        return matchup_data

    def _update_yahoo_tokens(
        self,
        ctx: PipelineContext,
        team: Team,
        league_info: dict,
        new_tokens: dict,
    ) -> None:
        """Persist refreshed Yahoo tokens wherever this team's credentials live."""
        try:
            # Migrated teams: the encrypted store owns the tokens. Take this
            # branch before touching league_info -- `league_info` here has been
            # hydrated, so writing it back would re-plant the secrets in
            # plaintext in the very column this migration empties.
            if credential_service.update_yahoo_tokens(
                team,
                new_tokens["access_token"],
                new_tokens["refresh_token"],
                new_tokens["token_expiry"],
            ):
                ctx.log.debug("yahoo_tokens_refreshed", team_id=team.team_id, store="encrypted")
                return

            # Not yet migrated: the secrets are still in league_info, so this
            # round-trip is lossless.
            league_info["yahoo_access_token"] = new_tokens["access_token"]
            league_info["yahoo_refresh_token"] = new_tokens["refresh_token"]
            league_info["yahoo_token_expiry"] = new_tokens["token_expiry"]

            team.league_info = json.dumps(league_info)
            team.save()

            ctx.log.debug(
                "yahoo_tokens_refreshed",
                team_id=team.team_id,
            )
        except Exception as e:
            ctx.log.warning(
                "yahoo_token_update_failed",
                team_id=team.team_id,
                error=str(e),
            )

    def _get_current_matchup_info(self, current_date) -> Optional[dict]:
        """Current matchup period and day index from the season calendar (None outside it)."""
        matchup = get_current_matchup(current_date)
        if not matchup:
            return None
        return {
            "matchup_number": matchup["matchup_number"],
            "start_date": matchup["start_date"],
            "end_date": matchup["end_date"],
            "day_index": matchup["current_day_index"],
        }
