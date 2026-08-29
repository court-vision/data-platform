"""
ESPN Injury Status Pipeline

Syncs player injury status from ESPN Fantasy API to nba.player_injuries.

This replaces the commented-out InjuryReportPipeline (which required the paid
BALLDONTLIE All-Star tier). ESPN's kona_player_info response already includes
`injured` and `injuryStatus` fields for all ~1500 tracked players — the same
API call used by the PlayerOwnershipPipeline.
"""

from datetime import date

from db.models.nba import Player, PlayerInjury
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor
from pipelines.transformers import normalize_name


# Map ESPN's injuryStatus values to our normalized status vocabulary
ESPN_STATUS_MAP: dict[str, str] = {
    "OUT": "Out",
    "SUSPENSION": "Out",
    "DOUBTFUL": "Doubtful",
    "QUESTIONABLE": "Questionable",
    "DAY_TO_DAY": "Questionable",
    "PROBABLE": "Probable",
    "ACTIVE": "Available",
    "INJURY_RESERVE": "Out",
}


class ESPNInjuryStatusPipeline(BasePipeline):
    """
    Fetch current player injury status from ESPN Fantasy API.

    This pipeline:
    1. Calls ESPNExtractor.get_player_data() (same call as PlayerOwnershipPipeline)
    2. For each player where injured=True, maps ESPN status to our vocabulary
    3. Matches players by ESPN ID (cross-referenced in Player dimension)
    4. Upserts into nba.player_injuries using PlayerInjury.upsert_injury()

    For players ESPN marks as active (injured=False), we write an "Available"
    record if they have a stale non-Available record in the DB. This clears
    stale Out/Doubtful/etc. records for players who have returned from injury.

    Note: ESPN does not provide injury_type/injury_detail in the kona_player_info
    view, so those fields are left null.
    """

    config = PipelineConfig(
        name="espn_injury_status",
        display_name="ESPN Injury Status",
        description="Syncs player injury/availability status from ESPN Fantasy API",
        target_table="nba.player_injuries",
        category=PipelineCategory.PRE_GAME,
        pre_game_window_minutes=120,
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the ESPN injury status pipeline."""
        report_date = ctx.game_date()

        ctx.log.info("fetching_espn_player_data", report_date=str(report_date))

        espn_data = self.espn_extractor.get_player_data()
        ctx.log.info("espn_data_fetched", player_count=len(espn_data))

        # Build ESPN ID → player_id lookup for fast matching
        espn_id_lookup = self._build_espn_id_lookup()
        ctx.log.info("espn_id_lookup_built", count=len(espn_id_lookup))

        # Also build name → player_id as fallback
        name_lookup = self._build_name_lookup()

        # Load player_ids with a stale non-Available record so we can clear them
        # when ESPN now shows them as active (returned from injury).
        stale_injured_ids = self._get_stale_injured_player_ids(report_date)
        ctx.log.info("stale_injured_player_ids_loaded", count=len(stale_injured_ids))

        matched = 0
        cleared = 0
        unmatched = 0
        skipped_active = 0

        for normalized_name, info in espn_data.items():
            espn_id = info.get("espn_id")

            # Resolve player_id regardless of injury flag (needed for both paths)
            player_id = espn_id_lookup.get(espn_id) if espn_id else None
            if not player_id:
                player_id = name_lookup.get(normalized_name)

            if not info.get("injured", False):
                # Player is active — clear any stale non-Available record
                if player_id and player_id in stale_injured_ids:
                    PlayerInjury.upsert_injury(
                        player_id=player_id,
                        report_date=report_date,
                        status="Available",
                        injury_type=None,
                        injury_detail=None,
                        expected_return=None,
                        pipeline_run_id=ctx.run_id,
                    )
                    cleared += 1
                else:
                    skipped_active += 1
                continue

            raw_status = info.get("injury_status") or "OUT"
            status = ESPN_STATUS_MAP.get(raw_status.upper(), "Out")

            if not player_id:
                ctx.log.debug(
                    "player_not_matched",
                    name=normalized_name,
                    espn_id=espn_id,
                    status=status,
                )
                unmatched += 1
                continue

            PlayerInjury.upsert_injury(
                player_id=player_id,
                report_date=report_date,
                status=status,
                injury_type=None,    # ESPN kona_player_info doesn't expose this
                injury_detail=None,
                expected_return=None,
                pipeline_run_id=ctx.run_id,
            )
            ctx.increment_records()
            matched += 1

        ctx.log.info(
            "processing_complete",
            matched=matched,
            cleared=cleared,
            unmatched=unmatched,
            skipped_active=skipped_active,
            records=ctx.records_processed,
        )

    def _get_stale_injured_player_ids(self, report_date: date) -> set[int]:
        """
        Return player_ids whose most recent injury record is non-Available.

        Used to detect players who have returned from injury: ESPN now shows
        them as active but we have a stale Out/Doubtful/etc. record in the DB.
        """
        from peewee import fn

        subquery = (
            PlayerInjury.select(
                PlayerInjury.player_id,
                fn.MAX(PlayerInjury.report_date).alias("max_date"),
            )
            .where(PlayerInjury.report_date <= report_date)
            .group_by(PlayerInjury.player_id)
        )
        stale = (
            PlayerInjury.select(PlayerInjury.player_id)
            .join(
                subquery,
                on=(
                    (PlayerInjury.player_id == subquery.c.player_id)
                    & (PlayerInjury.report_date == subquery.c.max_date)
                ),
            )
            .where(PlayerInjury.status != "Available")
        )
        return {row.player_id for row in stale}

    def _build_espn_id_lookup(self) -> dict[int, int]:
        """Build lookup dict mapping espn_id → player_id."""
        lookup = {}
        for player in Player.select(Player.id, Player.espn_id).where(Player.espn_id.is_null(False)):
            lookup[player.espn_id] = player.id
        return lookup

    def _build_name_lookup(self) -> dict[str, int]:
        """Build lookup dict mapping normalized name → player_id."""
        lookup = {}
        for player in Player.select(Player.id, Player.name_normalized):
            if player.name_normalized:
                lookup[player.name_normalized] = player.id
        return lookup
