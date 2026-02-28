"""
ESPN Injury Status Pipeline

Syncs player injury status from ESPN Fantasy API to nba.player_injuries.

This replaces the commented-out InjuryReportPipeline (which required the paid
BALLDONTLIE All-Star tier). ESPN's kona_player_info response already includes
`injured` and `injuryStatus` fields for all ~1500 tracked players — the same
API call used by the PlayerOwnershipPipeline.
"""

from datetime import date, timedelta

from db.models.nba import Player, PlayerInjury
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig
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

    Players not marked as injured by ESPN are not written — absence of a record
    for today means the player is available. The BreakoutDetectionPipeline reads
    the most recent record per player, so old Out records remain until overwritten.

    Note: ESPN does not provide injury_type/injury_detail in the kona_player_info
    view, so those fields are left null.
    """

    config = PipelineConfig(
        name="espn_injury_status",
        display_name="ESPN Injury Status",
        description="Syncs player injury/availability status from ESPN Fantasy API",
        target_table="nba.player_injuries",
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the ESPN injury status pipeline."""
        if ctx.date_override:
            report_date = ctx.date_override
        else:
            now_cst = ctx.started_at
            if now_cst.hour < 6:
                report_date = (now_cst - timedelta(days=1)).date()
            else:
                report_date = now_cst.date()

        ctx.log.info("fetching_espn_player_data", report_date=str(report_date))

        espn_data = self.espn_extractor.get_player_data()
        ctx.log.info("espn_data_fetched", player_count=len(espn_data))

        # Build ESPN ID → player_id lookup for fast matching
        espn_id_lookup = self._build_espn_id_lookup()
        ctx.log.info("espn_id_lookup_built", count=len(espn_id_lookup))

        # Also build name → player_id as fallback
        name_lookup = self._build_name_lookup()

        matched = 0
        unmatched = 0
        skipped_active = 0

        for normalized_name, info in espn_data.items():
            if not info.get("injured", False):
                skipped_active += 1
                continue

            espn_id = info.get("espn_id")
            raw_status = info.get("injury_status") or "OUT"
            status = ESPN_STATUS_MAP.get(raw_status.upper(), "Out")

            # Try ESPN ID match first (most reliable)
            player_id = espn_id_lookup.get(espn_id) if espn_id else None

            # Fall back to name match
            if not player_id:
                player_id = name_lookup.get(normalized_name)

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
            unmatched=unmatched,
            skipped_active=skipped_active,
            records=ctx.records_processed,
        )

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
