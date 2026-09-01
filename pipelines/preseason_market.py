"""
Preseason Market Pipeline

Daily draft-prep snapshot: ESPN editorial draft ranks and auction values plus
the crowd averages from real ESPN drafts (ADP, average auction price) into
nba.draft_market, and — once ESPN publishes them — projected per-game stat
lines into nba.player_projections. The market row also carries the position
fields the draft room needs (primary position, eligibility, injury), which
nba_api's coarse "G"/"F-C" positions cannot supply.

Gated to the preseason window (Aug 15 – Oct 31). Tolerant of the two known
late-arrival states: the public league not yet rolled to the target season
(extractor returns None) and projections not yet published (rows carry no
projected split). Unresolvable players are counted as skips, not failures —
draft-eligible rookies routinely reach ESPN before nba.players has them.
"""

from core.settings import settings
from db.base import db
from db.models.nba import DraftMarket, Player, PlayerProjection
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import ESPNExtractor
from pipelines.gates import preseason_market_window
from utils.stat_vocab import ESPN_ID_TO_KEY

# Presence of any of these is what makes a row worth a market snapshot. The
# position fields ride along on that row rather than gating it: a player ESPN
# has no draft opinion about is not draft-market data just because he has a
# primary position.
_MARKET_FIELDS = ("overall_rank", "auction_value", "adp", "auction_value_avg")


def projection_line(average_stats: dict) -> dict:
    """ESPN averageStats (keyed by stat id) -> our stat keys, counting stats only.

    Keeps just PlayerProjection.STAT_KEYS; rate ids and anything unmapped are
    dropped (rates are recomputed from makes/attempts at read time).
    """
    line: dict[str, float] = {}
    for stat_id, value in (average_stats or {}).items():
        try:
            key = ESPN_ID_TO_KEY.get(int(stat_id))
            value = float(value)
        except (TypeError, ValueError):
            continue
        if key in PlayerProjection.STAT_KEYS:
            line[key] = round(value, 2)
    return line


def projected_gp(total, avg) -> int | None:
    """Games played implied by ESPN's applied total/average, when both exist."""
    try:
        total, avg = float(total), float(avg)
    except (TypeError, ValueError):
        return None
    return round(total / avg) if avg else None


class PreseasonMarketPipeline(BasePipeline):
    """
    Snapshot ESPN draft-market data (and projections, once published) daily
    through the preseason.
    """

    config = PipelineConfig(
        name="preseason_market",
        display_name="Preseason Market",
        description="Snapshots ESPN draft ranks/ADP and preseason stat projections",
        target_table="nba.draft_market",
        category=PipelineCategory.SCHEDULED,
    )

    def __init__(self):
        super().__init__()
        self.espn_extractor = ESPNExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        as_of_date = ctx.game_date()
        season = settings.nba_season

        gate = preseason_market_window(as_of_date)
        if not gate.run:
            ctx.log.info("preseason_market_gated", reason=gate.reason, **gate.detail)
            return

        league_id = (ctx.options or {}).get("league_id")
        rows = self.espn_extractor.get_draft_market_data(league_id=league_id)
        if rows is None:
            ctx.log.warning(
                "preseason_market_league_not_rolled",
                espn_year=settings.espn_year,
                league_id=league_id or settings.espn_league_id,
            )
            return

        ctx.log.info("draft_market_fetched", player_count=len(rows), as_of_date=str(as_of_date))

        espn_ids = [row["espn_id"] for row in rows]
        by_espn_id = {
            p.espn_id: p
            for p in Player.select().where(Player.espn_id.in_(espn_ids))
        } if espn_ids else {}

        # Publish the day's snapshot atomically: readers resolve "the" snapshot
        # as max(as_of_date), so the first committed row would make a still-
        # partial day the latest. All-or-nothing keeps a mid-run failure from
        # superseding yesterday's complete snapshot.
        projections_written = 0
        with db.atomic():
            for row in rows:
                player = by_espn_id.get(row["espn_id"]) or Player.find_by_name(row["normalized_name"])
                if player is None:
                    ctx.increment_skipped(1, "unresolved_player")
                    continue

                has_market = any(row[field] is not None for field in _MARKET_FIELDS)
                line = projection_line(row["projected_stats"]) if row["projected_stats"] else {}
                if not has_market and not line:
                    ctx.increment_skipped(1, "no_market_data")
                    continue

                if has_market:
                    DraftMarket.record_market(
                        player_id=player.id,
                        season=season,
                        as_of_date=as_of_date,
                        overall_rank=row["overall_rank"],
                        auction_value=row["auction_value"],
                        adp=row["adp"],
                        auction_value_avg=row["auction_value_avg"],
                        default_position_id=row["default_position_id"],
                        eligible_slot_ids=row["eligible_slot_ids"],
                        injury_status=row["injury_status"],
                        pipeline_run_id=ctx.run_id,
                    )
                if line:
                    PlayerProjection.record_projection(
                        player_id=player.id,
                        season=season,
                        as_of_date=as_of_date,
                        line=line,
                        projected_gp=projected_gp(row["projected_total"], row["projected_avg"]),
                        raw={
                            "applied_total": row["projected_total"],
                            "applied_avg": row["projected_avg"],
                        },
                        pipeline_run_id=ctx.run_id,
                    )
                    projections_written += 1

                ctx.increment_records()

        ctx.log.info(
            "preseason_market_complete",
            records=ctx.records_processed,
            projections=projections_written,
            skipped=ctx.records_skipped,
        )
