"""
Playoff Bracket Pipeline

Fetches current NBA playoff series standings from NBA Stats API and
upserts them into nba.playoff_series. Runs once nightly after games
complete (~1 AM ET via the 'playoffs' cron job).
"""

from datetime import datetime

from db.models.nba import PlayoffSeries
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor


class PlayoffBracketPipeline(BasePipeline):
    """
    Fetch playoff series standings and write to nba.playoff_series.

    Columns returned by SeriesStandings vary slightly by NBA API version.
    This pipeline normalizes what it can and stores enough to render
    a bracket: conference, round, teams (abbr + id), wins per team.
    """

    config = PipelineConfig(
        name="playoff_bracket",
        display_name="Playoff Bracket",
        description="Fetches NBA playoff series standings from NBA Stats API",
        target_table="nba.playoff_series",
        category=PipelineCategory.SCHEDULED,
    )

    def __init__(self):
        super().__init__()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Fetch and upsert playoff series standings."""
        now = ctx.started_at

        # Season string: "2025-26" format
        if now.month >= 8:
            season = f"{now.year}-{str(now.year + 1)[-2:]}"
        else:
            season = f"{now.year - 1}-{str(now.year)[-2:]}"

        # Playoff season_id: "4" prefix + start year (e.g. "42025" for 2025-26)
        start_year = int(season.split("-")[0])
        playoff_season_id = f"4{start_year}"

        ctx.log.info("fetching_playoff_bracket", season=season, season_id=playoff_season_id)

        df = self.nba_extractor.get_playoff_bracket(playoff_season_id)

        if df is None or df.empty:
            ctx.log.warning("no_playoff_data")
            return

        ctx.log.debug("raw_columns", columns=list(df.columns))

        # Normalize column names to uppercase for consistent access
        df.columns = [c.upper() for c in df.columns]

        # Map conference label from conference number or series_id prefix
        def _conference(row) -> str:
            conf_raw = str(row.get("CONFERENCE", "")).upper()
            if "EAST" in conf_raw or conf_raw == "E":
                return "East"
            if "WEST" in conf_raw or conf_raw == "W":
                return "West"
            # Fallback: round 4 = Finals
            if row.get("ROUND_NUM", 0) == 4:
                return "Finals"
            return conf_raw or "Unknown"

        upserted = 0
        for _, row in df.iterrows():
            row = row.to_dict()

            series_id = str(row.get("SERIES_ID", ""))
            if not series_id:
                continue

            round_num = int(row.get("ROUND_NUM", 0))
            conference = _conference(row)

            # Home team = top seed in NBA's convention for SeriesStandings
            top_id = row.get("HOME_TEAM_ID") or row.get("TEAM_ID_HOME")
            top_name = row.get("HOME_TEAM_CITY") or row.get("TEAM_CITY_HOME", "")
            top_abbr = row.get("HOME_TEAM_ABBREVIATION") or row.get("TEAM_ABBREVIATION_HOME", "")
            top_wins = int(row.get("HOME_TEAM_WINS", 0))

            bottom_id = row.get("VISITOR_TEAM_ID") or row.get("TEAM_ID_AWAY")
            bottom_name = row.get("VISITOR_TEAM_CITY") or row.get("TEAM_CITY_AWAY", "")
            bottom_abbr = row.get("VISITOR_TEAM_ABBREVIATION") or row.get("TEAM_ABBREVIATION_AWAY", "")
            bottom_wins = int(row.get("VISITOR_TEAM_WINS", row.get("HOME_TEAM_LOSSES", 0)))

            # Series complete when either team reaches 4 wins
            series_complete = top_wins == 4 or bottom_wins == 4

            if top_wins > bottom_wins:
                leader = top_abbr
            elif bottom_wins > top_wins:
                leader = bottom_abbr
            else:
                leader = None

            try:
                record, created = PlayoffSeries.get_or_create(
                    season=season,
                    series_id=series_id,
                    defaults=dict(
                        conference=conference,
                        round_num=round_num,
                        top_seed_team_id=int(top_id) if top_id else None,
                        top_seed_name=str(top_name),
                        top_seed_abbr=str(top_abbr),
                        top_seed_wins=top_wins,
                        bottom_seed_team_id=int(bottom_id) if bottom_id else None,
                        bottom_seed_name=str(bottom_name),
                        bottom_seed_abbr=str(bottom_abbr),
                        bottom_seed_wins=bottom_wins,
                        series_complete=series_complete,
                        series_leader_abbr=leader,
                        updated_at=datetime.utcnow(),
                    ),
                )

                if not created:
                    PlayoffSeries.update(
                        conference=conference,
                        round_num=round_num,
                        top_seed_team_id=int(top_id) if top_id else None,
                        top_seed_name=str(top_name),
                        top_seed_abbr=str(top_abbr),
                        top_seed_wins=top_wins,
                        bottom_seed_team_id=int(bottom_id) if bottom_id else None,
                        bottom_seed_name=str(bottom_name),
                        bottom_seed_abbr=str(bottom_abbr),
                        bottom_seed_wins=bottom_wins,
                        series_complete=series_complete,
                        series_leader_abbr=leader,
                        updated_at=datetime.utcnow(),
                    ).where(
                        PlayoffSeries.season == season,
                        PlayoffSeries.series_id == series_id,
                    ).execute()

                upserted += 1

            except Exception as e:
                ctx.log.error("upsert_failed", series_id=series_id, error=str(e))

        ctx.increment_records(upserted)
        ctx.log.info("playoff_bracket_complete", upserted=upserted)
