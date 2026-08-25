"""
Playoff Bracket Pipeline

Fetches current NBA playoff series standings from NBA Stats API and
upserts them into nba.playoff_series. Runs once nightly after games
complete (~1 AM ET via the 'playoffs' cron job).
"""

from datetime import datetime

from core.season import season_for_date
from db.models.nba import PlayoffSeries
from db.models.nba.teams import NBATeam
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor


class PlayoffBracketPipeline(BasePipeline):
    """
    Fetch playoff series standings and write to nba.playoff_series.

    Uses CommonPlayoffSeries (game structure) + LeagueGameLog (results) to
    compute per-series win counts and store enough to render a bracket:
    conference, round, teams (abbr + id), wins per team.
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
        season = season_for_date(ctx.started_at.date())

        ctx.log.info("fetching_playoff_bracket", season=season)

        df = self.nba_extractor.get_playoff_bracket(season)

        if df is None or df.empty:
            ctx.log.warning("no_playoff_data")
            return

        ctx.log.debug("raw_columns", columns=list(df.columns))

        # Pre-load abbreviation → conference from nba.teams (avoids per-row queries).
        # NBATeam.id is the 3-letter abbreviation (primary key), not the numeric NBA API id.
        team_conf: dict[str, str] = {}
        for team in NBATeam.select(NBATeam.id, NBATeam.conference):
            team_conf[str(team.id)] = team.conference

        upserted = 0
        for _, row in df.iterrows():
            row = row.to_dict()

            series_id = str(row.get("SERIES_ID", ""))
            if not series_id:
                continue

            round_num = int(row.get("ROUND_NUM", 0))

            top_id = row.get("HOME_TEAM_ID")
            top_name = row.get("HOME_TEAM_CITY", "")
            top_abbr = row.get("HOME_TEAM_ABBREVIATION", "")
            top_wins = int(row.get("HOME_TEAM_WINS", 0))

            bottom_id = row.get("VISITOR_TEAM_ID")
            bottom_name = row.get("VISITOR_TEAM_CITY", "")
            bottom_abbr = row.get("VISITOR_TEAM_ABBREVIATION", "")
            bottom_wins = int(row.get("VISITOR_TEAM_WINS", 0))

            if round_num == 4:
                conference = "Finals"
            else:
                conference = team_conf.get(top_abbr, "Unknown") if top_abbr else "Unknown"

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
