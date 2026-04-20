"""
Playoff Series Table

NBA playoff series standings — round, teams, wins, status.
"""

from datetime import datetime

from peewee import (
    CharField,
    SmallIntegerField,
    IntegerField,
    BooleanField,
    DateTimeField,
)

from db.base import BaseModel


class PlayoffSeries(BaseModel):
    """
    One record per playoff series per season.

    Upserted daily (once per night after games finish) by PlayoffBracketPipeline.
    Each record captures the current state of a best-of-7 series.
    """

    # Natural key: season + series_id
    season = CharField(max_length=7, index=True)     # "2025-26"
    series_id = CharField(max_length=20, index=True)  # NBA's series identifier

    # Bracket position
    conference = CharField(max_length=10)             # "East", "West", "Finals"
    round_num = SmallIntegerField()                   # 1=first round, 2=semis, 3=conf finals, 4=Finals

    # Top seed
    top_seed_team_id = IntegerField(null=True)
    top_seed_name = CharField(max_length=50, null=True)
    top_seed_abbr = CharField(max_length=5)
    top_seed_wins = SmallIntegerField(default=0)

    # Bottom seed
    bottom_seed_team_id = IntegerField(null=True)
    bottom_seed_name = CharField(max_length=50, null=True)
    bottom_seed_abbr = CharField(max_length=5)
    bottom_seed_wins = SmallIntegerField(default=0)

    # Status
    series_complete = BooleanField(default=False)
    series_leader_abbr = CharField(max_length=5, null=True)  # abbr of team leading/winning

    updated_at = DateTimeField(default=datetime.utcnow)

    class Meta:
        table_name = "playoff_series"
        schema = "nba"
        indexes = (
            (("season", "series_id"), True),  # Composite unique
        )

    def __repr__(self):
        return (
            f"<PlayoffSeries(season={self.season}, r{self.round_num}, "
            f"{self.top_seed_abbr} {self.top_seed_wins}-{self.bottom_seed_wins} "
            f"{self.bottom_seed_abbr})>"
        )
