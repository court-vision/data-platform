"""Season rankings, in two shapes of the same query (backend migration 0006).

`Rankings` is the materialized copy the API serves; `RankingsSource` is the view
it is materialized from. data-platform refreshes the copy after the post-game
season-stats write — see pipelines/rankings_view.py.

Mirrored from backend/db/models/stats/rankings.py (PRODUCTION_READINESS item 5).
"""

from peewee import BigIntegerField, CharField, DateField, DecimalField, IntegerField, SmallIntegerField

from db.base import BaseModel


class _RankingsRow(BaseModel):
    id = IntegerField()
    curr_rank = BigIntegerField()   # RANK() returns bigint in Postgres
    name = CharField(max_length=100)
    team = CharField(max_length=3, null=True)
    fpts = IntegerField()           # cumulative season total
    avg_fpts = DecimalField(max_digits=6, decimal_places=2)
    rank_change = BigIntegerField()  # prev_rank - curr_rank, both bigint
    gp = SmallIntegerField(null=True)
    as_of_date = DateField(null=True)   # snapshot date this player's row runs through
    season = CharField(max_length=7, null=True)

    class Meta:
        schema = 'nba'
        primary_key = False


class Rankings(_RankingsRow):
    class Meta:
        table_name = 'rankings'


class RankingsSource(_RankingsRow):
    class Meta:
        table_name = 'rankings_source'
