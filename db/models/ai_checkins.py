"""
usr.ai_checkins — Dime's check-ins (backend migration 0042; the Dime autopilot
plan, step 3), as the data-platform sees them: the dime_checkins pipeline's
target table, for the freshness board. Read-only here, and only the columns the
board reads; the backend's db/models/autopilot.py is the whole model, and the
backend writes every row (data-platform only asks it to start what is due).
"""

from peewee import BigAutoField, BooleanField, DateField, DateTimeField, IntegerField, TextField

from db.base import BaseModel


class AiCheckin(BaseModel):
    id = BigAutoField(primary_key=True)
    team_id = IntegerField()
    kind = TextField()               # morning | weekly | pre_lock | night
    nba_date = DateField()           # the fantasy day it is about
    manual = BooleanField()
    state = TextField()              # running | done | failed | missed | skipped
    started_at = DateTimeField()

    class Meta:
        table_name = "ai_checkins"
        schema = "usr"
