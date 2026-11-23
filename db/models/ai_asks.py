"""
usr.ai_asks — what users asked Dime to do at a time (backend migration 0055; the
Dime desk redesign, step 2), as the data-platform sees them: the dime_asks
pipeline's target table, for the freshness board. Read-only here, and only the
columns the board reads; the backend's db/models/autopilot.py is the whole model,
and the backend writes every row (data-platform only asks it to run what is due).
"""

from peewee import BigAutoField, DateTimeField, IntegerField, TextField

from db.base import BaseModel


class AiAsk(BaseModel):
    id = BigAutoField(primary_key=True)
    team_id = IntegerField()
    state = TextField()              # pending | started | cancelled | missed | skipped | failed
    due_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        table_name = "ai_asks"
        schema = "usr"
