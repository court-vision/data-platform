from playhouse.pool import PooledPostgresqlExtDatabase
from playhouse.db_url import parse
from peewee import Model
import os

# Get database credentials from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
parsed_url = parse(DATABASE_URL)
db_name = parsed_url.pop('database')

db = PooledPostgresqlExtDatabase(
    db_name,
    max_connections=20,
    stale_timeout=300,
    **parsed_url
)

class BaseModel(Model):
    class Meta:
        database = db

def init_db():
    """Open the connection pool and refuse to start against an unmigrated database.

    The schema is owned by the backend repo's migration chain (backend/migrations);
    data-platform reads and writes these tables but never creates or alters them.
    Fresh environments must deploy the backend first.
    """
    db.connect()
    cursor = db.execute_sql("SELECT to_regclass('public._yoyo_migration')")
    if cursor.fetchone()[0] is None:
        raise RuntimeError(
            "Database has no migration state — deploy/run the backend first "
            "(it applies backend/migrations on startup)."
        )

# Function to close database connection
def close_db():
    """Close database connection."""
    if not db.is_closed():
        db.close()
        print("Database connection closed")
