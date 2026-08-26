import asyncio
import os
from typing import Any, Callable, TypeVar

from peewee import Model
from playhouse.db_url import parse
from playhouse.pool import PooledPostgresqlExtDatabase

from core.logging import get_logger

# Get database credentials from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
parsed_url = parse(DATABASE_URL)
db_name = parsed_url.pop('database')

db = PooledPostgresqlExtDatabase(
    db_name,
    max_connections=20,
    stale_timeout=300,
    # psycopg2 connect_timeout: a dead/unreachable host fails fast (503) instead of hanging a worker
    connect_timeout=10,
    **parsed_url
)

log = get_logger("db")

T = TypeVar("T")


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
        log.info("database_connection_closed")


async def run_in_db_thread(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run blocking DB work in a worker thread on that thread's own pooled connection.

    Peewee connection state is thread-local: the request's connection (opened by
    `core.db_middleware` on the event-loop thread) is invisible to
    `asyncio.to_thread` workers. `connection_context()` checks a connection out
    for the call and returns it to the pool afterwards, so nothing leaks.
    """

    def _call() -> T:
        with db.connection_context():
            return fn(*args, **kwargs)

    return await asyncio.to_thread(_call)
