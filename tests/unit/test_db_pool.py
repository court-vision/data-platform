"""
How long the connection pool keeps a connection.

Peewee's pool drops a connection once it is `stale_timeout` seconds old,
counted from when it was opened, and opens replacements one at a time under
the pool lock. At the old 300 s every connection died five minutes after it
opened, and the next burst queued one TCP+TLS+SCRAM handshake per connection
(14-28 ms each in prod). The age limit is now a setting, an hour by default.
"""

import pytest

from core.settings import Settings, settings
from db.base import db


@pytest.mark.unit
def test_the_pool_keeps_a_connection_for_an_hour_by_default():
    assert Settings.model_fields["db_pool_stale_timeout_seconds"].default == 3600
    assert db._stale_timeout == settings.db_pool_stale_timeout_seconds


@pytest.mark.unit
def test_the_age_limit_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("DB_POOL_STALE_TIMEOUT_SECONDS", "1800")
    assert Settings().db_pool_stale_timeout_seconds == 1800
