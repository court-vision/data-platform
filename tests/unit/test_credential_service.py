"""
credential_service.persist, data-platform's copy: the connection row is keyed
by the normalized SWID, and new credentials clear the old verdict.

These are the backend's rules (its credential_service, migration 0024). Nothing
in data-platform calls persist today, but it writes the mirrored
ProviderConnection model, whose columns promise both. ProviderConnection's
lookup and insert run over an in-memory list here, matched on the fields the
real query compares.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from cryptography.fernet import Fernet
from peewee import Expression

from core import crypto
from db.models.provider_connections import ProviderConnection
from services import credential_service

SWID = "{3F2A9C1E-1B2C-4D5E-8F90-A1B2C3D4E5F6}"
CHECKED = datetime(2026, 9, 11, tzinfo=timezone.utc)


@pytest.fixture
def keys(monkeypatch):
    from core.settings import settings

    monkeypatch.setattr(settings, "credential_keys", f"1:{Fernet.generate_key().decode()}", raising=False)
    crypto.reset_cache()
    yield
    crypto.reset_cache()


def _equalities(expr) -> dict:
    """{field name: value} for the `a == x & b == y` a lookup was built from."""
    if isinstance(expr, Expression) and expr.op == "AND":
        return {**_equalities(expr.lhs), **_equalities(expr.rhs)}
    return {expr.lhs.name: expr.rhs}


@pytest.fixture
def rows(monkeypatch, keys):
    stored = []

    def get_or_none(cls, expr):
        want = _equalities(expr)
        return next((r for r in stored if all(getattr(r, k) == v for k, v in want.items())), None)

    def create(cls, **fields):
        row = SimpleNamespace(id=len(stored) + 1, saves=0, **fields)
        row.save = lambda: setattr(row, "saves", row.saves + 1)
        stored.append(row)
        return row

    monkeypatch.setattr(ProviderConnection, "get_or_none", classmethod(get_or_none))
    monkeypatch.setattr(ProviderConnection, "create", classmethod(create))
    return stored


def _team():
    return SimpleNamespace(team_id=1, provider_connection_id=None, league_info="{}", save=lambda: None)


def _payload(espn_s2, swid):
    return {"provider": "espn", "league_id": 5, "team_name": "T", "year": 2027, "espn_s2": espn_s2, "swid": swid}


@pytest.mark.unit
def test_normalize_swid_is_the_backends_rule():
    assert credential_service.normalize_swid(" {3f2a9c1e-1b2c-4d5e-8f90-a1b2c3d4e5f6} ") == SWID
    assert credential_service.normalize_swid(SWID.strip("{}")) == SWID
    assert credential_service.normalize_swid(None) == ""


@pytest.mark.unit
@pytest.mark.parametrize("spelling", [SWID.lower(), SWID.strip("{}"), f"  {SWID}\n"])
def test_every_spelling_of_one_swid_is_one_connection(rows, spelling):
    first = credential_service.persist(10, _team(), _payload("AEB-1", SWID))
    again = credential_service.persist(10, _team(), _payload("AEB-2", spelling))
    assert first == again and len(rows) == 1
    assert rows[0].external_account_id == SWID


@pytest.mark.unit
def test_new_credentials_clear_the_old_verdict(rows):
    credential_service.persist(10, _team(), _payload("AEB-1", SWID))
    row = rows[0]
    row.verified_at = row.auth_failed_at = CHECKED

    credential_service.persist(10, _team(), _payload("AEB-2", SWID))

    assert (row.verified_at, row.auth_failed_at) == (None, None) and row.saves == 1


@pytest.mark.unit
def test_resaving_the_same_credentials_keeps_the_verdict(rows):
    credential_service.persist(10, _team(), _payload("AEB-1", SWID))
    row = rows[0]
    row.verified_at = CHECKED

    credential_service.persist(10, _team(), _payload("AEB-1", SWID.lower()))

    assert row.verified_at == CHECKED and row.saves == 0
