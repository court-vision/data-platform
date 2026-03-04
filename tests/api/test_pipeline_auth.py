import importlib

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials


@pytest.fixture(autouse=True)
def _reset_pipeline_auth_module():
    """Prevent module-level token state from leaking across tests."""
    yield
    pipeline_auth = importlib.import_module("core.pipeline_auth")
    importlib.reload(pipeline_auth)


@pytest.mark.api
def test_verify_pipeline_token_accepts_valid_bearer(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_API_TOKEN", "token-123")
    pipeline_auth = importlib.import_module("core.pipeline_auth")
    importlib.reload(pipeline_auth)

    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="token-123")
    assert pipeline_auth.verify_pipeline_token(creds) == "token-123"


@pytest.mark.api
def test_verify_pipeline_token_rejects_invalid_bearer(monkeypatch) -> None:
    monkeypatch.setenv("PIPELINE_API_TOKEN", "token-123")
    pipeline_auth = importlib.import_module("core.pipeline_auth")
    importlib.reload(pipeline_auth)

    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="wrong-token")
    with pytest.raises(HTTPException) as exc:
        pipeline_auth.verify_pipeline_token(creds)

    assert exc.value.status_code == 401
    assert exc.value.detail == "Invalid pipeline authentication token"


@pytest.mark.api
def test_verify_pipeline_token_requires_server_configuration(monkeypatch) -> None:
    monkeypatch.delenv("PIPELINE_API_TOKEN", raising=False)
    pipeline_auth = importlib.import_module("core.pipeline_auth")
    importlib.reload(pipeline_auth)

    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="anything")
    with pytest.raises(HTTPException) as exc:
        pipeline_auth.verify_pipeline_token(creds)

    assert exc.value.status_code == 500
    assert "PIPELINE_API_TOKEN not set" in exc.value.detail
