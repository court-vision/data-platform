"""
`DataQualityService.run_checks` posts one `quality_critical` alert per critical
check that failed or errored (never for warnings), deduped per check for 24 h.
The run/check models and `db.execute_sql` are stubbed — no database.
"""

import uuid
from datetime import timedelta
from types import SimpleNamespace

import pytest

from services import data_quality_service as dq_module
from services.data_quality_service import DataQualityService, SQLQualityCheck

CHECKS = {
    "crit_fail": SQLQualityCheck("crit_fail", "critical", "SELECT crit_fail", "null player ids"),
    "crit_ok": SQLQualityCheck("crit_ok", "critical", "SELECT crit_ok", "never"),
    "crit_error": SQLQualityCheck("crit_error", "critical", "SELECT crit_error", "unused"),
    "warn_fail": SQLQualityCheck("warn_fail", "warning", "SELECT warn_fail", "stale runs"),
}
COUNTS = {"SELECT crit_fail": 3, "SELECT crit_ok": 0, "SELECT warn_fail": 1}


class FakeRun:
    def __init__(self, triggered_by):
        self.id = uuid.uuid4()
        self.triggered_by = triggered_by
        self.completed = None

    def mark_completed(self, **fields):
        self.completed = fields


@pytest.fixture
def service(monkeypatch):
    created = []

    def execute_sql(sql):
        if sql == "SELECT crit_error":
            raise RuntimeError("relation does not exist")
        return SimpleNamespace(fetchone=lambda: (COUNTS[sql],))

    monkeypatch.setattr(dq_module.db, "execute_sql", execute_sql)
    monkeypatch.setattr(dq_module.DataQualityRun, "start_run", staticmethod(lambda triggered_by: FakeRun(triggered_by)))
    monkeypatch.setattr(dq_module.DataQualityCheck, "create", staticmethod(lambda **fields: created.append(fields)))

    svc = DataQualityService()
    svc._checks = dict(CHECKS)
    svc.created = created
    return svc


@pytest.mark.unit
def test_failed_and_errored_critical_checks_alert_once_each(service, alerts):
    run = service.run_checks(list(CHECKS), triggered_by="schedule")

    assert run.completed == {"total_checks": 4, "passed_checks": 1, "failed_checks": 3}
    assert {c["check_name"]: c["status"] for c in service.created} == {
        "crit_fail": "failed", "crit_ok": "passed", "crit_error": "error", "warn_fail": "failed",
    }

    assert alerts.keys() == ["quality_critical:crit_fail", "quality_critical:crit_error"]
    failed, errored = alerts.events
    assert failed.severity == "critical"
    assert failed.title == "Data quality: crit_fail"
    assert failed.body == "null player ids"
    assert failed.fields["failures"] == 3 and failed.fields["status"] == "failed"
    assert failed.fields["triggered_by"] == "schedule" and failed.fields["run_id"] == str(run.id)
    assert failed.dedupe == timedelta(hours=24)
    assert errored.fields["status"] == "error"
    assert "relation does not exist" in errored.body


@pytest.mark.unit
def test_repeat_runs_are_deduped_per_check(service, alerts):
    service.run_checks(["crit_fail"])
    service.run_checks(["crit_fail"])
    assert alerts.keys() == ["quality_critical:crit_fail"]


@pytest.mark.unit
def test_warnings_and_passing_checks_never_alert(service, alerts):
    service.run_checks(["crit_ok", "warn_fail"])
    assert alerts.events == []
