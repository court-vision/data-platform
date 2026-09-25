"""
The public app's OpenAPI schema: not served, but a contract surface.

Not served, because data.courtvision.dev is the one port the internet can
reach and /openapi.json alone lists every trigger route. A contract surface,
because the dashboard's types are generated from `scripts/export_openapi.py`,
so a route without a response model generates `unknown` and drops out of the
contract without anything failing.
"""

import pytest

from api.v1.dashboard import trigger_endpoint
from main_public import app
from pipelines import PIPELINE_REGISTRY


@pytest.fixture(scope="module")
def schemas():
    return app.openapi()["components"]["schemas"]


@pytest.fixture(scope="module")
def paths():
    return app.openapi()["paths"]


def _response_ref(paths, path, method):
    """Name of the 200-response model for a route, or None."""
    schema = (
        paths[path][method]
        .get("responses", {})
        .get("200", {})
        .get("content", {})
        .get("application/json", {})
        .get("schema")
    ) or {}
    return schema.get("$ref", "").rsplit("/", 1)[-1] or None


@pytest.mark.api
class TestPublicAppServesNoDocs:
    def test_no_schema_or_docs_routes(self):
        served = {route.path for route in app.routes}
        assert not served & {"/openapi.json", "/docs", "/redoc"}

    def test_schema_still_builds_offline(self, paths):
        assert "/v1/dashboard/status" in paths

    def test_the_legacy_redirect_is_not_part_of_the_contract(self, paths):
        assert "/v1/dashboard" not in paths

    def test_schema_does_not_depend_on_whether_the_dashboard_is_built(self, paths):
        # `/` is index.html with a build and a JSON stub without one. Either in
        # the schema would make the export differ between a laptop and CI.
        assert "/" not in paths
        assert not any("{path}" in path for path in paths)


@pytest.mark.api
class TestDashboardRoutesHaveConcreteSchemas:
    @pytest.mark.parametrize("path,method,ref", [
        ("/v1/dashboard/status", "get", "DashboardStatusResponse"),
        ("/v1/internal/quality/run", "post", "DataQualityRunResponse"),
        ("/v1/internal/quality/runs", "get", "DataQualityRunListResponse"),
        ("/v1/internal/quality/runs/{run_id}", "get", "DataQualityRunResponse"),
        ("/v1/internal/pipelines/jobs", "get", "JobListResponse"),
    ])
    def test_declares_its_response_model(self, paths, path, method, ref):
        assert _response_ref(paths, path, method) == ref

    @pytest.mark.parametrize("name,cls", sorted(PIPELINE_REGISTRY.items()))
    def test_every_trigger_route_declares_a_response_model(self, paths, name, cls):
        endpoint = trigger_endpoint(cls.config)
        assert _response_ref(paths, endpoint, "post"), (
            f"POST {endpoint} ({name}) has no response model"
        )


@pytest.mark.api
class TestSchemaNamesDoNotCollide:
    """Two models with one class name make the export non-deterministic.

    FastAPI keeps one under the bare class name and qualifies the other by
    module (`schemas__player__GameLog`), and which one wins follows hash
    ordering. It happened in backend: the same code exported two different
    schemas from one process to the next.
    """

    def test_no_schema_name_is_module_qualified(self, schemas):
        qualified = sorted(k for k in schemas if "__" in k)
        assert qualified == [], (
            "These schema names are module-qualified, which FastAPI only does when two "
            f"models share a class name: {qualified}. Rename one of each colliding pair — "
            "while the collision stands, the export is not reproducible."
        )
