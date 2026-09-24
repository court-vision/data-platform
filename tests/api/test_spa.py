"""
`core.spa.mount_dashboard`: the React dashboard served beside the API.

Every test builds its own `dist/` in a temp directory, so none depends on
whether `bun run build` has been run in this checkout.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.middleware import setup_middleware
from core.spa import IMMUTABLE, REVALIDATE, mount_dashboard

INDEX_HTML = "<!doctype html><title>dashboard</title><div id=root></div>"
BUNDLE_JS = "console.log('bundle')"


@pytest.fixture
def dist(tmp_path):
    root = tmp_path / "dist"
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text(INDEX_HTML)
    (root / "favicon.svg").write_text("<svg/>")
    (root / "assets" / "index-abc123.js").write_text(BUNDLE_JS)
    # Outside dist: must never be reachable.
    (tmp_path / "secret.txt").write_text("PIPELINE_API_TOKEN=hunter2")
    return root


@pytest.fixture
def client(dist):
    # As main_public.py: no docs routes, and the JSON error envelope.
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    setup_middleware(app)

    @app.get("/ping")
    async def ping():
        return {"message": "Pong!"}

    @app.post("/v1/internal/pipelines/post-game")
    async def trigger():
        return {"status": "success"}

    assert mount_dashboard(app, dist) is True
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.api
class TestServesTheApp:
    def test_root_is_index_html_and_revalidates(self, client):
        res = client.get("/")
        assert res.status_code == 200
        assert res.text == INDEX_HTML
        assert res.headers["content-type"].startswith("text/html")
        assert res.headers["cache-control"] == REVALIDATE

    @pytest.mark.parametrize("path", ["/pipelines/player_game_stats", "/schedule", "/a/b/c"])
    def test_client_side_routes_get_index_html(self, client, path):
        res = client.get(path)
        assert res.status_code == 200
        assert res.text == INDEX_HTML

    def test_hashed_assets_are_immutable(self, client):
        res = client.get("/assets/index-abc123.js")
        assert res.status_code == 200
        assert res.text == BUNDLE_JS
        assert res.headers["cache-control"] == IMMUTABLE

    def test_a_real_file_at_the_dist_root_is_served_as_itself(self, client):
        res = client.get("/favicon.svg")
        assert res.status_code == 200
        assert res.text == "<svg/>"
        assert "svg" in res.headers["content-type"]

    def test_head_works_for_uptime_checks(self, client):
        assert client.head("/").status_code == 200


@pytest.mark.api
class TestNeverAnswersTheApiWithHtml:
    def test_registered_routes_still_win(self, client):
        assert client.get("/ping").json() == {"message": "Pong!"}
        assert client.post("/v1/internal/pipelines/post-game").json() == {"status": "success"}

    @pytest.mark.parametrize("path", ["/v1/nonexistent", "/v1/internal/pipelines/typo", "/v1"])
    def test_unknown_api_paths_stay_json_404s(self, client, path):
        res = client.get(path)
        assert res.status_code == 404
        assert res.headers["content-type"].startswith("application/json")
        assert res.json()["status"] != "success"

    def test_an_unknown_post_is_404_not_405(self, client):
        # 405 would say "that trigger route exists" to whoever mistyped it.
        assert client.post("/v1/internal/pipelines/typo").status_code == 404
        assert client.post("/anything").status_code == 404

    def test_a_missing_asset_is_404_not_html(self, client):
        res = client.get("/assets/index-stale.js")
        assert res.status_code == 404
        assert INDEX_HTML not in res.text

    @pytest.mark.parametrize("path", ["/openapi.json", "/docs", "/redoc"])
    def test_docs_paths_are_not_the_app(self, client, path):
        assert client.get(path).status_code == 404

    @pytest.mark.parametrize("path", ["/health/", "/ping/", "/docs/", "/v1/"])
    def test_a_trailing_slash_does_not_make_a_probe_the_app(self, client, path):
        """The catch-all stops Starlette's slash redirect; /health/ must not read 200."""
        res = client.get(path)
        assert res.status_code == 404
        assert INDEX_HTML not in res.text

    def test_index_html_by_name_still_revalidates(self, client):
        res = client.get("/index.html")
        assert res.status_code == 200
        assert res.headers["cache-control"] == REVALIDATE


@pytest.mark.api
class TestStaysInsideDist:
    @pytest.mark.parametrize("path", [
        "/%2e%2e/secret.txt",
        "/..%2fsecret.txt",
        "/assets/%2e%2e/%2e%2e/secret.txt",
        "/assets/..%2f..%2fsecret.txt",
    ])
    def test_traversal_never_leaves_dist(self, client, path):
        res = client.get(path)
        assert "hunter2" not in res.text

    @pytest.mark.parametrize("path", ["/%00", "/a%00b", "/" + "a" * 300, "/assets/%00.js"])
    def test_a_scanners_odd_path_is_not_a_500(self, client, path):
        assert client.get(path).status_code in (200, 404)


@pytest.mark.api
class TestWithoutABuild:
    def test_mounts_nothing(self, tmp_path):
        app = FastAPI()
        before = len(app.routes)
        assert mount_dashboard(app, tmp_path / "dist") is False
        assert len(app.routes) == before

    def test_an_empty_dist_directory_counts_as_no_build(self, tmp_path):
        (tmp_path / "dist").mkdir()
        assert mount_dashboard(FastAPI(), tmp_path / "dist") is False
