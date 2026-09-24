"""
What the dashboard knows about a pipeline comes from its `PipelineConfig`.

It used to come from two dicts in `api/v1/dashboard.py`, keyed by pipeline name
and kept by hand. `preseason_market` was registered without being added to
either, so its card had a Run button that posted nowhere and never picked up
its cron runs. These tests make that a failure at the registry instead.
"""

import pytest

import main
import main_public
from api.v1.dashboard import PIPELINE_ROUTE_PREFIX, trigger_endpoint
from pipelines import PIPELINE_REGISTRY, PipelineCategory, PipelineConfig
from pipelines.config import CATEGORY_CRON_JOBS

# cron-runner's job names (internal/jobs/registry.go on its main). A separate
# repo, so this list is a copy: a typo in `cron_job=` fails nothing at runtime,
# the dashboard just never finds that pipeline's cron runs.
CRON_RUNNER_JOBS = {
    "pre-game",
    "live-stats",
    "post-game",
    "schedule-sync",
    "playoffs",
    "preseason-market",
}

# SCHEDULED pipelines that no cron-runner job fires (manual trigger only).
UNSCHEDULED = {"player_profiles"}


def _post_paths(app) -> set[str]:
    return {
        route.path
        for route in app.routes
        if "POST" in (getattr(route, "methods", None) or ())
    }


REGISTERED = sorted(PIPELINE_REGISTRY.items())
SCHEDULED = [(n, c) for n, c in REGISTERED if c.config.category == PipelineCategory.SCHEDULED]
BATCHED = [(n, c) for n, c in REGISTERED if c.config.category != PipelineCategory.SCHEDULED]


@pytest.mark.api
class TestTriggerRoutes:
    @pytest.mark.parametrize("name,cls", REGISTERED)
    def test_every_registered_pipeline_declares_a_trigger_slug(self, name, cls):
        assert cls.config.trigger_slug, (
            f"{name} has no trigger_slug: its dashboard card cannot run it"
        )

    @pytest.mark.parametrize("app_module", [main, main_public], ids=["private", "public"])
    @pytest.mark.parametrize("name,cls", REGISTERED)
    def test_trigger_endpoint_is_a_real_post_route(self, name, cls, app_module):
        endpoint = trigger_endpoint(cls.config)
        assert endpoint in _post_paths(app_module.app), (
            f"{name}: {endpoint} is not a POST route on {app_module.__name__}"
        )

    def test_trigger_slugs_are_unique(self):
        slugs = [cls.config.trigger_slug for cls in PIPELINE_REGISTRY.values()]
        assert len(slugs) == len(set(slugs))

    def test_a_pipeline_without_a_slug_has_no_endpoint(self):
        config = PipelineConfig(
            name="x", display_name="X", description="", target_table="nba.x",
            category=PipelineCategory.SCHEDULED,
        )
        assert trigger_endpoint(config) == ""

    def test_prefix_is_where_the_router_is_mounted(self):
        assert f"{PIPELINE_ROUTE_PREFIX}/post-game" in _post_paths(main_public.app)


@pytest.mark.api
class TestCronJobs:
    @pytest.mark.parametrize("name,cls", BATCHED)
    def test_batch_pipelines_resolve_to_their_category_job(self, name, cls):
        config = cls.config
        assert config.cron_job is None, (
            f"{name} is fired by its category's batch job; cron_job would hide that"
        )
        assert config.cron_job_name == CATEGORY_CRON_JOBS[config.category]

    @pytest.mark.parametrize("name,cls", SCHEDULED)
    def test_scheduled_pipelines_name_a_job_or_are_known_manual(self, name, cls):
        config = cls.config
        if name in UNSCHEDULED:
            assert config.cron_job_name is None
        else:
            assert config.cron_job_name, (
                f"{name} is SCHEDULED with no cron_job; add it, or list it in UNSCHEDULED"
            )

    @pytest.mark.parametrize("name,cls", REGISTERED)
    def test_cron_job_names_are_cron_runners(self, name, cls):
        job = cls.config.cron_job_name
        assert job is None or job in CRON_RUNNER_JOBS, (
            f"{name}: {job!r} is not a cron-runner job. If cron-runner gained one, "
            "add it to CRON_RUNNER_JOBS here"
        )

    def test_preseason_market_picks_up_its_cron_runs(self):
        config = PIPELINE_REGISTRY["preseason_market"].config
        assert config.cron_job_name == "preseason-market"
        assert trigger_endpoint(config) == "/v1/internal/pipelines/preseason-market"
