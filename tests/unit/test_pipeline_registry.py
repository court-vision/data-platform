"""
Pipeline Registry Validation Tests

Validates that the PIPELINE_REGISTRY ordering satisfies the declared dependency
graph and that all pipeline configurations are well-formed.
"""

import pytest

from pipelines import PIPELINE_REGISTRY, get_pipelines_by_category
from pipelines.config import PipelineCategory


@pytest.mark.unit
class TestRegistryOrder:
    """Validate registry ordering satisfies the dependency graph."""

    def test_registry_order_satisfies_dependencies(self):
        """Every pipeline's depends_on entries must appear earlier in the registry."""
        names = list(PIPELINE_REGISTRY.keys())
        for i, name in enumerate(names):
            config = PIPELINE_REGISTRY[name].config
            for dep in config.depends_on:
                assert dep in names, (
                    f"{name} depends on '{dep}', which is not in the registry"
                )
                dep_index = names.index(dep)
                assert dep_index < i, (
                    f"{name} (index {i}) depends on {dep} (index {dep_index}), "
                    f"but {dep} appears later in the registry"
                )

    def test_all_dependency_targets_exist(self):
        """All depends_on references point to real pipelines."""
        names = set(PIPELINE_REGISTRY.keys())
        for name, cls in PIPELINE_REGISTRY.items():
            for dep in cls.config.depends_on:
                assert dep in names, (
                    f"{name} declares dependency on '{dep}', which does not exist"
                )

    def test_no_circular_dependencies(self):
        """No pipeline has a circular dependency chain."""
        def _detect_cycle(name: str, visited: set, stack: set) -> bool:
            visited.add(name)
            stack.add(name)
            for dep in PIPELINE_REGISTRY[name].config.depends_on:
                if dep in stack:
                    return True
                if dep not in visited and _detect_cycle(dep, visited, stack):
                    return True
            stack.discard(name)
            return False

        visited: set[str] = set()
        for name in PIPELINE_REGISTRY:
            if name not in visited:
                assert not _detect_cycle(name, visited, set()), (
                    f"Circular dependency detected involving '{name}'"
                )


@pytest.mark.unit
class TestRegistryCompleteness:
    """Validate pipeline configurations are well-formed."""

    def test_every_pipeline_has_a_category(self):
        """Every registered pipeline must declare a category."""
        for name, cls in PIPELINE_REGISTRY.items():
            assert cls.config.category is not None, f"{name} has no category"
            assert isinstance(cls.config.category, PipelineCategory), (
                f"{name} category is {type(cls.config.category)}, expected PipelineCategory"
            )

    def test_every_pipeline_has_a_target_table(self):
        """Every registered pipeline must declare a target table."""
        for name, cls in PIPELINE_REGISTRY.items():
            assert cls.config.target_table, f"{name} has no target_table"

    def test_category_filter_returns_correct_pipelines(self):
        """get_pipelines_by_category returns only pipelines with matching category."""
        for category in PipelineCategory:
            filtered = get_pipelines_by_category(category)
            for cls in filtered:
                assert cls.config.category == category, (
                    f"{cls.config.name} has category {cls.config.category}, "
                    f"but was returned for {category}"
                )

    def test_category_filter_preserves_registry_order(self):
        """get_pipelines_by_category preserves the registry insertion order."""
        for category in PipelineCategory:
            filtered = get_pipelines_by_category(category)
            filtered_names = [cls.config.name for cls in filtered]
            registry_names = [
                name for name, cls in PIPELINE_REGISTRY.items()
                if cls.config.category == category
            ]
            assert filtered_names == registry_names, (
                f"Category {category} order mismatch: "
                f"expected {registry_names}, got {filtered_names}"
            )

    def test_known_dependencies_are_declared(self):
        """Verify the specific dependencies we know must exist."""
        expected_deps = {
            "player_season_stats": ("player_game_stats",),
            "player_rolling_stats": ("player_game_stats",),
            "breakout_detection": ("espn_injury_status", "player_season_stats", "player_game_stats"),
        }
        for name, expected in expected_deps.items():
            cls = PIPELINE_REGISTRY[name]
            actual = set(cls.config.depends_on)
            for dep in expected:
                assert dep in actual, (
                    f"{name} should depend on '{dep}' but depends_on={cls.config.depends_on}"
                )
