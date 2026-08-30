"""
Mirror-drift guard: the files still hand-duplicated with the backend.

cv-core (PRODUCTION_READINESS item 5) extracted the shareable modules, but
Peewee models, extractor bases and a handful of glue files remain duplicated —
each repo's copy must stay byte-identical to the other's, and until this
script existed the only thing enforcing that was memory. ~1,000 lines drifted
that way, including a Philadelphia/Phoenix disagreement in the Yahoo team map
and a rolling-stats freshness gate only one repo had.

    python scripts/check_backend_mirror.py [--backend ../backend]

Exit codes: 0 clean, 1 drift found, 2 backend checkout not found (skip, not
fail — CI without a sibling checkout should treat 2 as "not applicable").

Three lists, maintained here:
- MIRRORED: same path in both repos, must be byte-identical. Includes the
  cv-core shims — they are identical one-liners in both repos, so the guard
  watches them too.
- RENAMED_MIRRORS: (backend path, data-platform path) pairs that must be
  byte-identical across different paths.
- KNOWN_DIVERGED: same path, deliberately different (two DB runtimes, forked
  settings, role-specific model helpers). Documented so the WARN list below
  only ever names *new* convergence candidates.

Run this before touching any mirrored file; when a mirrored file must change,
change it in both repos in the same sitting and run this again.
"""

from __future__ import annotations

import argparse
import filecmp
import os
import sys
from pathlib import Path

DP_ROOT = Path(__file__).resolve().parent.parent

# Same path both sides, byte-identical required.
MIRRORED = [
    "api/__init__.py",
    "api/v1/__init__.py",
    "core/correlation_middleware.py",  # shim over cv_core
    "core/crypto.py",                  # shim over cv_core
    "core/errors.py",                  # shim over cv_core
    "core/logging.py",                 # shim over cv_core
    "core/nba_calendar.py",            # shim over cv_core
    "core/resilience.py",              # shim over cv_core
    "core/season.py",                  # shim over cv_core
    "db/models/api_keys.py",
    "db/models/lineups.py",
    "db/models/nba/breakout_candidates.py",
    "db/models/nba/player_advanced_stats.py",
    "db/models/nba/player_game_stats.py",
    "db/models/nba/player_ownership.py",
    "db/models/nba/player_profiles.py",
    "db/models/nba/player_season_stats.py",
    "db/models/nba/players.py",
    "db/models/nba/team_stats.py",
    "db/models/nba/teams.py",
    "db/models/notifications.py",
    "db/models/provider_connections.py",
    "db/models/stats/cumulative_player_stats.py",
    "db/models/stats/daily_matchup_score.py",
    "db/models/stats/daily_player_stats.py",
    "db/models/users.py",
    "db/models/verifications.py",
    "pipelines/extractors/__init__.py",
    "pipelines/extractors/base.py",
    "pipelines/extractors/injuries.py",
    "pipelines/transformers/__init__.py",  # shim over cv_core
    "schemas/__init__.py",
    "services/__init__.py",
    "services/lineup_check_service.py",
    "utils/__init__.py",
    "utils/constants.py",
]

# (backend path, data-platform path): byte-identical across different paths.
RENAMED_MIRRORS = [
    ("services/scoring/vocab.py", "utils/stat_vocab.py"),  # both shim cv_core.scoring_vocab
]

# Same path, deliberately different. Each entry is a decision, not an accident.
KNOWN_DIVERGED = {
    "core/health.py": "reports each service's own dependencies",
    "core/middleware.py": "dp adds dead-connection eviction",
    "core/settings.py": "deliberately forked per service",
    "core/telemetry.py": "one-line scrub difference",
    "db/base.py": "two DB runtimes (run_db executor vs run_in_db_thread)",
    "db/models/__init__.py": "different export sets",
    "db/models/nba/__init__.py": "dp adds pipeline/cron audit models",
    "db/models/nba/games.py": "timestamp default styles differ",
    "db/models/nba/live_game_score_snapshots.py": "dp adds the writer helpers",
    "db/models/nba/live_player_stats.py": "dp adds finalize_* writer helpers",
    "db/models/nba/player_injuries.py": "writer vs reader helpers",
    "db/models/nba/player_rolling_stats.py": "backend adds the freshness gate",
    "db/models/nba/playoff_series.py": "writer vs reader helpers",
    "db/models/pipeline_run.py": "dp adds run-tracking used by gates",
    "db/models/stats/rankings.py": "docstring drift only",
    "db/models/teams.py": "backend adds the League FK",
    "main.py": "different applications",
    "pipelines/__init__.py": "dp has the registry; backend a stub package",
    "pipelines/extractors/espn.py": "dp is ahead (playoffs, categories)",
    "pipelines/extractors/nba_api.py": "dp is ahead (playoff bracket, abbrevs)",
    "pipelines/extractors/yahoo.py": "dp is ahead (calendar watermark)",
    "schemas/common.py": "each repo keeps its own envelope extras",
    "schemas/pipeline.py": "dp adds partial-success fields",
    "services/credential_service.py": "backend is a superset (OAuth read path)",
    "services/notification_service.py": "two-line drift",
    "services/schedule_service.py": "backend adds the 2 AM fantasy-day rule",
    "utils/espn_helpers.py": "backend adds normalize_nba_abbrev",
    "utils/patches.py": "dp adds proxy support",
}

SKIP_DIRS = {".git", "__pycache__", ".venv", "node_modules", ".pytest_cache", "temp"}


def _pyfiles(root: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS or part.endswith(".egg-info") for part in p.parts):
            continue
        rel = str(p.relative_to(root))
        if rel.startswith(("tests/", "scripts/")):
            continue
        out[rel] = p
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--backend",
        default=os.environ.get("CV_BACKEND_PATH", str(DP_ROOT.parent / "backend")),
        help="Path to a backend checkout (default: ../backend, or $CV_BACKEND_PATH)",
    )
    args = parser.parse_args()

    backend = Path(args.backend).resolve()
    if not (backend / "main.py").exists():
        print(f"backend checkout not found at {backend} — skipping mirror check")
        return 2

    failures: list[str] = []

    for rel in MIRRORED:
        b, d = backend / rel, DP_ROOT / rel
        if not b.exists() or not d.exists():
            failures.append(f"MISSING  {rel} ({'backend' if not b.exists() else 'data-platform'} side)")
        elif not filecmp.cmp(b, d, shallow=False):
            failures.append(f"DIVERGED {rel}")

    for brel, drel in RENAMED_MIRRORS:
        b, d = backend / brel, DP_ROOT / drel
        if not b.exists() or not d.exists():
            failures.append(f"MISSING  {brel} <-> {drel}")
        elif not filecmp.cmp(b, d, shallow=False):
            failures.append(f"DIVERGED {brel} <-> {drel}")

    # New convergence candidates: identical same-path files in neither list.
    known = set(MIRRORED) | set(KNOWN_DIVERGED)
    bfiles, dfiles = _pyfiles(backend), _pyfiles(DP_ROOT)
    candidates = sorted(
        rel for rel in set(bfiles) & set(dfiles)
        if rel not in known and filecmp.cmp(bfiles[rel], dfiles[rel], shallow=False)
    )

    if failures:
        print("Mirror drift between backend and data-platform:")
        for f in failures:
            print("  ", f)
        print(
            "\nMirrored files must change in both repos in the same sitting"
            " (or move to cv-core). See scripts/check_backend_mirror.py."
        )
        return 1

    print(f"mirror check clean: {len(MIRRORED)} mirrored + {len(RENAMED_MIRRORS)} renamed pairs identical")
    if candidates:
        print("note — identical files not yet tracked (add to MIRRORED or KNOWN_DIVERGED):")
        for rel in candidates:
            print("  ", rel)
    return 0


if __name__ == "__main__":
    sys.exit(main())
