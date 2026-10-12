"""
Theme-drift guard: the dashboard's copy of the frontend's desk shell.

The React dashboard (`dashboard/`) is built and deployed from this repo alone,
so it cannot import the frontend's desks; it carries a copy of the two files
that make a desk look like one. Each must stay identical to its source, below
a generated header:

- `frontend/src/components/desk/desk.module.css` (the theme tokens and the
  shared primitives: bar, tabs, buttons, rails, chips, menus, dialogs, status
  line, toasts) -> `dashboard/src/desk/desk.module.css`
- `frontend/src/components/desk/themes.ts` (the theme list, its pairs and the
  default) -> `dashboard/src/desk/themes.ts`

    python scripts/check_frontend_theme.py [--frontend ../frontend]
    python scripts/check_frontend_theme.py --write     # re-sync from the frontend

Exit codes: 0 clean, 1 drift found, 2 frontend checkout not found (skip, not
fail, as with check_backend_mirror.py).

The rest of `dashboard/src/desk/` (the frame, the bar, the theme store and
picker) is the dashboard's own port: those files use Next.js, Clerk or zustand
in the frontend, so they are rewritten rather than copied, and not watched.

Never edit the two copied files by hand. Change the frontend's desk shell, then
run this with --write.
"""

from __future__ import annotations

import argparse
import difflib
import os
import sys
from pathlib import Path

DP_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DESK = DP_ROOT / "dashboard" / "src" / "desk"

# Source (relative to the frontend checkout) -> copy, and the comment syntax of each.
COPIES = {
    "src/components/desk/desk.module.css": (DASHBOARD_DESK / "desk.module.css", "/* {} */"),
    "src/components/desk/themes.ts": (DASHBOARD_DESK / "themes.ts", "// {}"),
}

HEADER = "Copied from frontend/{source} by `python scripts/check_frontend_theme.py --write`. Do not edit here: change the frontend's, then re-sync."


def copy_of(source: str, text: str, comment: str) -> str:
    return comment.format(HEADER.format(source=source)) + "\n" + text


def _diff(expected: str, actual: str, name: str) -> str:
    return "".join(
        difflib.unified_diff(
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=f"frontend -> {name}",
            tofile=f"dashboard {name}",
            n=2,
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--frontend",
        default=os.environ.get("CV_FRONTEND_PATH", str(DP_ROOT.parent / "frontend")),
        help="Path to a frontend checkout (default: ../frontend, or $CV_FRONTEND_PATH)",
    )
    parser.add_argument(
        "--write", action="store_true",
        help="Overwrite the dashboard's copies from the frontend instead of checking",
    )
    args = parser.parse_args()

    frontend = Path(args.frontend).resolve()
    if not all((frontend / source).exists() for source in COPIES):
        print(f"frontend checkout (with the desk shell) not found at {frontend} — skipping theme check")
        return 2

    expected = {
        path: copy_of(source, (frontend / source).read_text(), comment)
        for source, (path, comment) in COPIES.items()
    }

    if args.write:
        for path, text in expected.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            changed = not path.exists() or path.read_text() != text
            path.write_text(text)
            print(f"{'wrote' if changed else 'unchanged'}  {path.relative_to(DP_ROOT)}")
        return 0

    drift = []
    for path, text in expected.items():
        rel = str(path.relative_to(DP_ROOT))
        if not path.exists():
            drift.append(f"   MISSING {rel}")
        elif path.read_text() != text:
            drift.append(f"   DIVERGED {rel}\n{_diff(text, path.read_text(), path.name)}")

    if drift:
        print("Theme drift between frontend and the dashboard:")
        print("\n".join(drift))
        print("\nRe-sync with: python scripts/check_frontend_theme.py --write")
        return 1

    print(f"theme check clean: {len(expected)} copied files match the frontend")
    return 0


if __name__ == "__main__":
    sys.exit(main())
