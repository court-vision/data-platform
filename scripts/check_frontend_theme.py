"""
Theme-drift guard: the dashboard's copy of the frontend's look.

The React dashboard (`dashboard/`) is built and deployed from this repo alone,
so it cannot import the frontend's theme; it carries a copy. Two things are
copied, and each must stay identical to its source:

- the `:root` token block of `frontend/src/app/globals.css`
  -> `dashboard/src/styles/tokens.css`
- `frontend/tailwind.config.ts`, all but its `content` globs
  -> `dashboard/tailwind.config.ts`

    python scripts/check_frontend_theme.py [--frontend ../frontend]
    python scripts/check_frontend_theme.py --write     # re-sync from the frontend

Exit codes: 0 clean, 1 drift found, 2 frontend checkout not found (skip, not
fail, as with check_backend_mirror.py).

The shadcn primitives in `dashboard/src/components/ui/` are copies too, but
are not watched: they lose `"use client"` on the way in and are expected to
diverge as the dashboard grows its own variants.

Never edit the two copied files by hand. Change the frontend's theme, then run
this with --write.
"""

from __future__ import annotations

import argparse
import difflib
import os
import re
import sys
from pathlib import Path

DP_ROOT = Path(__file__).resolve().parent.parent

FRONTEND_CSS = "src/app/globals.css"
FRONTEND_TAILWIND = "tailwind.config.ts"
DASHBOARD_TOKENS = DP_ROOT / "dashboard" / "src" / "styles" / "tokens.css"
DASHBOARD_TAILWIND = DP_ROOT / "dashboard" / "tailwind.config.ts"

DASHBOARD_CONTENT = "content: ['./index.html', './src/**/*.{ts,tsx}'],"

TOKENS_HEADER = """\
/* Copied from frontend/src/app/globals.css by
   `python scripts/check_frontend_theme.py --write`. Do not edit here: change
   the frontend's tokens, then re-sync. */
"""

_CONTENT_ARRAY = re.compile(r"content:\s*\[.*?\],", re.DOTALL)


def root_block(css: str) -> str:
    """The first top-level-or-layered `:root { ... }` block, dedented."""
    start = css.index(":root {")
    depth = 0
    for i in range(css.index("{", start), len(css)):
        if css[i] == "{":
            depth += 1
        elif css[i] == "}":
            depth -= 1
            if depth == 0:
                block = css[start : i + 1]
                break
    else:
        raise ValueError("unterminated :root block")

    # Dedent by the block's own indentation so layered and bare blocks compare.
    line_start = css.rfind("\n", 0, start) + 1
    indent = css[line_start:start]
    lines = block.split("\n")
    return "\n".join(
        line[len(indent):] if line.startswith(indent) else line for line in lines
    )


def dashboard_tokens(frontend_css: str) -> str:
    return TOKENS_HEADER + root_block(frontend_css) + "\n"


def dashboard_tailwind(frontend_config: str) -> str:
    if len(_CONTENT_ARRAY.findall(frontend_config)) != 1:
        raise ValueError("expected exactly one `content: [...]` array")
    return _CONTENT_ARRAY.sub(lambda _: DASHBOARD_CONTENT, frontend_config)


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
    css_path, tailwind_path = frontend / FRONTEND_CSS, frontend / FRONTEND_TAILWIND
    if not (css_path.exists() and tailwind_path.exists()):
        print(f"frontend checkout not found at {frontend} — skipping theme check")
        return 2

    expected = {
        DASHBOARD_TOKENS: dashboard_tokens(css_path.read_text()),
        DASHBOARD_TAILWIND: dashboard_tailwind(tailwind_path.read_text()),
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
