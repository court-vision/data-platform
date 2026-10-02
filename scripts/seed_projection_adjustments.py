"""
Load a reviewed seed of projection adjustments into nba.projection_adjustments.

    python scripts/seed_projection_adjustments.py seeds/projection_adjustments_2026_27.csv           # dry run
    python scripts/seed_projection_adjustments.py seeds/projection_adjustments_2026_27.csv --apply   # write

The seed is the first draft of the curated layer, reviewed as a file in a PR
before it goes live; after that the dashboard's projections editor owns the
table. A row that differs from the player's live adjustment goes through
`ProjectionAdjustment.record` (the edit supersedes, and keeps its history); a
row that is already live as written is left alone, so re-applying an edited
seed versions only what changed.

Players are matched by normalized name against nba.players. A name that
matches nobody, or more than one player, is reported and skipped — never
guessed.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from core.settings import settings  # noqa: E402

AUTHOR = "seed"
KINDS = {"year2", "trade", "role", "injury_return", "injury_current", "age", "other"}


def normalize(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b\.?", "", s)
    return re.sub(r"[^a-z]", "", s)


def parse_row(row: dict) -> dict:
    """One CSV row as ProjectionAdjustment fields; raises ValueError on a bad value."""
    kind = row["kind"].strip()
    if kind not in KINDS:
        raise ValueError(f"unknown kind {kind!r}")
    fields: dict = {"kind": kind, "note": row["note"].strip(), "source_url": (row.get("source_url") or "").strip() or None}
    if row.get("minutes"):
        fields["minutes"] = float(row["minutes"])
    if row.get("games"):
        fields["games"] = int(row["games"])
    if row.get("return_date"):
        fields["return_date"] = date.fromisoformat(row["return_date"].strip())
    if row.get("usage"):
        fields["usage"] = float(row["usage"])
    if row.get("rates"):
        fields["rates"] = json.loads(row["rates"])
    if not any(k in fields for k in ("minutes", "games", "return_date", "usage", "rates")):
        raise ValueError("changes nothing")
    if not fields["note"]:
        raise ValueError("a note is required")
    return fields


COMPARED = ("kind", "minutes", "games", "return_date", "usage", "rates", "note", "source_url")


def same_as_live(live, fields: dict) -> bool:
    """Whether the live adjustment already says exactly what the seed row says.

    Re-applying a seed after editing three rows should version three rows, not
    rewrite the history of all fifty.
    """
    if live is None:
        return False

    def norm(key, value):
        if value is None or value == "" or value == {}:
            return None
        if key in ("minutes", "usage"):
            return round(float(value), 3)
        return value

    return all(norm(k, getattr(live, k)) == norm(k, fields.get(k)) for k in COMPARED)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("seed")
    parser.add_argument("--season", default=settings.nba_season)
    parser.add_argument("--apply", action="store_true", help="write; without it, only report")
    parser.add_argument("--author", default=AUTHOR)
    args = parser.parse_args()

    from db.models.nba import Player, ProjectionAdjustment

    by_name: dict[str, list[int]] = {}
    for p in Player.select(Player.id, Player.name):
        by_name.setdefault(normalize(p.name), []).append(p.id)

    problems = 0
    planned: list[tuple[int, str, dict]] = []
    with open(args.seed, newline="") as f:
        for line, row in enumerate(csv.DictReader(f), start=2):
            ids = by_name.get(normalize(row["player"]), [])
            if len(ids) != 1:
                print(f"line {line}: {row['player']!r} matches {len(ids)} players — skipped")
                problems += 1
                continue
            try:
                fields = parse_row(row)
            except (ValueError, KeyError) as e:
                print(f"line {line}: {row.get('player')!r}: {e} — skipped")
                problems += 1
                continue
            planned.append((ids[0], row["player"], fields))

    live = {a.player_id: a for a in ProjectionAdjustment.active_for(args.season)}
    written = unchanged = 0
    for pid, name, fields in planned:
        shown = {k: v for k, v in fields.items() if k not in ("note", "source_url")}
        if same_as_live(live.get(pid), fields):
            unchanged += 1
            continue
        print(f"{'WRITE' if args.apply else 'plan '} {name} ({pid}) {args.season}: {shown}")
        written += 1
        if args.apply:
            ProjectionAdjustment.record(pid, args.season, author=args.author, **fields)

    print(
        f"{written} adjustments {'written' if args.apply else 'planned (dry run)'}, "
        f"{unchanged} already live and unchanged, {problems} skipped"
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
