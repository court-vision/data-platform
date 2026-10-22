"""
Restore opening night's score snapshots (2026-10-20) to nba.live_game_score_snapshots.

The first live run of 2026-10-21 ran before data-platform#42, so it deleted
every snapshot older than that night, and opening night's three games lost
the margin line of their Scout sheet. Their score histories had been saved
from the public API (`/v1/teams/{abbrev}/live-game`) that morning; the
in-play and final points of them are in scripts/data/score_snapshots_2026-10-20.json.

The file keeps only both ends of each run of unchanged scores, which is all
the backend's `score_history` ever returns, so a restored game draws the same
line it drew that night. Pre-tip polls (game_status 1) are left out, as the
live pipeline deletes those itself.

Restored rows carry RESTORE_RUN_ID as their pipeline_run_id. A game that
already has in-play or final snapshots is skipped, so a second run does
nothing. To undo:
    DELETE FROM nba.live_game_score_snapshots WHERE pipeline_run_id = '<RESTORE_RUN_ID>';

Run from the data-platform/ directory with the venv active:
    python scripts/restore_score_snapshots.py           # dry run: what it would insert
    python scripts/restore_score_snapshots.py --apply   # insert
"""

import json
import os
import sys
from datetime import date, datetime
from uuid import UUID

# Ensure data-platform/ is on sys.path regardless of where the script is invoked from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.base import close_db, db, init_db
from db.models.nba.live_game_score_snapshots import LiveGameScoreSnapshot

DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "score_snapshots_2026-10-20.json")
RESTORE_RUN_ID = UUID("5c0e5a1e-2026-4020-a000-0000000000a1")


def load_rows(path: str = DATA_FILE) -> list[dict]:
    with open(path) as f:
        rows = json.load(f)
    for row in rows:
        row["game_date"] = date.fromisoformat(row["game_date"])
        row["captured_at"] = datetime.fromisoformat(row["captured_at"])
        row["pipeline_run_id"] = RESTORE_RUN_ID
    return rows


def restore(rows: list[dict], apply: bool = False) -> dict[str, int]:
    """Insert each game's rows unless it already has in-play or final snapshots.

    Returns the number of rows inserted (or, in a dry run, that would be) per game.
    """
    by_game: dict[str, list[dict]] = {}
    for row in rows:
        by_game.setdefault(row["game_id"], []).append(row)

    done: dict[str, int] = {}
    with db.atomic():
        for game_id, game_rows in sorted(by_game.items()):
            kept = (
                LiveGameScoreSnapshot.select()
                .where((LiveGameScoreSnapshot.game_id == game_id) & (LiveGameScoreSnapshot.game_status != 1))
                .count()
            )
            if kept:
                print(f"  {game_id}: already has {kept} in-play/final snapshots, skipped")
                done[game_id] = 0
                continue
            first = game_rows[0]
            print(f"  {game_id} {first['away_team']} @ {first['home_team']}: {len(game_rows)} snapshots"
                  f"{'' if apply else ' (dry run)'}")
            if apply:
                LiveGameScoreSnapshot.insert_many(game_rows).execute()
            done[game_id] = len(game_rows)
    return done


def main() -> None:
    apply = "--apply" in sys.argv[1:]
    rows = load_rows()
    print(f"Restoring {len(rows)} score snapshots for 2026-10-20 (apply={apply})")
    init_db()
    try:
        done = restore(rows, apply=apply)
    finally:
        close_db()
    print(f"{'Inserted' if apply else 'Would insert'} {sum(done.values())} rows; "
          f"undo with pipeline_run_id = '{RESTORE_RUN_ID}'")


if __name__ == "__main__":
    main()
