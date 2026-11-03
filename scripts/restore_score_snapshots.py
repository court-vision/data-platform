"""
Restore past nights' score snapshots to nba.live_game_score_snapshots.

Before data-platform#42, each night's first live run deleted every snapshot
older than that night. The run of 2026-10-21 deleted opening night's
(2026-10-20), so those three games lost the margin line of their Scout sheet.
#42 merged on 2026-10-22 while Railway's builds were paused, so its deploy could
also miss that night's first run, which would do the same to 2026-10-21's
eleven games.

Both nights' score histories were saved from the public API
(`/v1/teams/{abbrev}/live-game`) the morning after; their in-play and final
points are in scripts/data/score_snapshots_<date>.json. Each file keeps only
both ends of each run of unchanged scores, which is all the backend's
`score_history` ever returns, so a restored game draws the same line it drew
that night. Pre-tip polls (game_status 1) are left out, as the live pipeline
deletes those itself.

Run it after #42 is deployed: an older live pipeline would delete the restored
rows again on its next first run of the night.

Restored rows carry RESTORE_RUN_ID as their pipeline_run_id. A game that
already has in-play or final snapshots is skipped, so a night the pipeline kept
is left alone and a second run does nothing. To undo:
    DELETE FROM nba.live_game_score_snapshots WHERE pipeline_run_id = '<RESTORE_RUN_ID>';

Run from the data-platform/ directory with the venv active:
    python scripts/restore_score_snapshots.py           # dry run: what it would insert
    python scripts/restore_score_snapshots.py --apply   # insert
"""

import glob
import json
import os
import sys
from datetime import date, datetime
from uuid import UUID

# Ensure data-platform/ is on sys.path regardless of where the script is invoked from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.base import close_db, db, init_db
from db.models.nba.live_game_score_snapshots import LiveGameScoreSnapshot

DATA_FILES = sorted(glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "score_snapshots_*.json")))
RESTORE_RUN_ID = UUID("5c0e5a1e-2026-4020-a000-0000000000a1")


def load_rows(paths: list[str] = DATA_FILES) -> list[dict]:
    rows = []
    for path in paths:
        with open(path) as f:
            rows += json.load(f)
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
            print(f"  {game_id} {first['game_date']} {first['away_team']} @ {first['home_team']}: {len(game_rows)} snapshots"
                  f"{'' if apply else ' (dry run)'}")
            if apply:
                LiveGameScoreSnapshot.insert_many(game_rows).execute()
            done[game_id] = len(game_rows)
    return done


def main() -> None:
    apply = "--apply" in sys.argv[1:]
    rows = load_rows()
    nights = sorted({str(r["game_date"]) for r in rows})
    print(f"Restoring {len(rows)} score snapshots for {', '.join(nights)} (apply={apply})")
    init_db()
    try:
        done = restore(rows, apply=apply)
    finally:
        close_db()
    print(f"{'Inserted' if apply else 'Would insert'} {sum(done.values())} rows; "
          f"undo with pipeline_run_id = '{RESTORE_RUN_ID}'")


if __name__ == "__main__":
    main()
