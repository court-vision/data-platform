"""
Backfill nba.games rows for April 12, 2026.

The game_schedule pipeline runs post-game and normally keeps these rows
up to date, but this date was only touched by the live-game-stats pipeline
which does not write to nba.games directly.

This script:
1. Fetches LeagueGameLog for the 2025-26 season from the NBA Stats API
2. Filters to games played on 2026-04-12
3. Upserts each game into nba.games with final scores and status="final"

Run from the data-platform/ directory with the venv active:
    python scripts/backfill_games_apr12.py [--dry-run]
"""

import sys
import os

# Ensure data-platform/ is on sys.path regardless of where the script is invoked from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from datetime import datetime, date

import utils.patches  # applies browser impersonation + proxy patch to nba_api

from db.base import db, init_db, close_db
from db.models.nba import Game

TARGET_DATE = date(2026, 4, 12)
SEASON = "2025-26"


def parse_game_date(date_str: str | None) -> date | None:
    if not date_str:
        return None
    try:
        if "," in date_str:
            return datetime.strptime(date_str, "%b %d, %Y").date()
        return datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()
    except ValueError:
        return None


def fetch_games_for_date(target: date, season: str) -> dict[str, dict]:
    """Fetch LeagueGameLog and return game dicts keyed by game_id for target date."""
    from nba_api.stats.endpoints import leaguegamelog

    print(f"Fetching LeagueGameLog for {season} season (this may take a few seconds)...")
    time.sleep(0.6)  # polite delay before NBA API call

    game_log = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
    )
    records = game_log.get_normalized_dict()["LeagueGameLog"]
    print(f"  API returned {len(records)} team-game records for the season")

    games_by_id: dict[str, dict] = {}

    for record in records:
        game_date = parse_game_date(record.get("GAME_DATE"))
        if game_date != target:
            continue

        game_id = record["GAME_ID"]
        matchup = record.get("MATCHUP", "")
        is_home = " vs. " in matchup

        if game_id not in games_by_id:
            games_by_id[game_id] = {
                "game_id": game_id,
                "game_date": game_date,
                "season": season,
                "status": "final",
            }

        game = games_by_id[game_id]
        team_abbrev = record["TEAM_ABBREVIATION"]
        pts = record.get("PTS")

        if is_home:
            game["home_team_id"] = team_abbrev
            game["home_score"] = pts
        else:
            game["away_team_id"] = team_abbrev
            game["away_score"] = pts

    # Drop incomplete records (missing home or away)
    complete = {
        gid: g for gid, g in games_by_id.items()
        if g.get("home_team_id") and g.get("away_team_id")
    }

    return complete


def backfill(dry_run: bool = False) -> None:
    print(f"Backfilling nba.games for {TARGET_DATE.isoformat()} (dry_run={dry_run})")
    print()

    games = fetch_games_for_date(TARGET_DATE, SEASON)

    if not games:
        print(f"No games found for {TARGET_DATE.isoformat()} in the NBA API response.")
        print("The date may be out of range or the API may be unavailable.")
        sys.exit(1)

    print(f"Found {len(games)} game(s) on {TARGET_DATE.isoformat()}:")
    for g in games.values():
        print(f"  {g['game_id']}  {g['away_team_id']} @ {g['home_team_id']}  "
              f"{g['away_score']}-{g['home_score']}  status={g['status']}")
    print()

    if dry_run:
        print("[DRY RUN] No changes written.")
        return

    print("Connecting to database...")
    init_db()

    try:
        upserted = 0
        for game_id, game_data in games.items():
            Game.upsert_game(game_id, game_data)
            upserted += 1
            print(f"  upserted {game_id}")

        print()
        print(f"Done. {upserted} row(s) upserted into nba.games.")
    finally:
        close_db()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill nba.games for 2026-04-12")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print games without writing to the database",
    )
    args = parser.parse_args()

    backfill(dry_run=args.dry_run)
