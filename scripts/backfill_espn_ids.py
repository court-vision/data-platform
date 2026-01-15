"""
Backfill ESPN IDs for existing daily_player_stats records.

This script:
1. Loads ESPN player data
2. Queries unique player name/team combinations from daily_player_stats
3. Matches players using diacritic-normalized names
4. Updates records with the corresponding ESPN ID
"""
import unicodedata
import json
import requests
from db.base import db, init_db, close_db
from db.models.season2.daily_player_stats import DailyPlayerStats
from peewee import fn


def normalize_name(name: str) -> str:
    """
    Normalize a name by removing diacritics and converting to lowercase.

    Examples:
        "Nikola Jokić" -> "nikola jokic"
        "Luka Dončić" -> "luka doncic"
        "José Alvarado" -> "jose alvarado"
    """
    # Normalize to NFD (decomposed form), then filter out combining marks
    normalized = unicodedata.normalize('NFD', name)
    ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    return ascii_name.lower().strip()


def get_espn_player_data(year: int, league_id: int) -> dict:
    """
    Fetch ESPN player data including ESPN ID mapping.

    Returns:
        dict: Mapping from normalized player name to {'espn_id': int, 'name': str, 'rost_pct': float}
    """
    params = {
        'view': 'kona_player_info',
        'scoringPeriodId': 0,
    }
    endpoint = f'https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{year}/segments/0/leagues/{league_id}'
    filters = {
        "players": {
            "filterSlotIds": {"value": []},
            "limit": 750,
            "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
            "sortDraftRanks": {"sortPriority": 2, "sortAsc": True, "value": "STANDARD"}
        }
    }
    headers = {'x-fantasy-filter': json.dumps(filters)}

    response = requests.get(endpoint, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()

    players = data.get('players', [])
    players = [x.get('player', x) for x in players]

    cleaned_data = {}
    for player in players:
        if player and 'fullName' in player:
            original_name = player['fullName']
            normalized = normalize_name(original_name)
            cleaned_data[normalized] = {
                'espn_id': player['id'],
                'name': original_name,
                'rost_pct': player.get('ownership', {}).get('percentOwned', 0)
            }

    return cleaned_data


def get_unique_players() -> list[dict]:
    """
    Get unique player name/team combinations from daily_player_stats.

    Returns:
        List of dicts with 'name' and 'team' keys.
    """
    query = (
        DailyPlayerStats
        .select(DailyPlayerStats.name, DailyPlayerStats.team)
        .distinct()
    )

    return [{'name': row.name, 'team': row.team} for row in query]


def backfill_espn_ids(year: int = 2026, league_id: int = 993431466, dry_run: bool = False):
    """
    Main function to backfill ESPN IDs.

    Args:
        year: ESPN season year
        league_id: ESPN league ID
        dry_run: If True, only print what would be done without making changes
    """
    print("Initializing database connection...")
    init_db()

    try:
        print(f"Fetching ESPN player data for {year} season...")
        espn_data = get_espn_player_data(2026, league_id)
        print(f"Loaded {len(espn_data)} ESPN players")

        print("Querying unique players from daily_player_stats...")
        db_players = get_unique_players()
        print(f"Found {len(db_players)} unique player/team combinations")

        # Track statistics
        matched = 0
        unmatched = []
        updated = 0

        for player in db_players:
            name = player['name']
            normalized = normalize_name(name)

            espn_info = espn_data.get(normalized)

            if espn_info:
                matched += 1
                espn_id = espn_info['espn_id']

                if dry_run:
                    print(f"  [DRY RUN] Would update '{name}' -> espn_id={espn_id}")
                else:
                    # Update all records for this player
                    rows_updated = (
                        DailyPlayerStats
                        .update(espn_id=espn_id)
                        .where(DailyPlayerStats.name == name)
                        .execute()
                    )
                    updated += rows_updated
                    print(f"  Updated '{name}' -> espn_id={espn_id} ({rows_updated} rows)")
            else:
                unmatched.append(name)

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total players in database: {len(db_players)}")
        print(f"Matched to ESPN: {matched}")
        print(f"Unmatched: {len(unmatched)}")

        if not dry_run:
            print(f"Total rows updated: {updated}")

        if unmatched:
            print(f"\nUnmatched players ({len(unmatched)}):")
            for name in sorted(unmatched):
                print(f"  - {name}")

    finally:
        close_db()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill ESPN IDs for daily_player_stats")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without making changes")
    parser.add_argument("--year", type=int, default=2025, help="ESPN season year (default: 2025)")
    parser.add_argument("--league-id", type=int, default=993431466, help="ESPN league ID")

    args = parser.parse_args()

    backfill_espn_ids(year=args.year, league_id=args.league_id, dry_run=args.dry_run)
