import json
import unicodedata
import requests
from datetime import datetime, timedelta

import pytz
from peewee import fn

from db.models.season2.cumulative_player_stats import CumulativePlayerStats
from nba_api.stats.endpoints import leagueleaders


# Configuration
YEAR = 2026
LEAGUE_ID = 993431466


def remove_diacritics(s: str) -> str:
    """Removes diacritics from a string for name matching"""
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def get_espn_rostered_data(year: int, league_id: int) -> dict:
    """Fetches rostered percentages from ESPN Fantasy API"""
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
    data = response.json()
    players = data.get('players', [])
    players = [x.get('player', x) for x in players]

    # Map player name (with diacritics removed) to rostered percentage
    cleaned_data = {}
    for player in players:
        if player and 'fullName' in player:
            name = remove_diacritics(player['fullName'])
            pct = player.get('ownership', {}).get('percentOwned', 0)
            cleaned_data[name] = pct

    return cleaned_data


def calculate_fantasy_points(stats: dict) -> int:
    """Formula for the fantasy points of a player"""
    points_score = stats['pts']
    rebounds_score = stats['reb']
    assists_score = stats['ast'] * 2
    stocks_score = (stats['stl'] + stats['blk']) * 4
    turnovers_score = stats['tov'] * -2
    three_pointers_score = stats['fg3m']
    fg_eff_score = (stats['fgm'] * 2) - stats['fga']
    ft_eff_score = stats['ftm'] - stats['fta']
    return int(points_score + rebounds_score + assists_score + stocks_score + turnovers_score + three_pointers_score + fg_eff_score + ft_eff_score)


def fetch_nba_fpts_data(rostered_data: dict) -> dict:
    """Fetches and restructures the data from the NBA API"""
    leaders = leagueleaders.LeagueLeaders(
        season='2025-26',
        per_mode48='Totals',
        stat_category_abbreviation='PTS'
    )
    updated = leaders.get_normalized_dict()['LeagueLeaders']

    # Create a new dictionary with the id as the key
    updated_dict = {}
    for player in updated:
        player_name = player['PLAYER']
        rost_pct = rostered_data.get(remove_diacritics(player_name), 0)

        updated_dict[player['PLAYER_ID']] = {
            'id': player['PLAYER_ID'],
            'name': player_name,
            'team': player['TEAM'],
            'min': player['MIN'],
            'pts': player['PTS'],
            'reb': player['REB'],
            'ast': player['AST'],
            'stl': player['STL'],
            'blk': player['BLK'],
            'tov': player['TOV'],
            'fgm': player['FGM'],
            'fga': player['FGA'],
            'fg3m': player['FG3M'],
            'fg3a': player['FG3A'],
            'ftm': player['FTM'],
            'fta': player['FTA'],
            'gp': player['GP'],
            'rost_pct': rost_pct
        }

    return updated_dict


def get_latest_gp_by_player() -> dict:
    """Gets the most recent GP value for each player from the database"""
    # Get the latest record for each player by finding max date per player
    subquery = (
        CumulativePlayerStats
        .select(
            CumulativePlayerStats.id,
            fn.MAX(CumulativePlayerStats.date).alias('max_date')
        )
        .group_by(CumulativePlayerStats.id)
    )

    # Join back to get the full record for each player's latest date
    latest_records = (
        CumulativePlayerStats
        .select(CumulativePlayerStats.id, CumulativePlayerStats.gp)
        .join(
            subquery,
            on=(
                (CumulativePlayerStats.id == subquery.c.id) &
                (CumulativePlayerStats.date == subquery.c.max_date)
            )
        )
    )

    return {record.id: record.gp for record in latest_records}


def get_players_who_played(api_data: dict, db_gp_map: dict) -> list[dict]:
    """Compare the data from the NBA API and the database to find players who played"""
    played = []
    for player_id, player in api_data.items():
        # If player is not in DB, they must have played their first game
        if player_id not in db_gp_map:
            played.append(player)
            continue
        # If GP changed, they played
        if player['gp'] != db_gp_map[player_id]:
            played.append(player)
    return played


def main():
    """Main ETL pipeline for cumulative player stats"""
    print("Starting cumulative player stats ETL...")

    # Get yesterday's date (stats are for the previous day)
    central_tz = pytz.timezone('US/Central')
    yesterday = datetime.now(central_tz) - timedelta(days=1)
    date = yesterday.date()
    print(f"Processing stats for date: {date}")

    # Fetch rostered percentages from ESPN
    print("Fetching ESPN rostered data...")
    rostered_data = get_espn_rostered_data(YEAR, LEAGUE_ID)
    print(f"Found {len(rostered_data)} players with rostered data")

    # Fetch current season totals from NBA API
    print("Fetching NBA API data...")
    api_data = fetch_nba_fpts_data(rostered_data)
    print(f"Found {len(api_data)} players from NBA API")

    # Get latest GP for each player from database
    print("Fetching latest GP values from database...")
    db_gp_map = get_latest_gp_by_player()
    print(f"Found {len(db_gp_map)} players in database")

    # Determine which players played yesterday
    players_who_played = get_players_who_played(api_data, db_gp_map)
    print(f"Found {len(players_who_played)} players who played")

    if not players_who_played:
        print("No players played yesterday. Exiting.")
        return

    # Insert new rows for players who played
    print("Inserting cumulative stats for players who played...")
    entries = []
    for player in players_who_played:
        fpts = calculate_fantasy_points(player)
        entries.append({
            'id': player['id'],
            'name': player['name'],
            'team': player['team'],
            'date': date,
            'fpts': fpts,
            'pts': player['pts'],
            'reb': player['reb'],
            'ast': player['ast'],
            'stl': player['stl'],
            'blk': player['blk'],
            'tov': player['tov'],
            'fgm': player['fgm'],
            'fga': player['fga'],
            'fg3m': player['fg3m'],
            'fg3a': player['fg3a'],
            'ftm': player['ftm'],
            'fta': player['fta'],
            'min': player['min'],
            'gp': player['gp'],
            'rost_pct': player['rost_pct']
        })

    # Bulk insert all entries
    CumulativePlayerStats.insert_many(entries).execute()
    print(f"Inserted {len(entries)} new rows")

    # Update ranks for ALL players based on their latest fantasy points
    print("Calculating ranks for all players...")

    # Get the latest entry for each player
    subquery = (
        CumulativePlayerStats
        .select(
            CumulativePlayerStats.id,
            fn.MAX(CumulativePlayerStats.date).alias('max_date')
        )
        .group_by(CumulativePlayerStats.id)
    )

    # Get full records for each player's latest entry, ordered by fpts
    latest_entries = list(
        CumulativePlayerStats
        .select()
        .join(
            subquery,
            on=(
                (CumulativePlayerStats.id == subquery.c.id) &
                (CumulativePlayerStats.date == subquery.c.max_date)
            )
        )
        .order_by(CumulativePlayerStats.fpts.desc())
    )

    # Update rank for each player's latest entry
    for i, player in enumerate(latest_entries, start=1):
        CumulativePlayerStats.update(rank=i).where(
            (CumulativePlayerStats.id == player.id) &
            (CumulativePlayerStats.date == player.date)
        ).execute()

    print(f"Updated ranks for {len(latest_entries)} players")
    print("ETL process completed successfully")


if __name__ == "__main__":
    main()
