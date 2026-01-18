from datetime import timedelta
from datetime import datetime
import json
import unicodedata
from pandas.core.indexes.datetimes import pytz
import requests
from nba_api.stats.endpoints import scoreboardv2, playergamelogs
import pandas as pd
from db.models.season2.daily_player_stats import DailyPlayerStats


def normalize_name(name: str) -> str:
	"""
	Normalize a name by removing diacritics and converting to lowercase.

	Examples:
		"Nikola Jokić" -> "nikola jokic"
		"Luka Dončić" -> "luka doncic"
	"""
	normalized = unicodedata.normalize('NFD', name)
	ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
	return ascii_name.lower().strip()

def get_game_ids(date: str) -> list[str]:
	scoreboard = scoreboardv2.ScoreboardV2(game_date=date)
	games = scoreboard.get_dict()['resultSets'][0]['rowSet']
	game_ids = [game[2] for game in games]
	return game_ids

def get_espn_player_data(year: int, league_id: int) -> dict:
	"""
	Fetch ESPN player data including ESPN ID mapping.

	Returns:
		dict: Mapping from normalized player name to {'espn_id': int, 'rost_pct': float}
	"""
	params = {
			'view': 'kona_player_info',
			'scoringPeriodId': 0,
	}
	endpoint = 'https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{}/segments/0/leagues/{}'.format(year, league_id)
	filters = {"players":{"filterSlotIds":{"value":[]},"limit": 750, "sortPercOwned":{"sortPriority":1,"sortAsc":False},"sortDraftRanks":{"sortPriority":2,"sortAsc":True,"value":"STANDARD"}}}
	headers = {'x-fantasy-filter': json.dumps(filters)}

	data = requests.get(endpoint, params=params, headers=headers).json()
	data = data['players']
	data = [x.get('player', x) for x in data]

	cleaned_data = {}
	for player in data:
		if player and 'fullName' in player:
			normalized = normalize_name(player['fullName'])
			cleaned_data[normalized] = {
				'espn_id': player['id'],
				'rost_pct': player.get('ownership', {}).get('percentOwned', 0)
			}

	return cleaned_data

# Helper function to convert minutes from MM:SS format to integer minutes
def minutes_to_int(min_str: str) -> int:
	if isinstance(min_str, (int, float)):
		return int(min_str)
	if ':' in str(min_str):
		parts = str(min_str).split(':')
		return int(parts[0])
	return int(min_str)

year = 2026
league_id = 993431466

espn_player_data = get_espn_player_data(year, league_id)

def get_espn_info(player_name: str) -> dict | None:
	"""
	Get ESPN info (espn_id, rost_pct) for a player by name.

	Uses normalized name matching to handle diacritics (e.g., Jokić -> jokic).

	Args:
		player_name: The player's name from NBA API

	Returns:
		dict with 'espn_id' and 'rost_pct', or None if not found
	"""
	normalized = normalize_name(player_name)
	return espn_player_data.get(normalized)

def calculate_fantasy_points(stats: pd.Series) -> float:
	points_score = stats['PTS']
	rebounds_score = stats['REB']
	assists_score = stats['AST'] * 2
	stocks_score = (stats['STL'] + stats['BLK']) * 4
	turnovers_score = stats['TOV'] * -2
	three_pointers_score = stats['FG3M']
	fg_eff_score = (stats['FGM'] * 2) - stats['FGA']
	ft_eff_score = stats['FTM'] - stats['FTA']

	return points_score + rebounds_score + assists_score + stocks_score + turnovers_score + three_pointers_score + fg_eff_score + ft_eff_score


def main():
	central_tz = pytz.timezone('US/Central')
	yesterday = datetime.now(central_tz) - timedelta(days=1)
	game_date = yesterday.date()
	
	# Format date as YYYYMMDD for scoreboard
	date_str_scoreboard = yesterday.strftime('%Y%m%d')
	# Format date as MM/DD/YYYY for playergamelogs
	date_str = yesterday.strftime('%m/%d/%Y')
	
	# Try to get game IDs first to verify games exist
	try:
		scoreboard = scoreboardv2.ScoreboardV2(game_date=date_str_scoreboard)
		games = scoreboard.get_dict()['resultSets'][0]['rowSet']
		game_ids = [game[2] for game in games]
		print(f"Found {len(game_ids)} games for {date_str}")
	except Exception as e:
		print(f"Error getting game IDs: {e}")
		game_ids = []
	
	# Try to get player game logs for yesterday
	try:
		# Get current season (2025-26 format)
		season = f"{yesterday.year}-{str(yesterday.year + 1)[-2:]}"
		if yesterday.month < 8:  # Before August, use previous season
			season = f"{yesterday.year - 1}-{str(yesterday.year)[-2:]}"
		
		game_logs = playergamelogs.PlayerGameLogs(
			date_from_nullable=date_str,
			date_to_nullable=date_str,
			season_nullable=season
		)
		stats = game_logs.player_game_logs.get_data_frame()
		
		if stats.empty:
			print(f"No player stats found for {date_str}")
			return
		
		print(f"Found {len(stats)} player game logs for {date_str}")
		
		# Calculate fantasy scores
		stats.loc[:, "fantasyScore"] = stats.apply(calculate_fantasy_points, axis=1)
		
		for _, row in stats.iterrows():
			# Skip players who didn't play (indicated by blank/null/empty minutes)
			minutes_value = row['MIN']
			
			# Check for null, NaN, empty string, or None
			if pd.isna(minutes_value) or minutes_value == '' or minutes_value is None:
				continue
			
			# Convert to integer and skip if it's 0 (player didn't play)
			minutes_int = minutes_to_int(minutes_value)
			if minutes_int == 0:
				continue
			
			player_name = row['PLAYER_NAME']
			espn_info = get_espn_info(player_name)
			espn_id = espn_info['espn_id'] if espn_info else None
			rost_pct = espn_info['rost_pct'] if espn_info else None

			DailyPlayerStats.create(
				id=int(row['PLAYER_ID']),
				espn_id=espn_id,
				name=player_name,
				team=row['TEAM_ABBREVIATION'],
				date=game_date,
				fpts=int(round(row['fantasyScore'])),
				pts=int(row['PTS']),
				reb=int(row['REB']),
				ast=int(row['AST']),
				stl=int(row['STL']),
				blk=int(row['BLK']),
				tov=int(row['TOV']),
				fgm=int(row['FGM']),
				fga=int(row['FGA']),
				fg3m=int(row['FG3M']),
				fg3a=int(row['FG3A']),
				ftm=int(row['FTM']),
				fta=int(row['FTA']),
				min=minutes_int,
				rost_pct=rost_pct
			)
	except Exception as e:
		print(f"Error getting player game logs: {e}")
		import traceback
		traceback.print_exc()
		raise

if __name__ == "__main__":
	main()