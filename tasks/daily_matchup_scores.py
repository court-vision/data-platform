"""
Daily Matchup Scores ETL Pipeline

Fetches current matchup scores for all saved teams and records daily snapshots
for time-series visualization.

Run daily via cron/scheduler after games complete (e.g., 3am ET).

Usage:
    python -m tasks.daily_matchup_scores
"""

import os
import json
import requests
from datetime import datetime, date
from typing import Optional

import pytz
from playhouse.db_url import connect

from db.models.season2.daily_matchup_score import DailyMatchupScore


# ESPN API Configuration
ESPN_FANTASY_ENDPOINT = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/fba/seasons/{}/segments/0/leagues/{}"

# Schedule file path
SCHEDULE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "static", "schedule25-26.json"
)


def load_schedule() -> dict:
    """Load matchup schedule from JSON file."""
    with open(SCHEDULE_PATH, "r") as f:
        return json.load(f)


def get_current_matchup_info(current_date: date) -> Optional[dict]:
    """
    Determine current matchup period and day index.

    Returns:
        dict with matchup_number, start_date, end_date, day_index
        or None if no active matchup
    """
    schedule = load_schedule().get("schedule", {})

    for matchup_num, matchup_data in schedule.items():
        start = datetime.strptime(matchup_data["startDate"], "%m/%d/%Y").date()
        end = datetime.strptime(matchup_data["endDate"], "%m/%d/%Y").date()

        if start <= current_date <= end:
            return {
                "matchup_number": int(matchup_num),
                "start_date": start,
                "end_date": end,
                "day_index": (current_date - start).days,
            }

    return None


def get_all_saved_teams() -> list[dict]:
    """
    Fetch all saved teams from the backend database.

    Returns list of dicts with team_id and league_info.
    """
    backend_db_url = os.getenv("BACKEND_DATABASE_URL")
    if not backend_db_url:
        print("ERROR: BACKEND_DATABASE_URL environment variable not set")
        return []

    backend_db = connect(backend_db_url)

    try:
        cursor = backend_db.execute_sql(
            """
            SELECT team_id, league_info
            FROM usr.teams
            """
        )

        teams = []
        for row in cursor.fetchall():
            team_id, league_info_json = row
            league_info = json.loads(league_info_json)
            teams.append(
                {
                    "team_id": team_id,
                    "league_id": league_info["league_id"],
                    "team_name": league_info["team_name"],
                    "espn_s2": league_info["espn_s2"],
                    "swid": league_info["swid"],
                    "year": league_info["year"],
                }
            )

        return teams
    finally:
        backend_db.close()


def fetch_matchup_from_espn(
    league_id: int,
    team_name: str,
    espn_s2: str,
    swid: str,
    year: int,
    matchup_period: int,
) -> Optional[dict]:
    """
    Fetch matchup data from ESPN API for a specific team.

    Returns dict with team scores and opponent info, or None on error.
    """
    params = {"view": ["mTeam", "mMatchup", "mSchedule"]}

    cookies = {"espn_s2": espn_s2, "SWID": swid}

    endpoint = ESPN_FANTASY_ENDPOINT.format(year, league_id)

    try:
        response = requests.get(endpoint, params=params, cookies=cookies, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"  ESPN API error for league {league_id}: {e}")
        return None

    # Find our team
    teams = data.get("teams", [])
    our_team = None
    our_team_id = None

    for team in teams:
        if team.get("name", "").strip() == team_name.strip():
            our_team = team
            our_team_id = team.get("id")
            break

    if not our_team:
        print(f"  Team '{team_name}' not found in league {league_id}")
        return None

    # Find current matchup
    schedule = data.get("schedule", [])
    current_matchup = None
    opponent_team_id = None
    our_score = 0
    opponent_score = 0

    for matchup in schedule:
        if matchup.get("matchupPeriodId") == matchup_period:
            home_data = matchup.get("home", {})
            away_data = matchup.get("away", {})
            home_id = home_data.get("teamId")
            away_id = away_data.get("teamId")

            if home_id == our_team_id:
                current_matchup = matchup
                opponent_team_id = away_id
                our_score = home_data.get("totalPoints", 0)
                opponent_score = away_data.get("totalPoints", 0)
                break
            elif away_id == our_team_id:
                current_matchup = matchup
                opponent_team_id = home_id
                our_score = away_data.get("totalPoints", 0)
                opponent_score = home_data.get("totalPoints", 0)
                break

    if not current_matchup:
        print(f"  No matchup found for period {matchup_period}")
        return None

    # Find opponent team name
    opponent_team_name = "Unknown"
    for team in teams:
        if team.get("id") == opponent_team_id:
            opponent_team_name = team.get("name", "Unknown")
            break

    return {
        "team_name": our_team.get("name"),
        "current_score": our_score,
        "opponent_team_name": opponent_team_name,
        "opponent_current_score": opponent_score,
    }


def upsert_daily_score(
    team_id: int,
    matchup_period: int,
    espn_data: dict,
    snapshot_date: date,
    day_index: int,
) -> None:
    """
    Insert or update daily matchup score record.

    Uses ON CONFLICT for idempotency.
    """
    record = {
        "team_id": team_id,
        "team_name": espn_data["team_name"],
        "matchup_period": matchup_period,
        "opponent_team_name": espn_data["opponent_team_name"],
        "date": snapshot_date,
        "day_of_matchup": day_index,
        "current_score": espn_data["current_score"],
        "opponent_current_score": espn_data["opponent_current_score"],
    }

    # Upsert: insert or update on conflict
    DailyMatchupScore.insert(record).on_conflict(
        conflict_target=[
            DailyMatchupScore.team_id,
            DailyMatchupScore.matchup_period,
            DailyMatchupScore.date,
        ],
        update={
            "current_score": record["current_score"],
            "opponent_current_score": record["opponent_current_score"],
            "team_name": record["team_name"],
            "opponent_team_name": record["opponent_team_name"],
        },
    ).execute()


def main():
    """Main ETL pipeline for daily matchup scores."""
    print("Starting daily matchup scores ETL...")

    # Use Central timezone (NBA games end late)
    central_tz = pytz.timezone("US/Central")
    now = datetime.now(central_tz)
    today = now.date()

    print(f"Processing for date: {today}")

    # Get current matchup info
    matchup_info = get_current_matchup_info(today)
    if not matchup_info:
        print("No active matchup period. Exiting.")
        return

    print(
        f"Current matchup period: {matchup_info['matchup_number']}, day {matchup_info['day_index']}"
    )

    # Get all saved teams
    print("Fetching saved teams from backend database...")
    teams = get_all_saved_teams()
    print(f"Found {len(teams)} saved teams")

    if not teams:
        print("No teams to process. Exiting.")
        return

    # Process each team
    success_count = 0
    error_count = 0

    for team in teams:
        try:
            print(f"Processing team_id={team['team_id']}: {team['team_name']}")

            espn_data = fetch_matchup_from_espn(
                league_id=team["league_id"],
                team_name=team["team_name"],
                espn_s2=team["espn_s2"],
                swid=team["swid"],
                year=team["year"],
                matchup_period=matchup_info["matchup_number"],
            )

            if espn_data:
                upsert_daily_score(
                    team_id=team["team_id"],
                    matchup_period=matchup_info["matchup_number"],
                    espn_data=espn_data,
                    snapshot_date=today,
                    day_index=matchup_info["day_index"],
                )
                success_count += 1
                print(
                    f"  -> Score: {espn_data['current_score']} vs {espn_data['opponent_current_score']}"
                )
            else:
                error_count += 1
                print("  -> Failed to fetch data")

        except Exception as e:
            error_count += 1
            print(f"  -> Error: {e}")

    print(f"\nCompleted: {success_count} success, {error_count} errors")


if __name__ == "__main__":
    main()
