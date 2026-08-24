"""
Per-category matchup totals for head-to-head category leagues.

Produces the `category_scores` JSONB snapshot written to stats_s2.daily_matchup_scores:
    {"scoring_format": "categories",
     "you": {canonical stat -> total, rates as 0-1 fractions, plus raw makes/attempts},
     "opp": {...},
     "wins": int, "losses": int, "ties": int}
Mirrors the backend's services/scoring/providers parsers (kept dependency-free here).
"""

from typing import Any, Optional

from utils.stat_vocab import ESPN_ID_TO_KEY, STATS, YAHOO_COMPOSITE_IDS, YAHOO_ID_TO_KEY

ESPN_SCORING_TYPE_MAP = {
    "H2H_POINTS": "points",
    "H2H_CATEGORY": "categories",
    "H2H_MOST_CATEGORIES": "categories",
    "ROTO": "roto",
    "TOTAL_SEASON_POINTS": "points",
}

RAW_KEYS = ("fgm", "fga", "ftm", "fta", "fg3m", "fg3a")


def espn_scoring_format(league_settings: dict) -> str:
    scoring_type = (league_settings or {}).get("scoringSettings", {}).get("scoringType", "H2H_POINTS")
    return ESPN_SCORING_TYPE_MAP.get(scoring_type, "points")


def _with_derived_rates(totals: dict[str, float]) -> dict[str, float]:
    for key, d in STATS.items():
        if d.is_rate and key not in totals and d.numerator in totals and d.denominator in totals:
            den = totals[d.denominator]
            totals[key] = round(totals[d.numerator] / den, 4) if den else 0.0
    return totals


def espn_side_totals(team_matchup: dict) -> Optional[dict[str, float]]:
    """Totals from an ESPN matchup side's cumulativeScore.scoreByStat (None when absent)."""
    cumulative = team_matchup.get("cumulativeScore")
    if not isinstance(cumulative, dict):
        return None
    totals: dict[str, float] = {}
    for stat_id, entry in (cumulative.get("scoreByStat") or {}).items():
        try:
            key = ESPN_ID_TO_KEY.get(int(stat_id))
        except (TypeError, ValueError):
            continue
        if key is None:
            continue
        value = entry.get("score") if isinstance(entry, dict) else entry
        try:
            totals[key] = float(value or 0)
        except (TypeError, ValueError):
            continue
    return _with_derived_rates(totals)


def espn_category_scores(our_side: dict, opp_side: dict) -> Optional[dict[str, Any]]:
    you = espn_side_totals(our_side)
    opp = espn_side_totals(opp_side)
    if you is None or opp is None:
        return None
    cumulative = our_side.get("cumulativeScore") or {}
    return {
        "scoring_format": "categories",
        "you": you,
        "opp": opp,
        "wins": int(cumulative.get("wins") or 0),
        "losses": int(cumulative.get("losses") or 0),
        "ties": int(cumulative.get("ties") or 0),
    }


def _yahoo_stats_to_totals(stats: Any) -> dict[str, float]:
    totals: dict[str, float] = {}
    items = stats if isinstance(stats, list) else list((stats or {}).values())
    for entry in items:
        stat = entry.get("stat", entry) if isinstance(entry, dict) else {}
        try:
            sid = int(stat.get("stat_id"))
        except (TypeError, ValueError):
            continue
        value = stat.get("value")
        if sid in YAHOO_COMPOSITE_IDS and isinstance(value, str) and "/" in value:
            made, att = value.split("/", 1)
            mk, ak = YAHOO_COMPOSITE_IDS[sid]
            totals[mk] = float(made or 0)
            totals[ak] = float(att or 0)
            continue
        key = YAHOO_ID_TO_KEY.get(sid)
        if key is None or STATS[key].is_rate:
            continue
        try:
            totals[key] = float(value) if value not in (None, "", "-") else 0.0
        except (TypeError, ValueError):
            continue
    return _with_derived_rates(totals)


def yahoo_category_scores(matchup_info: dict, our_team_key: str) -> Optional[dict[str, Any]]:
    """Category snapshot from a Yahoo matchup dict (needs per-team team_stats and stat_winners)."""
    stat_winners = matchup_info.get("stat_winners")
    if not stat_winners:
        return None
    teams = matchup_info.get("0", {}).get("teams", {})
    entries = teams.values() if isinstance(teams, dict) else teams
    sides: dict[str, dict[str, float]] = {}
    for t_data in entries:
        if not isinstance(t_data, dict) or "team" not in t_data:
            continue
        details: dict = {}
        team_stats = None
        for item in t_data["team"]:
            if isinstance(item, list):
                for sub in item:
                    if isinstance(sub, dict):
                        details.update(sub)
            elif isinstance(item, dict):
                if "team_stats" in item:
                    team_stats = item["team_stats"]
                else:
                    details.update(item)
        key = details.get("team_key")
        if key:
            sides[key] = _yahoo_stats_to_totals((team_stats or {}).get("stats"))
    if our_team_key not in sides or len(sides) < 2:
        return None
    opp_key = next(k for k in sides if k != our_team_key)
    wins = losses = ties = 0
    for entry in (stat_winners if isinstance(stat_winners, list) else list(stat_winners.values())):
        sw = entry.get("stat_winner", entry) if isinstance(entry, dict) else {}
        if str(sw.get("is_tied", "0")) == "1":
            ties += 1
        elif sw.get("winner_team_key") == our_team_key:
            wins += 1
        elif sw.get("winner_team_key"):
            losses += 1
    return {"scoring_format": "categories", "you": sides[our_team_key], "opp": sides[opp_key],
            "wins": wins, "losses": losses, "ties": ties}
