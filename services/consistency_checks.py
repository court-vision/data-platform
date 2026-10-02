"""
Consistency checks: do the tables agree with each other?

The structural checks look at one table at a time. These hold one table to
account against another: a player's season totals must keep pace with his game
log, a team's record must be its final games in the schedule, a game's score
must be the points its players scored, a player tracked live must end up with
a game row.

Every check is written as the query that returns the offending rows. The count
of those rows is the result; the first few are kept with a failure so the
dashboard can say which player and which night, not just how many.

What is judged. Each check looks at the last `LOOKBACK_DAYS` game nights that
are due, where a night is due once its 6 AM Eastern deadline has passed (the
rule the pipelines date their rows by, `cv_core.nba_calendar`). Two kinds:

- A fact about one night (a game's score, a game row's link) is judged for
  every night in the window, so it stays failing until it is repaired or the
  night ages out.
- A running total (season stats, team records, rolling averages) is judged as
  it stands now, on its newest row. Last night's lag is a failure this
  morning and gone tomorrow once the table catches up; the history of who was
  behind when lives in the run history, not in the window.

`build_consistency_checks(through=...)` pins the window to a date instead,
which is how the checks are replayed over nights already played.

Regular season only where it matters: the season totals and the team records
count regular-season games, so the live table and the schedule are filtered to
regular-season game ids (the `002` prefix) when compared to them. The game log
is fetched as regular season but also carries the Cup final (`006`), which no
season total counts: it is left out when the log is held against the totals.

No `%` anywhere in the SQL: the service runs it through psycopg2 with an empty
parameter list, which would read one as a placeholder.
"""

from __future__ import annotations

import textwrap
from datetime import date

from services.quality_check import SAMPLE_LIMIT, SQLQualityCheck

LOOKBACK_DAYS = 7

# The last game night whose batch is due: the NBA date (6 AM Eastern rule)
# minus one. NOW() is timestamptz, so the session's timezone does not matter.
LAST_DUE_NIGHT_SQL = "((NOW() AT TIME ZONE 'America/New_York') - INTERVAL '6 hours')::date - 1"

# NBA game ids start with the kind of game: 001 preseason, 002 regular season,
# 004 playoffs, 006 the Cup final (which counts in no one's totals).
REGULAR_SEASON_PREFIX = "'002'"

# The counting stats both the game log and the season totals carry. Minutes are
# left out: the game log truncates them per game, so their sum is not the total.
COUNTING_STATS: tuple[str, ...] = (
    "pts", "reb", "ast", "stl", "blk", "tov",
    "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
)

# Rolling averages are rounded to 2 places in Python and compared to
# Postgres's own average: half a cent of slack covers the two rounding rules.
ROUNDING_SLACK = "0.006"


def _window(through: date | None, days: int) -> str:
    """The `win` CTE: the nights judged, and the season the last of them is in.

    A season runs August to July (`cv_core.season.season_for_date`), so seven
    months back from any night lands in the season's starting year.
    """
    night = f"DATE '{through.isoformat()}'" if through else LAST_DUE_NIGHT_SQL
    return "\n".join((
        "win AS (",
        f"    SELECT night - {days - 1} AS since, night AS through,",
        "           MAKE_DATE(y, 8, 1) AS season_start,",
        "           y || '-' || LPAD(MOD(y + 1, 100)::text, 2, '0') AS season",
        "    FROM (",
        "        SELECT night, EXTRACT(YEAR FROM night - INTERVAL '7 months')::int AS y",
        f"        FROM (SELECT {night} AS night) due",
        "    ) dated",
        ")",
    ))


def _season_gaps() -> str:
    """CTEs ending in `gaps`: season totals minus the game log, now and before.

    One row per player whose game log or season row moved inside the window.
    A gap is the season row's value minus the same thing summed from the game
    log. `_gap` is where the two stand at the end of the window. `_gap_before`
    is where they stood at the player's last season row before the window,
    measured on that row's own date: a season row is written when the games
    played move, so that is usually a moment the two had just been brought
    together. (Measured on the window's first morning instead, any player whose
    late game was still missing that morning would carry the lag in as his
    baseline.)

    Usually, not always. On a back-to-back where both games arrive late, the
    second night's row is written because the first night's game moved the
    count, and still lacks its own night's game: a baseline one game short. A
    week later the totals have caught up and the gap reads as new. The games
    check therefore lets a gap of exactly zero be, whatever the baseline says.
    What that gives up: a player with a standing surplus (a game the log never
    got) whose season row then falls that many games behind reads as zero and
    is missed.

    Comparing gaps rather than values is what lets old history be: a player
    whose totals have disagreed with his log since November (a stat correction,
    a game the log never got) carries the same gap all season, and only a gap
    that is new this week is a failure.

    The game log is read as the season totals count it: without the Cup final.
    A row with no game id is one the schedule did not have when it was written,
    and counts.
    """
    regular = f"(pgs.game_id IS NULL OR LEFT(pgs.game_id, 3) = {REGULAR_SEASON_PREFIX})"
    stats = ("gp", *COUNTING_STATS)
    columns = ", ".join(f"pss.{s}" for s in stats)
    before = "FILTER (WHERE pgs.game_date <= b.as_of_date)"
    logged = ",\n".join(
        f"           COUNT(*) AS gp, COUNT(*) {before} AS gp_before"
        if s == "gp" else
        f"           COALESCE(SUM(pgs.{s}), 0) AS {s},"
        f" COALESCE(SUM(pgs.{s}) {before}, 0) AS {s}_before"
        for s in stats
    )
    gaps = ",\n".join(
        f"           COALESCE(n.{s}, 0) - COALESCE(l.{s}, 0) AS {s}_gap,"
        f" COALESCE(b.{s}, 0) - COALESCE(l.{s}_before, 0) AS {s}_gap_before"
        for s in stats
    )
    return "\n".join((
        "active AS (",
        "    SELECT pgs.player_id FROM nba.player_game_stats pgs CROSS JOIN win",
        "    WHERE pgs.game_date BETWEEN win.since AND win.through",
        f"      AND {regular}",
        "    UNION",
        "    SELECT pss.player_id FROM nba.player_season_stats pss CROSS JOIN win",
        "    WHERE pss.as_of_date BETWEEN win.since AND win.through",
        "),",
        "season_now AS (",
        f"    SELECT DISTINCT ON (pss.player_id) pss.player_id, pss.as_of_date, {columns}",
        "    FROM nba.player_season_stats pss",
        "    CROSS JOIN win",
        "    JOIN active a ON a.player_id = pss.player_id",
        "    WHERE pss.season = win.season AND pss.as_of_date <= win.through",
        "    ORDER BY pss.player_id, pss.as_of_date DESC",
        "),",
        "season_before AS (",
        f"    SELECT DISTINCT ON (pss.player_id) pss.player_id, pss.as_of_date, {columns}",
        "    FROM nba.player_season_stats pss",
        "    CROSS JOIN win",
        "    JOIN active a ON a.player_id = pss.player_id",
        "    WHERE pss.season = win.season AND pss.as_of_date < win.since",
        "    ORDER BY pss.player_id, pss.as_of_date DESC",
        "),",
        "logged AS (",
        "    SELECT pgs.player_id,",
        logged,
        "    FROM nba.player_game_stats pgs",
        "    CROSS JOIN win",
        "    JOIN active a ON a.player_id = pgs.player_id",
        "    LEFT JOIN season_before b ON b.player_id = pgs.player_id",
        "    WHERE pgs.game_date BETWEEN win.season_start AND win.through",
        f"      AND {regular}",
        "    GROUP BY pgs.player_id",
        "),",
        "gaps AS (",
        "    SELECT a.player_id, n.as_of_date AS season_row,",
        "           COALESCE(n.gp, 0) AS season_gp, COALESCE(l.gp, 0) AS games_logged,",
        gaps,
        "    FROM active a",
        "    LEFT JOIN logged l ON l.player_id = a.player_id",
        "    LEFT JOIN season_now n ON n.player_id = a.player_id",
        "    LEFT JOIN season_before b ON b.player_id = a.player_id",
        ")",
    ))


def _fragments(window: str) -> dict[str, str]:
    """The pieces the check queries share, each written flush left."""
    def new_gap(stat: str) -> str:
        return f"g.{stat}_gap <> g.{stat}_gap_before"

    return {
        "win": window,
        "season_gaps": _season_gaps(),
        "regular": REGULAR_SEASON_PREFIX,
        "slack": ROUNDING_SLACK,
        # The gap is not what it was before the window.
        "new_games_gap": new_gap("gp"),
        "new_stat_gap": "\nOR ".join(new_gap(stat) for stat in COUNTING_STATS),
        # A readable list of the stats that moved apart: "pts +1, ftm +1".
        "stat_list": ",\n".join(
            f"CASE WHEN {new_gap(stat)} THEN '{stat} '"
            f" || TO_CHAR(g.{stat}_gap - g.{stat}_gap_before, 'FMSG999990') END"
            for stat in COUNTING_STATS
        ),
        "won": (
            "(g.home_team_id = ts.team_id AND g.home_score > g.away_score)\n"
            "OR (g.away_team_id = ts.team_id AND g.away_score > g.home_score)"
        ),
    }


def _fill(template: str, fragments: dict[str, str]) -> str:
    """Put each `{fragment}` into the query, its later lines aligned under its first."""
    lines = []
    for line in textwrap.dedent(template).strip().splitlines():
        for token, fragment in fragments.items():
            marker = "{" + token + "}"
            column = line.find(marker)
            if column >= 0:
                line = line.replace(marker, fragment.replace("\n", "\n" + " " * column))
        lines.append(line)
    return "\n".join(lines)


def _check(
    name: str,
    table: str,
    against: tuple[str, ...],
    rows: str,
    failure_message: str,
    fragments: dict[str, str],
    severity: str = "warning",
) -> SQLQualityCheck:
    """A check from the query that returns its offending rows."""
    body = textwrap.indent(_fill(rows, fragments), "    ")
    return SQLQualityCheck(
        name=name,
        severity=severity,
        table=table,
        group="consistency",
        against=against,
        sql=f"SELECT COUNT(*) FROM (\n{body}\n) offending",
        sample_sql=f"SELECT * FROM (\n{body}\n) offending\nLIMIT {SAMPLE_LIMIT}",
        failure_message=failure_message,
    )


def build_consistency_checks(
    through: date | None = None,
    days: int = LOOKBACK_DAYS,
) -> tuple[SQLQualityCheck, ...]:
    """The consistency checks, over the `days` game nights ending at `through`.

    `through=None` is the live rule (the last night that is due). A date pins
    the window, for replaying the checks over nights already played.

    All of them are warnings: they have not yet run against a live season, and
    a critical check pages. Promote one once its failures have proved real.
    """
    fragments = _fragments(_window(through, days))

    return (
        # --- the game log against the tables built from it ---------------------
        _check(
            name="player_season_stats_games_keep_pace_with_game_log",
            table="nba.player_season_stats",
            against=("nba.player_game_stats",),
            fragments=fragments,
            rows="""
                WITH {win},
                {season_gaps}
                SELECT g.player_id, p.name AS player, g.season_row,
                       g.season_gp, g.games_logged,
                       g.gp_gap AS gap, g.gp_gap_before AS gap_before
                FROM gaps g
                JOIN nba.players p ON p.id = g.player_id
                WHERE {new_games_gap}
                  AND g.gp_gap <> 0
                ORDER BY g.season_row DESC NULLS FIRST, g.player_id
            """,
            failure_message=(
                "season games played and the game log have moved apart this week:"
                " a negative gap is season totals a game behind (and a stale"
                " ranking), a positive one a game the log never got"
            ),
        ),
        _check(
            name="player_season_stats_totals_keep_pace_with_game_log",
            table="nba.player_season_stats",
            against=("nba.player_game_stats",),
            fragments=fragments,
            rows="""
                WITH {win},
                {season_gaps}
                SELECT g.player_id, p.name AS player, g.season_row, g.season_gp,
                       CONCAT_WS(', ',
                           {stat_list}
                       ) AS season_moved_vs_game_log
                FROM gaps g
                JOIN nba.players p ON p.id = g.player_id
                WHERE NOT ({new_games_gap})
                  AND ({new_stat_gap})
                ORDER BY g.season_row DESC NULLS FIRST, g.player_id
            """,
            failure_message=(
                "season totals and the game log count the same games but moved by"
                " different amounts this week: a stat correction one of them missed"
            ),
        ),
        _check(
            name="player_rolling_stats_row_for_every_game_played",
            table="nba.player_rolling_stats",
            against=("nba.player_game_stats",),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT pgs.player_id, p.name AS player, pgs.game_date,
                       COUNT(DISTINCT r.window_days) AS windows_refreshed
                FROM nba.player_game_stats pgs
                CROSS JOIN win
                JOIN nba.players p ON p.id = pgs.player_id
                LEFT JOIN nba.player_rolling_stats r
                  ON r.player_id = pgs.player_id
                 AND r.as_of_date >= pgs.game_date
                WHERE pgs.game_date BETWEEN win.since AND win.through
                GROUP BY pgs.player_id, p.name, pgs.game_date
                HAVING COUNT(DISTINCT r.window_days) < 3
                ORDER BY pgs.game_date DESC, pgs.player_id
            """,
            failure_message=(
                "players have a game row but their 7/14/30-day averages were not"
                " recomputed on or after that night"
            ),
        ),
        _check(
            name="player_rolling_stats_match_game_log",
            table="nba.player_rolling_stats",
            against=("nba.player_game_stats",),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT r.player_id, p.name AS player, r.as_of_date, r.window_days,
                       r.gp AS rolling_gp, COUNT(pgs.id) AS games_logged,
                       r.pts AS rolling_pts, ROUND(AVG(pgs.pts), 2) AS game_log_pts
                FROM nba.player_rolling_stats r
                CROSS JOIN win
                JOIN nba.players p ON p.id = r.player_id
                LEFT JOIN nba.player_game_stats pgs
                  ON pgs.player_id = r.player_id
                 AND pgs.game_date BETWEEN r.as_of_date - (r.window_days - 1) AND r.as_of_date
                WHERE r.as_of_date >= win.since
                  AND r.as_of_date = (
                      SELECT MAX(newest.as_of_date)
                      FROM nba.player_rolling_stats newest
                      WHERE newest.as_of_date <= win.through
                  )
                GROUP BY r.player_id, p.name, r.as_of_date, r.window_days, r.gp, r.pts, r.fpts
                HAVING r.gp <> COUNT(pgs.id)
                    OR ABS(r.pts - COALESCE(AVG(pgs.pts), 0)) > {slack}
                    OR ABS(r.fpts - COALESCE(AVG(pgs.fpts), 0)) > {slack}
                ORDER BY r.player_id, r.window_days
            """,
            failure_message=(
                "the newest rolling averages no longer equal the game log they were"
                " computed from: a game row changed after they were built"
            ),
        ),
        _check(
            name="player_game_stats_game_link_agrees",
            table="nba.player_game_stats",
            against=("nba.games",),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT pgs.player_id, p.name AS player, pgs.game_date, pgs.team_id AS team,
                       pgs.game_id, g.game_date AS linked_game_date,
                       g.away_team_id || ' @ ' || g.home_team_id AS linked_game
                FROM nba.player_game_stats pgs
                CROSS JOIN win
                JOIN nba.players p ON p.id = pgs.player_id
                LEFT JOIN nba.games g ON g.game_id = pgs.game_id
                WHERE pgs.game_date BETWEEN win.since AND win.through
                  AND (
                      pgs.game_id IS NULL
                      OR g.game_date <> pgs.game_date
                      OR pgs.team_id NOT IN (g.home_team_id, g.away_team_id)
                  )
                ORDER BY pgs.game_date DESC, pgs.player_id
            """,
            failure_message=(
                "game rows are not linked to a game, or are linked to one on another"
                " date or between two other teams"
            ),
        ),
        # --- the schedule against what was played -----------------------------
        _check(
            name="games_score_matches_player_points",
            table="nba.games",
            against=("nba.player_game_stats",),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT g.game_id, g.game_date, side.team, side.score,
                       COALESCE(SUM(pgs.pts), 0) AS player_points,
                       COUNT(pgs.id) AS player_rows
                FROM nba.games g
                CROSS JOIN win
                CROSS JOIN LATERAL (
                    VALUES (g.home_team_id, g.home_score), (g.away_team_id, g.away_score)
                ) AS side(team, score)
                LEFT JOIN nba.player_game_stats pgs
                  ON pgs.game_id = g.game_id
                 AND pgs.team_id = side.team
                WHERE g.status = 'final'
                  AND LEFT(g.game_id, 3) = {regular}
                  AND g.game_date BETWEEN win.since AND win.through
                GROUP BY g.game_id, g.game_date, side.team, side.score
                HAVING side.score IS DISTINCT FROM COALESCE(SUM(pgs.pts), 0)
                ORDER BY g.game_date DESC, g.game_id, side.team
            """,
            failure_message=(
                "a final game's score is not the sum of its players' points in the"
                " game log: player rows are missing, extra or on the wrong team"
            ),
        ),
        _check(
            name="games_final_after_game_night",
            table="nba.games",
            against=(),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT g.game_id, g.game_date,
                       g.away_team_id || ' @ ' || g.home_team_id AS game,
                       g.status, g.away_score, g.home_score
                FROM nba.games g
                CROSS JOIN win
                WHERE g.game_date BETWEEN win.since AND win.through
                  AND LEFT(g.game_id, 3) = {regular}
                  AND (g.status <> 'final' OR g.home_score IS NULL OR g.away_score IS NULL)
                ORDER BY g.game_date DESC, g.game_id
            """,
            failure_message=(
                "regular-season games on a night that is already due are not final"
                " with a score: the schedule has not caught up, or a game was postponed"
            ),
        ),
        # Apart from the check above, so a regular-season miss is not buried
        # under this one: the nightly game_schedule pipeline fetches
        # regular-season results only, and until it fetches the rest these rows
        # wait for Monday's schedule-sync.
        _check(
            name="games_outside_regular_season_final_after_game_night",
            table="nba.games",
            against=(),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT g.game_id, g.game_date,
                       g.away_team_id || ' @ ' || g.home_team_id AS game,
                       g.status, g.away_score, g.home_score
                FROM nba.games g
                CROSS JOIN win
                WHERE g.game_date BETWEEN win.since AND win.through
                  AND LEFT(g.game_id, 3) <> {regular}
                  AND (g.status <> 'final' OR g.home_score IS NULL OR g.away_score IS NULL)
                ORDER BY g.game_date DESC, g.game_id
            """,
            failure_message=(
                "play-in, playoff or Cup games on a night that is already due are"
                " not final with a score: the nightly schedule pipeline fetches"
                " regular-season results only, so the app shows them as scheduled"
                " until the weekly schedule sync"
            ),
        ),
        _check(
            name="team_stats_record_matches_schedule",
            table="nba.team_stats",
            against=("nba.games",),
            fragments=fragments,
            rows="""
                WITH {win},
                newest AS (
                    SELECT DISTINCT ON (ts.team_id)
                           ts.team_id, ts.season, ts.as_of_date, ts.gp, ts.w
                    FROM nba.team_stats ts
                    CROSS JOIN win
                    WHERE ts.as_of_date BETWEEN win.since AND win.through
                    ORDER BY ts.team_id, ts.as_of_date DESC
                )
                SELECT ts.team_id AS team, ts.as_of_date AS team_stats_row,
                       ts.gp AS team_stats_gp, COUNT(g.game_id) AS final_games,
                       ts.w AS team_stats_wins,
                       COUNT(g.game_id) FILTER (WHERE
                           {won}
                       ) AS wins_in_schedule
                FROM newest ts
                CROSS JOIN win
                LEFT JOIN nba.games g
                  ON g.season = ts.season
                 AND g.status = 'final'
                 AND LEFT(g.game_id, 3) = {regular}
                 AND g.game_date <= win.through
                 AND ts.team_id IN (g.home_team_id, g.away_team_id)
                GROUP BY ts.team_id, ts.as_of_date, ts.gp, ts.w
                HAVING ts.gp IS DISTINCT FROM COUNT(g.game_id)
                    OR ts.w IS DISTINCT FROM COUNT(g.game_id) FILTER (WHERE
                           {won}
                       )
                ORDER BY ts.team_id
            """,
            failure_message=(
                "a team's games played or wins differ from its final games in the"
                " schedule: team stats are a game behind, or moved without a game"
            ),
        ),
        # --- live against settled ---------------------------------------------
        _check(
            name="live_players_have_game_log",
            table="nba.player_game_stats",
            against=("nba.live_player_stats",),
            fragments=fragments,
            rows="""
                WITH {win}
                SELECT l.player_id, p.name AS player, l.game_date, l.game_id,
                       l.min AS live_minutes, l.pts AS live_points, l.fpts AS live_fpts
                FROM nba.live_player_stats l
                CROSS JOIN win
                JOIN nba.players p ON p.id = l.player_id
                WHERE l.game_date <= win.through
                  AND LEFT(l.game_id, 3) = {regular}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM nba.player_game_stats pgs
                      WHERE pgs.player_id = l.player_id
                        AND pgs.game_date = l.game_date
                  )
                ORDER BY l.game_id, l.player_id
            """,
            failure_message=(
                "players were tracked live on a night that is already due but have"
                " no game row for it: the app showed points that never settled"
            ),
        ),
    )


CONSISTENCY_CHECKS: tuple[SQLQualityCheck, ...] = build_consistency_checks()
