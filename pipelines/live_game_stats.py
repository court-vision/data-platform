"""
Live Game Stats Pipeline

Fetches in-progress box scores from NBA API live endpoints and writes
current player stats to nba.live_player_stats. Designed to run every
~60 seconds during active game windows.

Does no time-window gating itself — gating is handled by the trigger
endpoint (POST /v1/internal/pipelines/live-stats).
"""

from collections.abc import Iterable
from datetime import date, datetime, timezone
from typing import NamedTuple

from core.settings import settings
from db.base import db
from db.models.nba import Player, LivePlayerStats, LiveGameScoreSnapshot
from pipelines.availability import record_boxscore
from pipelines.base import BasePipeline
from pipelines.config import PipelineConfig, PipelineCategory
from pipelines.context import PipelineContext
from pipelines.extractors import NBAApiExtractor
from pipelines.transformers import calculate_fantasy_points, minutes_to_int
from pipelines.transformers.minutes import seconds_played


# Which of a night's games already have their final box score stored.
#
# A game is settled when none of its rows is still live and a *successful* run
# stored its box score at least `settle` minutes after the scoreboard called it
# final (the last time: a final taken back after a buzzer review starts the
# margin again). A row carries the id of the last run that upserted it, and
# that run's snapshot of the game (recorded just before it fetched the box
# score) says when that was. Row timestamps cannot answer this: the finalize_*
# helpers mark rows final and touch `last_updated` without reading a box score,
# so a game whose last box-score read was mid-fourth can look final and freshly
# written.
#
# The query starts from the rows' writers (a run or two per game), not from the
# snapshots: only the newest writer's id survives on a row, so walking a night
# of snapshots to find it took 2.9 s on 2026-10-12's 6,884 snapshot rows.
#
# The last step joins stored_final and open_games rather than asking EXISTS of
# each per game. Correlated, the planner prices those subqueries once per
# estimated game (200, its default for a CTE's distinct values), which put the
# plan at a cost of 102,576 on 2026-10-12's data: over prod's jit_above_cost of
# 100,000, so every call paid about 58 ms of LLVM compilation and took 70-115
# ms. Joined, the plan costs 862, compiles nothing and runs in 10-15 ms
# (EXPLAIN ANALYZE, TIMING OFF) on the same data, returning the same rows. Its
# cost grows with the night's snapshots, linearly now, so a 15-game slate stays
# far under the threshold; tests/integration/test_live_stats_settled.py holds
# it there.
#
# `seen` is every game the scoreboard has listed for the date on a run (each
# run records a snapshot of each game it does not skip), so "every game of the
# night" needs no scoreboard call. It knows only what the runs' scoreboards
# listed, though, so the endpoint's check (stored_final_night) adds the slate it
# gated on: a game on tonight's schedule that no run has listed keeps the night
# open. `open_rows` is any row of the date not yet final, whichever game it is for.
STORED_FINAL_GAMES_SQL = """
    WITH snaps AS (
        SELECT game_id, game_status, captured_at, pipeline_run_id
        FROM nba.live_game_score_snapshots
        WHERE game_date = %(game_date)s
    ),
    last_in_play AS (
        SELECT game_id, MAX(captured_at) FILTER (WHERE game_status <> 3) AS at
        FROM snaps
        GROUP BY game_id
    ),
    went_final AS (
        SELECT s.game_id, MIN(s.captured_at) AS final_at
        FROM snaps s
        JOIN last_in_play p ON p.game_id = s.game_id
        WHERE s.game_status = 3
          AND (p.at IS NULL OR s.captured_at > p.at)
        GROUP BY s.game_id
    ),
    writers AS (
        SELECT DISTINCT game_id, pipeline_run_id
        FROM nba.live_player_stats
        WHERE game_date = %(game_date)s
    ),
    stored_final AS (
        SELECT DISTINCT w.game_id
        FROM writers w
        JOIN snaps s
          ON s.game_id = w.game_id AND s.pipeline_run_id = w.pipeline_run_id
        JOIN went_final f ON f.game_id = w.game_id
        JOIN nba.pipeline_runs r ON r.id = w.pipeline_run_id
        WHERE s.game_status = 3
          AND s.captured_at >= f.final_at + make_interval(mins => %(settle_minutes)s)
          AND r.status = 'success'
    ),
    open_games AS (
        SELECT DISTINCT game_id
        FROM nba.live_player_stats
        WHERE game_date = %(game_date)s
          AND game_status <> 3
    )
    SELECT g.game_id,
           sf.game_id IS NOT NULL AND o.game_id IS NULL AS settled,
           EXISTS (SELECT 1 FROM open_games) AS open_rows
    FROM (SELECT DISTINCT game_id FROM snaps) g
    LEFT JOIN stored_final sf ON sf.game_id = g.game_id
    LEFT JOIN open_games o ON o.game_id = g.game_id
"""


class StoredFinalGames(NamedTuple):
    """The night as `nba.live_player_stats` has it; see STORED_FINAL_GAMES_SQL."""

    seen: frozenset[str]
    settled: frozenset[str]
    open_rows: bool

    @property
    def complete(self) -> bool:
        """Every game seen is settled and no row is still live."""
        return bool(self.seen) and self.seen <= self.settled and not self.open_rows


def stored_final_games(
    game_date: date, settle_minutes: int | None = None
) -> StoredFinalGames:
    """Read which of `game_date`'s games need no more box-score reads. Blocking DB."""
    if settle_minutes is None:
        settle_minutes = settings.live_stats_settle_minutes
    rows = db.execute_sql(
        STORED_FINAL_GAMES_SQL,
        {"game_date": game_date, "settle_minutes": settle_minutes},
    ).fetchall()
    return StoredFinalGames(
        seen=frozenset(game_id for game_id, _, _ in rows),
        settled=frozenset(game_id for game_id, settled, _ in rows if settled),
        open_rows=any(open_rows for _, _, open_rows in rows),
    )


def live_rows_open(game_date: date) -> bool:
    """Whether any of `game_date`'s rows is not yet final. Blocking DB.

    The table only ever holds one night's rows (the pipeline deletes older
    dates first), so this is a read of a few hundred rows at most.
    """
    return (
        LivePlayerStats.select()
        .where(
            (LivePlayerStats.game_date == game_date)
            & (LivePlayerStats.game_status != 3)
        )
        .exists()
    )


def stored_final_night(game_date: date, slate: Iterable[str]) -> StoredFinalGames | None:
    """`stored_final_games` for the endpoint's is-the-night-over check. Blocking DB.

    `slate` is the night's games as the endpoint gated on them (nba.games for
    the date, or the scoreboard when that has none), and they count as seen.
    The store knows only the games a run has snapshotted, so without them a
    game on tonight's schedule that no run has listed would leave every game
    the store knows settled: the endpoint would call the night complete and
    skip the run, and since only a run snapshots a game, so would every later
    poll. The game would never be read.

    None while a row of the date is still live: the night cannot be complete
    then, and the endpoint asks this on every poll, so the polls while games
    are on answer it with `live_rows_open` and never run STORED_FINAL_GAMES_SQL
    here. (The pipeline runs it once a poll, and only once a game is final.)
    """
    if live_rows_open(game_date):
        return None
    stored = stored_final_games(game_date)
    return stored._replace(seen=stored.seen | frozenset(slate))


class LiveGameStatsPipeline(BasePipeline):
    """
    Fetch in-progress game box scores from NBA API live endpoints.

    This pipeline:
    1. Fetches the live scoreboard to get active game IDs for today
    2. For each game, fetches the live BoxScore
    3. Upserts stats for each active player into nba.live_player_stats

    Records are continuously overwritten on each poll — this table is a
    snapshot of current game state, not a historical record. A game whose
    final box score is already stored (`stored_final_games`) is skipped.
    """

    config = PipelineConfig(
        name="live_game_stats",
        display_name="Live Game Stats",
        description="Fetches in-progress game box scores from NBA API live endpoints",
        target_table="nba.live_player_stats",
        category=PipelineCategory.LIVE,
        trigger_slug="live-stats",
    )

    def __init__(self):
        super().__init__()
        self.nba_extractor = NBAApiExtractor()

    def execute(self, ctx: PipelineContext) -> None:
        """Execute the live game stats pipeline."""
        # The date the trigger endpoint gated on, so the rows this writes carry
        # the same date the API asks for when it reads them back.
        game_date = ctx.game_date()

        ctx.log.info("live_stats_start", game_date=str(game_date))

        # Clean up stale records from previous game days.
        # Without this, old final-game records persist and get picked up by
        # the live matchup endpoint, causing double-counting against the
        # already-settled daily_matchup_scores baseline.
        deleted = (
            LivePlayerStats
            .delete()
            .where(LivePlayerStats.game_date < game_date)
            .execute()
        )
        if deleted:
            ctx.log.info("live_stats_cleanup", deleted_count=deleted, game_date=str(game_date))

        # Clean up score snapshots from previous game days.
        LiveGameScoreSnapshot.delete().where(
            LiveGameScoreSnapshot.game_date < game_date
        ).execute()

        # Get all games on the scoreboard for today
        scoreboard_games = self.nba_extractor.get_scoreboard_games(game_date)

        if not scoreboard_games:
            ctx.log.info("live_stats_no_games", game_date=str(game_date))
            return

        # Games whose final box score is already stored. Re-reading them is
        # all this pipeline did from the night's last final until the cron
        # window closes at 4 AM ET: about 138 identical row rewrites every 30 s,
        # half of all DB execution time in prod (2026-10-13 audit). Only a
        # saving: if it cannot be read, every game is read as before.
        #
        # Only a game the scoreboard calls final can be skipped, so until one
        # is (the first hours of every night) the query is not run at all.
        settled: frozenset[str] = frozenset()
        if any(game["game_status"] == 3 for game in scoreboard_games):
            try:
                settled = stored_final_games(game_date).settled
            except Exception as e:
                ctx.log.warning("live_stats_stored_final_check_failed", error=str(e))

        ctx.log.info(
            "live_stats_games_found",
            game_count=len(scoreboard_games),
            settled_count=len(settled),
            game_date=str(game_date),
        )

        # Every box score read, for the availability log (see _record_availability).
        boxes: list[tuple[str, dict, bool]] = []

        # Process each game
        for game_meta in scoreboard_games:
            game_id = game_meta["game_id"]
            game_status = game_meta["game_status"]
            period = game_meta.get("period") or None
            game_clock = game_meta.get("game_clock") or None

            # No snapshot either: the score of a settled game no longer moves,
            # and the last snapshot taken is already the final one.
            if game_id in settled and game_status == 3:
                ctx.increment_skipped(1, "final_stored")
                continue

            # Record a score snapshot for this game using scoreboard data.
            # This appends one row per game per poll, building a time series
            # for the score-over-time chart. No extra API call — scoreboard
            # data already has home/away scores, team abbreviations, and status.
            LiveGameScoreSnapshot.record_snapshot(
                game_id=game_id,
                game_date=game_date,
                game_meta=game_meta,
                pipeline_run_id=ctx.run_id,
            )

            # Fetch live box score for this game
            game_data = self.nba_extractor.get_live_box_score(game_id)
            if not game_data:
                ctx.log.warning("live_box_score_skip", game_id=game_id, game_status=game_status)
                if game_status == 1:  # not tipped off yet: nothing to fetch
                    ctx.increment_skipped(1, "not_started")
                else:
                    ctx.increment_failed(1, "live_box_score_unavailable")
                continue
            boxes.append((game_id, game_data, game_status >= 2))

            # Combine home and away players
            home_players = game_data.get("homeTeam", {}).get("players", [])
            away_players = game_data.get("awayTeam", {}).get("players", [])
            all_players = home_players + away_players

            for player_data in all_players:
                # Only process players who are active (dressed for the game). The
                # inactive ones, and why, go to the availability log below.
                status = player_data.get("status", "")
                if status != "ACTIVE":
                    continue

                stats_raw = player_data.get("statistics", {})
                minutes_str = stats_raw.get("minutesCalculated", "PT00M00.00S")

                # Skip only a player who has not played. `minutesCalculated` is
                # whole minutes ("PT00M" for a 40-second appearance), so the
                # question goes to `minutes`, which keeps the seconds
                # ("PT00M40.00S"). Same rule as player_game_stats, so live and
                # settled agree about who played; the row is stored with `min` 0.
                if seconds_played(stats_raw.get("minutes") or minutes_str) <= 0:
                    continue

                min_int = minutes_to_int(minutes_str)

                player_id = player_data.get("personId")
                if not player_id:
                    continue

                player_id = int(player_id)
                player_name = (
                    f"{player_data.get('firstName', '')} {player_data.get('familyName', '')}".strip()
                )

                # Build stats dict matching calculate_fantasy_points signature
                player_stats = {
                    "pts": int(stats_raw.get("points", 0)),
                    "reb": int(stats_raw.get("reboundsTotal", 0)),
                    "ast": int(stats_raw.get("assists", 0)),
                    "stl": int(stats_raw.get("steals", 0)),
                    "blk": int(stats_raw.get("blocks", 0)),
                    "tov": int(stats_raw.get("turnovers", 0)),
                    "fgm": int(stats_raw.get("fieldGoalsMade", 0)),
                    "fga": int(stats_raw.get("fieldGoalsAttempted", 0)),
                    "fg3m": int(stats_raw.get("threePointersMade", 0)),
                    "fg3a": int(stats_raw.get("threePointersAttempted", 0)),
                    "ftm": int(stats_raw.get("freeThrowsMade", 0)),
                    "fta": int(stats_raw.get("freeThrowsAttempted", 0)),
                }
                fpts = calculate_fantasy_points(player_stats)

                # Upsert Player dimension record if needed
                Player.upsert_player(
                    player_id=player_id,
                    name=player_name,
                    espn_id=None,  # Not available from live BoxScore
                )

                # Upsert live stats record
                LivePlayerStats.upsert_live_stats(
                    player_id=player_id,
                    game_id=game_id,
                    game_date=game_date,
                    stats={
                        "period": period,
                        "game_clock": game_clock,
                        "game_status": game_status,
                        "fpts": fpts,
                        "min": min_int,
                        **player_stats,
                    },
                    pipeline_run_id=ctx.run_id,
                )

                ctx.increment_records()

        self._record_availability(ctx, game_date, boxes)

        ctx.log.info(
            "live_stats_complete",
            game_date=str(game_date),
            records_processed=ctx.records_processed,
        )

    @staticmethod
    def _record_availability(ctx: PipelineContext, game_date: date, boxes: list[tuple[str, dict, bool]]) -> None:
        """Hand the box scores read this poll to the availability log
        (pipelines/availability.py): who is inactive and why, which the loop above
        skips, so a DNP is on record for the post-game view, and from the tip each
        listed player's final word. A poll that changes nothing writes nothing.

        Never fails the run: a write error costs this poll's capture, and the next
        poll hands over the same box scores.
        """
        if not boxes:
            return
        try:
            result = record_boxscore(game_date, boxes, now=datetime.now(timezone.utc), run_id=ctx.run_id)
        except Exception as exc:
            ctx.increment_failed(1, "availability_capture_failed")
            ctx.log.warning("live_stats_availability_capture_failed", error=f"{type(exc).__name__}: {exc}")
            return
        if result.reports:
            ctx.log.info(
                "live_stats_availability_captured",
                reports=result.reports,
                verdicts_changed=len(result.changes),
            )
