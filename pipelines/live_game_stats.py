"""
Live Game Stats Pipeline

Reads the NBA's live scoreboard and the live box score of each game still in
play, and writes the players' current lines to nba.live_player_stats and one
score snapshot a game to nba.live_game_score_snapshots. cron-runner triggers it
every 15 seconds through the game window (every 30 until 2026-11-11).

A game's clock, period, status and score are its box score's, read in the same
run. They used to be the scoreboard's, and that one file moves about once in
ten minutes (the median gap between changes was 600 s in every game of
2026-11-09, and a live sample on 2026-11-11 saw it change once in 12 minutes
while the box scores changed on nearly every 10 s read): the stat lines were
30 s old and the clock beside them up to ten minutes. The scoreboard now says
which games there are, and stands in for a box score that cannot be read.

The box scores are read together and each game is written in three statements
(its snapshot, a read of its players, its lines). One after another, with three
statements a player, a run took about 8 s on an 8-game night.

Does no time-window gating itself — gating is handled by the trigger
endpoint (POST /v1/internal/pipelines/live-stats).
"""

import contextvars
import time
from collections.abc import Iterable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
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
from pipelines.transformers import calculate_fantasy_points
from pipelines.transformers.minutes import seconds_played


# Which of a night's games already have their final box score stored.
#
# A game is settled when none of its rows is still live and a *successful* run
# stored its box score at least `settle` minutes after the game went final (the
# last time: a final taken back after a buzzer review starts the margin again).
# A row carries the id of the last run that upserted it, and that run's
# snapshot of the game (recorded with the rows, from the same box score) says
# when that was and that the game was final by then. Row timestamps cannot
# answer this: the finalize_* helpers mark rows final and touch `last_updated`
# without reading a box score, so a game whose last box-score read was
# mid-fourth can look final and freshly written.
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
# it there. (Polled every 15 s, as since 2026-11-11, a night has twice the
# snapshots: that test's five-game night costs 1,673, and 847 polled every 30.)
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


def game_state(listed: Mapping, box: Mapping | None) -> dict:
    """A game as this poll has it, in the scoreboard's shape (NBAApiExtractor.get_scoreboard_games).

    The box score's word wherever it has one, the scoreboard's for the rest
    (and for all of it without a box score). Each field is taken on its own
    presence, bar the two scores, which only go together: one team's points
    from each file would be a margin neither of them had.

    The status is the later of the two. The box score is the fresher file by
    minutes, so it is first to say a game has tipped or ended; the scoreboard
    still counts for a game whose box score never says final (the endpoint's
    finalize helpers exist for the same case), so nothing is called live for
    longer than it was before.
    """
    state = dict(listed)
    if not box:
        return state
    listed_status = listed.get("game_status") or 1
    status = box.get("gameStatus")
    state["game_status"] = max(status, listed_status) if status in (1, 2, 3) else listed_status
    if isinstance(box.get("period"), int):
        state["period"] = box["period"]
    if isinstance(box.get("gameClock"), str):
        state["game_clock"] = box["gameClock"]
    home, away = box.get("homeTeam") or {}, box.get("awayTeam") or {}
    if isinstance(home.get("score"), int) and isinstance(away.get("score"), int):
        state["home_score"], state["away_score"] = home["score"], away["score"]
    for side, team in (("home_team", home), ("away_team", away)):
        if team.get("teamTricode"):
            state[side] = team["teamTricode"]
    return state


def box_score_lines(box: Mapping) -> list[dict]:
    """The stat line of every player in a box score who has played: `player_id`, `name`, `min`, `fpts` and the stats."""
    lines = []
    for team in ("homeTeam", "awayTeam"):
        for player_data in (box.get(team) or {}).get("players") or []:
            # Only players who are active (dressed for the game). The inactive
            # ones, and why, go to the availability log (_record_availability).
            if player_data.get("status", "") != "ACTIVE":
                continue

            stats_raw = player_data.get("statistics", {})
            minutes_str = stats_raw.get("minutesCalculated", "PT00M00.00S")

            # Skip only a player who has not played. `minutesCalculated` is
            # whole minutes ("PT00M" for a 40-second appearance), so the
            # question goes to `minutes`, which keeps the seconds
            # ("PT00M40.00S"). Same rule as player_game_stats, so live and
            # settled agree about who played; the row is stored with `min` 0.
            seconds = seconds_played(stats_raw.get("minutes") or minutes_str)
            if seconds <= 0:
                continue

            player_id = player_data.get("personId")
            if not player_id:
                continue

            # Stats dict matching calculate_fantasy_points' signature.
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
            lines.append(
                {
                    "player_id": int(player_id),
                    "name": f"{player_data.get('firstName', '')} {player_data.get('familyName', '')}".strip(),
                    "fpts": calculate_fantasy_points(player_stats),
                    # Whole minutes, truncated, from the same field. Read off
                    # `minutesCalculated`, every row of 2026-10-20 was stored
                    # with `min` 0 (Paul George: 38 points, 30 minutes in the
                    # game log), while `minutes` told each of them apart from
                    # a DNP.
                    "min": int(seconds // 60),
                    **player_stats,
                }
            )
    return lines


def ensure_players(names: Mapping[int, str]) -> int:
    """Give each of these players an nba.players row under this name. Blocking DB.

    What `Player.upsert_player(player_id, name)` does for one player (create
    him, or rename him when the name differs), for a game's worth at once: one
    read, and a write only for a player who is new or renamed, which on nearly
    every poll is nobody. Returns how many were written.
    """
    if not names:
        return 0
    known = dict(Player.select(Player.id, Player.name).where(Player.id.in_(list(names))).tuples())
    now = datetime.utcnow()
    rows = [
        {
            "id": player_id,
            "name": name,
            "name_normalized": name.lower().strip(),
            "created_at": now,
            "updated_at": now,
        }
        # In id order, so two writers of the same players lock them the same way round.
        for player_id, name in sorted(names.items())
        if known.get(player_id) != name
    ]
    if rows:
        (
            Player.insert_many(rows)
            .on_conflict(
                conflict_target=[Player.id],
                preserve=[Player.name, Player.name_normalized, Player.updated_at],
            )
            .execute()
        )
    return len(rows)


def store_game(game_id: str, game_date: date, state: Mapping, lines: list[dict], run_id) -> int:
    """Write one game's poll: its snapshot and its players' lines. Blocking DB; returns the lines written.

    `state` is the game from `game_state` and `lines` its box score's
    (`box_score_lines`). One transaction, so the snapshot is the game as its
    lines had it and a reader never sees one without the other: the snapshot
    is the time series behind the margin chart, and with the run id on the
    lines it is what tells a settled game (STORED_FINAL_GAMES_SQL).
    """
    with db.atomic():
        LiveGameScoreSnapshot.record_snapshot(
            game_id=game_id,
            game_date=game_date,
            game_meta=state,
            pipeline_run_id=run_id,
        )
        # The Player dimension rows the lines point at (a live box score has no ESPN id).
        ensure_players({line["player_id"]: line["name"] for line in lines})
        return LivePlayerStats.upsert_lines(
            game_id,
            [
                {
                    **line,
                    "game_date": game_date,
                    "period": state.get("period") or None,
                    "game_clock": state.get("game_clock") or None,
                    "game_status": state["game_status"],
                }
                for line in lines
            ],
            pipeline_run_id=run_id,
        )


class LiveGameStatsPipeline(BasePipeline):
    """
    Fetch in-progress game box scores from NBA API live endpoints.

    This pipeline:
    1. Fetches the live scoreboard to get the game IDs for today
    2. Fetches the live BoxScore of every game not yet settled, together
    3. For each game, appends a score snapshot and upserts the line of each
       player who has played into nba.live_player_stats

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

        # A past night keeps its in-play and final score snapshots: they are
        # the margin line of each game's sheet once the night is over (the
        # backend's GET /v1/games/{date}/{game_id}). Only the pre-tip polls go,
        # 0-0 each and most of a night's rows: 870 of the 1,201 for the 9:30 PM
        # game of 2026-10-20, whose slate first tipped at 3 PM.
        LiveGameScoreSnapshot.delete().where(
            (LiveGameScoreSnapshot.game_date < game_date)
            & (LiveGameScoreSnapshot.game_status == 1)
        ).execute()

        # Get all games on the scoreboard for today
        started = time.monotonic()
        scoreboard_games = self.nba_extractor.get_scoreboard_games(game_date)
        scoreboard_seconds = time.monotonic() - started

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
        # is (the first hours of every night) the query is not run at all. The
        # box score says final first, by up to ten minutes, and a game can
        # settle on its word alone; it is still read until the scoreboard
        # agrees, since that is the only word there is on a game not read.
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

        # The games to read, as the scoreboard lists them.
        listed: dict[str, dict] = {}
        for game in scoreboard_games:
            # No snapshot either: the score of a settled game no longer moves,
            # and the last snapshot taken is already the final one.
            if game["game_id"] in settled and game["game_status"] == 3:
                ctx.increment_skipped(1, "final_stored")
                continue
            listed[game["game_id"]] = game

        # Every box score read, with whether its game has tipped, for the
        # availability log (see _record_availability).
        read: dict[str, tuple[dict, bool]] = {}
        # The newest stored snapshot of each game, loaded if a box score is missing.
        latest: dict | None = None
        first_error: Exception | None = None

        started = time.monotonic()
        with closing(self._read_box_scores(list(listed))) as box_scores:
            for game_id, box, error in box_scores:
                game = listed[game_id]

                if error is not None:
                    # One game's read has failed for good (the extractor has
                    # already retried it). The others are still stored; the run
                    # fails once they are, so nothing it wrote counts as settled.
                    ctx.log.warning(
                        "live_box_score_failed",
                        game_id=game_id,
                        error=f"{type(error).__name__}: {error}",
                    )
                    first_error = first_error or error
                    continue

                if not box:
                    if latest is None:
                        latest = LiveGameScoreSnapshot.latest_by_game(listed)
                    self._store_without_box_score(ctx, game_date, game, latest.get(game_id))
                    continue

                state = game_state(game, box)
                written = store_game(game_id, game_date, state, box_score_lines(box), ctx.run_id)
                ctx.increment_records(written)
                read[game_id] = (box, state["game_status"] >= 2)
        games_seconds = time.monotonic() - started

        # In the scoreboard's order, whichever read came back first.
        self._record_availability(
            ctx, game_date, [(game_id, *read[game_id]) for game_id in listed if game_id in read]
        )

        ctx.log.info(
            "live_stats_complete",
            game_date=str(game_date),
            records_processed=ctx.records_processed,
            box_scores_read=len(read),
            scoreboard_seconds=round(scoreboard_seconds, 3),
            games_seconds=round(games_seconds, 3),
        )

        if first_error is not None:
            raise first_error

    def _read_box_scores(
        self, game_ids: list[str]
    ) -> Iterator[tuple[str, dict | None, Exception | None]]:
        """Each game's live box score as it arrives: (game_id, box score or None, why the read failed or None).

        Read `settings.live_box_score_workers` at a time, so a night's box
        scores take about as long as the slowest of them, and one slow file
        holds up no other game's write. Only the reads leave this thread: they
        touch no database, and every write stays with the caller, on the
        thread whose connection it is. With one worker, or one game, they are
        read in turn right here.
        """
        read = self.nba_extractor.get_live_box_score
        workers = min(settings.live_box_score_workers, len(game_ids))
        if workers <= 1:
            for game_id in game_ids:
                try:
                    yield game_id, read(game_id), None
                except Exception as error:
                    yield game_id, None, error
            return

        readers = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="live-box-score")
        try:
            # Each read in a copy of this thread's context, for the log fields bound to it.
            reads = {
                readers.submit(contextvars.copy_context().run, read, game_id): game_id
                for game_id in game_ids
            }
            for done in as_completed(reads):
                try:
                    yield reads[done], done.result(), None
                except Exception as error:
                    yield reads[done], None, error
        finally:
            # Nothing is waited for: by here every read is in, or the caller has
            # failed and no longer wants them.
            readers.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _store_without_box_score(ctx: PipelineContext, game_date: date, game: dict, latest) -> None:
        """A game whose box score could not be had this poll: its snapshot from the scoreboard, and the count.

        Before the tip there is no box score to have (the file appears about
        half an hour ahead), and the scoreboard's 0-0 is the snapshot, as it
        always was. Once box scores have been stored the scoreboard is the
        older word, by up to ten minutes: its snapshot would put a point on
        the margin line behind the ones already there, and one that says "in
        play" after a final would start the settle margin again. So a
        snapshot that is behind the newest one stored is not written.
        """
        game_id = game["game_id"]
        status = game["game_status"]
        if latest is not None and latest.ahead_of(game):
            status = max(status, latest.game_status)
        else:
            LiveGameScoreSnapshot.record_snapshot(
                game_id=game_id,
                game_date=game_date,
                game_meta=game,
                pipeline_run_id=ctx.run_id,
            )
        ctx.log.warning("live_box_score_skip", game_id=game_id, game_status=status)
        if status == 1:  # not tipped off yet: nothing to fetch
            ctx.increment_skipped(1, "not_started")
        else:
            ctx.increment_failed(1, "live_box_score_unavailable")

    @staticmethod
    def _record_availability(ctx: PipelineContext, game_date: date, boxes: list[tuple[str, dict, bool]]) -> None:
        """Hand the box scores read this poll to the availability log
        (pipelines/availability.py): who is inactive and why, which the lines
        leave out, so a DNP is on record for the post-game view, and from the tip
        each listed player's final word. A poll that changes nothing writes nothing.

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
